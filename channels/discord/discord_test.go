package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

const testToken = "MTIz.abc.def"

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

type recordedDiscordCall struct {
	Method      string
	Path        string
	ContentType string
	RawBody     []byte
	JSONBody    map[string]any
}

func newFakeDiscordHTTP(routes map[string]func(req *http.Request, raw []byte) *http.Response) (*http.Client, func() []recordedDiscordCall) {
	var mu sync.Mutex
	var calls []recordedDiscordCall

	client := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
			_ = req.Body.Close()
		}

		u := req.URL.String()
		path := req.URL.Path
		if idx := strings.Index(path, "/api/v10"); idx >= 0 {
			path = path[idx+len("/api/v10"):]
		} else if strings.HasPrefix(u, "https://cdn.discordapp.com") {
			path = u
		}

		ct := req.Header.Get("Content-Type")
		var jsonMap map[string]any
		if strings.HasPrefix(ct, "application/json") && len(raw) > 0 {
			_ = json.Unmarshal(raw, &jsonMap)
		}

		mu.Lock()
		calls = append(calls, recordedDiscordCall{
			Method:      req.Method,
			Path:        path,
			ContentType: ct,
			RawBody:     raw,
			JSONBody:    jsonMap,
		})
		mu.Unlock()

		if fn, ok := routes[req.Method+" "+path]; ok {
			return fn(req, raw), nil
		}
		return imtest.BytesResponse(http.StatusNoContent, nil), nil
	})

	getCalls := func() []recordedDiscordCall {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
	return client, getCalls
}

func baseIdentityRoutes(gwURL string, flagsPtr *atomic.Int64) map[string]func(*http.Request, []byte) *http.Response {
	return map[string]func(*http.Request, []byte) *http.Response{
		"GET /users/@me": func(*http.Request, []byte) *http.Response {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"id":          "B1",
				"username":    "alex",
				"global_name": "Alex",
				"bot":         true,
			})
		},
		"GET /oauth2/applications/@me": func(*http.Request, []byte) *http.Response {
			f := int64(FlagMessageContent)
			if flagsPtr != nil {
				f = flagsPtr.Load()
			}
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"id":    "A1",
				"flags": f,
				"owner": map[string]any{"id": "U0"},
			})
		},
		"GET /gateway/bot": func(*http.Request, []byte) *http.Response {
			return imtest.JSONResponse(http.StatusOK, map[string]any{"url": gwURL})
		},
	}
}

func TestMetadataAndInviteURL(t *testing.T) {
	ch := New(Options{})
	if ch.Platform() != imsdk.PlatformDiscord {
		t.Fatalf("Platform() = %q, want discord", ch.Platform())
	}
	if ch.Trust() != imsdk.TrustPublic {
		t.Fatalf("Trust() = %q, want public", ch.Trust())
	}
	if !ch.GroupMessages() || !ch.DirectMessages() {
		t.Fatal("expected group and direct messages enabled")
	}

	invite := ch.InviteURL(imsdk.BotIdentity{ID: "B1", Name: "Alex"})
	u, err := url.Parse(invite)
	if err != nil {
		t.Fatalf("parse InviteURL: %v", err)
	}
	if u.Query().Get("client_id") != "B1" || u.Query().Get("scope") != "bot" {
		t.Fatalf("unexpected invite query: %s", invite)
	}
	perms, err := strconv.ParseUint(u.Query().Get("permissions"), 10, 64)
	if err != nil || perms != botPermissionsMask {
		t.Fatalf("permissions = %q, want %d", u.Query().Get("permissions"), botPermissionsMask)
	}
}

func TestOnboarding(t *testing.T) {
	hc, getCalls := newFakeDiscordHTTP(baseIdentityRoutes("wss://gw", nil))
	ch := New(Options{HTTPClient: hc})

	var bad []imsdk.OnboardingEvent
	for ev := range ch.Onboard(context.Background(), imsdk.Credentials{"token": "invalid-shape"}, imsdk.OnboardingOptions{}) {
		bad = append(bad, ev)
	}
	if len(bad) != 1 || bad[0].Kind != imsdk.EventFailed || bad[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("bad token shape result = %+v", bad)
	}
	if len(getCalls()) != 0 {
		t.Fatalf("expected 0 HTTP calls for invalid shape, got %d", len(getCalls()))
	}

	var okEvents []imsdk.OnboardingEvent
	for ev := range ch.Onboard(context.Background(), imsdk.Credentials{"token": " Bot " + testToken}, imsdk.OnboardingOptions{}) {
		okEvents = append(okEvents, ev)
	}
	if len(okEvents) != 1 || okEvents[0].Kind != imsdk.EventSucceeded {
		t.Fatalf("valid token result = %+v", okEvents)
	}
	if okEvents[0].Bot.ID != "B1" || okEvents[0].Bot.Name != "Alex" || okEvents[0].OwnerID != "U0" || okEvents[0].Credentials["token"] != testToken {
		t.Fatalf("unexpected onboarding event: %+v", okEvents[0])
	}

	// Team owner takes precedence over application owner.
	teamRoutes := baseIdentityRoutes("wss://gw", nil)
	teamRoutes["GET /oauth2/applications/@me"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"id":    "A1",
			"owner": map[string]any{"id": "U0"},
			"team":  map[string]any{"owner_user_id": "UTEAM"},
		})
	}
	teamHC, _ := newFakeDiscordHTTP(teamRoutes)
	var teamEv []imsdk.OnboardingEvent
	for ev := range New(Options{HTTPClient: teamHC}).Onboard(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.OnboardingOptions{}) {
		teamEv = append(teamEv, ev)
	}
	if len(teamEv) != 1 || teamEv[0].OwnerID != "UTEAM" {
		t.Fatalf("expected team owner UTEAM, got %+v", teamEv)
	}

	// 401 maps to FailureInvalidCredentials.
	unauthHC, _ := newFakeDiscordHTTP(map[string]func(*http.Request, []byte) *http.Response{
		"GET /users/@me": func(*http.Request, []byte) *http.Response {
			return imtest.JSONResponse(http.StatusUnauthorized, map[string]any{"message": "401: Unauthorized"})
		},
	})
	var denied []imsdk.OnboardingEvent
	for ev := range New(Options{HTTPClient: unauthHC}).Onboard(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.OnboardingOptions{}) {
		denied = append(denied, ev)
	}
	if len(denied) != 1 || denied[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("expected invalid_credentials on 401, got %+v", denied)
	}
}

func TestToInboundReplyPingVsExplicitMention(t *testing.T) {
	group := imsdk.ChatRef{PlatformID: "C1", Kind: imsdk.ChatGroup}
	baseMsg := Message{
		ID:        "M1",
		ChannelID: "C1",
		GuildID:   "G1",
		Type:      0,
		Author:    User{ID: "U1", Username: "zhangsan", GlobalName: "Zhang San"},
		Timestamp: "2026-11-12T10:00:00.000Z",
	}

	// Unaddressed chatter is kept as background.
	chatter := baseMsg
	chatter.Content = "lunch?"
	gotChatter := ToInbound(chatter, "B1", group)
	if gotChatter == nil || gotChatter.AddressedToBot || *gotChatter.Mentioned {
		t.Fatalf("unexpected chatter: %+v", gotChatter)
	}

	// Explicit <@id> mention of bot and other users (including another bot).
	explicit := baseMsg
	explicit.Content = "<@B1> ask <@!U2> and <@B9> about it"
	explicit.Member = &Member{Nick: "San"}
	explicit.Mentions = []MentionUser{
		{User: User{ID: "B1", Username: "alex"}},
		{User: User{ID: "U2", Username: "lisi", GlobalName: "Li Si"}},
		{User: User{ID: "B9", Username: "helper", GlobalName: "HelperBot", Bot: true}},
	}
	gotExplicit := ToInbound(explicit, "B1", group)
	if gotExplicit == nil || !gotExplicit.AddressedToBot || !*gotExplicit.Mentioned {
		t.Fatalf("expected explicit mention to set AddressedToBot=true and Mentioned=true: %+v", gotExplicit)
	}
	if gotExplicit.Sender.Name != "San" || gotExplicit.Text != "ask @Li Si and @HelperBot about it" {
		t.Fatalf("unexpected explicit sender/text: %+v", gotExplicit)
	}
	if len(gotExplicit.Mentions) != 2 || !gotExplicit.Mentions[1].IsBot || gotExplicit.Mentions[1].ID != "B9" {
		t.Fatalf("unexpected Mentions slice: %+v", gotExplicit.Mentions)
	}

	// Discord reply-ping: lists B1 (and U3) in Mentions, but content has NO <@B1> or <@U3>.
	replyPing := baseMsg
	replyPing.Type = 19
	replyPing.Author = User{ID: "U9", Username: "bob", Bot: true}
	replyPing.Content = "done"
	replyPing.Mentions = []MentionUser{
		{User: User{ID: "B1", Username: "alex"}},
		{User: User{ID: "U3", Username: "wangwu"}},
	}
	replyPing.MessageReference = &MessageReference{MessageID: "M0"}
	replyPing.ReferencedMessage = &ReferencedMessage{Author: &User{ID: "B1", Username: "alex"}}
	gotReply := ToInbound(replyPing, "B1", group)
	if gotReply == nil || !gotReply.Sender.IsBot || gotReply.ReplyTo != "M0" {
		t.Fatalf("unexpected reply-ping message: %+v", gotReply)
	}
	if !gotReply.AddressedToBot || gotReply.Mentioned == nil || *gotReply.Mentioned {
		t.Fatalf("reply-ping must set AddressedToBot=true and Mentioned=false, got Addressed=%v Mentioned=%v",
			gotReply.AddressedToBot, *gotReply.Mentioned)
	}
	if len(gotReply.Mentions) != 0 {
		t.Fatalf("expected no untyped reply-ping targets in Mentions, got %+v", gotReply.Mentions)
	}

	// Non-conversation type (7 = member join) and self message are ignored.
	joinMsg := baseMsg
	joinMsg.Type = 7
	if ToInbound(joinMsg, "B1", group) != nil {
		t.Fatal("expected type 7 join message to be ignored")
	}
	selfMsg := baseMsg
	selfMsg.Author = User{ID: "B1", Username: "alex"}
	if ToInbound(selfMsg, "B1", group) != nil {
		t.Fatal("expected self message to be ignored")
	}

	// DM is addressed without mention.
	dmMsg := baseMsg
	dmMsg.GuildID = ""
	dmMsg.Content = "hi"
	gotDM := ToInbound(dmMsg, "B1", imsdk.ChatRef{PlatformID: "D1", Kind: imsdk.ChatDM})
	if gotDM == nil || !gotDM.AddressedToBot || *gotDM.Mentioned {
		t.Fatalf("unexpected DM result: %+v", gotDM)
	}
}

type wsSession struct {
	conn *websocket.Conn
	url  string
	mu   sync.Mutex
	sent []gatewayPayload
}

func (s *wsSession) frames() []gatewayPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sent)
}

func (s *wsSession) push(t *testing.T, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

func startGatewayHarness(t *testing.T) (string, chan *wsSession) {
	t.Helper()
	sessions := make(chan *wsSession, 8)
	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		sess := &wsSession{
			conn: conn,
			url:  r.URL.RequestURI(),
		}
		sessions <- sess
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var p gatewayPayload
			if json.Unmarshal(raw, &p) == nil {
				sess.mu.Lock()
				sess.sent = append(sess.sent, p)
				sess.mu.Unlock()
			}
		}
	})
	return wsURL, sessions
}

func TestGatewayIdentifyHeartbeatResumeAndChatMapping(t *testing.T) {
	gwURL, gwSessions := startGatewayHarness(t)
	resumeURL, resumeSessions := startGatewayHarness(t)

	routes := baseIdentityRoutes(gwURL, nil)
	routes["GET /channels/T9"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"id":       "T9",
			"type":     11,
			"name":     "plans",
			"guild_id": "G1",
		})
	}
	hc, _ := newFakeDiscordHTTP(routes)

	var mu sync.Mutex
	var received []imsdk.InboundMessage
	var states []imsdk.ConnectionStateKind

	conn, err := New(Options{
		HTTPClient:      hc,
		Backoff:         func(int) time.Duration { return 5 * time.Millisecond },
		FirstBeatJitter: func(time.Duration) time.Duration { return 5 * time.Millisecond },
	}).Connect(context.Background(), imsdk.Credentials{
		"token": testToken,
		"name":  "Alex",
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			mu.Lock()
			received = append(received, m)
			mu.Unlock()
		},
		OnState: func(st imsdk.ConnectionState) {
			mu.Lock()
			states = append(states, st.Kind)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	s0 := <-gwSessions
	if s0.url != "/?v=10&encoding=json" {
		t.Fatalf("socket 0 url = %q", s0.url)
	}

	// Send Hello with short heartbeat interval to verify Op 2 Identify + Op 1 Heartbeat + Op 11 ACK.
	s0.push(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 40}})
	waitUntil(t, func() bool {
		frames := s0.frames()
		hasIdentify := false
		hasBeat := false
		for _, f := range frames {
			if f.Op == 2 {
				hasIdentify = true
			}
			if f.Op == 1 {
				hasBeat = true
			}
		}
		return hasIdentify && hasBeat
	})

	var identifyData struct {
		Token   string `json:"token"`
		Intents int    `json:"intents"`
	}
	for _, f := range s0.frames() {
		if f.Op == 2 {
			_ = json.Unmarshal(f.D, &identifyData)
		}
	}
	if identifyData.Intents&intentMessageContent == 0 {
		t.Fatalf("expected messageContent intent bit set, got %d", identifyData.Intents)
	}

	// Acknowledge heartbeat so connection stays alive across beats.
	s0.push(t, map[string]any{"op": 11, "d": nil})

	s0.push(t, map[string]any{
		"op": 0, "s": 1, "t": "READY",
		"d": map[string]any{"session_id": "S1", "resume_gateway_url": resumeURL},
	})
	s0.push(t, map[string]any{
		"op": 0, "s": 2, "t": "GUILD_CREATE",
		"d": map[string]any{
			"id":   "G1",
			"name": "Hikers",
			"channels": []map[string]any{
				{"id": "C1", "type": 0, "name": "general"},
			},
		},
	})
	s0.push(t, map[string]any{
		"op": 0, "s": 3, "t": "MESSAGE_CREATE",
		"d": map[string]any{
			"id": "M1", "channel_id": "C1", "guild_id": "G1", "type": 0,
			"author":    map[string]any{"id": "U1", "username": "zs"},
			"content":   "<@B1> hi",
			"mentions":  []map[string]any{{"id": "B1", "username": "alex"}},
			"timestamp": "2026-11-12T10:00:00.000Z",
		},
	})
	s0.push(t, map[string]any{
		"op": 0, "s": 4, "t": "MESSAGE_CREATE",
		"d": map[string]any{
			"id": "M2", "channel_id": "T9", "guild_id": "G1", "type": 0,
			"author":    map[string]any{"id": "U1", "username": "zs"},
			"content":   "in a thread",
			"timestamp": "2026-11-12T10:00:01.000Z",
		},
	})
	s0.push(t, map[string]any{
		"op": 0, "s": 5, "t": "MESSAGE_CREATE",
		"d": map[string]any{
			"id": "M3", "channel_id": "D1", "type": 0,
			"author":    map[string]any{"id": "U1", "username": "zs"},
			"content":   "dm",
			"timestamp": "2026-11-12T10:00:02.000Z",
		},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 3
	})

	byID := make(map[string]imsdk.ChatRef)
	mu.Lock()
	for _, m := range received {
		byID[m.PlatformID] = m.Chat
	}
	mu.Unlock()

	if byID["M1"].Kind != imsdk.ChatGroup || byID["M1"].Name != "Hikers #general" || byID["M1"].FollowUp {
		t.Fatalf("unexpected M1 chat: %+v", byID["M1"])
	}
	if byID["M2"].Kind != imsdk.ChatThread || byID["M2"].Name != "Hikers #plans" || !byID["M2"].FollowUp {
		t.Fatalf("unexpected M2 thread chat: %+v", byID["M2"])
	}
	if byID["M3"].Kind != imsdk.ChatDM || byID["M3"].FollowUp {
		t.Fatalf("unexpected M3 DM chat: %+v", byID["M3"])
	}

	// Op 7 Reconnect -> dials resumeURL and sends Op 6 Resume with seq=5.
	s0.push(t, map[string]any{"op": 7, "d": nil})
	s1 := <-resumeSessions
	s1.push(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 60000}})
	waitUntil(t, func() bool { return len(s1.frames()) >= 1 })

	var resumeFrame struct {
		Token     string `json:"token"`
		SessionID string `json:"session_id"`
		Seq       int64  `json:"seq"`
	}
	for _, f := range s1.frames() {
		if f.Op == 6 {
			_ = json.Unmarshal(f.D, &resumeFrame)
		}
	}
	if resumeFrame.SessionID != "S1" || resumeFrame.Seq != 5 || resumeFrame.Token != testToken {
		t.Fatalf("unexpected Op 6 Resume payload: %+v", resumeFrame)
	}
	s1.push(t, map[string]any{"op": 0, "s": 6, "t": "RESUMED", "d": map[string]any{}})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Equal(states, []imsdk.ConnectionStateKind{imsdk.StateReconnecting, imsdk.StateConnected})
	})

	// Op 1 server heartbeat request gets an immediate Op 1 response.
	beforeFrames := len(s1.frames())
	s1.push(t, map[string]any{"op": 1, "d": nil})
	waitUntil(t, func() bool { return len(s1.frames()) > beforeFrames })
	if got := s1.frames()[beforeFrames]; got.Op != 1 {
		t.Fatalf("expected Op 1 heartbeat response, got %+v", got)
	}

	// Op 9 Invalid Session (resumable=false) clears session and reconnects to gwURL with Op 2 Identify.
	s1.push(t, map[string]any{"op": 9, "d": false})
	s2 := <-gwSessions
	// Also omit Op 11 ACK on s2 with a short 15ms heartbeat interval to trigger zombie reconnect to s3!
	s2.push(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 15}})
	waitUntil(t, func() bool {
		for _, f := range s2.frames() {
			if f.Op == 2 {
				return true
			}
		}
		return false
	})
	// Without Op 11 ACK, second beat detects zombie socket and reconnects to s3.
	s3 := <-gwSessions
	if s3 == nil {
		t.Fatal("expected reconnect session after missed heartbeat ACK")
	}
}

func TestGateway4014FallbackAnd4004Revoked(t *testing.T) {
	var attemptNum atomic.Int32
	identifiedIntents := make(chan int, 4)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		n := attemptNum.Add(1)
		ctx := r.Context()
		b, _ := json.Marshal(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 60000}})
		_ = conn.Write(ctx, websocket.MessageText, b)

		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var p gatewayPayload
		if json.Unmarshal(raw, &p) == nil && p.Op == 2 {
			var id struct {
				Intents int `json:"intents"`
			}
			_ = json.Unmarshal(p.D, &id)
			identifiedIntents <- id.Intents
		}
		if n <= 2 {
			_ = conn.Close(websocket.StatusCode(4014), "Disallowed intent(s)")
		}
	})

	hc, _ := newFakeDiscordHTTP(baseIdentityRoutes(wsURL, nil))
	var stateMu sync.Mutex
	var lastState imsdk.ConnectionState

	conn, err := New(Options{
		HTTPClient:      hc,
		Backoff:         func(int) time.Duration { return 5 * time.Millisecond },
		FirstBeatJitter: func(time.Duration) time.Duration { return time.Hour },
	}).Connect(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.ChannelEvents{
		OnState: func(st imsdk.ConnectionState) {
			stateMu.Lock()
			lastState = st
			stateMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	firstIntents := <-identifiedIntents
	if firstIntents&intentMessageContent == 0 {
		t.Fatalf("expected first Identify to include messageContent intent, got %d", firstIntents)
	}
	secondIntents := <-identifiedIntents
	if secondIntents&intentMessageContent != 0 {
		t.Fatalf("expected second Identify after 4014 to drop messageContent intent, got %d", secondIntents)
	}

	waitUntil(t, func() bool {
		stateMu.Lock()
		defer stateMu.Unlock()
		return lastState.Kind == imsdk.StateError && lastState.Code == imsdk.ConnErrPermissionMissing
	})

	// Verify close code 4004 maps to ConnErrCredentialsRevoked.
	ws4004 := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		_ = conn.Close(websocket.StatusCode(4004), "Authentication failed")
	})
	hc4004, _ := newFakeDiscordHTTP(baseIdentityRoutes(ws4004, nil))
	var revokedState atomic.Value
	conn2, err := New(Options{HTTPClient: hc4004}).Connect(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.ChannelEvents{
		OnState: func(st imsdk.ConnectionState) { revokedState.Store(st) },
	})
	if err != nil {
		t.Fatalf("Connect 4004: %v", err)
	}
	defer conn2.Close()
	waitUntil(t, func() bool {
		st, ok := revokedState.Load().(imsdk.ConnectionState)
		return ok && st.Kind == imsdk.StateError && st.Code == imsdk.ConnErrCredentialsRevoked
	})
}

func TestPermissionsCacheAndLiveIntentUpgrade(t *testing.T) {
	gwURL, gwSessions := startGatewayHarness(t)
	var flags atomic.Int64
	flags.Store(0)

	var nowMu sync.Mutex
	fakeNow := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	hc, getCalls := newFakeDiscordHTTP(baseIdentityRoutes(gwURL, &flags))
	conn, err := New(Options{
		HTTPClient:      hc,
		Backoff:         func(int) time.Duration { return 5 * time.Millisecond },
		FirstBeatJitter: func(time.Duration) time.Duration { return time.Hour },
		Now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return fakeNow
		},
	}).Connect(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.ChannelEvents{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	s0 := <-gwSessions
	s0.push(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 60000}})
	waitUntil(t, func() bool { return len(s0.frames()) >= 1 })

	var id0 struct {
		Intents int `json:"intents"`
	}
	_ = json.Unmarshal(s0.frames()[0].D, &id0)
	if id0.Intents&intentMessageContent != 0 {
		t.Fatalf("expected initial Identify without messageContent, got %d", id0.Intents)
	}

	ctx := context.Background()
	perms, err := conn.Permissions(ctx)
	if err != nil || len(perms) != 1 || perms[0].Granted || perms[0].Grant == nil ||
		perms[0].Grant.URL != "https://discord.com/developers/applications/A1/bot" {
		t.Fatalf("unexpected missing permission report: %+v, err=%v", perms, err)
	}

	// Within 15s TTL, Permissions returns cached value even after flags change remotely.
	flags.Store(FlagMessageContent)
	callsBefore := len(getCalls())
	cachedPerms, _ := conn.Permissions(ctx)
	if len(cachedPerms) != 1 || cachedPerms[0].Granted || len(getCalls()) != callsBefore {
		t.Fatalf("expected cached Permissions within 15s TTL: perms=%+v callsDelta=%d", cachedPerms, len(getCalls())-callsBefore)
	}

	// Advance past 15s TTL: Permissions re-checks, reports Granted=true, and reconnects with messageContent!
	nowMu.Lock()
	fakeNow = fakeNow.Add(16 * time.Second)
	nowMu.Unlock()

	upgraded, err := conn.Permissions(ctx)
	if err != nil || len(upgraded) != 1 || !upgraded[0].Granted || upgraded[0].Grant != nil {
		t.Fatalf("unexpected upgraded permissions: %+v, err=%v", upgraded, err)
	}

	s1 := <-gwSessions
	s1.push(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 60000}})
	waitUntil(t, func() bool { return len(s1.frames()) >= 1 })

	var id1 struct {
		Intents int `json:"intents"`
	}
	_ = json.Unmarshal(s1.frames()[0].D, &id1)
	if id1.Intents&intentMessageContent == 0 {
		t.Fatalf("expected upgraded Identify to include messageContent intent, got %d", id1.Intents)
	}
}

func TestOutboundLiveReactionsMultipart429AndDirect(t *testing.T) {
	gwURL, _ := startGatewayHarness(t)
	var postSeq atomic.Int32
	var rateLimitOnce atomic.Bool
	rateLimitOnce.Store(true)

	routes := baseIdentityRoutes(gwURL, nil)
	routes["POST /channels/C1/messages"] = func(*http.Request, []byte) *http.Response {
		if rateLimitOnce.Swap(false) {
			return imtest.JSONResponse(http.StatusTooManyRequests, map[string]any{
				"message":     "You are being rate limited.",
				"retry_after": 0.005,
			})
		}
		n := postSeq.Add(1)
		return imtest.JSONResponse(http.StatusOK, map[string]any{"id": "R" + strconv.Itoa(int(n))})
	}
	routes["PATCH /channels/C1/messages/R1"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusOK, map[string]any{"id": "R1"})
	}
	routes["POST /channels/C403/messages"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusForbidden, map[string]any{"code": 50013, "message": "Missing Permissions"})
	}
	routes["POST /channels/C404/messages"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusNotFound, map[string]any{"code": 10003, "message": "Unknown Channel"})
	}
	routes["POST /users/@me/channels"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusOK, map[string]any{"id": "DM9"})
	}
	routes["POST /channels/DM9/messages"] = func(*http.Request, []byte) *http.Response {
		return imtest.JSONResponse(http.StatusOK, map[string]any{"id": "RDM"})
	}
	routes["GET https://cdn.discordapp.com/attachments/1/2/pic.bin"] = func(*http.Request, []byte) *http.Response {
		return imtest.BytesResponse(http.StatusOK, []byte("DISCORD-CDN-OK"))
	}

	hc, getCalls := newFakeDiscordHTTP(routes)
	conn, err := New(Options{
		HTTPClient:      hc,
		FirstBeatJitter: func(time.Duration) time.Duration { return time.Hour },
	}).Connect(context.Background(), imsdk.Credentials{"token": testToken}, imsdk.ChannelEvents{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	groupChat := imsdk.ChatRef{PlatformID: "C1", Kind: imsdk.ChatGroup}

	// Reactions: 👀 -> ✅ -> ❌.
	_ = conn.React(ctx, groupChat, "M1", imsdk.ReactionProcessing)
	_ = conn.React(ctx, groupChat, "M1", imsdk.ReactionDone)
	_ = conn.React(ctx, groupChat, "M1", imsdk.ReactionError)

	var reactCalls []string
	for _, c := range getCalls() {
		if strings.Contains(c.Path, "/reactions/") {
			reactCalls = append(reactCalls, c.Method+" "+c.Path)
		}
	}
	wantReacts := []string{
		"PUT /channels/C1/messages/M1/reactions/👀/@me",
		"DELETE /channels/C1/messages/M1/reactions/👀/@me",
		"PUT /channels/C1/messages/M1/reactions/✅/@me",
		"DELETE /channels/C1/messages/M1/reactions/👀/@me",
		"PUT /channels/C1/messages/M1/reactions/❌/@me",
	}
	if !slices.Equal(reactCalls, wantReacts) {
		t.Fatalf("reactions = %v, want %v", reactCalls, wantReacts)
	}

	// StartLive: initial POST auto-retries short 429, Update deduplicates, Finish renders @mentions.
	lr, err := conn.StartLive(ctx, groupChat, imsdk.LiveOptions{
		ReplyTo: "M1",
		Initial: "*Thinking* · 0s",
	})
	if err != nil {
		t.Fatalf("StartLive: %v", err)
	}
	_ = lr.Update(ctx, "*Working* · 2s")
	_ = lr.Update(ctx, "*Working* · 2s")
	finRes, err := lr.Finish(ctx, "Done @Li Si", []imsdk.Mention{{ID: "U2", Name: "Li Si"}})
	if err != nil || finRes.Outcome != imsdk.OutcomeSent || finRes.PlatformID != "R1" {
		t.Fatalf("Finish = %+v, err = %v", finRes, err)
	}

	var patches []string
	for _, c := range getCalls() {
		if c.Method == http.MethodPatch {
			patches = append(patches, c.JSONBody["content"].(string))
		}
	}
	if !slices.Equal(patches, []string{"*Working* · 2s", "Done <@U2>"}) {
		t.Fatalf("PATCH contents = %v", patches)
	}

	// Multipart upload + 10 MiB total cap degradation.
	dir := t.TempDir()
	smallPath := filepath.Join(dir, "a.txt")
	bigPath := filepath.Join(dir, "huge.bin")
	_ = os.WriteFile(smallPath, []byte("hello"), 0o644)
	_ = os.WriteFile(bigPath, bytes.Repeat([]byte("x"), MaxUploadBytes+1), 0o644)

	sentRes, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:    groupChat,
		Text:    "here",
		ReplyTo: "M1",
		Files: []imsdk.OutboundFile{
			{Path: smallPath},
			{Path: bigPath},
		},
	})
	if err != nil || sentRes.Outcome != imsdk.OutcomeSent || sentRes.PlatformID != "R2" {
		t.Fatalf("Send multipart = %+v, err = %v", sentRes, err)
	}
	if !slices.Equal(sentRes.Degraded, []imsdk.Degradation{imsdk.DegradedFilesDropped}) {
		t.Fatalf("expected DegradedFilesDropped, got %v", sentRes.Degraded)
	}

	lastCall := getCalls()[len(getCalls())-1]
	_, params, err := mime.ParseMediaType(lastCall.ContentType)
	if err != nil {
		t.Fatalf("parse multipart content-type: %v", err)
	}
	mr := multipart.NewReader(bytes.NewReader(lastCall.RawBody), params["boundary"])
	fields := make(map[string]string)
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		b, _ := io.ReadAll(part)
		fields[part.FormName()] = string(b)
	}
	if fields["files[0]"] != "hello" || !strings.Contains(fields["payload_json"], `"filename":"a.txt"`) {
		t.Fatalf("unexpected multipart fields: %+v", fields)
	}

	// Oversized file alone with empty text returns SendErrTooLarge.
	tooBig, _ := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  groupChat,
		Files: []imsdk.OutboundFile{{Path: bigPath}},
	})
	if tooBig.Outcome != imsdk.OutcomeFailed || tooBig.Code != imsdk.SendErrTooLarge {
		t.Fatalf("expected SendErrTooLarge for oversized-only upload, got %+v", tooBig)
	}

	// Refusals: 403 -> forbidden, 404 -> chat_gone.
	r403, _ := conn.Send(ctx, imsdk.OutboundMessage{Chat: imsdk.ChatRef{PlatformID: "C403", Kind: imsdk.ChatGroup}, Text: "x"})
	r404, _ := conn.Send(ctx, imsdk.OutboundMessage{Chat: imsdk.ChatRef{PlatformID: "C404", Kind: imsdk.ChatGroup}, Text: "x"})
	if r403.Code != imsdk.SendErrForbidden || r404.Code != imsdk.SendErrChatGone {
		t.Fatalf("unexpected refusal codes: 403=%+v 404=%+v", r403, r404)
	}

	// Download, SetTyping, OpenDirect & MessageUser.
	_ = conn.SetTyping(ctx, groupChat, true)
	dc := conn.(*discordConnection)
	dc.files.Set("ATT1", Attachment{ID: "ATT1", Filename: "pic.bin", URL: "https://cdn.discordapp.com/attachments/1/2/pic.bin"})
	dlPath := filepath.Join(dir, "pic.bin")
	if err := conn.Download(ctx, imsdk.InboundAttachment{ID: "ATT1", Name: "pic.bin"}, dlPath); err != nil {
		t.Fatalf("Download: %v", err)
	}
	dlBytes, _ := os.ReadFile(dlPath)
	if string(dlBytes) != "DISCORD-CDN-OK" {
		t.Fatalf("downloaded bytes = %q", string(dlBytes))
	}

	receipt, err := conn.MessageUser(ctx, "U1", "hello dm")
	if err != nil || receipt.Chat != "DM9" || receipt.PlatformID != "RDM" {
		t.Fatalf("MessageUser = %+v, err = %v", receipt, err)
	}
}
