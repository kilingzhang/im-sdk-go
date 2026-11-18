package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

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

type recordedSlackCall struct {
	Method string
	Form   bool
	Body   map[string]any
	Bytes  string
	Auth   string
}

func newFakeSlackHTTP(routes map[string]func(body map[string]any) map[string]any) (*http.Client, func() []recordedSlackCall) {
	var mu sync.Mutex
	var calls []recordedSlackCall

	client := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
			_ = req.Body.Close()
		}

		u := req.URL.String()
		method := u
		if idx := strings.Index(u, "/api/"); idx >= 0 {
			method = u[idx+len("/api/"):]
		}
		ct := req.Header.Get("Content-Type")
		auth := req.Header.Get("Authorization")

		if req.Method == http.MethodGet {
			mu.Lock()
			calls = append(calls, recordedSlackCall{Method: method, Auth: auth})
			mu.Unlock()
			if fn, ok := routes[method]; ok {
				res := fn(nil)
				if data, ok := res["_bytes"].([]byte); ok {
					return imtest.BytesResponse(http.StatusOK, data), nil
				}
			}
			return imtest.BytesResponse(http.StatusNotFound, []byte("not found")), nil
		}

		if strings.HasPrefix(ct, "application/octet-stream") {
			mu.Lock()
			calls = append(calls, recordedSlackCall{
				Method: method,
				Bytes:  string(raw),
				Auth:   auth,
			})
			mu.Unlock()
			if fn, ok := routes[method]; ok {
				res := fn(nil)
				if status, ok := res["_status"].(int); ok && status >= 400 {
					return imtest.BytesResponse(status, []byte("fail")), nil
				}
			}
			return imtest.BytesResponse(http.StatusOK, []byte("OK")), nil
		}

		isForm := strings.HasPrefix(ct, "application/x-www-form-urlencoded")
		body := make(map[string]any)
		if isForm {
			vals, _ := url.ParseQuery(string(raw))
			for k, v := range vals {
				if len(v) > 0 {
					body[k] = v[0]
				}
			}
		} else if len(raw) > 0 && method != "files.getUploadURLExternal" {
			_ = json.Unmarshal(raw, &body)
		}

		mu.Lock()
		calls = append(calls, recordedSlackCall{
			Method: method,
			Form:   isForm,
			Body:   body,
			Auth:   auth,
		})
		mu.Unlock()

		respMap := map[string]any{"ok": true}
		if fn, ok := routes[method]; ok {
			for k, v := range fn(body) {
				respMap[k] = v
			}
		}
		return imtest.JSONResponse(http.StatusOK, respMap), nil
	})

	getCalls := func() []recordedSlackCall {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
	return client, getCalls
}

func authRoute(_ map[string]any) map[string]any {
	return map[string]any{
		"user_id": "UBOT",
		"user":    "alex",
		"bot_id":  "B1",
	}
}

func TestMetadataAndManifest(t *testing.T) {
	ch := New(Options{})
	if ch.Platform() != imsdk.PlatformSlack {
		t.Fatalf("Platform() = %q, want slack", ch.Platform())
	}
	if ch.Trust() != imsdk.TrustOrg {
		t.Fatalf("Trust() = %q, want org", ch.Trust())
	}
	if !ch.GroupMessages() || !ch.DirectMessages() {
		t.Fatalf("expected group and direct messages enabled")
	}

	ob := ch.Onboarding()
	if ob.Kind != imsdk.OnboardingCredentials || len(ob.Fields) != 2 {
		t.Fatalf("unexpected onboarding config: %+v", ob)
	}

	u, err := url.Parse(ob.SetupURL)
	if err != nil {
		t.Fatalf("parse SetupURL: %v", err)
	}
	var manifest struct {
		DisplayInformation struct {
			Name string `json:"name"`
		} `json:"display_information"`
		Features struct {
			BotUser struct {
				DisplayName string `json:"display_name"`
			} `json:"bot_user"`
			SlashCommands []struct {
				Command      string `json:"command"`
				Description  string `json:"description"`
				ShouldEscape bool   `json:"should_escape"`
			} `json:"slash_commands"`
		} `json:"features"`
		OAuthConfig struct {
			Scopes struct {
				Bot []string `json:"bot"`
			} `json:"scopes"`
		} `json:"oauth_config"`
		Settings struct {
			EventSubscriptions struct {
				BotEvents []string `json:"bot_events"`
			} `json:"event_subscriptions"`
			SocketModeEnabled bool `json:"socket_mode_enabled"`
		} `json:"settings"`
	}
	if err := json.Unmarshal([]byte(u.Query().Get("manifest_json")), &manifest); err != nil {
		t.Fatalf("unmarshal manifest_json: %v", err)
	}
	if manifest.DisplayInformation.Name != AgentNamePlaceholder {
		t.Fatalf("name placeholder = %q", manifest.DisplayInformation.Name)
	}
	if manifest.Features.BotUser.DisplayName != AgentHandlePlaceholder {
		t.Fatalf("handle placeholder = %q", manifest.Features.BotUser.DisplayName)
	}
	if !manifest.Settings.SocketModeEnabled {
		t.Fatal("expected socket_mode_enabled = true")
	}
	if len(manifest.Features.SlashCommands) != 1 || manifest.Features.SlashCommands[0].Command != "/stop" || manifest.Features.SlashCommands[0].ShouldEscape {
		t.Fatalf("unexpected slash_commands: %+v", manifest.Features.SlashCommands)
	}
	wantScopes := []string{
		"channels:history", "groups:history", "im:history", "chat:write",
		"reactions:write", "files:read", "files:write", "users:read",
	}
	if !slices.Equal(manifest.OAuthConfig.Scopes.Bot, wantScopes) {
		t.Fatalf("scopes = %v, want %v", manifest.OAuthConfig.Scopes.Bot, wantScopes)
	}
	wantEvents := []string{"message.im", "message.channels", "message.groups"}
	if !slices.Equal(manifest.Settings.EventSubscriptions.BotEvents, wantEvents) {
		t.Fatalf("bot_events = %v, want %v", manifest.Settings.EventSubscriptions.BotEvents, wantEvents)
	}
}

func TestOnboarding(t *testing.T) {
	hc, getCalls := newFakeSlackHTTP(map[string]func(map[string]any) map[string]any{
		"auth.test":             authRoute,
		"apps.connections.open": func(_ map[string]any) map[string]any { return map[string]any{"url": "wss://x"} },
		"users.info": func(_ map[string]any) map[string]any {
			return map[string]any{
				"user": map[string]any{
					"name":    "alex",
					"profile": map[string]any{"display_name": "Alex"},
				},
			}
		},
	})
	ch := New(Options{HTTPClient: hc})

	var bad []imsdk.OnboardingEvent
	for ev := range ch.Onboard(context.Background(), imsdk.Credentials{
		"botToken": "xapp-wrong",
		"appToken": "xoxb-wrong",
	}, imsdk.OnboardingOptions{}) {
		bad = append(bad, ev)
	}
	if len(bad) != 1 || bad[0].Kind != imsdk.EventFailed || bad[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("bad tokens result = %+v", bad)
	}
	if len(getCalls()) != 0 {
		t.Fatalf("expected 0 HTTP calls for malformed prefixes, got %d", len(getCalls()))
	}

	var okEvents []imsdk.OnboardingEvent
	for ev := range ch.Onboard(context.Background(), imsdk.Credentials{
		"botToken": " xoxb-1 ",
		"appToken": " xapp-1 ",
	}, imsdk.OnboardingOptions{}) {
		okEvents = append(okEvents, ev)
	}
	if len(okEvents) != 1 || okEvents[0].Kind != imsdk.EventSucceeded {
		t.Fatalf("valid tokens result = %+v", okEvents)
	}
	if okEvents[0].Bot.ID != "UBOT" || okEvents[0].Bot.Name != "Alex" {
		t.Fatalf("unexpected bot identity: %+v", okEvents[0].Bot)
	}
	if okEvents[0].Credentials["botToken"] != "xoxb-1" || okEvents[0].Credentials["appToken"] != "xapp-1" {
		t.Fatalf("unexpected saved credentials: %+v", okEvents[0].Credentials)
	}

	revokedHC, _ := newFakeSlackHTTP(map[string]func(map[string]any) map[string]any{
		"auth.test": func(_ map[string]any) map[string]any {
			return map[string]any{"ok": false, "error": "invalid_auth"}
		},
	})
	var denied []imsdk.OnboardingEvent
	for ev := range New(Options{HTTPClient: revokedHC}).Onboard(context.Background(), imsdk.Credentials{
		"botToken": "xoxb-1",
		"appToken": "xapp-1",
	}, imsdk.OnboardingOptions{}) {
		denied = append(denied, ev)
	}
	if len(denied) != 1 || denied[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("revoked result = %+v", denied)
	}
}

func TestToInboundMapping(t *testing.T) {
	names := func(id string) string {
		switch id {
		case "U1":
			return "Zhang San"
		case "U2":
			return "Li Si"
		default:
			return ""
		}
	}
	threads := NewThreadSet()

	dm := ToInbound(Event{
		Type:        "message",
		Channel:     "D1",
		ChannelType: "im",
		User:        "U1",
		Text:        "hi &amp; &lt;bye&gt;",
		TS:          "1700000000.000100",
	}, "UBOT", names, threads, "B1")
	if dm == nil || dm.Chat.Kind != imsdk.ChatDM || dm.Chat.PlatformID != "D1" {
		t.Fatalf("unexpected DM inbound: %+v", dm)
	}
	if dm.Text != "hi & <bye>" || dm.SentAt.UnixMilli() != 1700000000000 || !dm.AddressedToBot {
		t.Fatalf("unexpected DM text/time: %+v", dm)
	}

	// Unaddressed channel message stays on C1 as group context.
	chatter := ToInbound(Event{
		Type:        "message",
		Channel:     "C1",
		ChannelType: "channel",
		User:        "U1",
		Text:        "lunch?",
		TS:          "1.0",
	}, "UBOT", names, threads, "B1")
	if chatter == nil || chatter.Chat.Kind != imsdk.ChatGroup || chatter.Chat.PlatformID != "C1" || chatter.AddressedToBot {
		t.Fatalf("unexpected channel chatter: %+v", chatter)
	}

	// Reply in someone else's thread stays on C1 with ReplyTo pointing to thread root.
	otherThread := ToInbound(Event{
		Type:        "message",
		Channel:     "C1",
		ChannelType: "channel",
		User:        "U1",
		Text:        "pizza",
		TS:          "1.5",
		ThreadTS:    "1.0",
	}, "UBOT", names, threads, "B1")
	if otherThread == nil || otherThread.Chat.PlatformID != "C1" || otherThread.ReplyTo != "1.0" || otherThread.AddressedToBot {
		t.Fatalf("unexpected other thread reply: %+v", otherThread)
	}
	if threads.Len() != 0 {
		t.Fatalf("expected 0 tracked threads, got %d", threads.Len())
	}

	// Top-level mention opens C1:2.0 thread with Parent=C1 and tracks it for follow-ups.
	mentionMsg := ToInbound(Event{
		Type:        "message",
		Channel:     "C1",
		ChannelType: "channel",
		User:        "U1",
		Text:        "<@UBOT> ask <@U2> about <https://x.org|this> in <#C9|general>",
		TS:          "2.0",
	}, "UBOT", names, threads, "B1")
	if mentionMsg == nil || mentionMsg.Chat.Kind != imsdk.ChatThread || mentionMsg.Chat.PlatformID != "C1:2.0" {
		t.Fatalf("unexpected mention chat: %+v", mentionMsg)
	}
	if mentionMsg.Chat.Parent == nil || mentionMsg.Chat.Parent.PlatformID != "C1" {
		t.Fatalf("expected parent C1, got %+v", mentionMsg.Chat.Parent)
	}
	if mentionMsg.Text != "ask @Li Si about this (https://x.org) in #general" {
		t.Fatalf("unexpected rendered text: %q", mentionMsg.Text)
	}
	if !mentionMsg.AddressedToBot || mentionMsg.Mentioned == nil || !*mentionMsg.Mentioned {
		t.Fatalf("expected addressed and mentioned=true: %+v", mentionMsg)
	}
	if len(mentionMsg.Mentions) != 1 || mentionMsg.Mentions[0].ID != "U2" || mentionMsg.Mentions[0].Name != "Li Si" {
		t.Fatalf("unexpected Mentions: %+v", mentionMsg.Mentions)
	}

	// Follow-up in tracked thread needs no @mention.
	followUp := ToInbound(Event{
		Type:        "message",
		Channel:     "C1",
		ChannelType: "channel",
		User:        "U1",
		Text:        "and Sunday?",
		TS:          "3.0",
		ThreadTS:    "2.0",
	}, "UBOT", names, threads, "B1")
	if followUp == nil || followUp.Chat.PlatformID != "C1:2.0" || !followUp.AddressedToBot || *followUp.Mentioned {
		t.Fatalf("unexpected thread follow-up: %+v", followUp)
	}

	// Other bots (both app bot_id and webhook bot_message) are kept and marked IsBot=true.
	ciBot := ToInbound(Event{
		Type:       "message",
		Channel:    "C1",
		User:       "UB2",
		BotID:      "B2",
		BotProfile: &BotProfile{Name: "CI"},
		Text:       "build red",
		TS:         "4.0",
	}, "UBOT", names, threads, "B1")
	if ciBot == nil || !ciBot.Sender.IsBot || ciBot.Sender.ID != "UB2" || ciBot.Sender.Name != "CI" {
		t.Fatalf("unexpected CI bot message: %+v", ciBot)
	}

	webhookBot := ToInbound(Event{
		Type:     "message",
		Subtype:  "bot_message",
		Channel:  "C1",
		BotID:    "B3",
		Username: "deploy",
		Text:     "<@UBOT> shipped",
		TS:       "5.0",
	}, "UBOT", names, threads, "B1")
	if webhookBot == nil || !webhookBot.Sender.IsBot || webhookBot.Sender.ID != "B3" || webhookBot.Sender.Name != "deploy" || !*webhookBot.Mentioned {
		t.Fatalf("unexpected webhook bot message: %+v", webhookBot)
	}

	// Self messages and edit subtypes are ignored.
	if got := ToInbound(Event{Type: "message", Subtype: "bot_message", Channel: "C1", BotID: "B1", Text: "<@UBOT>", TS: "5.5"}, "UBOT", names, threads, "B1"); got != nil {
		t.Fatalf("expected own bot_id message to be ignored, got %+v", got)
	}
	if got := ToInbound(Event{Type: "message", Subtype: "message_changed", Channel: "C1", User: "U1", Text: "x", TS: "6.0"}, "UBOT", names, threads, "B1"); got != nil {
		t.Fatalf("expected message_changed to be ignored, got %+v", got)
	}
}

func TestSocketModeEnvelopesSlashAndDisconnect(t *testing.T) {
	var acksMu sync.Mutex
	var acks []string
	sockConns := make(chan *websocket.Conn, 4)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		sockConns <- conn
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var ack struct {
				EnvelopeID string `json:"envelope_id"`
			}
			if json.Unmarshal(raw, &ack) == nil && ack.EnvelopeID != "" {
				acksMu.Lock()
				acks = append(acks, ack.EnvelopeID)
				acksMu.Unlock()
			}
		}
	})

	hc, _ := newFakeSlackHTTP(map[string]func(map[string]any) map[string]any{
		"auth.test":             authRoute,
		"apps.connections.open": func(_ map[string]any) map[string]any { return map[string]any{"url": wsURL} },
		"users.info": func(_ map[string]any) map[string]any {
			return map[string]any{"user": map[string]any{"name": "zs", "real_name": "Zhang San"}}
		},
	})

	var msgMu sync.Mutex
	var received []imsdk.InboundMessage
	var states []imsdk.ConnectionStateKind

	conn, err := New(Options{
		HTTPClient: hc,
		Backoff:    func(int) time.Duration { return 5 * time.Millisecond },
	}).Connect(context.Background(), imsdk.Credentials{
		"botToken": "xoxb-1",
		"appToken": "xapp-1",
		"name":     "Alex",
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			msgMu.Lock()
			received = append(received, m)
			msgMu.Unlock()
		},
		OnState: func(st imsdk.ConnectionState) {
			msgMu.Lock()
			states = append(states, st.Kind)
			msgMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	push := func(ws *websocket.Conn, v any) {
		t.Helper()
		b, _ := json.Marshal(v)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := ws.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatalf("ws write: %v", err)
		}
	}

	ws1 := <-sockConns
	push(ws1, map[string]any{"type": "hello"})

	env1 := map[string]any{
		"type":        "events_api",
		"envelope_id": "e1",
		"payload": map[string]any{
			"event_id": "Ev1",
			"event": map[string]any{
				"type":         "message",
				"channel":      "C1",
				"channel_type": "channel",
				"user":         "U1",
				"text":         "<@UBOT> hi",
				"ts":           "2.0",
			},
		},
	}
	push(ws1, env1)
	push(ws1, map[string]any{
		"type":        "events_api",
		"envelope_id": "e2",
		"payload":     env1["payload"],
	})

	// Slash command /stop in DM (retried once with same trigger_id) and ignored in C1.
	push(ws1, map[string]any{
		"type":        "slash_commands",
		"envelope_id": "s1",
		"payload": map[string]any{
			"command":    "/stop",
			"channel_id": "D1",
			"user_id":    "U1",
			"trigger_id": "trig1",
		},
	})
	push(ws1, map[string]any{
		"type":        "slash_commands",
		"envelope_id": "s2",
		"payload":     `{"command":"/stop","channel_id":"D1","user_id":"U1","trigger_id":"trig1"}`,
	})
	push(ws1, map[string]any{
		"type":        "slash_commands",
		"envelope_id": "s3",
		"payload": map[string]any{
			"command":    "/stop",
			"channel_id": "C1",
			"user_id":    "U1",
			"trigger_id": "trig2",
		},
	})

	waitUntil(t, func() bool {
		acksMu.Lock()
		defer acksMu.Unlock()
		return len(acks) >= 5
	})
	waitUntil(t, func() bool {
		msgMu.Lock()
		defer msgMu.Unlock()
		return len(received) == 2
	})

	msgMu.Lock()
	if received[0].Chat.PlatformID != "C1:2.0" || received[1].Chat.PlatformID != "D1" || received[1].Text != "/stop" || received[1].PlatformID != "trig1" {
		msgMu.Unlock()
		t.Fatalf("unexpected received messages: %+v", received)
	}
	msgMu.Unlock()

	// Socket rotation via "disconnect" opens a fresh socket without entering reconnecting state.
	push(ws1, map[string]any{"type": "disconnect"})
	ws2 := <-sockConns

	msgMu.Lock()
	if len(states) != 0 {
		msgMu.Unlock()
		t.Fatalf("expected no reconnecting state on graceful disconnect, got %v", states)
	}
	msgMu.Unlock()

	// Unexpected close triggers reconnecting -> hello -> connected.
	_ = ws2.CloseNow()
	ws3 := <-sockConns
	push(ws3, map[string]any{"type": "hello"})

	waitUntil(t, func() bool {
		msgMu.Lock()
		defer msgMu.Unlock()
		return slices.Equal(states, []imsdk.ConnectionStateKind{imsdk.StateReconnecting, imsdk.StateConnected})
	})

	// Verify 5000-entry FIFO dedup eviction directly.
	seen := newFIFOSet(5000)
	for i := range 5001 {
		if !seen.Add("k-" + string(rune(i+1))) {
			t.Fatalf("unexpected duplicate at %d", i)
		}
	}
	if !seen.Add("k-" + string(rune(1))) {
		t.Fatal("oldest entry should have been evicted after 5001 insertions")
	}
	if seen.Add("k-" + string(rune(5001))) {
		t.Fatal("recent entry should still be deduplicated")
	}
}

func TestOutboundLiveReactionsFilesAndDirect(t *testing.T) {
	const uploadURL = "https://files.slack.com/upload/v1/signed"
	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	})

	dir := t.TempDir()
	srcFile := filepath.Join(dir, "smoke-result.txt")
	if err := os.WriteFile(srcFile, []byte("MINGLE-FILE-OK"), 0o644); err != nil {
		t.Fatal(err)
	}

	var modeMu sync.Mutex
	uploadMode := "ok"
	completeErr := ""

	hc, getCalls := newFakeSlackHTTP(map[string]func(map[string]any) map[string]any{
		"auth.test":             authRoute,
		"apps.connections.open": func(_ map[string]any) map[string]any { return map[string]any{"url": wsURL} },
		"chat.postMessage":      func(_ map[string]any) map[string]any { return map[string]any{"ts": "5.0"} },
		"chat.update":           func(_ map[string]any) map[string]any { return map[string]any{} },
		"conversations.open": func(_ map[string]any) map[string]any {
			return map[string]any{"channel": map[string]any{"id": "D99"}}
		},
		"files.getUploadURLExternal": func(body map[string]any) map[string]any {
			modeMu.Lock()
			m := uploadMode
			modeMu.Unlock()
			if m == "forbidden" {
				return map[string]any{"ok": false, "error": "file_uploads_disabled"}
			}
			if body["filename"] == nil || body["length"] == nil {
				return map[string]any{"ok": false, "error": "missing_argument"}
			}
			return map[string]any{"upload_url": uploadURL, "file_id": "F1"}
		},
		"files.completeUploadExternal": func(_ map[string]any) map[string]any {
			modeMu.Lock()
			ce := completeErr
			modeMu.Unlock()
			if ce != "" {
				return map[string]any{"ok": false, "error": ce}
			}
			return map[string]any{}
		},
		"https://files.slack.com/files-pri/T1-F9/doc.txt": func(_ map[string]any) map[string]any {
			return map[string]any{"_bytes": []byte("DOWNLOADED-OK")}
		},
	})

	conn, err := New(Options{HTTPClient: hc}).Connect(context.Background(), imsdk.Credentials{
		"botToken": "xoxb-1",
		"appToken": "xapp-1",
		"name":     "Alex",
	}, imsdk.ChannelEvents{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	threadChat := imsdk.ChatRef{PlatformID: "C1:2.0", Kind: imsdk.ChatThread}
	ctx := context.Background()

	// Reactions: processing -> done -> error.
	_ = conn.React(ctx, threadChat, "2.0", imsdk.ReactionProcessing)
	_ = conn.React(ctx, threadChat, "2.0", imsdk.ReactionDone)
	_ = conn.React(ctx, threadChat, "2.0", imsdk.ReactionError)

	var reactOps []string
	for _, c := range getCalls() {
		if strings.HasPrefix(c.Method, "reactions.") {
			reactOps = append(reactOps, c.Method+":"+c.Body["name"].(string))
		}
	}
	wantReacts := []string{
		"reactions.add:eyes",
		"reactions.remove:eyes",
		"reactions.add:white_check_mark",
		"reactions.remove:eyes",
		"reactions.add:x",
	}
	if !slices.Equal(reactOps, wantReacts) {
		t.Fatalf("reactions = %v, want %v", reactOps, wantReacts)
	}

	// StartLive: posts initial markdown block, deduplicates identical Update, prepends <@U1> on Finish.
	lr, err := conn.StartLive(ctx, threadChat, imsdk.LiveOptions{
		ReplyTo: "2.0",
		Mention: "U1",
		Initial: "**Thinking** · 0s",
	})
	if err != nil {
		t.Fatalf("StartLive: %v", err)
	}
	_ = lr.Update(ctx, "**Working** · 2s")
	_ = lr.Update(ctx, "**Working** · 2s")
	finRes, err := lr.Finish(ctx, "Ask @Li Si", []imsdk.Mention{{ID: "U2", Name: "Li Si"}, {ID: "B9", Name: "Bot"}})
	if err != nil || finRes.Outcome != imsdk.OutcomeSent || finRes.PlatformID != "5.0" {
		t.Fatalf("Finish result = %+v, err = %v", finRes, err)
	}

	var updates []string
	for _, c := range getCalls() {
		if c.Method == "chat.update" {
			updates = append(updates, c.Body["text"].(string))
		}
	}
	if !slices.Equal(updates, []string{"**Working** · 2s", "<@U1> Ask <@U2>"}) {
		t.Fatalf("chat.update texts = %v", updates)
	}

	// External file upload v2: form-encoded reserve -> raw upload -> postMessage -> completeUploadExternal.
	dmChat := imsdk.ChatRef{PlatformID: "D1", Kind: imsdk.ChatDM}
	before := len(getCalls())
	sendRes, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  dmChat,
		Text:  "Attached.",
		Files: []imsdk.OutboundFile{{Path: srcFile}},
	})
	if err != nil || sendRes.Outcome != imsdk.OutcomeSent || sendRes.PlatformID != "5.0" {
		t.Fatalf("Send file+text = %+v, err = %v", sendRes, err)
	}
	steps := getCalls()[before:]
	if len(steps) != 4 ||
		steps[0].Method != "files.getUploadURLExternal" || !steps[0].Form || steps[0].Body["length"] != "14" ||
		steps[1].Method != uploadURL || steps[1].Bytes != "MINGLE-FILE-OK" ||
		steps[2].Method != "chat.postMessage" ||
		steps[3].Method != "files.completeUploadExternal" {
		t.Fatalf("unexpected upload step sequence: %+v", steps)
	}

	// Refused upload before text posts nothing.
	modeMu.Lock()
	uploadMode = "forbidden"
	modeMu.Unlock()
	before = len(getCalls())
	refused, _ := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  dmChat,
		Text:  "Attached.",
		Files: []imsdk.OutboundFile{{Path: srcFile}},
	})
	if refused.Outcome != imsdk.OutcomeFailed || refused.Code != imsdk.SendErrForbidden || refused.Detail != "file_uploads_disabled" || refused.TextSentPlatformID != "" {
		t.Fatalf("unexpected refused upload result: %+v", refused)
	}
	for _, c := range getCalls()[before:] {
		if c.Method == "chat.postMessage" {
			t.Fatal("chat.postMessage must not be called when staging upload fails")
		}
	}

	// Complete upload failure after text records TextSentPlatformID via WithTextSent.
	modeMu.Lock()
	uploadMode = "ok"
	completeErr = "internal_error"
	modeMu.Unlock()
	partial, _ := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  dmChat,
		Text:  "Attached.",
		Files: []imsdk.OutboundFile{{Path: srcFile}},
	})
	if partial.Outcome != imsdk.OutcomeFailed || !partial.Retryable || partial.Detail != "internal_error" || partial.TextSentPlatformID != "5.0" {
		t.Fatalf("unexpected partial failure result: %+v", partial)
	}

	// Download with Bearer token + OpenDirect & MessageUser.
	sc := conn.(*slackConnection)
	sc.handleEvent(Event{
		Type:        "message",
		Channel:     "D1",
		ChannelType: "im",
		User:        "U1",
		Text:        "doc",
		TS:          "8.0",
		Files: []File{{
			ID:                 "F9",
			Name:               "doc.txt",
			URLPrivateDownload: "https://files.slack.com/files-pri/T1-F9/doc.txt",
		}},
	})
	dlPath := filepath.Join(dir, "downloaded.txt")
	if err := conn.Download(ctx, imsdk.InboundAttachment{ID: "F9", Name: "doc.txt"}, dlPath); err != nil {
		t.Fatalf("Download: %v", err)
	}
	dlBytes, _ := os.ReadFile(dlPath)
	if string(dlBytes) != "DOWNLOADED-OK" {
		t.Fatalf("downloaded bytes = %q", string(dlBytes))
	}

	receipt, err := conn.MessageUser(ctx, "U1", "hello direct")
	if err != nil || receipt.Chat != "D99" || receipt.PlatformID != "5.0" {
		t.Fatalf("MessageUser = %+v, err = %v", receipt, err)
	}
}
