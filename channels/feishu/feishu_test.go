package feishu

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

func TestFrameRoundTripAndFragments(t *testing.T) {
	orig := Frame{
		SeqID:           42,
		LogID:           99887766,
		Service:         12345,
		Method:          FrameMethodData,
		Headers:         []FrameHeader{{Key: "type", Value: "event"}, {Key: "message_id", Value: "m_1"}, {Key: "empty", Value: ""}},
		PayloadEncoding: "json",
		PayloadType:     "event_callback",
		Payload:         []byte(`{"schema":"2.0"}`),
		LogIDNew:        "202611180001",
	}
	wire := EncodeFrame(orig)
	got, err := DecodeFrame(wire)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}
	if got.SeqID != orig.SeqID || got.LogID != orig.LogID || got.Service != orig.Service || got.Method != orig.Method {
		t.Fatalf("scalar mismatch: got %+v, want %+v", got, orig)
	}
	if !slices.Equal(got.Headers, orig.Headers) {
		t.Fatalf("headers mismatch: got %+v, want %+v", got.Headers, orig.Headers)
	}
	if !bytes.Equal(got.Payload, orig.Payload) || got.PayloadEncoding != orig.PayloadEncoding || got.PayloadType != orig.PayloadType || got.LogIDNew != orig.LogIDNew {
		t.Fatalf("payload metadata mismatch: got %+v, want %+v", got, orig)
	}

	// Zero required fields must still encode and decode cleanly.
	zeroWire := EncodeFrame(Frame{})
	if len(zeroWire) == 0 {
		t.Fatal("expected required proto2 fields to produce non-empty wire bytes")
	}
	if _, err := DecodeFrame(zeroWire); err != nil {
		t.Fatalf("DecodeFrame zero frame failed: %v", err)
	}
	if _, err := DecodeFrame(nil); err == nil {
		t.Fatal("expected empty buffer to fail required field check")
	}

	// Multi-frame fragment assembly.
	fa := newFragmentAssembler()
	now := time.Now()
	full := []byte(`{"schema":"2.0","header":{"event_type":"im.message.receive_v1"}}`)
	half := len(full) / 2
	m1, err := fa.add("frag_1", 2, 0, full[:half], now)
	if err != nil || m1 != nil {
		t.Fatalf("expected nil on first fragment, got %v, %v", m1, err)
	}
	m2, err := fa.add("frag_1", 2, 1, full[half:], now)
	if err != nil || !bytes.Equal(m2, full) {
		t.Fatalf("reassembly mismatch: got %s, err=%v", string(m2), err)
	}
}

func decodeAddonsFromQR(t *testing.T, qrContent string) ([]string, []string) {
	t.Helper()
	u, err := url.Parse(qrContent)
	if err != nil {
		t.Fatalf("parse QR URL: %v", err)
	}
	enc := u.Query().Get("addons")
	gzBytes, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("base64url decode addons: %v", err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(gzBytes))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()
	raw, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	var parsed struct {
		Scopes struct {
			Tenant []string `json:"tenant"`
		} `json:"scopes"`
		Events struct {
			Items struct {
				Tenant []string `json:"tenant"`
			} `json:"items"`
		} `json:"events"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal addons: %v", err)
	}
	return parsed.Scopes.Tenant, parsed.Events.Items.Tenant
}

func collectEvents(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func TestFeishuOnboarding(t *testing.T) {
	var (
		mu             sync.Mutex
		pollCount      int
		profileGranted bool
	)

	client := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var bodyBytes []byte
		if req.Body != nil {
			bodyBytes, _ = io.ReadAll(req.Body)
		}
		switch {
		case req.URL.Path == "/oauth/v1/app/registration":
			vals, _ := url.ParseQuery(string(bodyBytes))
			switch vals.Get("action") {
			case "begin":
				return imtest.JSONResponse(200, map[string]any{
					"verification_uri_complete": "https://open.feishu.cn/page/launcher?user_code=ABCD",
					"device_code":               "dc_1",
					"expires_in":                600,
					"interval":                  0.01,
				}), nil
			case "poll":
				mu.Lock()
				pollCount++
				n := pollCount
				mu.Unlock()
				if n == 1 {
					return imtest.JSONResponse(400, map[string]any{"error": "authorization_pending"}), nil
				}
				if req.URL.Host == "accounts.feishu.cn" && n == 2 {
					return imtest.JSONResponse(200, map[string]any{
						"user_info": map[string]any{"tenant_brand": "lark"},
					}), nil
				}
				return imtest.JSONResponse(200, map[string]any{
					"client_id":     "cli_aabbccddeeff0011",
					"client_secret": "sec_1",
					"user_info":     map[string]any{"open_id": "ou_owner", "tenant_brand": "lark"},
				}), nil
			}
		case req.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
			var auth struct {
				AppID     string `json:"app_id"`
				AppSecret string `json:"app_secret"`
			}
			_ = json.Unmarshal(bodyBytes, &auth)
			if auth.AppSecret == "bad" {
				return imtest.JSONResponse(401, map[string]any{"code": 99991663, "msg": "invalid secret"}), nil
			}
			return imtest.JSONResponse(200, map[string]any{"code": 0, "tenant_access_token": "tat_1", "expire": 7200}), nil
		case req.URL.Path == "/open-apis/bot/v3/info/":
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"bot":  map[string]any{"app_name": "Alex", "open_id": "ou_bot", "activate_status": 1},
			}), nil
		case req.URL.Path == "/open-apis/application/v6/scopes":
			mu.Lock()
			granted := profileGranted
			mu.Unlock()
			var items []map[string]any
			if granted {
				for _, s := range ProfileScopes {
					items = append(items, map[string]any{"scope_name": s, "grant_status": 1})
				}
			}
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"scopes": items}}), nil
		}
		return imtest.JSONResponse(404, map[string]any{"code": 404}), nil
	})

	ch := New(Options{HTTPClient: client})

	// 1. Fresh QR registration with Lark domain switch.
	evs := collectEvents(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{AgentName: "Alex"}))
	if len(evs) != 2 || evs[0].Kind != imsdk.EventQRReady || evs[1].Kind != imsdk.EventSucceeded {
		t.Fatalf("unexpected onboarding events: %+v", evs)
	}
	u0, _ := url.Parse(evs[0].QRContent)
	if u0.Query().Get("createOnly") != "true" || u0.Query().Get("name") != "Alex" {
		t.Fatalf("missing createOnly/name in QR URL: %s", evs[0].QRContent)
	}
	scopes0, events0 := decodeAddonsFromQR(t, evs[0].QRContent)
	if !slices.Equal(scopes0, FeishuScopes) {
		t.Fatalf("unexpected base scopes: %v", scopes0)
	}
	if !slices.Equal(events0, []string{"im.message.receive_v1", "im.chat.member.bot.added_v1"}) {
		t.Fatalf("unexpected event subscriptions: %v", events0)
	}
	if evs[1].Credentials["domain"] != "lark" || evs[1].OwnerID != "ou_owner" || !evs[1].PendingApproval {
		t.Fatalf("unexpected succeeded event: %+v", evs[1])
	}

	// 2. Reauthorize adding PermMemberProfiles, then reauthorize again preserving ProfileScopes.
	reauthEvs := collectEvents(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{
		Reauthorize: imsdk.Credentials{"appId": "cli_aabbccddeeff0011", "appSecret": "sec_1"},
		Adding:      []imsdk.PermissionID{imsdk.PermMemberProfiles},
	}))
	if len(reauthEvs) != 2 || reauthEvs[0].Kind != imsdk.EventQRReady {
		t.Fatalf("reauth events: %+v", reauthEvs)
	}
	u1, _ := url.Parse(reauthEvs[0].QRContent)
	if u1.Query().Get("clientID") != "cli_aabbccddeeff0011" || u1.Query().Get("createOnly") != "" {
		t.Fatalf("expected clientID without createOnly on reauthorize: %s", reauthEvs[0].QRContent)
	}
	scopes1, _ := decodeAddonsFromQR(t, reauthEvs[0].QRContent)
	wantWithProfile := append(slices.Clone(FeishuScopes), ProfileScopes...)
	if !slices.Equal(scopes1, wantWithProfile) {
		t.Fatalf("expected profile scopes on Adding: %v", scopes1)
	}

	mu.Lock()
	profileGranted = true
	mu.Unlock()
	reauthPreserved := collectEvents(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{
		Reauthorize: imsdk.Credentials{"appId": "cli_aabbccddeeff0011", "appSecret": "sec_1"},
	}))
	scopes2, _ := decodeAddonsFromQR(t, reauthPreserved[0].QRContent)
	if !slices.Equal(scopes2, wantWithProfile) {
		t.Fatalf("expected already-granted profile scopes preserved: %v", scopes2)
	}

	// 3. Manual credentials valid & invalid.
	manualOK := collectEvents(ch.Onboard(context.Background(), imsdk.Credentials{"appId": "cli_1", "appSecret": "sec_1"}, imsdk.OnboardingOptions{}))
	if len(manualOK) != 1 || manualOK[0].Kind != imsdk.EventSucceeded {
		t.Fatalf("manualOK: %+v", manualOK)
	}
	manualBad := collectEvents(ch.Onboard(context.Background(), imsdk.Credentials{"appId": "cli_1", "appSecret": "bad"}, imsdk.OnboardingOptions{}))
	if len(manualBad) != 1 || manualBad[0].Kind != imsdk.EventFailed || manualBad[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("manualBad: %+v", manualBad)
	}
}

func TestFeishuApprovalWSInboundDedupBackfillAndFeatures(t *testing.T) {
	var (
		activated      atomic.Bool
		contactLookups atomic.Int32
		acks           chan Frame = make(chan Frame, 16)
		serverPushes   chan Frame = make(chan Frame, 16)
	)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				case f := <-serverPushes:
					_ = conn.Write(r.Context(), websocket.MessageBinary, EncodeFrame(f))
				}
			}
		}()
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			f, err := DecodeFrame(raw)
			if err != nil {
				continue
			}
			if f.Method == FrameMethodControl && f.HeaderValue("type") == "ping" {
				pong := Frame{
					Service: f.Service,
					Method:  FrameMethodControl,
					Headers: []FrameHeader{{Key: "type", Value: "pong"}},
					Payload: []byte(`{"PingInterval":60,"ReconnectCount":-1,"ReconnectInterval":1,"ReconnectNonce":0}`),
				}
				_ = conn.Write(r.Context(), websocket.MessageBinary, EncodeFrame(pong))
				continue
			}
			acks <- f
		}
	})

	var (
		mu         sync.Mutex
		nowTime    = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
		grantedMap = map[string]bool{}
		reactions  []string
		cardState  struct {
			streaming bool
			summary   string
			contents  []string
			sequences []int
			failOnce  int
		}
		failFileUpload bool
	)

	httpClient := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var bodyBytes []byte
		if req.Body != nil {
			bodyBytes, _ = io.ReadAll(req.Body)
		}
		p := req.URL.Path
		switch {
		case p == "/open-apis/auth/v3/tenant_access_token/internal":
			return imtest.JSONResponse(200, map[string]any{"code": 0, "tenant_access_token": "tat_ok", "expire": 7200}), nil

		case p == "/open-apis/bot/v3/info/":
			status := 1
			if activated.Load() {
				status = 2
			}
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"bot":  map[string]any{"app_name": "Alex", "open_id": "ou_bot", "activate_status": status},
			}), nil

		case p == "/callback/ws/endpoint":
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{
					"URL": wsURL + "?device_id=d1&service_id=77",
					"ClientConfig": map[string]any{
						"PingInterval":      1,
						"ReconnectCount":    -1,
						"ReconnectInterval": 1,
						"ReconnectNonce":    0,
					},
				},
			}), nil

		case p == "/open-apis/im/v1/chats/oc_1" || p == "/open-apis/im/v1/chats/oc_9":
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"name": "周末徒步"}}), nil

		case strings.HasPrefix(p, "/open-apis/contact/v3/users/"):
			contactLookups.Add(1)
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{
					"user": map[string]any{
						"name":           "李四",
						"job_title":      "产品经理",
						"department_ids": []string{"od_pd", "od_hidden"},
					},
				},
			}), nil

		case strings.HasPrefix(p, "/open-apis/contact/v3/departments/"):
			if strings.HasSuffix(p, "/od_pd") {
				return imtest.JSONResponse(200, map[string]any{
					"code": 0,
					"data": map[string]any{"department": map[string]any{"name": "产品部"}},
				}), nil
			}
			return imtest.JSONResponse(403, map[string]any{"code": 41050, "msg": "no authority"}), nil

		case p == "/open-apis/application/v6/scopes":
			mu.Lock()
			var items []map[string]any
			for k, v := range grantedMap {
				gs := 2
				if v {
					gs = 1
				}
				items = append(items, map[string]any{"scope_name": k, "grant_status": gs})
			}
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"scopes": items}}), nil

		case p == "/open-apis/im/v1/chats/oc_1/members":
			pt := req.URL.Query().Get("page_token")
			if pt == "" {
				return imtest.JSONResponse(200, map[string]any{
					"code": 0,
					"data": map[string]any{
						"items":      []map[string]any{{"member_id": "ou_1", "name": "张三"}},
						"has_more":   true,
						"page_token": "p2",
					},
				}), nil
			}
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{
					"items":    []map[string]any{{"member_id": "ou_2", "name": "李四"}},
					"has_more": false,
				},
			}), nil

		case p == "/open-apis/im/v1/chats/oc_1/members/bots":
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{
					"items": []map[string]any{{"bot_id": "ou_other_bot", "bot_name": "值班机器人"}},
				},
			}), nil

		case p == "/open-apis/im/v1/messages" && req.Method == http.MethodGet:
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{
					"items": []map[string]any{
						{
							"message_id":  "om_h4",
							"msg_type":    "text",
							"create_time": "1700000004000",
							"chat_id":     "oc_9",
							"sender":      map[string]any{"id": "cli_1", "sender_type": "app"},
							"body":        map[string]any{"content": `{"text":"own earlier"}`},
						},
						{
							"message_id":  "om_h3",
							"msg_type":    "text",
							"create_time": "1700000003000",
							"chat_id":     "oc_9",
							"sender":      map[string]any{"id": "cli_ci", "sender_type": "app"},
							"body":        map[string]any{"content": `{"text":"deploy failed"}`},
						},
						{
							"message_id":  "om_h2",
							"msg_type":    "text",
							"create_time": "1700000002000",
							"chat_id":     "oc_9",
							"sender":      map[string]any{"id": "ou_li", "sender_type": "user"},
							"body":        map[string]any{"content": `{"text":"@_user_1 周六去哪？"}`},
							"mentions":    []map[string]any{{"key": "@_user_1", "id": "ou_bot", "name": "Alex"}},
						},
						{
							"message_id": "om_h1",
							"msg_type":   "system",
							"chat_id":    "oc_9",
							"sender":     map[string]any{"id": "sys", "sender_type": "system"},
						},
						{
							"message_id": "om_h0",
							"msg_type":   "text",
							"deleted":    true,
							"sender":     map[string]any{"id": "ou_li", "sender_type": "user"},
						},
					},
				},
			}), nil

		case p == "/open-apis/im/v1/messages" && req.Method == http.MethodPost:
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{"message_id": "om_sent_1", "chat_id": "oc_dm_resolved"},
			}), nil

		case strings.HasSuffix(p, "/reply") && req.Method == http.MethodPost:
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{"message_id": "om_reply_1"},
			}), nil

		case p == "/open-apis/im/v1/messages/om_sent_1" && req.Method == http.MethodGet:
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{"items": []map[string]any{{"chat_id": "oc_dm_ou_9"}}},
			}), nil

		case p == "/open-apis/im/v1/files" && req.Method == http.MethodPost:
			mu.Lock()
			fail := failFileUpload
			mu.Unlock()
			if fail {
				return imtest.JSONResponse(413, map[string]any{"code": 234006, "msg": "too large"}), nil
			}
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"file_key": "fk_1"}}), nil

		case p == "/open-apis/im/v1/images" && req.Method == http.MethodPost:
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"image_key": "ik_1"}}), nil

		case p == "/open-apis/im/v1/messages/om_dl/resources/fk_dl":
			return imtest.BytesResponse(200, []byte("file-binary-ok")), nil

		case strings.Contains(p, "/reactions"):
			mu.Lock()
			defer mu.Unlock()
			if req.Method == http.MethodDelete {
				parts := strings.Split(p, "/")
				reactions = append(reactions, "-"+parts[len(parts)-1])
				return imtest.JSONResponse(200, map[string]any{"code": 0}), nil
			}
			var rb struct {
				ReactionType struct {
					EmojiType string `json:"emoji_type"`
				} `json:"reaction_type"`
			}
			_ = json.Unmarshal(bodyBytes, &rb)
			reactions = append(reactions, "+"+rb.ReactionType.EmojiType)
			return imtest.JSONResponse(200, map[string]any{
				"code": 0,
				"data": map[string]any{"reaction_id": "r-" + rb.ReactionType.EmojiType},
			}), nil

		case p == "/open-apis/cardkit/v1/cards" && req.Method == http.MethodPost:
			var cb struct {
				Data string `json:"data"`
			}
			_ = json.Unmarshal(bodyBytes, &cb)
			var spec struct {
				Body struct {
					Elements []struct {
						Content string `json:"content"`
					} `json:"elements"`
				} `json:"body"`
			}
			_ = json.Unmarshal([]byte(cb.Data), &spec)
			mu.Lock()
			cardState.streaming = true
			cardState.contents = []string{spec.Body.Elements[0].Content}
			cardState.sequences = nil
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"card_id": "card_1"}}), nil

		case p == "/open-apis/cardkit/v1/cards/card_1/settings" && req.Method == http.MethodPatch:
			var sb struct {
				Settings string `json:"settings"`
				Sequence int    `json:"sequence"`
			}
			_ = json.Unmarshal(bodyBytes, &sb)
			var cfgWrap struct {
				Config struct {
					StreamingMode bool `json:"streaming_mode"`
				} `json:"config"`
			}
			_ = json.Unmarshal([]byte(sb.Settings), &cfgWrap)
			mu.Lock()
			cardState.sequences = append(cardState.sequences, sb.Sequence)
			cardState.streaming = cfgWrap.Config.StreamingMode
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"code": 0}), nil

		case p == "/open-apis/cardkit/v1/cards/card_1/elements/stream_md/content" && req.Method == http.MethodPut:
			var eb struct {
				Content  string `json:"content"`
				Sequence int    `json:"sequence"`
			}
			_ = json.Unmarshal(bodyBytes, &eb)
			mu.Lock()
			cardState.sequences = append(cardState.sequences, eb.Sequence)
			if cardState.failOnce != 0 {
				code := cardState.failOnce
				cardState.failOnce = 0
				mu.Unlock()
				return imtest.JSONResponse(400, map[string]any{"code": code, "msg": "stream closed"}), nil
			}
			cardState.contents = append(cardState.contents, eb.Content)
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"code": 0}), nil

		case p == "/open-apis/cardkit/v1/cards/card_1" && req.Method == http.MethodPut:
			var ub struct {
				Card struct {
					Data string `json:"data"`
				} `json:"card"`
				Sequence int `json:"sequence"`
			}
			_ = json.Unmarshal(bodyBytes, &ub)
			var spec struct {
				Config struct {
					StreamingMode bool `json:"streaming_mode"`
					Summary       struct {
						Content string `json:"content"`
					} `json:"summary"`
				} `json:"config"`
				Body struct {
					Elements []struct {
						Content string `json:"content"`
					} `json:"elements"`
				} `json:"body"`
			}
			_ = json.Unmarshal([]byte(ub.Card.Data), &spec)
			mu.Lock()
			cardState.sequences = append(cardState.sequences, ub.Sequence)
			cardState.streaming = spec.Config.StreamingMode
			cardState.summary = spec.Config.Summary.Content
			cardState.contents = append(cardState.contents, spec.Body.Elements[0].Content)
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"code": 0}), nil
		}
		return imtest.JSONResponse(404, map[string]any{"code": 404}), nil
	})

	ch := New(Options{
		HTTPClient:           httpClient,
		ApprovalPollInterval: 20 * time.Millisecond,
		ScopesTTL:            15 * time.Second,
		StreamRenewAfter:     8 * time.Minute,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return nowTime
		},
	})

	var (
		evMu     sync.Mutex
		states   []imsdk.ConnectionStateKind
		messages []imsdk.InboundMessage
		joined   []string
	)
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{"appId": "cli_1", "appSecret": "s"}, imsdk.ChannelEvents{
		OnState: func(s imsdk.ConnectionState) {
			evMu.Lock()
			states = append(states, s.Kind)
			evMu.Unlock()
		},
		OnMessage: func(m imsdk.InboundMessage) {
			evMu.Lock()
			messages = append(messages, m)
			evMu.Unlock()
		},
		OnMembership: func(mc imsdk.MembershipChange) {
			evMu.Lock()
			joined = append(joined, mc.Chat.PlatformID)
			evMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	// Initially awaiting approval; Send must fail with forbidden.
	preSend, _ := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat: imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup},
		Text: "before approval",
	})
	if preSend.Outcome != imsdk.OutcomeFailed || preSend.Code != imsdk.SendErrForbidden {
		t.Fatalf("expected forbidden before approval, got %+v", preSend)
	}

	// Activate app and wait for StateConnected.
	activated.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		evMu.Lock()
		ok := slices.Contains(states, imsdk.StateConnected)
		evMu.Unlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	evMu.Lock()
	if !slices.Equal(states, []imsdk.ConnectionStateKind{imsdk.StateAwaitingApproval, imsdk.StateConnected}) {
		evMu.Unlock()
		t.Fatalf("unexpected state sequence: %v", states)
	}
	evMu.Unlock()

	// Push unmentioned group message + duplicate + bot message over WS.
	makeMsgFrame := func(frameID, msgID, senderType, senderID, text string, mentioned bool) Frame {
		var mentions []map[string]any
		if mentioned {
			mentions = []map[string]any{
				{"key": "@_user_1", "id": map[string]string{"open_id": "ou_bot"}, "name": "Alex"},
				{"key": "@_user_2", "id": map[string]string{"open_id": "ou_peer"}, "name": "王五"},
			}
		}
		payload, _ := json.Marshal(map[string]any{
			"schema": "2.0",
			"header": map[string]any{"event_id": "ev_" + frameID, "event_type": "im.message.receive_v1"},
			"event": map[string]any{
				"sender": map[string]any{
					"sender_id":   map[string]string{"open_id": senderID},
					"sender_type": senderType,
				},
				"message": map[string]any{
					"message_id":   msgID,
					"chat_id":      "oc_1",
					"chat_type":    "group",
					"message_type": "text",
					"create_time":  "1700000000000",
					"content":      `{"text":"` + text + `"}`,
					"mentions":     mentions,
				},
			},
		})
		return Frame{
			SeqID:   1,
			LogID:   1,
			Service: 77,
			Method:  FrameMethodData,
			Headers: []FrameHeader{
				{Key: "type", Value: "event"},
				{Key: "message_id", Value: frameID},
				{Key: "sum", Value: "1"},
				{Key: "seq", Value: "0"},
			},
			Payload: payload,
		}
	}

	lookupsBefore := contactLookups.Load()
	serverPushes <- makeMsgFrame("f1", "om_dup", "user", "ou_u1", "大家好", false)
	serverPushes <- makeMsgFrame("f2", "om_dup", "user", "ou_u1", "大家好", false)
	serverPushes <- makeMsgFrame("f3", "om_bot_msg", "app", "ou_ci_bot", "CI passed", false)
	serverPushes <- makeMsgFrame("f4", "om_at", "user", "ou_u1", "@_user_1 @_user_2 请看", true)

	// Verify event ACK frame.
	select {
	case ack := <-acks:
		if ack.HeaderValue("biz_rt") == "" || string(ack.Payload) != `{"code":200}` {
			t.Fatalf("unexpected ACK frame: %+v payload=%s", ack, string(ack.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WS event ACK")
	}

	for time.Now().Before(time.Now().Add(2 * time.Second)) {
		evMu.Lock()
		n := len(messages)
		evMu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	evMu.Lock()
	if len(messages) != 3 {
		evMu.Unlock()
		t.Fatalf("expected 3 deduplicated messages, got %d: %+v", len(messages), messages)
	}
	if messages[0].AddressedToBot || *messages[0].Mentioned {
		t.Fatalf("expected unmentioned group message to have AddressedToBot=false, got %+v", messages[0])
	}
	if !messages[1].Sender.IsBot || messages[1].Sender.Name != "" {
		t.Fatalf("expected app sender IsBot=true with empty name, got %+v", messages[1].Sender)
	}
	if !messages[2].AddressedToBot || !*messages[2].Mentioned || messages[2].Text != "@王五 请看" || len(messages[2].Mentions) != 1 || messages[2].Mentions[0].ID != "ou_peer" {
		t.Fatalf("unexpected @mention normalization: %+v", messages[2])
	}
	evMu.Unlock()

	// Only ou_u1 (human) should have triggered a contact lookup, never ou_ci_bot!
	if gotLookups := contactLookups.Load() - lookupsBefore; gotLookups != 1 {
		t.Fatalf("expected exactly 1 contact lookup for human sender, got %d", gotLookups)
	}

	// Per-connection dedup isolation: a second connection must NOT share the first connection's dedup cache.
	var secondMsgs atomic.Int32
	wsURL2 := imtest.WSServer(t, func(c2 *websocket.Conn, r *http.Request) {
		_ = c2.Write(r.Context(), websocket.MessageBinary, EncodeFrame(makeMsgFrame("f_other", "om_dup", "user", "ou_u1", "大家好", false)))
		_, _, _ = c2.Read(r.Context())
		<-r.Context().Done()
	})
	ch2 := New(Options{
		HTTPClient: imtest.Client(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/callback/ws/endpoint" {
				return imtest.JSONResponse(200, map[string]any{
					"code": 0,
					"data": map[string]any{"URL": wsURL2 + "?service_id=88", "ClientConfig": map[string]any{"PingInterval": 60}},
				}), nil
			}
			return httpClient.Transport.RoundTrip(req)
		}),
	})
	conn2, err := ch2.Connect(context.Background(), imsdk.Credentials{"appId": "cli_2", "appSecret": "s"}, imsdk.ChannelEvents{
		OnMessage: func(imsdk.InboundMessage) { secondMsgs.Add(1) },
	})
	if err != nil {
		t.Fatalf("conn2 Connect: %v", err)
	}
	for i := 0; i < 100 && secondMsgs.Load() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	_ = conn2.Close()
	if secondMsgs.Load() != 1 {
		t.Fatalf("expected conn2 to receive om_dup independently, got %d", secondMsgs.Load())
	}

	// Push botAdded event and verify Earlier=true history backfill (skipping system, deleted, and own cli_1).
	botAddedPayload, _ := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{"event_id": "ev_join", "event_type": "im.chat.member.bot.added_v1"},
		"event":  map[string]any{"chat_id": "oc_9"},
	})
	serverPushes <- Frame{
		SeqID:   2,
		LogID:   2,
		Service: 77,
		Method:  FrameMethodData,
		Headers: []FrameHeader{
			{Key: "type", Value: "event"},
			{Key: "message_id", Value: "f_join"},
			{Key: "sum", Value: "1"},
			{Key: "seq", Value: "0"},
		},
		Payload: botAddedPayload,
	}
	for i := 0; i < 100; i++ {
		evMu.Lock()
		n := len(messages)
		evMu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	evMu.Lock()
	if !slices.Equal(joined, []string{"oc_9"}) {
		t.Fatalf("expected joined oc_9, got %v", joined)
	}
	backfilled := messages[3:]
	evMu.Unlock()
	if len(backfilled) != 2 || !backfilled[0].Earlier || backfilled[0].PlatformID != "om_h2" || backfilled[0].Text != "周六去哪？" ||
		!backfilled[1].Earlier || backfilled[1].PlatformID != "om_h3" || !backfilled[1].Sender.IsBot {
		t.Fatalf("unexpected backfilled messages: %+v", backfilled)
	}

	// ListMembers (humans + bots).
	members, err := conn.ListMembers(context.Background(), imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup})
	if err != nil || len(members) != 3 || !members[2].IsBot || members[2].ID != "ou_other_bot" {
		t.Fatalf("unexpected ListMembers: %+v, err=%v", members, err)
	}

	// Permissions 15s TTL & Profile.
	perms, _ := conn.Permissions(context.Background())
	if len(perms) != 3 || perms[0].Granted || perms[2].Granted {
		t.Fatalf("expected initial permissions ungranted: %+v", perms)
	}
	mu.Lock()
	grantedMap[GroupMessagesScope] = true
	for _, s := range ProfileScopes {
		grantedMap[s] = true
	}
	mu.Unlock()
	// Still cached within 15s TTL.
	permsCached, _ := conn.Permissions(context.Background())
	if permsCached[0].Granted {
		t.Fatal("expected scopes to remain cached within 15s TTL")
	}
	mu.Lock()
	nowTime = nowTime.Add(20 * time.Second)
	mu.Unlock()
	prof, err := conn.Profile(context.Background(), "ou_1")
	if err != nil || prof != "产品部 · 产品经理" {
		t.Fatalf("unexpected Profile: %q, err=%v", prof, err)
	}

	// OpenDirect + Send to ou_* resolves DM chat ID; WithTextSent when attachment fails.
	dmTarget, _ := conn.OpenDirect(context.Background(), "ou_9")
	dmSent, _ := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:     imsdk.ChatRef{PlatformID: dmTarget, Kind: imsdk.ChatDM},
		Text:     "@Mingle产品经理 你好",
		Mentions: []imsdk.Mention{{ID: "ou_pm", Name: "Mingle产品经理"}},
	})
	if dmSent.Outcome != imsdk.OutcomeSent || dmSent.Chat != "oc_dm_ou_9" {
		t.Fatalf("unexpected DM send result: %+v", dmSent)
	}

	tmpDir := t.TempDir()
	badFile := filepath.Join(tmpDir, "doc.pdf")
	_ = os.WriteFile(badFile, []byte("pdf"), 0o600)
	mu.Lock()
	failFileUpload = true
	mu.Unlock()
	partialRes, _ := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup},
		Text:  "先发文字",
		Files: []imsdk.OutboundFile{{Path: badFile}},
	})
	if partialRes.Outcome != imsdk.OutcomeFailed || partialRes.TextSentPlatformID != "om_sent_1" {
		t.Fatalf("expected WithTextSent on file upload failure, got %+v", partialRes)
	}

	// Download via im/v1/messages/:id/resources/:key.
	dlPath := filepath.Join(tmpDir, "downloaded.bin")
	if err := conn.Download(context.Background(), imsdk.InboundAttachment{ID: "om_dl|file|fk_dl"}, dlPath); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	if raw, _ := os.ReadFile(dlPath); string(raw) != "file-binary-ok" {
		t.Fatalf("unexpected downloaded bytes: %q", string(raw))
	}

	// React OnIt -> DONE.
	_ = conn.React(context.Background(), imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup}, "om_1", imsdk.ReactionProcessing)
	_ = conn.React(context.Background(), imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup}, "om_1", imsdk.ReactionDone)
	mu.Lock()
	gotReactions := slices.Clone(reactions)
	mu.Unlock()
	if !slices.Equal(gotReactions, []string{"+OnIt", "-r-OnIt", "+DONE"}) {
		t.Fatalf("unexpected reactions: %v", gotReactions)
	}

	// CardKit 2.0 StartLive: 300309 reopen + 8m renew + finish with summary & <at id=...>.
	live, err := conn.StartLive(context.Background(), imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup}, imsdk.LiveOptions{
		ReplyTo: "om_1",
		Mention: "ou_asker",
		Initial: "思考中...",
	})
	if err != nil {
		t.Fatalf("StartLive failed: %v", err)
	}
	mu.Lock()
	cardState.failOnce = 300309
	mu.Unlock()
	_ = live.Update(context.Background(), "第一段进度")

	// Advance past 8m StreamRenewAfter to trigger proactive reopen.
	mu.Lock()
	nowTime = nowTime.Add(8*time.Minute + time.Second)
	mu.Unlock()
	_ = live.Update(context.Background(), "第9分钟进度")

	finRes, err := live.Finish(context.Background(), "@李四 最终结论", []imsdk.Mention{{ID: "ou_2", Name: "李四"}})
	if err != nil || finRes.Outcome != imsdk.OutcomeSent || finRes.PlatformID != "om_reply_1" {
		t.Fatalf("live.Finish failed: %+v, err=%v", finRes, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if cardState.streaming {
		t.Fatal("expected final card update to disable streaming_mode")
	}
	if cardState.summary != "最终结论" {
		t.Fatalf("expected stripped summary '最终结论', got %q", cardState.summary)
	}
	wantFinal := "<at id=ou_asker></at> <at id=ou_2></at> 最终结论"
	if cardState.contents[len(cardState.contents)-1] != wantFinal {
		t.Fatalf("unexpected final card content: got %q, want %q", cardState.contents[len(cardState.contents)-1], wantFinal)
	}
	// Sequences must be strictly monotonically increasing: [1 (fail 300309), 2 (reopen), 3 (retry), 4 (8m reopen), 5 (update), 6 (finish)].
	if !slices.Equal(cardState.sequences, []int{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("unexpected CardKit sequence numbers: %v", cardState.sequences)
	}
}

func TestFeishuPostConvertMessageUserAndCardFailure(t *testing.T) {
	// Verify post content normalization, interactive card walking, and >50 rune summary truncation.
	ev := rawMessageEvent{
		Sender: rawSender{SenderID: rawSenderID{OpenID: "ou_1"}, SenderType: "user"},
		Message: rawMessage{
			MessageID:   "om_post_1",
			ChatID:      "oc_1",
			ChatType:    "group",
			MessageType: "post",
			CreateTime:  "1700000005000",
			Content:     `{"zh_cn":{"title":"周报","content":[[{"tag":"text","text":"完成","style":["bold"]},{"tag":"a","text":"链接","href":"https://example.com"},{"tag":"img","image_key":"img_post_1"}]]}}`,
		},
	}
	norm := normalizeRawEvent(ev, "ou_bot")
	inb, ok := toInbound(norm, imsdk.BotIdentity{ID: "ou_bot", Name: "Alex"}, "群", false, time.Now())
	if !ok || !strings.Contains(inb.Text, "**周报**") || !strings.Contains(inb.Text, "**完成**[链接](https://example.com)") || len(inb.Attachments) != 1 || inb.Attachments[0].ID != "om_post_1|image|img_post_1" {
		t.Fatalf("unexpected post normalization: %+v", inb)
	}

	// Verify MessageUser and CardKit Finish failure cleanup (settings streaming_mode=false).
	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		<-r.Context().Done()
	})
	var closedSettings atomic.Bool
	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var body []byte
		if req.Body != nil {
			body, _ = io.ReadAll(req.Body)
		}
		p := req.URL.Path
		switch {
		case p == "/open-apis/auth/v3/tenant_access_token/internal":
			return imtest.JSONResponse(200, map[string]any{"code": 0, "tenant_access_token": "t1", "expire": 7200}), nil
		case p == "/open-apis/bot/v3/info/":
			return imtest.JSONResponse(200, map[string]any{"code": 0, "bot": map[string]any{"app_name": "Alex", "open_id": "ou_bot", "activate_status": 2}}), nil
		case p == "/callback/ws/endpoint":
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"URL": wsURL + "?service_id=1", "ClientConfig": map[string]any{"PingInterval": 60}}}), nil
		case p == "/open-apis/im/v1/messages" && req.Method == http.MethodPost:
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"chat_id": "oc_p2p_1", "message_id": "om_mu_1"}}), nil
		case p == "/open-apis/cardkit/v1/cards" && req.Method == http.MethodPost:
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"card_id": "c_fail"}}), nil
		case strings.HasSuffix(p, "/reply"):
			return imtest.JSONResponse(200, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_card_fail"}}), nil
		case p == "/open-apis/cardkit/v1/cards/c_fail" && req.Method == http.MethodPut:
			return imtest.JSONResponse(400, map[string]any{"code": 200860, "msg": "card rejected"}), nil
		case p == "/open-apis/cardkit/v1/cards/c_fail/settings" && req.Method == http.MethodPatch:
			var sb struct {
				Settings string `json:"settings"`
			}
			_ = json.Unmarshal(body, &sb)
			if strings.Contains(sb.Settings, `"streaming_mode":false`) {
				closedSettings.Store(true)
			}
			return imtest.JSONResponse(200, map[string]any{"code": 0}), nil
		}
		return imtest.JSONResponse(404, map[string]any{"code": 404}), nil
	})

	ch := New(Options{HTTPClient: hc})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{"appId": "cli_1", "appSecret": "s"}, imsdk.ChannelEvents{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	rec, err := conn.MessageUser(context.Background(), "ou_target", "hello direct")
	if err != nil || rec == nil || rec.Chat != "oc_p2p_1" || rec.PlatformID != "om_mu_1" {
		t.Fatalf("unexpected MessageUser receipt: %+v, err=%v", rec, err)
	}

	live, err := conn.StartLive(context.Background(), imsdk.ChatRef{PlatformID: "oc_1", Kind: imsdk.ChatGroup}, imsdk.LiveOptions{ReplyTo: "om_1", Initial: "..."})
	if err != nil {
		t.Fatalf("StartLive: %v", err)
	}
	res, err := live.Finish(context.Background(), strings.Repeat("长", 60), nil)
	if err != nil || res.Outcome != imsdk.OutcomeFailed || !closedSettings.Load() {
		t.Fatalf("expected failed Finish with best-effort streaming_mode=false cleanup: res=%+v err=%v closed=%v", res, err, closedSettings.Load())
	}
}
