package dingtalk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
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

func collectOnboarding(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func TestDingTalkOnboardingAndQRValidation(t *testing.T) {
	var (
		polls   atomic.Int32
		initSrc string
		badHost atomic.Bool
	)

	client := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
		}
		switch req.URL.Path {
		case "/app/registration/init":
			var b struct {
				Source string `json:"source"`
			}
			_ = json.Unmarshal(raw, &b)
			initSrc = b.Source
			return imtest.JSONResponse(200, map[string]any{"errcode": 0, "nonce": "nonce_1"}), nil

		case "/app/registration/begin":
			uri := "https://open-dev.dingtalk.com/openapp/registration/openClaw?user_code=AB"
			if badHost.Load() {
				uri = "https://evil.example.com/phish"
			}
			return imtest.JSONResponse(200, map[string]any{
				"errcode":                   0,
				"device_code":               "dev_1",
				"verification_uri_complete": uri,
				"expires_in":                600,
				"interval":                  0.01,
			}), nil

		case "/app/registration/poll":
			if polls.Add(1) < 2 {
				return imtest.JSONResponse(200, map[string]any{"errcode": 0, "status": "WAITING"}), nil
			}
			return imtest.JSONResponse(200, map[string]any{
				"errcode":       0,
				"status":        "SUCCESS",
				"client_id":     "ding_app_1",
				"client_secret": "ding_sec_1",
			}), nil

		case "/v1.0/oauth2/accessToken":
			var b struct {
				AppSecret string `json:"appSecret"`
			}
			_ = json.Unmarshal(raw, &b)
			if b.AppSecret == "wrong" {
				return imtest.JSONResponse(400, map[string]any{"code": "InvalidAuthentication"}), nil
			}
			return imtest.JSONResponse(200, map[string]any{"accessToken": "tok_1", "expireIn": 7200}), nil
		}
		return imtest.JSONResponse(404, map[string]any{}), nil
	})

	ch := New(Options{HTTPClient: client})
	evs := collectOnboarding(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{AgentName: "Alex"}))
	if initSrc != "DING_DWS_CLAW" {
		t.Fatalf("expected source DING_DWS_CLAW, got %q", initSrc)
	}
	if len(evs) != 2 || evs[0].Kind != imsdk.EventQRReady || evs[1].Kind != imsdk.EventSucceeded {
		t.Fatalf("unexpected QR onboarding events: %+v", evs)
	}
	if evs[1].Credentials["clientId"] != "ding_app_1" || evs[1].Bot.Name != "Alex" {
		t.Fatalf("unexpected credentials/bot: %+v", evs[1])
	}

	// Reject non-dingtalk.com QR URLs.
	badHost.Store(true)
	badEvs := collectOnboarding(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{}))
	if len(badEvs) != 1 || badEvs[0].Kind != imsdk.EventFailed {
		t.Fatalf("expected QR outside dingtalk.com to fail, got %+v", badEvs)
	}

	// Manual credentials validation.
	manualBad := collectOnboarding(ch.Onboard(context.Background(), imsdk.Credentials{"clientId": "ding_app_1", "clientSecret": "wrong"}, imsdk.OnboardingOptions{}))
	if len(manualBad) != 1 || manualBad[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("expected invalid_credentials, got %+v", manualBad)
	}
}

func TestDingTalkStreamWSAndAICardAndSend(t *testing.T) {
	var (
		wsConnects atomic.Int32
		wsAckCh    = make(chan map[string]any, 16)
		wsPushCh   = make(chan map[string]any, 16)
	)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		connNum := wsConnects.Add(1)
		if r.URL.Query().Get("ticket") == "" {
			t.Errorf("missing ticket query param on WS connect")
		}
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				case pkt := <-wsPushCh:
					raw, _ := json.Marshal(pkt)
					_ = conn.Write(r.Context(), websocket.MessageText, raw)
				}
			}
		}()

		// On first connection, test SYSTEM ping -> pong, CALLBACK -> ack, then SYSTEM disconnect -> reconnect!
		if connNum == 1 {
			pingRaw, _ := json.Marshal(map[string]any{
				"specVersion": "1.0",
				"type":        "SYSTEM",
				"headers":     map[string]string{"topic": "ping", "messageId": "sys_ping_1"},
				"data":        `{"opaque":"123"}`,
			})
			_ = conn.Write(r.Context(), websocket.MessageText, pingRaw)
		}

		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var ack map[string]any
			if json.Unmarshal(raw, &ack) == nil {
				wsAckCh <- ack
			}
		}
	})

	type recordedCall struct {
		Method string
		Path   string
		Body   map[string]any
	}
	var (
		mu         sync.Mutex
		tokenCalls int
		mediaCount int
		calls      []recordedCall
	)

	httpClient := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
		}
		p := req.URL.Path
		switch p {
		case "/v1.0/oauth2/accessToken":
			mu.Lock()
			tokenCalls++
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"accessToken": "dt_tok_1", "expireIn": 7200}), nil

		case "/v1.0/gateway/connections/open":
			return imtest.JSONResponse(200, map[string]any{
				"endpoint": wsURL,
				"ticket":   "tkt_1",
			}), nil

		case "/media/upload":
			mu.Lock()
			mediaCount++
			id := "@media_" + string(rune('0'+mediaCount))
			calls = append(calls, recordedCall{
				Method: req.Method,
				Path:   p,
				Body: map[string]any{
					"type":         req.URL.Query().Get("type"),
					"access_token": req.URL.Query().Get("access_token"),
				},
			})
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"errcode": 0, "media_id": id}), nil

		case "/v1.0/robot/messageFiles/download":
			return imtest.JSONResponse(200, map[string]any{"downloadUrl": "https://cdn.dingtalk.com/files/f1.bin"}), nil

		case "/files/f1.bin":
			return imtest.BytesResponse(200, []byte("dingtalk-dl-content")), nil

		default:
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			mu.Lock()
			calls = append(calls, recordedCall{Method: req.Method, Path: p, Body: body})
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"errcode": 0}), nil
		}
	})

	ch := New(Options{
		HTTPClient:        httpClient,
		ReconnectInterval: 15 * time.Millisecond,
	})

	var (
		evMu     sync.Mutex
		states   []imsdk.ConnectionStateKind
		received []imsdk.InboundMessage
	)
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{
		"clientId":     "ding_1",
		"clientSecret": "sec_1",
		"name":         "Alex",
	}, imsdk.ChannelEvents{
		OnState: func(s imsdk.ConnectionState) {
			evMu.Lock()
			states = append(states, s.Kind)
			evMu.Unlock()
		},
		OnMessage: func(m imsdk.InboundMessage) {
			evMu.Lock()
			received = append(received, m)
			evMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	// 1. Expect SYSTEM ping ack.
	select {
	case ack := <-wsAckCh:
		hdrs, _ := ack["headers"].(map[string]any)
		if hdrs["messageId"] != "sys_ping_1" {
			t.Fatalf("expected ping ack for sys_ping_1, got %+v", ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SYSTEM ping ack")
	}

	// 2. Push CALLBACK robot message and verify callback ack + inbound normalization.
	cbPayload, _ := json.Marshal(map[string]any{
		"msgId":             "msg_1",
		"conversationId":    "cid_group_1",
		"conversationType":  "2",
		"conversationTitle": "研发群",
		"senderStaffId":     "staff_1",
		"senderNick":        "张三",
		"isInAtList":        true,
		"createAt":          1700000000000,
		"msgtype":           "text",
		"text":              map[string]string{"content": " 你好机器人 "},
	})
	wsPushCh <- map[string]any{
		"specVersion": "1.0",
		"type":        "CALLBACK",
		"headers":     map[string]string{"topic": TopicRobot, "messageId": "cb_msg_1"},
		"data":        string(cbPayload),
	}

	select {
	case ack := <-wsAckCh:
		hdrs, _ := ack["headers"].(map[string]any)
		if hdrs["messageId"] != "cb_msg_1" || ack["data"] != `{"response":{"success":true}}` {
			t.Fatalf("unexpected callback ack: %+v", ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for CALLBACK ack")
	}

	for i := 0; i < 100; i++ {
		evMu.Lock()
		n := len(received)
		evMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	evMu.Lock()
	if len(received) != 1 || received[0].Text != "你好机器人" || received[0].Chat.Name != "研发群" || !received[0].AddressedToBot {
		evMu.Unlock()
		t.Fatalf("unexpected inbound message: %+v", received)
	}
	groupChat := received[0].Chat
	evMu.Unlock()

	// 3. Push SYSTEM disconnect and verify auto-reconnect!
	wsPushCh <- map[string]any{
		"specVersion": "1.0",
		"type":        "SYSTEM",
		"headers":     map[string]string{"topic": "disconnect", "messageId": "sys_disc"},
	}
	for i := 0; i < 100 && wsConnects.Load() < 2; i++ {
		time.Sleep(15 * time.Millisecond)
	}
	if wsConnects.Load() < 2 {
		t.Fatalf("expected SYSTEM disconnect to trigger reconnect, connects=%d", wsConnects.Load())
	}
	evMu.Lock()
	if !slices.Contains(states, imsdk.StateReconnecting) {
		evMu.Unlock()
		t.Fatalf("expected StateReconnecting in states: %v", states)
	}
	evMu.Unlock()

	// 4. React processing (🤔思考中 reply) -> done (recall).
	_ = conn.React(context.Background(), groupChat, "msg_1", imsdk.ReactionProcessing)
	_ = conn.React(context.Background(), groupChat, "msg_1", imsdk.ReactionDone)

	// 5. Shared AI card StartLive: createAndDeliver -> streaming -> instances.
	live, err := conn.StartLive(context.Background(), groupChat, imsdk.LiveOptions{
		ReplyTo: "msg_1",
		Initial: "**思考中**",
	})
	if err != nil {
		t.Fatalf("StartLive: %v", err)
	}
	_ = live.Update(context.Background(), "**处理中** · 2s")
	liveRes, err := live.Finish(context.Background(), "完成答案", nil)
	if err != nil || liveRes.Outcome != imsdk.OutcomeSent || !strings.HasPrefix(liveRes.PlatformID, "mingle_") {
		t.Fatalf("live.Finish: %+v, err=%v", liveRes, err)
	}

	// 6. Send DM markdown + Group image/file + empty file dropped + Download.
	tmpDir := t.TempDir()
	imgPath := filepath.Join(tmpDir, "pic.png")
	docPath := filepath.Join(tmpDir, "spec.pdf")
	emptyPath := filepath.Join(tmpDir, "empty.txt")
	_ = os.WriteFile(imgPath, []byte("png-bytes"), 0o600)
	_ = os.WriteFile(docPath, []byte("pdf-bytes"), 0o600)
	_ = os.WriteFile(emptyPath, nil, 0o600)

	dmRes, _ := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    imsdk.ChatRef{PlatformID: "staff_1", Kind: imsdk.ChatDM},
		Text:    "私聊回复",
		ReplyTo: "msg_1",
	})
	if dmRes.Outcome != imsdk.OutcomeSent || !slices.Equal(dmRes.Degraded, []imsdk.Degradation{imsdk.DegradedQuoteDropped}) {
		t.Fatalf("unexpected DM send result: %+v", dmRes)
	}

	fileRes, _ := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  groupChat,
		Files: []imsdk.OutboundFile{{Path: imgPath}, {Path: docPath}, {Path: emptyPath}},
	})
	if fileRes.Outcome != imsdk.OutcomeSent || !slices.Equal(fileRes.Degraded, []imsdk.Degradation{imsdk.DegradedFilesDropped}) {
		t.Fatalf("unexpected group file send result: %+v", fileRes)
	}

	dlTarget := filepath.Join(tmpDir, "out.bin")
	if err := conn.Download(context.Background(), imsdk.InboundAttachment{ID: "dl_code_1"}, dlTarget); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if b, _ := os.ReadFile(dlTarget); string(b) != "dingtalk-dl-content" {
		t.Fatalf("unexpected downloaded content: %q", string(b))
	}

	// Verify OAuth token was cached (only 1 call) and exact API routes/payloads were hit.
	mu.Lock()
	defer mu.Unlock()
	if tokenCalls != 1 {
		t.Fatalf("expected OAuth token to be cached (1 call), got %d", tokenCalls)
	}
	hasPath := func(p string) *recordedCall {
		for i := range calls {
			if calls[i].Path == p {
				return &calls[i]
			}
		}
		return nil
	}
	if c := hasPath("/v1.0/robot/emotion/reply"); c == nil || c.Body["emotionName"] != "🤔思考中" {
		t.Fatalf("missing or wrong emotion/reply call: %+v", c)
	}
	if c := hasPath("/v1.0/robot/emotion/recall"); c == nil {
		t.Fatal("missing emotion/recall call")
	}
	if c := hasPath("/v1.0/card/instances/createAndDeliver"); c == nil || c.Body["cardTemplateId"] != AICardTemplate || c.Body["openSpaceId"] != "dtv1.card//IM_GROUP.cid_group_1" {
		t.Fatalf("unexpected createAndDeliver call: %+v", c)
	}
	var streamingContents []string
	for _, c := range calls {
		if c.Path == "/v1.0/card/streaming" {
			s, _ := c.Body["content"].(string)
			streamingContents = append(streamingContents, s)
		}
	}
	if !slices.Equal(streamingContents, []string{"**处理中** · 2s", `<a atId="staff_1">@张三</a> 完成答案`}) {
		t.Fatalf("unexpected card streaming contents: %v", streamingContents)
	}
	if c := hasPath("/v1.0/card/instances"); c == nil || c.Method != http.MethodPut {
		t.Fatalf("missing PUT /v1.0/card/instances: %+v", c)
	}
}

func TestDingTalkToInboundRichTypesAndInjectedStream(t *testing.T) {
	now := time.UnixMilli(1700000000000)
	// Audio recognition + richText images.
	inb, ok := ToInbound(DingTalkMessage{
		MsgID:            "m_rich",
		ConversationID:   "cid_1",
		ConversationType: "1",
		SenderStaffID:    "staff_9",
		SenderNick:       "王五",
		MsgType:          "richText",
		Content: &struct {
			DownloadCode string `json:"downloadCode"`
			FileName     string `json:"fileName"`
			Recognition  string `json:"recognition"`
			RichText     []struct {
				Text         string `json:"text"`
				DownloadCode string `json:"downloadCode"`
				Type         string `json:"type"`
			} `json:"richText"`
		}{
			RichText: []struct {
				Text         string `json:"text"`
				DownloadCode string `json:"downloadCode"`
				Type         string `json:"type"`
			}{
				{Text: "看这张图"},
				{DownloadCode: "dc_img_1"},
			},
		},
	}, now)
	if !ok || inb.Chat.Kind != imsdk.ChatDM || inb.Chat.PlatformID != "staff_9" || inb.Text != "看这张图" || len(inb.Attachments) != 1 || inb.Attachments[0].ID != "dc_img_1" {
		t.Fatalf("unexpected richText ToInbound: %+v", inb)
	}

	if _, err := validDingTalkQRURL("http://dingtalk.com/insecure"); err == nil {
		t.Fatal("expected http:// QR URL to be rejected")
	}
}
