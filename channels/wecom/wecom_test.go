package wecom

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
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

func collectOnboard(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestMetadataAndToInbound(t *testing.T) {
	ch := New(Options{})
	if ch.Platform() != imsdk.PlatformWeCom {
		t.Fatalf("Platform() = %q, want wecom", ch.Platform())
	}
	if ch.Trust() != imsdk.TrustOrg {
		t.Fatalf("Trust() = %q, want org", ch.Trust())
	}
	if ch.Onboarding().Kind != imsdk.OnboardingQR {
		t.Fatalf("Onboarding().Kind = %q, want qr", ch.Onboarding().Kind)
	}
	if len(ch.ManualFields()) != 2 || ch.ManualFields()[0].Key != "botId" || !ch.ManualFields()[1].Secret {
		t.Fatalf("unexpected ManualFields: %+v", ch.ManualFields())
	}

	dm, ok := ToInbound(Frame{
		Headers: Headers{ReqID: "req-1"},
		Body: &Message{
			MsgID:    "msg1",
			ChatType: "single",
			From:     &UserRef{UserID: "zhangsan"},
			MsgType:  "text",
			Text:     &TextContent{Content: "周六天气？"},
		},
	})
	if !ok || dm.Chat.Kind != imsdk.ChatDM || dm.Chat.PlatformID != "zhangsan" || dm.Text != "周六天气？" || !dm.AddressedToBot {
		t.Fatalf("unexpected DM inbound: %+v", dm)
	}

	grp, ok := ToInbound(Frame{
		Headers: Headers{ReqID: "req-2"},
		Body: &Message{
			MsgID:    "msg2",
			ChatType: "group",
			ChatID:   "wr123",
			From:     &UserRef{UserID: "zhangsan"},
			MsgType:  "mixed",
			Mixed: &MixedContent{
				MsgItem: []MixedItem{
					{MsgType: "text", Text: &TextContent{Content: "@Alex 看图"}},
					{MsgType: "image", Image: &MediaRef{URL: "https://work.weixin.qq.com/img/1"}},
				},
			},
		},
	})
	if !ok || grp.Chat.Kind != imsdk.ChatGroup || grp.Chat.PlatformID != "wr123" || grp.Text != "看图" {
		t.Fatalf("unexpected group inbound: %+v", grp)
	}
	if len(grp.Attachments) != 1 || grp.Attachments[0].ID != "msg2:0" || grp.Attachments[0].Name != "image-msg2-0.jpg" {
		t.Fatalf("unexpected mixed attachments: %+v", grp.Attachments)
	}
}

func TestQROnboardingAndManualVerification(t *testing.T) {
	var polls atomic.Int32
	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/generate") {
			if req.URL.Query().Get("source") != "mingle" || req.URL.Query().Get("plat") == "" {
				t.Errorf("unexpected generate query: %s", req.URL.RawQuery)
			}
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"data": map[string]any{
					"scode":    "S1",
					"auth_url": "https://work.weixin.qq.com/ai/qc/auth?scode=S1",
				},
			}), nil
		}
		if n := polls.Add(1); n < 2 {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"data": map[string]any{"status": "waiting"},
			}), nil
		}
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"data": map[string]any{
				"status":   "success",
				"bot_info": map[string]any{"botid": "aib1", "secret": "sec1"},
			},
		}), nil
	})

	ch := New(Options{HTTPClient: hc, PollInterval: 5 * time.Millisecond})
	evs := collectOnboard(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{AgentName: "Alex"}))
	if len(evs) != 2 || evs[0].Kind != imsdk.EventQRReady || evs[1].Kind != imsdk.EventSucceeded {
		t.Fatalf("unexpected QR events: %+v", evs)
	}
	if evs[1].Credentials["botId"] != "aib1" || evs[1].Credentials["secret"] != "sec1" || evs[1].Bot.Name != "Alex" {
		t.Fatalf("unexpected QR credentials: %+v", evs[1])
	}

	// Untrusted auth_url outside work.weixin.qq.com must fail.
	hcUntrusted := imtest.Client(func(*http.Request) (*http.Response, error) {
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"data": map[string]any{
				"scode":    "S1",
				"auth_url": "https://evil.example/qc",
			},
		}), nil
	})
	badEvs := collectOnboard(New(Options{HTTPClient: hcUntrusted}).Onboard(context.Background(), nil, imsdk.OnboardingOptions{}))
	if len(badEvs) != 1 || badEvs[0].Kind != imsdk.EventFailed || badEvs[0].Code != imsdk.FailureNetwork {
		t.Fatalf("expected FailureNetwork for untrusted auth_url, got %+v", badEvs)
	}

	// Manual botId + secret verification over real loopback WebSocket.
	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var frame struct {
			Cmd     string            `json:"cmd"`
			Headers Headers           `json:"headers"`
			Body    map[string]string `json:"body"`
		}
		_ = json.Unmarshal(data, &frame)
		errCode := 0
		errMsg := "ok"
		if frame.Cmd != "aibot_subscribe" || frame.Body["secret"] != "good-secret" {
			errCode = 40001
			errMsg = "invalid secret"
		}
		ack, _ := json.Marshal(map[string]any{
			"headers": frame.Headers,
			"errcode": errCode,
			"errmsg":  errMsg,
		})
		_ = conn.Write(context.Background(), websocket.MessageText, ack)
		<-conn.CloseRead(context.Background()).Done()
	})

	wsCh := New(Options{WSURL: wsURL, ConnectTimeout: 500 * time.Millisecond})
	okManual := collectOnboard(wsCh.Onboard(context.Background(), imsdk.Credentials{
		"botId":  "aib1",
		"secret": "good-secret",
	}, imsdk.OnboardingOptions{AgentName: "Helper"}))
	if len(okManual) != 1 || okManual[0].Kind != imsdk.EventSucceeded {
		t.Fatalf("expected manual success, got %+v", okManual)
	}

	badManual := collectOnboard(wsCh.Onboard(context.Background(), imsdk.Credentials{
		"botId":  "aib1",
		"secret": "wrong",
	}, imsdk.OnboardingOptions{}))
	if len(badManual) != 1 || badManual[0].Kind != imsdk.EventFailed || badManual[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("expected invalid_credentials, got %+v", badManual)
	}
}

func TestWebSocketStreamingFallbackAndFrameTTL(t *testing.T) {
	var mu sync.Mutex
	var receivedCmds []map[string]any
	var pingSeen atomic.Bool
	var nextReplyErrCode atomic.Int32
	var dropReplyAck atomic.Bool
	serverPushCh := make(chan []byte, 8)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		conn.SetReadLimit(16 << 20)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case payload := <-serverPushCh:
					_ = conn.Write(ctx, websocket.MessageText, payload)
				}
			}
		}()

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			cmd, _ := msg["cmd"].(string)
			hdr, _ := msg["headers"].(map[string]any)
			reqID, _ := hdr["req_id"].(string)

			switch cmd {
			case "aibot_subscribe":
				ack, _ := json.Marshal(map[string]any{
					"headers": map[string]string{"req_id": reqID},
					"errcode": 0,
					"errmsg":  "ok",
				})
				_ = conn.Write(ctx, websocket.MessageText, ack)
			case "ping":
				pingSeen.Store(true)
				ack, _ := json.Marshal(map[string]any{
					"headers": map[string]string{"req_id": reqID},
					"errcode": 0,
					"errmsg":  "ok",
				})
				_ = conn.Write(ctx, websocket.MessageText, ack)
			case "aibot_respond_msg", "aibot_send_msg":
				mu.Lock()
				receivedCmds = append(receivedCmds, msg)
				mu.Unlock()
				if cmd == "aibot_respond_msg" && dropReplyAck.Load() {
					continue
				}
				code := 0
				if cmd == "aibot_respond_msg" {
					code = int(nextReplyErrCode.Swap(0))
				}
				ack, _ := json.Marshal(map[string]any{
					"headers": map[string]string{"req_id": reqID},
					"errcode": code,
					"errmsg":  "ack",
				})
				_ = conn.Write(ctx, websocket.MessageText, ack)
			}
		}
	})

	var clockMu sync.Mutex
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	var stateMu sync.Mutex
	var states []imsdk.ConnectionState
	var inbound []imsdk.InboundMessage

	ch := New(Options{
		WSURL:             wsURL,
		HeartbeatInterval: 25 * time.Millisecond,
		ReplyAckTimeout:   60 * time.Millisecond,
		Now:               nowFn,
	})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{
		"botId":  "aib1",
		"secret": "sec",
		"name":   "Alex",
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			stateMu.Lock()
			inbound = append(inbound, m)
			stateMu.Unlock()
		},
		OnState: func(s imsdk.ConnectionState) {
			stateMu.Lock()
			states = append(states, s)
			stateMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	waitUntil(t, pingSeen.Load)

	// Push inbound message 1 (with embedded control char 0x07 stripped by WSClient).
	cb1 := []byte("{\"cmd\":\"aibot_msg_callback\",\"headers\":{\"req_id\":\"req-1\"},\"body\":{\"msgid\":\"msg1\",\"chattype\":\"single\",\"from\":{\"userid\":\"zhangsan\"},\"msgtype\":\"text\",\"text\":{\"content\":\"hi\x07\"}}}")
	serverPushCh <- cb1

	waitUntil(t, func() bool {
		stateMu.Lock()
		defer stateMu.Unlock()
		return len(inbound) == 1
	})

	chat := imsdk.ChatRef{PlatformID: "zhangsan", Kind: imsdk.ChatDM}
	lr, err := conn.StartLive(context.Background(), chat, imsdk.LiveOptions{
		ReplyTo: "msg1",
		Initial: "正在思考…",
	})
	if err != nil {
		t.Fatalf("StartLive: %v", err)
	}
	if gotUntil, wantUntil := lr.Until(), nowFn().Add(StreamWindow); !gotUntil.Equal(wantUntil) {
		t.Fatalf("Until() = %v, want %v", gotUntil, wantUntil)
	}
	if err := lr.Update(context.Background(), "正在处理…"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	finRes, err := lr.Finish(context.Background(), "最终答案", nil)
	if err != nil || finRes.Outcome != imsdk.OutcomeSent || !strings.HasPrefix(finRes.PlatformID, "stream-") {
		t.Fatalf("Finish result = %+v err=%v", finRes, err)
	}

	mu.Lock()
	if len(receivedCmds) != 3 {
		t.Fatalf("expected 3 stream frames, got %d", len(receivedCmds))
	}
	stream0 := receivedCmds[0]["body"].(map[string]any)["stream"].(map[string]any)
	stream2 := receivedCmds[2]["body"].(map[string]any)["stream"].(map[string]any)
	if stream0["id"] != stream2["id"] || stream0["finish"] != false || stream2["finish"] != true || stream2["content"] != "最终答案" {
		t.Fatalf("unexpected stream frames: %+v / %+v", stream0, stream2)
	}
	receivedCmds = nil
	mu.Unlock()

	// Refused passive frame (846608) falls back once to active sendMessage with DegradedQuoteDropped,
	// and evicts msg1 so a second reply goes directly to sendMessage.
	nextReplyErrCode.Store(ErrCodeStreamExpired)
	fallbackRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    chat,
		Text:    "补充回答",
		ReplyTo: "msg1",
	})
	if err != nil {
		t.Fatalf("Send 846608 fallback: %v", err)
	}
	if fallbackRes.Outcome != imsdk.OutcomeSent || !slices.Equal(fallbackRes.Degraded, []imsdk.Degradation{imsdk.DegradedQuoteDropped}) {
		t.Fatalf("unexpected fallback result: %+v", fallbackRes)
	}
	_, _ = conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    chat,
		Text:    "再次补充",
		ReplyTo: "msg1",
	})

	mu.Lock()
	var cmdOrder []string
	for _, m := range receivedCmds {
		cmdOrder = append(cmdOrder, m["cmd"].(string))
	}
	receivedCmds = nil
	mu.Unlock()
	if !slices.Equal(cmdOrder, []string{"aibot_respond_msg", "aibot_send_msg", "aibot_send_msg"}) {
		t.Fatalf("unexpected command sequence after 846608 eviction: %v", cmdOrder)
	}

	// Ambiguous timeout on passive reply must NOT fall back to active sendMessage.
	cb2, _ := json.Marshal(map[string]any{
		"cmd":     "aibot_msg_callback",
		"headers": map[string]string{"req_id": "req-2"},
		"body": map[string]any{
			"msgid":    "msg2",
			"chattype": "single",
			"from":     map[string]string{"userid": "zhangsan"},
			"msgtype":  "text",
			"text":     map[string]string{"content": "hello"},
		},
	})
	serverPushCh <- cb2
	waitUntil(t, func() bool {
		stateMu.Lock()
		defer stateMu.Unlock()
		return len(inbound) == 2
	})

	dropReplyAck.Store(true)
	ambRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    chat,
		Text:    "no duplicate",
		ReplyTo: "msg2",
	})
	dropReplyAck.Store(false)
	if err != nil || ambRes.Outcome != imsdk.OutcomeFailed || ambRes.Code != imsdk.SendErrNetwork {
		t.Fatalf("expected failed/network without active fallback on timeout, got %+v err=%v", ambRes, err)
	}

	// Advance past 30m FrameTTL: msg2 expires, Send goes directly to aibot_send_msg with DegradedQuoteDropped,
	// and StartLive refuses the expired frame.
	mu.Lock()
	receivedCmds = nil
	mu.Unlock()
	advance(31 * time.Minute)

	ttlRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    chat,
		Text:    "after 31m",
		ReplyTo: "msg2",
	})
	if err != nil || ttlRes.Outcome != imsdk.OutcomeSent || !slices.Equal(ttlRes.Degraded, []imsdk.Degradation{imsdk.DegradedQuoteDropped}) {
		t.Fatalf("unexpected post-TTL SendResult: %+v err=%v", ttlRes, err)
	}
	if _, err := conn.StartLive(context.Background(), chat, imsdk.LiveOptions{ReplyTo: "msg2", Initial: "…"}); err == nil {
		t.Fatal("expected StartLive on expired frame to fail")
	}
}

func TestChunkedMediaUploadWithTextSentAndAESDownload(t *testing.T) {
	// Generate 43-char unpadded Base64 AES-256 key (exact WeCom format) and encrypt a 5-byte file (padLen = 27 > 16).
	rawKey := make([]byte, 32)
	_, _ = rand.Read(rawKey)
	aesKey43 := strings.TrimRight(base64.StdEncoding.EncodeToString(rawKey), "=")
	encryptedMedia, err := EncryptFile([]byte("hello"), aesKey43)
	if err != nil {
		t.Fatalf("EncryptFile: %v", err)
	}

	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		return imtest.BytesResponse(http.StatusOK, encryptedMedia, "Content-Disposition", `attachment; filename="secret.txt"`), nil
	})

	var mu sync.Mutex
	chunks := make(map[int][]byte)
	var failUploadInit atomic.Bool
	var sentMediaTypes []string
	serverPushCh := make(chan []byte, 4)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		conn.SetReadLimit(16 << 20)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p := <-serverPushCh:
					_ = conn.Write(ctx, websocket.MessageText, p)
				}
			}
		}()

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg map[string]any
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			cmd, _ := msg["cmd"].(string)
			hdr, _ := msg["headers"].(map[string]any)
			reqID, _ := hdr["req_id"].(string)
			body, _ := msg["body"].(map[string]any)

			reply := func(code int, ackBody map[string]any) {
				env := map[string]any{
					"headers": map[string]string{"req_id": reqID},
					"errcode": code,
					"errmsg":  "ok",
				}
				if ackBody != nil {
					env["body"] = ackBody
				}
				raw, _ := json.Marshal(env)
				_ = conn.Write(ctx, websocket.MessageText, raw)
			}

			switch cmd {
			case "aibot_subscribe", "ping":
				reply(0, nil)
			case "aibot_upload_media_init":
				if failUploadInit.Load() {
					reply(90001, nil)
					continue
				}
				reply(0, map[string]any{"upload_id": "up-1"})
			case "aibot_upload_media_chunk":
				idx := int(body["chunk_index"].(float64))
				rawChunk, _ := base64.StdEncoding.DecodeString(body["base64_data"].(string))
				mu.Lock()
				chunks[idx] = rawChunk
				mu.Unlock()
				reply(0, nil)
			case "aibot_upload_media_finish":
				reply(0, map[string]any{"media_id": "media-123", "type": "file"})
			case "aibot_send_msg":
				if mt, _ := body["msgtype"].(string); mt != "markdown" {
					mu.Lock()
					sentMediaTypes = append(sentMediaTypes, mt)
					mu.Unlock()
				}
				reply(0, nil)
			}
		}
	})

	var msgMu sync.Mutex
	var got []imsdk.InboundMessage
	ch := New(Options{WSURL: wsURL, HTTPClient: hc})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{"botId": "b1", "secret": "s1"}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			msgMu.Lock()
			got = append(got, m)
			msgMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	// 1. Test AES-256-CBC inbound file download decryption.
	cb, _ := json.Marshal(map[string]any{
		"cmd":     "aibot_msg_callback",
		"headers": map[string]string{"req_id": "r-file"},
		"body": map[string]any{
			"msgid":    "m-file",
			"chattype": "single",
			"from":     map[string]string{"userid": "lisi"},
			"msgtype":  "file",
			"file": map[string]string{
				"url":      "https://work.weixin.qq.com/cdn/f1",
				"aeskey":   aesKey43,
				"filename": "secret.txt",
			},
		},
	})
	serverPushCh <- cb
	waitUntil(t, func() bool {
		msgMu.Lock()
		defer msgMu.Unlock()
		return len(got) == 1
	})

	dir := t.TempDir()
	dlPath := filepath.Join(dir, "decrypted.txt")
	if err := conn.Download(context.Background(), got[0].Attachments[0], dlPath); err != nil {
		t.Fatalf("Download: %v", err)
	}
	dlBytes, err := os.ReadFile(dlPath)
	if err != nil || string(dlBytes) != "hello" {
		t.Fatalf("decrypted = %q err=%v", string(dlBytes), err)
	}

	// 2. Test multi-chunk (>512 KiB) media upload + sendMediaMessage.
	bigPayload := bytes.Repeat([]byte("ABCD"), (mediaChunkSize+1024)/4)
	bigPath := filepath.Join(dir, "archive.bin")
	_ = os.WriteFile(bigPath, bigPayload, 0o644)

	chat := imsdk.ChatRef{PlatformID: "lisi", Kind: imsdk.ChatDM}
	upRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  chat,
		Text:  "see attached",
		Files: []imsdk.OutboundFile{{Path: bigPath}},
	})
	if err != nil || upRes.Outcome != imsdk.OutcomeSent {
		t.Fatalf("multi-chunk Send failed: %+v err=%v", upRes, err)
	}

	mu.Lock()
	reassembled := append(append([]byte(nil), chunks[0]...), chunks[1]...)
	gotTypes := append([]string(nil), sentMediaTypes...)
	mu.Unlock()
	if !bytes.Equal(reassembled, bigPayload) {
		t.Fatalf("reassembled chunks len=%d, want %d", len(reassembled), len(bigPayload))
	}
	if !slices.Equal(gotTypes, []string{"file"}) {
		t.Fatalf("sentMediaTypes = %v, want [file]", gotTypes)
	}

	// 3. Test WithTextSent when text succeeds before file upload fails.
	failUploadInit.Store(true)
	partialRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  chat,
		Text:  "text already delivered",
		Files: []imsdk.OutboundFile{{Path: bigPath}},
	})
	if err != nil {
		t.Fatalf("Send partial: %v", err)
	}
	if partialRes.Outcome != imsdk.OutcomeFailed || partialRes.TextSentPlatformID == "" {
		t.Fatalf("expected OutcomeFailed with non-empty TextSentPlatformID, got %+v", partialRes)
	}
}

func TestDisconnectedEventAnd846605Fallback(t *testing.T) {
	var connections atomic.Int32
	pushCh := make(chan []byte, 4)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		connections.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p := <-pushCh:
					_ = conn.Write(ctx, websocket.MessageText, p)
				}
			}
		}()

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg map[string]any
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			cmd, _ := msg["cmd"].(string)
			hdr, _ := msg["headers"].(map[string]any)
			reqID, _ := hdr["req_id"].(string)
			code := 0
			if cmd == "aibot_respond_msg" {
				code = ErrCodeInvalidReqID
			}
			ack, _ := json.Marshal(map[string]any{
				"headers": map[string]string{"req_id": reqID},
				"errcode": code,
				"errmsg":  "ack",
			})
			_ = conn.Write(ctx, websocket.MessageText, ack)
		}
	})

	var reconnected atomic.Bool
	var msgReady atomic.Bool
	ch := New(Options{
		WSURL:          wsURL,
		ReconnectDelay: 20 * time.Millisecond,
	})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{"botId": "b", "secret": "s"}, imsdk.ChannelEvents{
		OnMessage: func(imsdk.InboundMessage) { msgReady.Store(true) },
		OnState: func(s imsdk.ConnectionState) {
			if s.Kind == imsdk.StateReconnecting {
				reconnected.Store(true)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cb, _ := json.Marshal(map[string]any{
		"cmd":     "aibot_msg_callback",
		"headers": map[string]string{"req_id": "r-846605"},
		"body": map[string]any{
			"msgid":    "m-846605",
			"chattype": "single",
			"from":     map[string]string{"userid": "wangwu"},
			"msgtype":  "text",
			"text":     map[string]string{"content": "ping"},
		},
	})
	pushCh <- cb
	waitUntil(t, msgReady.Load)

	res, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    imsdk.ChatRef{PlatformID: "wangwu", Kind: imsdk.ChatDM},
		Text:    "pong",
		ReplyTo: "m-846605",
	})
	if err != nil || res.Outcome != imsdk.OutcomeSent || !slices.Equal(res.Degraded, []imsdk.Degradation{imsdk.DegradedQuoteDropped}) {
		t.Fatalf("expected 846605 fallback with DegradedQuoteDropped, got %+v err=%v", res, err)
	}

	// Server sends disconnected_event: client must close without auto-reconnecting.
	kick, _ := json.Marshal(map[string]any{
		"cmd":     "aibot_event_callback",
		"headers": map[string]string{"req_id": "ev-1"},
		"body": map[string]any{
			"msgid":   "ev-1",
			"msgtype": "event",
			"event":   map[string]string{"eventtype": "disconnected_event"},
		},
	})
	pushCh <- kick
	time.Sleep(60 * time.Millisecond)
	if reconnected.Load() || connections.Load() != 1 {
		t.Fatalf("expected no reconnect after disconnected_event: reconnected=%v connections=%d", reconnected.Load(), connections.Load())
	}
}
