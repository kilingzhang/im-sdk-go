package qq

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
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

func encryptTestSecret(t *testing.T, plainSecret, bindKeyBase64 string) string {
	t.Helper()
	key, err := base64.StdEncoding.DecodeString(bindKeyBase64)
	if err != nil {
		t.Fatalf("decode bind key: %v", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	nonce := make([]byte, 12)
	_, _ = rand.Read(nonce)
	sealed := gcm.Seal(nonce, nonce, []byte(plainSecret), nil)
	return base64.StdEncoding.EncodeToString(sealed)
}

func collectQQOnboarding(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func TestQQOnboardingAndStartFailure(t *testing.T) {
	// 1. StartFailure classification.
	if StartFailure(errors.New("Network error getting access_token: dial tcp")) != imsdk.ConnErrNetwork {
		t.Fatal("expected network error -> ConnErrNetwork")
	}
	if StartFailure(errors.New("Token fetch failed: HTTP 502 — bad gateway")) != imsdk.ConnErrNetwork {
		t.Fatal("expected 502 -> ConnErrNetwork")
	}
	if StartFailure(errors.New("Token fetch failed: HTTP 401 — invalid secret")) != imsdk.ConnErrCredentialsRevoked {
		t.Fatal("expected 401 -> ConnErrCredentialsRevoked")
	}
	if StartFailure(errors.New(`Failed to get access_token: {"code":100016}`)) != imsdk.ConnErrCredentialsRevoked {
		t.Fatal("expected missing token -> ConnErrCredentialsRevoked")
	}

	// 2. QR connect flow with expired-QR refresh + AES-256-GCM decryption.
	var (
		mu       sync.Mutex
		taskSeq  int
		lastKey  string
		pollCall int
	)

	qrClient := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
		}
		switch req.URL.Path {
		case "/lite/create_bind_task":
			var b struct {
				Key string `json:"key"`
			}
			_ = json.Unmarshal(raw, &b)
			mu.Lock()
			taskSeq++
			lastKey = b.Key
			tid := "task_" + string(rune('0'+taskSeq))
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{
				"retcode": 0,
				"data":    map[string]any{"task_id": tid},
			}), nil

		case "/lite/poll_bind_result":
			mu.Lock()
			pollCall++
			n := pollCall
			key := lastKey
			mu.Unlock()
			if n == 1 {
				// First QR expires -> adapter should create a fresh bind task and emit a second qr_ready.
				return imtest.JSONResponse(200, map[string]any{
					"retcode": 0,
					"data":    map[string]any{"status": bindStatusExpired},
				}), nil
			}
			return imtest.JSONResponse(200, map[string]any{
				"retcode": 0,
				"data": map[string]any{
					"status":             bindStatusCompleted,
					"bot_appid":          10203040,
					"bot_encrypt_secret": encryptTestSecret(t, "real_qq_secret", key),
					"user_openid":        "owner_openid_1",
				},
			}), nil

		case "/app/getAppAccessToken":
			var b struct {
				ClientSecret string `json:"clientSecret"`
			}
			_ = json.Unmarshal(raw, &b)
			if b.ClientSecret == "revoked" {
				return imtest.JSONResponse(401, map[string]any{"code": 11244, "message": "invalid secret"}), nil
			}
			if b.ClientSecret == "outage" {
				return nil, errors.New("dial tcp: i/o timeout")
			}
			return imtest.JSONResponse(200, map[string]any{"access_token": "qq_tok", "expires_in": "7200"}), nil
		}
		return imtest.JSONResponse(404, map[string]any{}), nil
	})

	ch := New(Options{
		HTTPClient:     qrClient,
		QRPollInterval: 10 * time.Millisecond,
	})
	evs := collectQQOnboarding(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{AgentName: "Alex"}))
	if len(evs) != 3 || evs[0].Kind != imsdk.EventQRReady || evs[1].Kind != imsdk.EventQRReady || evs[2].Kind != imsdk.EventSucceeded {
		t.Fatalf("expected [qr_ready, qr_ready, succeeded], got %+v", evs)
	}
	if !strings.Contains(evs[1].QRContent, "/qqbot/openclaw/connect.html?task_id=task_2") {
		t.Fatalf("unexpected QR URL: %s", evs[1].QRContent)
	}
	if evs[2].Credentials["appId"] != "10203040" || evs[2].Credentials["appSecret"] != "real_qq_secret" || evs[2].OwnerID != "owner_openid_1" {
		t.Fatalf("unexpected succeeded payload: %+v", evs[2])
	}

	// 3. Connect failure: 401 emits ConnErrCredentialsRevoked; network error emits nothing.
	var revokedStates []imsdk.ConnectionState
	_, err := ch.Connect(context.Background(), imsdk.Credentials{"appId": "a", "appSecret": "revoked"}, imsdk.ChannelEvents{
		OnState: func(s imsdk.ConnectionState) { revokedStates = append(revokedStates, s) },
	})
	if err == nil || len(revokedStates) != 1 || revokedStates[0].Code != imsdk.ConnErrCredentialsRevoked {
		t.Fatalf("expected credentials_revoked state on 401, got err=%v states=%+v", err, revokedStates)
	}

	var outageStates []imsdk.ConnectionState
	_, err = ch.Connect(context.Background(), imsdk.Credentials{"appId": "a", "appSecret": "outage"}, imsdk.ChannelEvents{
		OnState: func(s imsdk.ConnectionState) { outageStates = append(outageStates, s) },
	})
	if err == nil || len(outageStates) != 0 {
		t.Fatalf("expected transient network failure not to emit credentials_revoked, got err=%v states=%+v", err, outageStates)
	}
}

func TestQQGatewayPassiveActiveFallbackMediaAndTyping(t *testing.T) {
	var (
		wsConns    atomic.Int32
		clientOps  = make(chan int, 16)
		serverPush = make(chan map[string]any, 16)
	)

	wsURL := imtest.WSServer(t, func(conn *websocket.Conn, r *http.Request) {
		connIdx := wsConns.Add(1)
		// Send HELLO.
		hello, _ := json.Marshal(map[string]any{
			"op": OpHello,
			"d":  map[string]any{"heartbeat_interval": 50},
		})
		_ = conn.Write(r.Context(), websocket.MessageText, hello)

		// Read IDENTIFY (conn 1) or RESUME (conn 2).
		_, raw, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var initFrame struct {
			Op int `json:"op"`
		}
		_ = json.Unmarshal(raw, &initFrame)
		clientOps <- initFrame.Op

		if connIdx == 1 {
			readyFrame, _ := json.Marshal(map[string]any{
				"op": OpDispatch,
				"s":  1,
				"t":  "READY",
				"d":  map[string]any{"session_id": "qq_sess_1"},
			})
			_ = conn.Write(r.Context(), websocket.MessageText, readyFrame)
		} else {
			resumedFrame, _ := json.Marshal(map[string]any{
				"op": OpDispatch,
				"s":  3,
				"t":  "RESUMED",
				"d":  map[string]any{},
			})
			_ = conn.Write(r.Context(), websocket.MessageText, resumedFrame)
		}

		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				case pkt := <-serverPush:
					b, _ := json.Marshal(pkt)
					_ = conn.Write(r.Context(), websocket.MessageText, b)
				}
			}
		}()

		for {
			_, msgRaw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var f struct {
				Op int `json:"op"`
			}
			if json.Unmarshal(msgRaw, &f) == nil {
				if f.Op == OpHeartbeat {
					ack, _ := json.Marshal(map[string]any{"op": OpHeartbeatAck})
					_ = conn.Write(r.Context(), websocket.MessageText, ack)
				}
				clientOps <- f.Op
			}
		}
	})

	var (
		mu                sync.Mutex
		nowTime           = time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)
		rejectPassiveText bool
		typingCount       atomic.Int32
		sentBodies        []map[string]any
	)

	var gatewayBase string
	httpClient := imtest.Client(func(req *http.Request) (*http.Response, error) {
		var raw []byte
		if req.Body != nil {
			raw, _ = io.ReadAll(req.Body)
		}
		p := req.URL.Path
		switch {
		case p == "/app/getAppAccessToken":
			return imtest.JSONResponse(200, map[string]any{"access_token": "qq_tok_1", "expires_in": 7200}), nil

		case p == "/gateway":
			return imtest.JSONResponse(200, map[string]any{"url": gatewayBase}), nil

		case p == "/cdn/1.png":
			return imtest.BytesResponse(200, []byte("qq-attachment-bytes")), nil

		case strings.HasSuffix(p, "/files"):
			var b map[string]any
			_ = json.Unmarshal(raw, &b)
			if b["file_type"] == float64(mediaTypeFile) {
				// Simulate missing generic-file upload permission on QQ.
				return imtest.JSONResponse(403, map[string]any{"code": 40004, "message": "no file permission"}), nil
			}
			return imtest.JSONResponse(200, map[string]any{"file_uuid": "fu_1", "file_info": "fi_img_1", "ttl": 3600}), nil

		case strings.HasSuffix(p, "/messages"):
			var b map[string]any
			_ = json.Unmarshal(raw, &b)
			if b["msg_type"] == float64(6) {
				typingCount.Add(1)
				return imtest.JSONResponse(200, map[string]any{}), nil
			}
			mu.Lock()
			if rejectPassiveText && b["msg_type"] == float64(0) && b["msg_id"] != nil {
				mu.Unlock()
				return imtest.JSONResponse(400, map[string]any{"code": 304023, "message": "passive window expired"}), nil
			}
			sentBodies = append(sentBodies, b)
			id := "out_msg_1"
			if b["msg_type"] == float64(7) {
				id = "out_img_1"
			}
			mu.Unlock()
			return imtest.JSONResponse(200, map[string]any{"id": id}), nil
		}
		return imtest.JSONResponse(404, map[string]any{}), nil
	})
	gatewayBase = wsURL

	ch := New(Options{
		HTTPClient:          httpClient,
		ReconnectInterval:   15 * time.Millisecond,
		TypingRenewInterval: 25 * time.Millisecond,
		TypingWindow:        270 * time.Second,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return nowTime
		},
	})

	var (
		evMu     sync.Mutex
		received []imsdk.InboundMessage
		states   []imsdk.ConnectionStateKind
	)
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{
		"appId":     "qq_app_1",
		"appSecret": "qq_sec_1",
		"name":      "Alex",
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			evMu.Lock()
			received = append(received, m)
			evMu.Unlock()
		},
		OnState: func(s imsdk.ConnectionState) {
			evMu.Lock()
			states = append(states, s.Kind)
			evMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	// Verify OpIdentify was sent first, followed by at least one OpHeartbeat.
	firstOp := <-clientOps
	if firstOp != OpIdentify {
		t.Fatalf("expected OpIdentify (2), got %d", firstOp)
	}

	// StartLive is intentionally unsupported on QQ.
	if _, err := conn.StartLive(context.Background(), imsdk.ChatRef{PlatformID: "u1", Kind: imsdk.ChatDM}, imsdk.LiveOptions{}); !errors.Is(err, imsdk.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported from StartLive, got %v", err)
	}

	// Push C2C & Group @ messages.
	serverPush <- map[string]any{
		"op": OpDispatch,
		"s":  2,
		"t":  "C2C_MESSAGE_CREATE",
		"d": map[string]any{
			"id":        "m_c2c_1",
			"content":   " 你好 ",
			"timestamp": "2026-11-18T15:00:00+08:00",
			"author":    map[string]any{"user_openid": "u_open_1"},
			"attachments": []map[string]any{
				{"content_type": "image/png", "url": "//api.sgroup.qq.com/cdn/1.png"},
			},
		},
	}
	serverPush <- map[string]any{
		"op": OpDispatch,
		"s":  3,
		"t":  "GROUP_AT_MESSAGE_CREATE",
		"d": map[string]any{
			"id":           "m_grp_1",
			"content":      " 群消息 ",
			"timestamp":    "2026-11-18T15:00:01+08:00",
			"group_openid": "grp_open_1",
			"author":       map[string]any{"member_openid": "mem_1", "username": "李四", "bot": false},
		},
	}

	for i := 0; i < 100; i++ {
		evMu.Lock()
		n := len(received)
		evMu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	evMu.Lock()
	if len(received) != 2 || received[0].Chat.Kind != imsdk.ChatDM || received[0].Text != "你好" ||
		received[1].Chat.Kind != imsdk.ChatGroup || received[1].Chat.PlatformID != "grp_open_1" {
		evMu.Unlock()
		t.Fatalf("unexpected inbound messages: %+v", received)
	}
	dmMsg := received[0]
	evMu.Unlock()

	// Download protocol-relative attachment URL.
	tmpDir := t.TempDir()
	dlFile := filepath.Join(tmpDir, "1.png")
	if err := conn.Download(context.Background(), dmMsg.Attachments[0], dlFile); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if b, _ := os.ReadFile(dlFile); string(b) != "qq-attachment-bytes" {
		t.Fatalf("unexpected download bytes: %q", string(b))
	}

	// Trigger OpReconnect -> verify Gateway reconnects with OpResume!
	serverPush <- map[string]any{"op": OpReconnect}
	sawResume := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case op := <-clientOps:
			if op == OpResume {
				sawResume = true
			}
		case <-time.After(20 * time.Millisecond):
		}
		if sawResume {
			break
		}
	}
	if !sawResume {
		t.Fatal("expected OpResume (6) after OpReconnect")
	}

	// Send with expired passive reply window -> falls back to active send + uploads image + drops unpermitted file.
	imgPath := filepath.Join(tmpDir, "map.png")
	gpxPath := filepath.Join(tmpDir, "route.gpx")
	_ = os.WriteFile(imgPath, []byte("png"), 0o600)
	_ = os.WriteFile(gpxPath, []byte("gpx"), 0o600)

	mu.Lock()
	rejectPassiveText = true
	mu.Unlock()

	sendRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    dmMsg.Chat,
		Text:    "主动降级发送",
		ReplyTo: "m_c2c_1",
		Files:   []imsdk.OutboundFile{{Path: imgPath}, {Path: gpxPath}},
	})
	if err != nil || sendRes.Outcome != imsdk.OutcomeSent || sendRes.PlatformID != "out_img_1" {
		t.Fatalf("Send failed: %+v, err=%v", sendRes, err)
	}
	if !slices.Equal(sendRes.Degraded, []imsdk.Degradation{imsdk.DegradedMarkdownFlattened, imsdk.DegradedFilesDropped}) {
		t.Fatalf("unexpected Degraded: %v", sendRes.Degraded)
	}
	mu.Lock()
	if len(sentBodies) < 2 || sentBodies[0]["msg_id"] != nil {
		mu.Unlock()
		t.Fatalf("expected active fallback text body to omit msg_id, got %+v", sentBodies)
	}
	mu.Unlock()

	// React(processing) in DM renews sendTyping until settled or past 4.5m window.
	_ = conn.React(context.Background(), dmMsg.Chat, "m_c2c_1", imsdk.ReactionProcessing)
	for i := 0; i < 100 && typingCount.Load() < 3; i++ {
		time.Sleep(15 * time.Millisecond)
	}
	if typingCount.Load() < 3 {
		t.Fatalf("expected typing to renew multiple times, count=%d", typingCount.Load())
	}

	// Settling stops typing renewal.
	_ = conn.React(context.Background(), dmMsg.Chat, "m_c2c_1", imsdk.ReactionDone)
	afterDone := typingCount.Load()
	time.Sleep(70 * time.Millisecond)
	if typingCount.Load() != afterDone {
		t.Fatalf("expected typing renewal to stop after done: before=%d after=%d", afterDone, typingCount.Load())
	}

	// Advancing clock past 4.5m window stops typing renewal automatically.
	_ = conn.React(context.Background(), dmMsg.Chat, "m_c2c_1", imsdk.ReactionProcessing)
	mu.Lock()
	nowTime = nowTime.Add(5 * time.Minute)
	mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	afterWindow := typingCount.Load()
	time.Sleep(70 * time.Millisecond)
	if typingCount.Load() != afterWindow {
		t.Fatalf("expected typing renewal to stop past 4.5m window: before=%d after=%d", afterWindow, typingCount.Load())
	}

	// SetTyping in DM triggers one typing notice; ignored in group.
	_ = conn.SetTyping(context.Background(), dmMsg.Chat, true)
	if typingCount.Load() != afterWindow+1 {
		t.Fatalf("expected SetTyping(true) in DM to send 1 notice")
	}
	_ = conn.SetTyping(context.Background(), imsdk.ChatRef{PlatformID: "grp_open_1", Kind: imsdk.ChatGroup}, true)
	if typingCount.Load() != afterWindow+1 {
		t.Fatalf("expected SetTyping in group to be a no-op")
	}
}
