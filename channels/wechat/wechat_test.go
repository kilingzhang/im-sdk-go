package wechat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

type recordedCall struct {
	Method   string
	URL      string
	Endpoint string
	Header   http.Header
	RawBody  []byte
}

func (c recordedCall) decodeJSON(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if len(c.RawBody) > 0 {
		if err := json.Unmarshal(c.RawBody, &m); err != nil {
			t.Fatalf("decode request JSON (%s): %v", c.Endpoint, err)
		}
	}
	return m
}

func readReqBody(req *http.Request) []byte {
	if req == nil || req.Body == nil {
		return nil
	}
	raw, _ := io.ReadAll(req.Body)
	return raw
}

func collectEvents(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestMetadataAndSSRFValidation(t *testing.T) {
	ch := New(Options{})
	if ch.Platform() != imsdk.PlatformWeChat {
		t.Fatalf("Platform() = %q, want %q", ch.Platform(), imsdk.PlatformWeChat)
	}
	if ch.Trust() != imsdk.TrustOwner {
		t.Fatalf("Trust() = %q, want %q", ch.Trust(), imsdk.TrustOwner)
	}
	if ch.Onboarding().Kind != imsdk.OnboardingQR {
		t.Fatalf("Onboarding().Kind = %q, want qr", ch.Onboarding().Kind)
	}
	if !ch.DirectMessages() || ch.GroupMessages() {
		t.Fatalf("unexpected DM/group flags: dm=%v group=%v", ch.DirectMessages(), ch.GroupMessages())
	}

	if got, err := TrustedQRURL("https://liteapp.weixin.qq.com/q/abc?qrcode=QR1"); err != nil || got != "https://liteapp.weixin.qq.com/q/abc?qrcode=QR1" {
		t.Fatalf("TrustedQRURL valid failed: got=%q err=%v", got, err)
	}
	for _, badQR := range []string{
		"",
		"http://liteapp.weixin.qq.com/q/abc",
		"https://liteapp.weixin.qq.com:8443/q/abc",
		"https://user@liteapp.weixin.qq.com/q/abc",
		"https://evil.example.com/q/abc",
		"https://weixin.qq.com.evil.com/q/abc",
	} {
		if _, err := TrustedQRURL(badQR); err == nil {
			t.Fatalf("TrustedQRURL(%q) unexpectedly succeeded", badQR)
		}
	}

	if got, err := TrustedBaseURL("sh.ilinkai.weixin.qq.com", QRBaseURL); err != nil || got != "https://sh.ilinkai.weixin.qq.com/" {
		t.Fatalf("TrustedBaseURL bare host: got=%q err=%v", got, err)
	}
	if got, err := TrustedBaseURL("", QRBaseURL); err != nil || got != QRBaseURL {
		t.Fatalf("TrustedBaseURL empty fallback: got=%q err=%v", got, err)
	}
	for _, badBase := range []string{
		"https://evil.example.com",
		"http://ilinkai.weixin.qq.com",
		"https://ilinkai.weixin.qq.com:8080",
		"https://user:pass@ilinkai.weixin.qq.com",
	} {
		if _, err := TrustedBaseURL(badBase, QRBaseURL); err == nil {
			t.Fatalf("TrustedBaseURL(%q) unexpectedly succeeded", badBase)
		}
	}

	if got, err := MediaDownloadURL(&Media{EncryptQueryParam: "enc+1"}); err != nil || got != "https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=enc%2B1" {
		t.Fatalf("MediaDownloadURL param: got=%q err=%v", got, err)
	}
	if got, err := MediaDownloadURL(&Media{FullURL: "https://novac2c.cdn.weixin.qq.com:443/c2c/file?x=1#frag"}); err != nil || got != "https://novac2c.cdn.weixin.qq.com/c2c/file?x=1" {
		t.Fatalf("MediaDownloadURL full_url: got=%q err=%v", got, err)
	}
	for _, badMedia := range []*Media{
		nil,
		{},
		{FullURL: "http://novac2c.cdn.weixin.qq.com/c2c/file"},
		{FullURL: "https://novac2c.cdn.weixin.qq.com:8443/c2c/file"},
		{FullURL: "https://novac2c.cdn.weixin.qq.com/other/file"},
		{FullURL: "https://novac2c.cdn.weixin.qq.com/c2c/../other"},
		{FullURL: "https://evil@novac2c.cdn.weixin.qq.com/c2c/file"},
		{FullURL: "https://ilinkai.weixin.qq.com/c2c/file"},
	} {
		if _, err := MediaDownloadURL(badMedia); err == nil {
			t.Fatalf("MediaDownloadURL(%+v) unexpectedly succeeded", badMedia)
		}
	}

	if got, err := UploadURL(UploadReservation{UploadParam: "up-1"}, "fk-1"); err != nil || got != "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=up-1&filekey=fk-1" {
		t.Fatalf("UploadURL param: got=%q err=%v", got, err)
	}
	for _, badUp := range []UploadReservation{
		{},
		{UploadFullURL: "https://novac2c.cdn.weixin.qq.com/c2c/download"},
		{UploadFullURL: "http://novac2c.cdn.weixin.qq.com/c2c/upload"},
		{UploadFullURL: "https://novac2c.cdn.weixin.qq.com:9443/c2c/upload"},
		{UploadFullURL: "https://user@novac2c.cdn.weixin.qq.com/c2c/upload"},
		{UploadFullURL: "https://evil.weixin.qq.com/c2c/upload"},
	} {
		if _, err := UploadURL(badUp, "fk"); err == nil {
			t.Fatalf("UploadURL(%+v) unexpectedly succeeded", badUp)
		}
	}
}

func TestAES128ECBRoundTripAndMediaKey(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 16)
	for _, plain := range [][]byte{
		{},
		[]byte("a"),
		[]byte(" fifteen bytes!"),
		[]byte("exact16byteblock"),
		bytes.Repeat([]byte("wechat-ilink-cdn-payload-"), 17),
	} {
		ct, err := EncryptAES128ECB(plain, key)
		if err != nil {
			t.Fatalf("EncryptAES128ECB(len=%d): %v", len(plain), err)
		}
		if len(ct)%16 != 0 || len(ct) <= len(plain) {
			t.Fatalf("unexpected ciphertext length %d for plaintext %d", len(ct), len(plain))
		}
		got, err := DecryptMedia(ct, key)
		if err != nil {
			t.Fatalf("DecryptMedia(len=%d): %v", len(plain), err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("round-trip mismatch for len=%d", len(plain))
		}
	}

	// Tampered padding must fail.
	ct, _ := EncryptAES128ECB([]byte("hello"), key)
	ct[len(ct)-1] ^= 0xff
	if _, err := DecryptMedia(ct, key); err == nil {
		t.Fatal("expected tampered ciphertext to fail DecryptMedia")
	}

	hexKey := hex.EncodeToString(key)
	k1, err := MediaKey(MediaHolder{AESKey: hexKey})
	if err != nil || !bytes.Equal(k1, key) {
		t.Fatalf("MediaKey hex failed: %v", err)
	}
	k2, err := MediaKey(MediaHolder{Media: &Media{AESKey: base64.StdEncoding.EncodeToString(key)}})
	if err != nil || !bytes.Equal(k2, key) {
		t.Fatalf("MediaKey base64 raw failed: %v", err)
	}
	k3, err := MediaKey(MediaHolder{Media: &Media{AESKey: base64.StdEncoding.EncodeToString([]byte(hexKey))}})
	if err != nil || !bytes.Equal(k3, key) {
		t.Fatalf("MediaKey base64-hex failed: %v", err)
	}
	if _, err := MediaKey(MediaHolder{AESKey: "not-hex"}); err == nil {
		t.Fatal("expected invalid hex key to fail")
	}
}

func TestQRLoginVerifyCodeRedirectAndBindedStates(t *testing.T) {
	var mu sync.Mutex
	var calls []recordedCall
	statuses := []qrStatusResponse{
		{Status: "wait"},
		{Status: "scaned_but_redirect", RedirectHost: "sh.ilinkai.weixin.qq.com"},
		{Status: "need_verifycode"},
		{Status: "need_verifycode"},
		{
			Status:      "confirmed",
			BotToken:    "tok-final",
			ILinkBotID:  "bot@im.bot",
			ILinkUserID: "owner@im.wechat",
			BaseURL:     "sz.ilinkai.weixin.qq.com",
		},
	}

	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		raw := readReqBody(req)
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, recordedCall{
			Method:   req.Method,
			URL:      req.URL.String(),
			Endpoint: strings.TrimPrefix(req.URL.Path, "/"),
			Header:   req.Header.Clone(),
			RawBody:  raw,
		})
		switch strings.TrimPrefix(req.URL.Path, "/") {
		case "ilink/bot/get_bot_qrcode":
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"qrcode":             "QR1",
				"qrcode_img_content": "https://liteapp.weixin.qq.com/q/abc?qrcode=QR1",
			}), nil
		case "ilink/bot/get_qrcode_status":
			next := statuses[0]
			statuses = statuses[1:]
			return imtest.JSONResponse(http.StatusOK, next), nil
		default:
			return imtest.JSONResponse(http.StatusNotFound, map[string]any{}), nil
		}
	})

	codes := []string{"111111", "654321"}
	ch := New(Options{HTTPClient: hc})
	events := collectEvents(ch.Onboard(context.Background(), nil, imsdk.OnboardingOptions{
		AgentName: "Alex",
		AwaitCode: func(context.Context) (string, error) {
			c := codes[0]
			codes = codes[1:]
			return c, nil
		},
	}))

	var kinds []imsdk.OnboardingEventKind
	for _, e := range events {
		kinds = append(kinds, e.Kind)
	}
	wantKinds := []imsdk.OnboardingEventKind{
		imsdk.EventQRReady,
		imsdk.EventScanned,
		imsdk.EventNeedsCode,
		imsdk.EventNeedsCode,
		imsdk.EventSucceeded,
	}
	if !slices.Equal(kinds, wantKinds) {
		t.Fatalf("event kinds = %v, want %v", kinds, wantKinds)
	}
	if events[2].Retry || !events[3].Retry {
		t.Fatalf("unexpected retry flags: first=%v second=%v", events[2].Retry, events[3].Retry)
	}
	last := events[len(events)-1]
	if last.OwnerID != "owner@im.wechat" || last.Credentials["token"] != "tok-final" || last.Credentials["baseUrl"] != "https://sz.ilinkai.weixin.qq.com/" {
		t.Fatalf("unexpected succeeded event: %+v", last)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := calls[0].Header.Get("iLink-App-Id"); got != "bot" {
		t.Fatalf("iLink-App-Id = %q, want bot", got)
	}
	// Poll after scaned_but_redirect must target sh.ilinkai.weixin.qq.com and carry verify_code.
	if !strings.HasPrefix(calls[3].URL, "https://sh.ilinkai.weixin.qq.com/ilink/bot/get_qrcode_status") {
		t.Fatalf("redirected poll URL = %q", calls[3].URL)
	}
	if !strings.Contains(calls[4].URL, "verify_code=111111") || !strings.Contains(calls[5].URL, "verify_code=654321") {
		t.Fatalf("missing verify_code in polls: %q / %q", calls[4].URL, calls[5].URL)
	}
}

func TestQRLoginLocalTokenListAndUntrustedRedirect(t *testing.T) {
	// 1. local_token_list capped to newest 10 + binded_redirect => FailureAlreadyConnected
	var qrReqBody map[string]any
	hcConnected := imtest.Client(func(req *http.Request) (*http.Response, error) {
		raw := readReqBody(req)
		if strings.HasSuffix(req.URL.Path, "get_bot_qrcode") {
			_ = json.Unmarshal(raw, &qrReqBody)
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"qrcode":             "Q",
				"qrcode_img_content": "https://liteapp.weixin.qq.com/q/x",
			}), nil
		}
		return imtest.JSONResponse(http.StatusOK, map[string]any{"status": "binded_redirect"}), nil
	})

	var known []imsdk.Credentials
	for i := range 12 {
		known = append(known, imsdk.Credentials{"token": "tok-" + string(rune('a'+i)), "botId": "b"})
	}
	known = append(known, imsdk.Credentials{"botId": "no-token"})

	evs := collectEvents(New(Options{HTTPClient: hcConnected}).Onboard(context.Background(), nil, imsdk.OnboardingOptions{
		KnownCredentials: func(context.Context) ([]imsdk.Credentials, error) { return known, nil },
	}))
	gotList, _ := qrReqBody["local_token_list"].([]any)
	if len(gotList) != MaxLocalTokens {
		t.Fatalf("local_token_list len = %d, want %d", len(gotList), MaxLocalTokens)
	}
	if evs[len(evs)-1].Code != imsdk.FailureAlreadyConnected {
		t.Fatalf("code = %q, want already_connected", evs[len(evs)-1].Code)
	}

	// 2. No local tokens + binded_redirect => FailureAlreadyBound
	evsBound := collectEvents(New(Options{HTTPClient: hcConnected}).Onboard(context.Background(), nil, imsdk.OnboardingOptions{}))
	if evsBound[len(evsBound)-1].Code != imsdk.FailureAlreadyBound {
		t.Fatalf("code = %q, want already_bound", evsBound[len(evsBound)-1].Code)
	}

	// 3. Untrusted redirect_host during login => FailureNetwork
	hcEvil := imtest.Client(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "get_bot_qrcode") {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"qrcode":             "Q",
				"qrcode_img_content": "https://liteapp.weixin.qq.com/q/x",
			}), nil
		}
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"status":        "scaned_but_redirect",
			"redirect_host": "evil.example.com",
		}), nil
	})
	evsEvil := collectEvents(New(Options{HTTPClient: hcEvil}).Onboard(context.Background(), nil, imsdk.OnboardingOptions{}))
	if evsEvil[len(evsEvil)-1].Kind != imsdk.EventFailed || evsEvil[len(evsEvil)-1].Code != imsdk.FailureNetwork {
		t.Fatalf("expected FailureNetwork on untrusted redirect_host, got %+v", evsEvil[len(evsEvil)-1])
	}
}

func TestToInboundPollingContextTokenAndRevocation(t *testing.T) {
	var rawMsg Message
	_ = json.Unmarshal([]byte(`{
		"message_id": 7300000000000000001,
		"from_user_id": "u@im.wechat",
		"context_token": "ctx",
		"create_time_ms": 1700000000000,
		"item_list": [
			{"type": 1, "text_item": {"text": "周六去哪？"}},
			{"type": 3, "voice_item": {"text": "带水"}},
			{"type": 2},
			{"type": 5},
			{"type": 4, "file_item": {"file_name": "route.gpx", "len": "42", "media": {"encrypt_query_param": "q"}}}
		]
	}`), &rawMsg)

	in, ok := ToInbound(rawMsg)
	if !ok {
		t.Fatal("ToInbound returned false")
	}
	if in.PlatformID != "7300000000000000001" {
		t.Fatalf("64-bit message_id lost precision: %q", in.PlatformID)
	}
	if in.Text != "周六去哪？\n带水\n[image]\n[video]" {
		t.Fatalf("unexpected text: %q", in.Text)
	}
	if len(in.Attachments) != 1 || in.Attachments[0].ID != "7300000000000000001#4" || in.Attachments[0].Size != 42 {
		t.Fatalf("unexpected attachments: %+v", in.Attachments)
	}
	if _, ok := ToInbound(Message{MessageID: json.RawMessage(`"1"`), FromUserID: "bot", MessageType: 2}); ok {
		t.Fatal("expected self-echo message_type=2 to be skipped")
	}

	var mu sync.Mutex
	var calls []recordedCall
	polls := 0
	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		raw := readReqBody(req)
		ep := strings.TrimPrefix(req.URL.Path, "/")
		mu.Lock()
		calls = append(calls, recordedCall{
			Method:   req.Method,
			URL:      req.URL.String(),
			Endpoint: ep,
			Header:   req.Header.Clone(),
			RawBody:  raw,
		})
		switch ep {
		case "ilink/bot/msg/notifystart", "ilink/bot/msg/notifystop", "ilink/bot/sendmessage":
			mu.Unlock()
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0}), nil
		case "ilink/bot/getupdates":
			polls++
			p := polls
			mu.Unlock()
			switch p {
			case 1:
				return imtest.JSONResponse(http.StatusOK, map[string]any{
					"ret":             0,
					"get_updates_buf": "cursor-1",
					"msgs": []map[string]any{
						{
							"message_id":    "m1",
							"from_user_id":  "u@im.wechat",
							"context_token": "ctx-1",
							"item_list":     []map[string]any{{"type": 1, "text_item": map[string]any{"text": "hi"}}},
						},
					},
				}), nil
			case 2:
				// Simulate idle long-poll timeout; must not trigger reconnecting state.
				<-req.Context().Done()
				return nil, req.Context().Err()
			case 3:
				return imtest.JSONResponse(http.StatusOK, map[string]any{
					"ret":             0,
					"get_updates_buf": "cursor-2",
					"msgs":            []any{},
				}), nil
			default:
				return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": -14}), nil
			}
		default:
			mu.Unlock()
			return imtest.JSONResponse(http.StatusNotFound, map[string]any{}), nil
		}
	})

	var msgMu sync.Mutex
	var received []imsdk.InboundMessage
	var states []imsdk.ConnectionState

	ch := New(Options{
		HTTPClient:      hc,
		LongPollTimeout: 25 * time.Millisecond,
	})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{
		"token":   "tok",
		"botId":   "bot",
		"baseUrl": QRBaseURL,
		"name":    "Alex",
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			msgMu.Lock()
			received = append(received, m)
			msgMu.Unlock()
		},
		OnState: func(s imsdk.ConnectionState) {
			msgMu.Lock()
			states = append(states, s)
			msgMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	waitFor(t, func() bool {
		msgMu.Lock()
		defer msgMu.Unlock()
		return len(received) == 1 && len(states) >= 1 && states[len(states)-1].Kind == imsdk.StateError
	})

	msgMu.Lock()
	if len(states) != 1 || states[0].Code != imsdk.ConnErrCredentialsRevoked {
		t.Fatalf("expected only credentials_revoked state (idle timeout must not emit reconnecting), got %+v", states)
	}
	msgMu.Unlock()

	res, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:    imsdk.ChatRef{PlatformID: "u@im.wechat", Kind: imsdk.ChatDM},
		Text:    "hello",
		ReplyTo: "m1",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.Outcome != imsdk.OutcomeSent || !slices.Equal(res.Degraded, []imsdk.Degradation{imsdk.DegradedMarkdownFlattened, imsdk.DegradedQuoteDropped}) {
		t.Fatalf("unexpected SendResult: %+v", res)
	}

	_ = conn.Close()

	mu.Lock()
	defer mu.Unlock()
	var cursors []string
	var sendCall *recordedCall
	hasStop := false
	for i := range calls {
		c := &calls[i]
		if c.Endpoint == "ilink/bot/getupdates" {
			body := c.decodeJSON(t)
			cursors = append(cursors, body["get_updates_buf"].(string))
		}
		if c.Endpoint == "ilink/bot/sendmessage" {
			sendCall = c
		}
		if c.Endpoint == "ilink/bot/msg/notifystop" {
			hasStop = true
		}
	}
	if !slices.Equal(cursors, []string{"", "cursor-1", "cursor-1", "cursor-2"}) {
		t.Fatalf("getupdates cursors = %v", cursors)
	}
	if sendCall == nil {
		t.Fatal("missing sendmessage call")
	}
	if sendCall.Header.Get("AuthorizationType") != "ilink_bot_token" || sendCall.Header.Get("Authorization") != "Bearer tok" || sendCall.Header.Get("X-WECHAT-UIN") == "" {
		t.Fatalf("unexpected auth headers: %+v", sendCall.Header)
	}
	sentMsg := sendCall.decodeJSON(t)["msg"].(map[string]any)
	if sentMsg["context_token"] != "ctx-1" || sentMsg["to_user_id"] != "u@im.wechat" {
		t.Fatalf("unexpected sent msg payload: %+v", sentMsg)
	}
	if !hasStop {
		t.Fatal("expected notifystop on Close")
	}
}

func TestCDNUploadDownloadTypingAndSendOutcomes(t *testing.T) {
	peerKey := bytes.Repeat([]byte{7}, 16)
	peerCiphertext, err := EncryptAES128ECB([]byte("gpx data"), peerKey)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var reserved []map[string]any
	var uploaded [][]byte
	var typingStatuses []int
	polled := false
	cdnAttempt := 0
	timeoutSend := false

	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == CDNHost && req.URL.Path == "/c2c/upload" {
			raw := readReqBody(req)
			mu.Lock()
			cdnAttempt++
			attempt := cdnAttempt
			if attempt == 1 {
				// Transient 502 triggers retry.
				mu.Unlock()
				return imtest.BytesResponse(http.StatusBadGateway, nil), nil
			}
			uploaded = append(uploaded, raw)
			idx := len(uploaded)
			mu.Unlock()
			return imtest.BytesResponse(http.StatusOK, nil, "x-encrypted-param", "down-"+string(rune('0'+idx))), nil
		}
		if req.URL.Host == CDNHost && req.URL.Path == "/c2c/download" {
			return imtest.BytesResponse(http.StatusOK, peerCiphertext), nil
		}

		raw := readReqBody(req)
		ep := strings.TrimPrefix(req.URL.Path, "/")
		switch ep {
		case "ilink/bot/msg/notifystart", "ilink/bot/msg/notifystop":
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0}), nil
		case "ilink/bot/getupdates":
			mu.Lock()
			first := !polled
			polled = true
			mu.Unlock()
			if !first {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"ret": 0,
				"msgs": []map[string]any{
					{
						"message_id":    "m1",
						"from_user_id":  "u@im.wechat",
						"context_token": "ctx",
						"item_list": []map[string]any{
							{
								"type": 4,
								"file_item": map[string]any{
									"file_name": "route.gpx",
									"media": map[string]any{
										"encrypt_query_param": "dl",
										"aes_key":             base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(peerKey))),
									},
								},
							},
						},
					},
				},
			}), nil
		case "ilink/bot/getuploadurl":
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			mu.Lock()
			reserved = append(reserved, body)
			idx := len(reserved)
			mu.Unlock()
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"ret":          0,
				"upload_param": "up-" + string(rune('0'+idx)),
			}), nil
		case "ilink/bot/sendmessage":
			mu.Lock()
			shouldTimeout := timeoutSend
			mu.Unlock()
			if shouldTimeout {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0}), nil
		case "ilink/bot/getconfig":
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0, "typing_ticket": "ticket-1"}), nil
		case "ilink/bot/sendtyping":
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			mu.Lock()
			typingStatuses = append(typingStatuses, int(body["status"].(float64)))
			mu.Unlock()
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0}), nil
		default:
			return imtest.JSONResponse(http.StatusNotFound, map[string]any{}), nil
		}
	})

	var gotMsgs []imsdk.InboundMessage
	var msgMu sync.Mutex
	ch := New(Options{
		HTTPClient:     hc,
		RequestTimeout: 40 * time.Millisecond,
	})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{
		"token":   "tok",
		"botId":   "bot",
		"baseUrl": QRBaseURL,
	}, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			msgMu.Lock()
			gotMsgs = append(gotMsgs, m)
			msgMu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	waitFor(t, func() bool {
		msgMu.Lock()
		defer msgMu.Unlock()
		return len(gotMsgs) == 1
	})

	dir := t.TempDir()
	inPath := filepath.Join(dir, "in.gpx")
	if err := conn.Download(context.Background(), gotMsgs[0].Attachments[0], inPath); err != nil {
		t.Fatalf("Download: %v", err)
	}
	gotPlain, err := os.ReadFile(inPath)
	if err != nil || string(gotPlain) != "gpx data" {
		t.Fatalf("downloaded content = %q, err = %v", string(gotPlain), err)
	}

	// Typing via React(processing) and SetTyping(false).
	chat := imsdk.ChatRef{PlatformID: "u@im.wechat", Kind: imsdk.ChatDM}
	_ = conn.React(context.Background(), chat, "m1", imsdk.ReactionProcessing)
	_ = conn.SetTyping(context.Background(), chat, false)

	// Upload image + file + zero-byte file (dropped -> DegradedFilesDropped).
	imgPath := filepath.Join(dir, "photo.PNG")
	pdfPath := filepath.Join(dir, "notes.pdf")
	emptyPath := filepath.Join(dir, "empty.txt")
	_ = os.WriteFile(imgPath, []byte("png bytes"), 0o644)
	_ = os.WriteFile(pdfPath, []byte("pdf bytes"), 0o644)
	_ = os.WriteFile(emptyPath, nil, 0o644)

	res, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  chat,
		Text:  "here you go",
		Files: []imsdk.OutboundFile{{Path: imgPath}, {Path: pdfPath}, {Path: emptyPath}},
	})
	if err != nil {
		t.Fatalf("Send files: %v", err)
	}
	if res.Outcome != imsdk.OutcomeSent || !slices.Equal(res.Degraded, []imsdk.Degradation{imsdk.DegradedMarkdownFlattened, imsdk.DegradedFilesDropped}) {
		t.Fatalf("unexpected SendResult: %+v", res)
	}

	mu.Lock()
	if len(reserved) != 2 || int(reserved[0]["media_type"].(float64)) != 1 || int(reserved[1]["media_type"].(float64)) != 3 {
		t.Fatalf("unexpected getuploadurl calls: %+v", reserved)
	}
	pdfKey, _ := hex.DecodeString(reserved[1]["aeskey"].(string))
	decPDF, decErr := DecryptMedia(uploaded[1], pdfKey)
	if decErr != nil || string(decPDF) != "pdf bytes" {
		t.Fatalf("uploaded PDF decrypt mismatch: %q err=%v", string(decPDF), decErr)
	}
	if !slices.Equal(typingStatuses, []int{1, 2}) {
		t.Fatalf("typingStatuses = %v, want [1 2]", typingStatuses)
	}
	timeoutSend = true
	mu.Unlock()

	// Timeout during sendmessage => OutcomeUncertain.
	uncertainRes, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat: chat,
		Text: "may arrive",
	})
	if err != nil || uncertainRes.Outcome != imsdk.OutcomeUncertain {
		t.Fatalf("expected OutcomeUncertain on timeout, got %+v err=%v", uncertainRes, err)
	}
}

func TestRedirectRejectionPartialSendAndMediaCacheCap(t *testing.T) {
	// Verify 2000 FIFO media cache eviction.
	c := &connection{
		contexts: make(map[string]string),
		media:    make(map[string]MediaHolder),
	}
	for i := range 2005 {
		c.recordInbound("u1", "ctx", "msg-"+string(rune(i+1)), []Item{
			{Type: 4, FileItem: &FileItem{Media: &Media{EncryptQueryParam: "q"}}},
		})
	}
	if len(c.media) != MaxCachedMedia {
		t.Fatalf("media cache size = %d, want %d", len(c.media), MaxCachedMedia)
	}
	if _, ok := c.getMedia("msg-\x01#0"); ok {
		t.Fatal("expected oldest entry to be evicted beyond 2000")
	}

	// Verify 302 redirect on CDN upload fails and partial send after text returns OutcomeSent + DegradedFilesDropped.
	hc := imtest.Client(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == CDNHost {
			return imtest.BytesResponse(http.StatusFound, nil, "Location", "https://evil.example.com/steal"), nil
		}
		switch strings.TrimPrefix(req.URL.Path, "/") {
		case "ilink/bot/msg/notifystart", "ilink/bot/msg/notifystop", "ilink/bot/sendmessage":
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0}), nil
		case "ilink/bot/getuploadurl":
			return imtest.JSONResponse(http.StatusOK, map[string]any{"ret": 0, "upload_param": "up"}), nil
		case "ilink/bot/getupdates":
			<-req.Context().Done()
			return nil, req.Context().Err()
		default:
			return imtest.JSONResponse(http.StatusNotFound, map[string]any{}), nil
		}
	})

	ch := New(Options{HTTPClient: hc})
	conn, err := ch.Connect(context.Background(), imsdk.Credentials{"token": "tok", "baseUrl": QRBaseURL}, imsdk.ChannelEvents{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fPath := filepath.Join(t.TempDir(), "doc.txt")
	_ = os.WriteFile(fPath, []byte("secret"), 0o644)

	res, err := conn.Send(context.Background(), imsdk.OutboundMessage{
		Chat:  imsdk.ChatRef{PlatformID: "u@im.wechat", Kind: imsdk.ChatDM},
		Text:  "text succeeded first",
		Files: []imsdk.OutboundFile{{Path: fPath}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.Outcome != imsdk.OutcomeSent || !slices.Equal(res.Degraded, []imsdk.Degradation{imsdk.DegradedMarkdownFlattened, imsdk.DegradedFilesDropped}) {
		t.Fatalf("expected partial send OutcomeSent + DegradedFilesDropped, got %+v", res)
	}
}
