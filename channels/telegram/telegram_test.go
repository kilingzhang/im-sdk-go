package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

const testToken = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var (
	testMe    = User{ID: 99, IsBot: true, FirstName: "Alex", Username: "AlexBot"}
	testZhang = User{ID: 1, IsBot: false, FirstName: "Zhang", LastName: "San"}
	testGroup = Chat{ID: -100, Type: "supergroup", Title: "Hikers"}
)

type recordedCall struct {
	Method string
	Body   map[string]any
	File   *uploadedDoc
}

type uploadedDoc struct {
	Name    string
	Content string
}

type mockReply struct {
	OK          bool
	Status      int
	Result      any
	Description string
	RetryAfter  float64
	Err         error
}

type fakeBotServer struct {
	mu      sync.Mutex
	calls   []recordedCall
	batches []mockReply
	routes  map[string]func(body map[string]any) mockReply
	notify  chan struct{}
}

func newFakeBot(routes map[string]func(body map[string]any) mockReply) *fakeBotServer {
	if routes == nil {
		routes = make(map[string]func(body map[string]any) mockReply)
	}
	return &fakeBotServer{
		routes: routes,
		notify: make(chan struct{}, 16),
	}
}

func (fb *fakeBotServer) pushUpdates(updates any) {
	fb.mu.Lock()
	fb.batches = append(fb.batches, mockReply{OK: true, Status: http.StatusOK, Result: updates})
	fb.mu.Unlock()
	select {
	case fb.notify <- struct{}{}:
	default:
	}
}

func (fb *fakeBotServer) pushError(status int, desc string, retryAfter float64) {
	fb.mu.Lock()
	fb.batches = append(fb.batches, mockReply{
		OK:          false,
		Status:      status,
		Description: desc,
		RetryAfter:  retryAfter,
	})
	fb.mu.Unlock()
	select {
	case fb.notify <- struct{}{}:
	default:
	}
}

func (fb *fakeBotServer) getCalls() []recordedCall {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	out := make([]recordedCall, len(fb.calls))
	copy(out, fb.calls)
	return out
}

func (fb *fakeBotServer) client() *http.Client {
	return imtest.Client(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/file/bot") {
			return imtest.BytesResponse(http.StatusOK, []byte("file bytes")), nil
		}
		parts := strings.Split(req.URL.Path, "/")
		method := parts[len(parts)-1]

		bodyMap := make(map[string]any)
		var doc *uploadedDoc
		if req.Body != nil {
			ct := req.Header.Get("Content-Type")
			mediaType, params, _ := mime.ParseMediaType(ct)
			if strings.HasPrefix(mediaType, "multipart/") {
				mr := multipart.NewReader(req.Body, params["boundary"])
				for {
					part, err := mr.NextPart()
					if err == io.EOF {
						break
					}
					if err != nil {
						return nil, err
					}
					raw, _ := io.ReadAll(part)
					if part.FileName() != "" {
						doc = &uploadedDoc{Name: part.FileName(), Content: string(raw)}
					} else {
						bodyMap[part.FormName()] = string(raw)
					}
				}
			} else {
				raw, _ := io.ReadAll(req.Body)
				if len(raw) > 0 {
					_ = json.Unmarshal(raw, &bodyMap)
				}
			}
		}

		fb.mu.Lock()
		fb.calls = append(fb.calls, recordedCall{Method: method, Body: bodyMap, File: doc})
		var routeFn func(map[string]any) mockReply
		if fb.routes != nil {
			routeFn = fb.routes[method]
		}
		fb.mu.Unlock()

		var rep mockReply
		if method == "getUpdates" {
			for {
				fb.mu.Lock()
				if len(fb.batches) > 0 {
					rep = fb.batches[0]
					fb.batches = fb.batches[1:]
					fb.mu.Unlock()
					break
				}
				fb.mu.Unlock()
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				case <-fb.notify:
				}
			}
		} else if routeFn != nil {
			rep = routeFn(bodyMap)
		} else if method == "getMe" {
			rep = mockReply{OK: true, Status: http.StatusOK, Result: testMe}
		} else {
			rep = mockReply{OK: true, Status: http.StatusOK, Result: map[string]any{"message_id": 7}}
		}

		if rep.Err != nil {
			return nil, rep.Err
		}
		if !rep.OK {
			status := rep.Status
			if status == 0 {
				status = http.StatusBadRequest
			}
			errBody := map[string]any{
				"ok":          false,
				"description": rep.Description,
			}
			if rep.RetryAfter > 0 {
				errBody["parameters"] = map[string]any{"retry_after": rep.RetryAfter}
			}
			return imtest.JSONResponse(status, errBody), nil
		}
		return imtest.JSONResponse(http.StatusOK, map[string]any{
			"ok":     true,
			"result": rep.Result,
		}), nil
	})
}

type eventRecorder struct {
	mu          sync.Mutex
	messages    []imsdk.InboundMessage
	states      []imsdk.ConnectionState
	memberships []imsdk.MembershipChange
}

func newEventRecorder() (*eventRecorder, imsdk.ChannelEvents) {
	r := &eventRecorder{}
	return r, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			r.mu.Lock()
			r.messages = append(r.messages, m)
			r.mu.Unlock()
		},
		OnState: func(s imsdk.ConnectionState) {
			r.mu.Lock()
			r.states = append(r.states, s)
			r.mu.Unlock()
		},
		OnMembership: func(m imsdk.MembershipChange) {
			r.mu.Lock()
			r.memberships = append(r.memberships, m)
			r.mu.Unlock()
		},
	}
}

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

func collectEvents(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func TestClaimURLAndMetadata(t *testing.T) {
	ch := New(Options{})
	if ch.Platform() != imsdk.PlatformTelegram {
		t.Fatalf("Platform = %q, want telegram", ch.Platform())
	}
	if !ch.GroupMessages() || !ch.DirectMessages() {
		t.Fatalf("expected group and direct messages enabled")
	}
	got := ch.ClaimURL(imsdk.BotIdentity{ID: "99", Name: "@AlexBot"}, "123456")
	if got != "https://t.me/AlexBot?start=123456" {
		t.Fatalf("ClaimURL = %q", got)
	}
}

func TestToInboundAndUTF16Entities(t *testing.T) {
	dm, ok := ToInbound(Message{
		MessageID: 10,
		From:      &testZhang,
		Chat:      Chat{ID: 1, Type: "private", FirstName: "Zhang"},
		Date:      1_700_000_000,
		Text:      "hello",
	}, testMe)
	if !ok || dm.Chat.Kind != imsdk.ChatDM || dm.Chat.PlatformID != "1" || !dm.AddressedToBot || dm.Sender.Name != "Zhang San" {
		t.Fatalf("unexpected DM inbound: %+v (ok=%v)", dm, ok)
	}
	if dm.SentAt.UnixMilli() != 1_700_000_000_000 {
		t.Fatalf("SentAt = %v", dm.SentAt)
	}

	chatter, ok := ToInbound(Message{
		MessageID: 11,
		From:      &testZhang,
		Chat:      testGroup,
		Date:      1_700_000_000,
		Text:      "lunch?",
	}, testMe)
	if !ok || chatter.AddressedToBot || (chatter.Mentioned != nil && *chatter.Mentioned) {
		t.Fatalf("expected non-addressed group chatter, got %+v", chatter)
	}

	// UTF-16 offset test with supplementary-plane emojis (2 UTF-16 units each) + CJK (1 UTF-16 unit each).
	// "🎉 你好 " -> 🎉(2) + ' '(1) + 你(1) + 好(1) + ' '(1) = 6 UTF-16 units.
	// "@AlexBot" -> 8 UTF-16 units (offset 6, length 8).
	// " 请看 🚀 " -> ' '(1) + 请(1) + 看(1) + ' '(1) + 🚀(2) + ' '(1) = 7 UTF-16 units -> next offset = 21.
	// "@Alex" text_mention -> 5 UTF-16 units (offset 21, length 5).
	rawUTF16 := "🎉 你好 @AlexBot 请看 🚀 @Alex 现在出发"
	mentionedMsg, ok := ToInbound(Message{
		MessageID: 12,
		From:      &testZhang,
		Chat:      testGroup,
		Date:      1_700_000_000,
		Text:      rawUTF16,
		Entities: []Entity{
			{Type: "mention", Offset: 6, Length: 8},
			{Type: "text_mention", Offset: 21, Length: 5, User: &testMe},
		},
	}, testMe)
	if !ok || !mentionedMsg.AddressedToBot || mentionedMsg.Mentioned == nil || !*mentionedMsg.Mentioned {
		t.Fatalf("expected UTF-16 mention addressed to bot: %+v", mentionedMsg)
	}
	if mentionedMsg.Text != "🎉 你好 请看 🚀 现在出发" {
		t.Fatalf("unexpected stripped UTF-16 text: %q", mentionedMsg.Text)
	}

	// /stop@AlexBot command normalization
	stopCmd, ok := ToInbound(Message{
		MessageID: 13,
		From:      &testZhang,
		Chat:      testGroup,
		Date:      1_700_000_000,
		Text:      "/stop@AlexBot",
	}, testMe)
	if !ok || stopCmd.Text != "/stop" || !stopCmd.AddressedToBot {
		t.Fatalf("unexpected command normalization: %+v", stopCmd)
	}

	// Outbound UTF-16 text_mention entities with emoji + CJK + ASCII
	// "🎉🔥 你好 @张三 和 @Alex!"
	// "🎉🔥 你好 " -> 2 + 2 + 1 + 1 + 1 + 1 = 8 UTF-16 units
	// "@张三" -> 3 UTF-16 units (offset 8, length 3)
	// " 和 " -> 1 + 1 + 1 = 3 UTF-16 units -> next offset = 8 + 3 + 3 = 14
	// "@Alex" -> 5 UTF-16 units (offset 14, length 5)
	outboundText := "🎉🔥 你好 @张三 和 @Alex!"
	ents := MentionEntities(outboundText, []imsdk.Mention{
		{ID: "1001", Name: "张三"},
		{ID: "1002", Name: "Alex"},
		{ID: "non-numeric", Name: "Ignored"},
	})
	if len(ents) != 2 {
		t.Fatalf("expected 2 entities, got %+v", ents)
	}
	if ents[0].Offset != 8 || ents[0].Length != 3 || ents[0].User.ID != 1001 {
		t.Fatalf("unexpected first entity: %+v", ents[0])
	}
	if ents[1].Offset != 14 || ents[1].Length != 5 || ents[1].User.ID != 1002 {
		t.Fatalf("unexpected second entity: %+v", ents[1])
	}
}

func TestForumTopicsAndAttachments(t *testing.T) {
	// Forum topic root fake reply_to_message should be ignored as ReplyTo.
	topicMsg, ok := ToInbound(Message{
		MessageID:       20,
		From:            &testZhang,
		Chat:            Chat{ID: -100, Type: "supergroup", Title: "Hikers", IsForum: true},
		IsTopicMessage:  true,
		MessageThreadID: 3,
		ReplyToMessage:  &Message{MessageID: 3},
		Date:            1_700_000_000,
		Caption:         "the route",
		Document:        &File{FileID: "d1", FileName: "route.gpx", FileSize: 12, MimeType: "application/gpx+xml"},
		Photo: []PhotoSize{
			{File: File{FileID: "small"}, Width: 90},
			{File: File{FileID: "big"}, Width: 1280},
		},
		Voice: &File{FileID: "v1"},
	}, testMe)
	if !ok {
		t.Fatal("expected valid forum topic message")
	}
	if topicMsg.Chat.PlatformID != "-100:3" || topicMsg.Chat.Kind != imsdk.ChatThread {
		t.Fatalf("unexpected forum chat ref: %+v", topicMsg.Chat)
	}
	if topicMsg.ReplyTo != "" {
		t.Fatalf("expected empty ReplyTo for forum topic root, got %q", topicMsg.ReplyTo)
	}
	if len(topicMsg.Attachments) != 3 || topicMsg.Attachments[1].ID != "big" || topicMsg.Attachments[1].Mime != "image/jpeg" {
		t.Fatalf("unexpected attachments: %+v", topicMsg.Attachments)
	}

	if _, ok := ToInbound(Message{MessageID: 21, From: &testZhang, Chat: Chat{ID: -5, Type: "channel"}}, testMe); ok {
		t.Fatal("expected channel post to be ignored")
	}
	if _, ok := ToInbound(Message{MessageID: 22, Chat: testGroup}, testMe); ok {
		t.Fatal("expected senderless message to be ignored")
	}
}

func TestOnboarding(t *testing.T) {
	ctx := context.Background()
	fb := newFakeBot(nil)
	ch := New(Options{HTTPClient: fb.client()})

	bad := collectEvents(ch.Onboard(ctx, imsdk.Credentials{"token": "nope"}, imsdk.OnboardingOptions{}))
	if len(bad) != 1 || bad[0].Kind != imsdk.EventFailed || bad[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("unexpected bad token result: %+v", bad)
	}
	if len(fb.getCalls()) != 0 {
		t.Fatal("expected invalid token shape to fail before calling Telegram")
	}

	good := collectEvents(ch.Onboard(ctx, imsdk.Credentials{"token": " " + testToken + " "}, imsdk.OnboardingOptions{}))
	if len(good) != 1 || good[0].Kind != imsdk.EventSucceeded || good[0].Bot.Name != "@AlexBot" || good[0].Credentials["token"] != testToken {
		t.Fatalf("unexpected good onboarding: %+v", good)
	}

	refused := newFakeBot(map[string]func(map[string]any) mockReply{
		"getMe": func(map[string]any) mockReply {
			return mockReply{OK: false, Status: http.StatusUnauthorized, Description: "Unauthorized"}
		},
	})
	outRefused := collectEvents(New(Options{HTTPClient: refused.client()}).Onboard(ctx, imsdk.Credentials{"token": testToken}, imsdk.OnboardingOptions{}))
	if len(outRefused) != 1 || outRefused[0].Code != imsdk.FailureInvalidCredentials {
		t.Fatalf("expected invalid_credentials on 401, got %+v", outRefused)
	}

	down := newFakeBot(map[string]func(map[string]any) mockReply{
		"getMe": func(map[string]any) mockReply {
			return mockReply{OK: false, Status: http.StatusBadGateway, Description: "Bad Gateway"}
		},
	})
	outDown := collectEvents(New(Options{HTTPClient: down.client()}).Onboard(ctx, imsdk.Credentials{"token": testToken}, imsdk.OnboardingOptions{}))
	if len(outDown) != 1 || outDown[0].Code != imsdk.FailureNetwork {
		t.Fatalf("expected network failure on 502, got %+v", outDown)
	}
}

func TestConnectPollingErrorsAndLifecycle(t *testing.T) {
	ctx := context.Background()
	fb := newFakeBot(nil)
	rec, events := newEventRecorder()

	fb.pushUpdates([]map[string]any{
		{
			"update_id": 1,
			"message": Message{
				MessageID: 10,
				From:      &testZhang,
				Chat:      testGroup,
				Date:      1_700_000_000,
				Text:      "@AlexBot hi",
				Entities:  []Entity{{Type: "mention", Offset: 0, Length: 8}},
			},
		},
		{
			"update_id":      2,
			"my_chat_member": map[string]any{"chat": testGroup, "new_chat_member": map[string]any{"status": "member"}},
		},
		{
			"update_id":      3,
			"my_chat_member": map[string]any{"chat": testGroup, "new_chat_member": map[string]any{"status": "left"}},
		},
	})

	conn, err := New(Options{HTTPClient: fb.client()}).Connect(ctx, imsdk.Credentials{"token": testToken}, events)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	waitUntil(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.memberships) == 2 && len(rec.messages) == 1
	})
	waitUntil(t, func() bool {
		polls := 0
		for _, c := range fb.getCalls() {
			if c.Method == "getUpdates" {
				polls++
			}
		}
		return polls >= 2
	})

	var secondOffset float64
	polls := 0
	for _, c := range fb.getCalls() {
		if c.Method == "getUpdates" {
			polls++
			if polls == 2 {
				secondOffset, _ = c.Body["offset"].(float64)
			}
		}
	}
	if secondOffset != 4 {
		t.Fatalf("expected second poll offset 4, got %v", secondOffset)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 401 -> ConnErrCredentialsRevoked
	fb401 := newFakeBot(nil)
	rec401, ev401 := newEventRecorder()
	fb401.pushError(http.StatusUnauthorized, "Unauthorized", 0)
	conn401, err := New(Options{HTTPClient: fb401.client()}).Connect(ctx, imsdk.Credentials{"token": testToken}, ev401)
	if err != nil {
		t.Fatalf("Connect 401 failed: %v", err)
	}
	defer conn401.Close()
	waitUntil(t, func() bool {
		rec401.mu.Lock()
		defer rec401.mu.Unlock()
		return len(rec401.states) == 1
	})
	if rec401.states[0].Kind != imsdk.StateError || rec401.states[0].Code != imsdk.ConnErrCredentialsRevoked {
		t.Fatalf("unexpected 401 state: %+v", rec401.states)
	}

	// 409 -> ConnErrUnknown
	fb409 := newFakeBot(nil)
	rec409, ev409 := newEventRecorder()
	fb409.pushError(http.StatusConflict, "Conflict", 0)
	conn409, err := New(Options{HTTPClient: fb409.client()}).Connect(ctx, imsdk.Credentials{"token": testToken}, ev409)
	if err != nil {
		t.Fatalf("Connect 409 failed: %v", err)
	}
	defer conn409.Close()
	waitUntil(t, func() bool {
		rec409.mu.Lock()
		defer rec409.mu.Unlock()
		return len(rec409.states) == 1
	})
	if rec409.states[0].Kind != imsdk.StateError || rec409.states[0].Code != imsdk.ConnErrUnknown {
		t.Fatalf("unexpected 409 state: %+v", rec409.states)
	}

	// 429 retry_after -> reconnecting -> connected
	fb429 := newFakeBot(nil)
	rec429, ev429 := newEventRecorder()
	fb429.pushError(http.StatusTooManyRequests, "Too Many Requests", 0.01)
	fb429.pushUpdates([]any{})
	conn429, err := New(Options{HTTPClient: fb429.client()}).Connect(ctx, imsdk.Credentials{"token": testToken}, ev429)
	if err != nil {
		t.Fatalf("Connect 429 failed: %v", err)
	}
	defer conn429.Close()
	waitUntil(t, func() bool {
		rec429.mu.Lock()
		defer rec429.mu.Unlock()
		return len(rec429.states) == 2
	})
	if rec429.states[0].Kind != imsdk.StateReconnecting || rec429.states[1].Kind != imsdk.StateConnected {
		t.Fatalf("unexpected 429 states: %+v", rec429.states)
	}
}

func TestSendLiveReactDownloadAndPermissions(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	planPath := filepath.Join(tmp, "plan.txt")
	if err := os.WriteFile(planPath, []byte("day 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	failDoc := false
	refuseReaction := false
	readsAll := false
	filePath := "documents/file_1.gpx"

	fb := newFakeBot(map[string]func(map[string]any) mockReply{
		"getMe": func(map[string]any) mockReply {
			mu.Lock()
			ra := readsAll
			mu.Unlock()
			u := testMe
			u.CanReadAllGroupMessages = &ra
			return mockReply{OK: true, Status: http.StatusOK, Result: u}
		},
		"sendMessage": func(map[string]any) mockReply {
			return mockReply{OK: true, Status: http.StatusOK, Result: map[string]any{"message_id": 20}}
		},
		"sendDocument": func(map[string]any) mockReply {
			mu.Lock()
			fd := failDoc
			mu.Unlock()
			if fd {
				return mockReply{OK: false, Status: http.StatusInternalServerError, Description: "Internal Server Error"}
			}
			return mockReply{OK: true, Status: http.StatusOK, Result: map[string]any{"message_id": 21}}
		},
		"setMessageReaction": func(map[string]any) mockReply {
			mu.Lock()
			rr := refuseReaction
			mu.Unlock()
			if rr {
				return mockReply{OK: false, Status: http.StatusBadRequest, Description: "REACTION_INVALID"}
			}
			return mockReply{OK: true, Status: http.StatusOK, Result: true}
		},
		"getFile": func(map[string]any) mockReply {
			mu.Lock()
			fp := filePath
			mu.Unlock()
			if fp == "" {
				return mockReply{OK: true, Status: http.StatusOK, Result: map[string]any{}}
			}
			return mockReply{OK: true, Status: http.StatusOK, Result: map[string]any{"file_path": fp}}
		},
	})

	nowVal := time.Unix(1_700_000_000, 0)
	_, events := newEventRecorder()
	conn, err := New(Options{
		HTTPClient: fb.client(),
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return nowVal
		},
	}).Connect(ctx, imsdk.Credentials{"token": testToken}, events)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	// Send text + document to forum topic
	res, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:    imsdk.ChatRef{PlatformID: "-100:3", Kind: imsdk.ChatThread},
		Text:    "🎉 @张三 see plan",
		ReplyTo: "10",
		Files:   []imsdk.OutboundFile{{Path: planPath}},
		Mentions: []imsdk.Mention{
			{ID: "77", Name: "张三"},
		},
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if res.Outcome != imsdk.OutcomeSent || res.PlatformID != "21" {
		t.Fatalf("unexpected Send result: %+v", res)
	}

	calls := fb.getCalls()
	var docCall *recordedCall
	for i := range calls {
		if calls[i].Method == "sendDocument" {
			docCall = &calls[i]
		}
	}
	if docCall == nil || docCall.File == nil || docCall.File.Name != "plan.txt" || docCall.File.Content != "day 1" {
		t.Fatalf("unexpected sendDocument call: %+v", docCall)
	}

	// WithTextSent when sendDocument fails after sendMessage succeeds
	mu.Lock()
	failDoc = true
	mu.Unlock()
	partial, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  imsdk.ChatRef{PlatformID: "1", Kind: imsdk.ChatDM},
		Text:  "Attached.",
		Files: []imsdk.OutboundFile{{Path: planPath}},
	})
	if err != nil {
		t.Fatalf("partial Send returned error: %v", err)
	}
	if partial.Outcome != imsdk.OutcomeFailed || !partial.Retryable || partial.TextSentPlatformID != "20" {
		t.Fatalf("expected TextSentPlatformID=20 on partial failure, got %+v", partial)
	}

	// React + fallback to sendChatAction(typing)
	topicChat := imsdk.ChatRef{PlatformID: "-100:3", Kind: imsdk.ChatThread}
	_ = conn.React(ctx, topicChat, "10", imsdk.ReactionProcessing)
	_ = conn.React(ctx, topicChat, "10", imsdk.ReactionDone)
	_ = conn.React(ctx, topicChat, "10", imsdk.ReactionError)
	mu.Lock()
	refuseReaction = true
	mu.Unlock()
	_ = conn.React(ctx, topicChat, "10", imsdk.ReactionProcessing)

	lastCalls := fb.getCalls()
	last := lastCalls[len(lastCalls)-1]
	if last.Method != "sendChatAction" || last.Body["action"] != "typing" || last.Body["message_thread_id"] != float64(3) {
		t.Fatalf("expected typing fallback in forum thread, got %+v", last)
	}

	// StartLive
	lr, err := conn.StartLive(ctx, imsdk.ChatRef{PlatformID: "1", Kind: imsdk.ChatDM}, imsdk.LiveOptions{
		ReplyTo: "10",
		Initial: "**Thinking**",
	})
	if err != nil {
		t.Fatalf("StartLive failed: %v", err)
	}
	_ = lr.Update(ctx, "**Thinking**") // unchanged after plain(), skipped
	_ = lr.Update(ctx, "Reading *notes* and *maps*")
	fin, err := lr.Finish(ctx, "**Done**: hi @张三", []imsdk.Mention{{ID: "77", Name: "张三"}})
	if err != nil || fin.Outcome != imsdk.OutcomeSent || fin.PlatformID != "20" {
		t.Fatalf("unexpected live Finish: %+v, %v", fin, err)
	}
	var edits []string
	for _, c := range fb.getCalls() {
		if c.Method == "editMessageText" {
			txt, _ := c.Body["text"].(string)
			edits = append(edits, txt)
		}
	}
	if len(edits) != 2 || edits[0] != "Reading notes and maps" || edits[1] != "Done: hi @张三" {
		t.Fatalf("unexpected edits: %+v", edits)
	}

	// Download
	dlTarget := filepath.Join(tmp, "route.gpx")
	if err := conn.Download(ctx, imsdk.InboundAttachment{ID: "d1", Name: "route.gpx"}, dlTarget); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	gotBytes, _ := os.ReadFile(dlTarget)
	if string(gotBytes) != "file bytes" {
		t.Fatalf("unexpected downloaded content: %q", string(gotBytes))
	}
	mu.Lock()
	filePath = ""
	mu.Unlock()
	if err := conn.Download(ctx, imsdk.InboundAttachment{ID: "d2", Name: "big.mp4"}, dlTarget); err == nil || !strings.Contains(err.Error(), "20 MB") {
		t.Fatalf("expected >20 MB error, got %v", err)
	}

	// 15s cached Permissions
	perms, err := conn.Permissions(ctx)
	if err != nil || len(perms) != 1 || perms[0].Granted || perms[0].Grant == nil || perms[0].Grant.URL != botFatherURL {
		t.Fatalf("unexpected initial permissions: %+v, %v", perms, err)
	}
	mu.Lock()
	readsAll = true
	nowVal = nowVal.Add(5 * time.Second) // still within 15s TTL
	mu.Unlock()
	permsCached, _ := conn.Permissions(ctx)
	if permsCached[0].Granted {
		t.Fatal("expected permissions to stay cached within 15s TTL")
	}
	mu.Lock()
	nowVal = nowVal.Add(15 * time.Second) // past 15s TTL
	mu.Unlock()
	permsRefreshed, _ := conn.Permissions(ctx)
	if !permsRefreshed[0].Granted || permsRefreshed[0].Grant != nil {
		t.Fatalf("expected refreshed granted permissions, got %+v", permsRefreshed)
	}
}

func TestSendFailureMappingAndUncertainTimeout(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		status    int
		desc      string
		err       error
		outcome   imsdk.SendOutcome
		code      imsdk.SendFailureCode
		retryable bool
	}{
		{status: 429, desc: "Too Many Requests", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrRateLimited, retryable: true},
		{status: 403, desc: "Forbidden: bot was kicked", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrForbidden, retryable: false},
		{status: 413, desc: "Request Entity Too Large", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrTooLarge, retryable: false},
		{status: 400, desc: "Bad Request: chat not found", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrChatGone, retryable: false},
		{status: 400, desc: "Bad Request: other", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrUnknown, retryable: false},
		{status: 500, desc: "Internal Server Error", outcome: imsdk.OutcomeFailed, code: imsdk.SendErrUnknown, retryable: true},
		{err: context.DeadlineExceeded, outcome: imsdk.OutcomeUncertain},
		{err: errors.New("connection reset"), outcome: imsdk.OutcomeFailed, code: imsdk.SendErrNetwork, retryable: true},
	}

	for _, tc := range cases {
		fb := newFakeBot(map[string]func(map[string]any) mockReply{
			"sendMessage": func(map[string]any) mockReply {
				if tc.err != nil {
					return mockReply{Err: tc.err}
				}
				return mockReply{OK: false, Status: tc.status, Description: tc.desc}
			},
		})
		_, ev := newEventRecorder()
		conn, err := New(Options{HTTPClient: fb.client()}).Connect(ctx, imsdk.Credentials{"token": testToken}, ev)
		if err != nil {
			t.Fatal(err)
		}
		res, err := conn.Send(ctx, imsdk.OutboundMessage{
			Chat: imsdk.ChatRef{PlatformID: "1", Kind: imsdk.ChatDM},
			Text: "hi",
		})
		_ = conn.Close()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != tc.outcome || res.Code != tc.code || res.Retryable != tc.retryable {
			t.Fatalf("case (%d %q %v): got %+v", tc.status, tc.desc, tc.err, res)
		}
	}
}
