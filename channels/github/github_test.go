package github

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/imtest"
)

var (
	ghMe       = map[string]any{"login": "alex-agent", "id": 42, "type": "User"}
	ghAna      = map[string]any{"login": "ana", "id": 1, "type": "User"}
	ghStranger = map[string]any{"login": "drive-by", "id": 2, "type": "User"}
)

type ghCall struct {
	Method  string
	Path    string
	Body    map[string]any
	Headers http.Header
}

type ghHandler func(c ghCall, ctx context.Context) (*http.Response, error)

type fakeGitHub struct {
	mu     sync.Mutex
	calls  []ghCall
	routes map[string]ghHandler
}

func newFakeGitHub(routes map[string]ghHandler) *fakeGitHub {
	if routes == nil {
		routes = make(map[string]ghHandler)
	}
	return &fakeGitHub{routes: routes}
}

func (fg *fakeGitHub) getCalls() []ghCall {
	fg.mu.Lock()
	defer fg.mu.Unlock()
	out := make([]ghCall, len(fg.calls))
	copy(out, fg.calls)
	return out
}

func (fg *fakeGitHub) client() *http.Client {
	return imtest.Client(func(req *http.Request) (*http.Response, error) {
		pathWithQuery := req.URL.Path
		if req.URL.RawQuery != "" {
			pathWithQuery += "?" + req.URL.RawQuery
		}
		var bodyMap map[string]any
		if req.Body != nil {
			raw, _ := io.ReadAll(req.Body)
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &bodyMap)
			}
		}
		call := ghCall{
			Method:  req.Method,
			Path:    pathWithQuery,
			Body:    bodyMap,
			Headers: req.Header.Clone(),
		}

		fg.mu.Lock()
		fg.calls = append(fg.calls, call)
		var bestKey string
		var handler ghHandler
		target := call.Method + " " + call.Path
		for k, h := range fg.routes {
			if strings.HasPrefix(target, k) && len(k) > len(bestKey) {
				bestKey = k
				handler = h
			}
		}
		fg.mu.Unlock()

		if handler != nil {
			resp, err := handler(call, req.Context())
			if resp != nil || err != nil {
				return resp, err
			}
		}

		if call.Path == "/user" && call.Method == http.MethodGet {
			return imtest.JSONResponse(http.StatusOK, ghMe, "X-OAuth-Scopes", "notifications, repo"), nil
		}
		if strings.HasPrefix(call.Path, "/notifications") && call.Method == http.MethodGet {
			if call.Headers.Get("If-None-Match") != "" {
				return imtest.BytesResponse(http.StatusNotModified, nil), nil
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		if call.Method == http.MethodPost {
			return imtest.JSONResponse(http.StatusCreated, map[string]any{"id": 900}), nil
		}
		if call.Method == http.MethodPut {
			return imtest.BytesResponse(http.StatusResetContent, nil), nil
		}
		return imtest.BytesResponse(http.StatusNoContent, nil), nil
	})
}

func serveOnce(body any, extraHeaders ...string) ghHandler {
	var mu sync.Mutex
	served := false
	return func(ghCall, context.Context) (*http.Response, error) {
		mu.Lock()
		if served {
			mu.Unlock()
			return nil, nil
		}
		served = true
		mu.Unlock()
		hdrs := append([]string{"X-Poll-Interval", "60"}, extraHeaders...)
		return imtest.JSONResponse(http.StatusOK, body, hdrs...), nil
	}
}

type ghRecorder struct {
	mu       sync.Mutex
	messages []imsdk.InboundMessage
	states   []imsdk.ConnectionState
}

func newGHRecorder() (*ghRecorder, imsdk.ChannelEvents) {
	r := &ghRecorder{}
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
	}
}

func waitGH(t *testing.T, cond func() bool) {
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

func collectGHEvents(ch <-chan imsdk.OnboardingEvent) []imsdk.OnboardingEvent {
	var out []imsdk.OnboardingEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func strPtr(s string) *string { return &s }

func TestMentionsAndSubjectParsing(t *testing.T) {
	if !Mentions("@alex-agent can you look?", "alex-agent") {
		t.Fatal("expected leading mention match")
	}
	if !Mentions("cc @Alex-Agent.", "alex-agent") {
		t.Fatal("expected case-insensitive mention match")
	}
	if Mentions("mail alex@alex-agent.dev", "alex-agent") {
		t.Fatal("unexpected match inside email")
	}
	if Mentions("@alex-agent-two", "alex-agent") {
		t.Fatal("unexpected match inside longer handle")
	}
	if Mentions("org/@alex-agent", "alex-agent") {
		t.Fatal("unexpected match after slash")
	}
	if got := StripMention("@alex-agent  please rebase", "alex-agent"); got != "please rebase" {
		t.Fatalf("StripMention = %q", got)
	}

	repo, num, ok := ParseSubject(strPtr("https://api.github.com/repos/o/r/issues/7"))
	if !ok || repo != "o/r" || num != 7 {
		t.Fatalf("unexpected ParseSubject issue: %q %d %v", repo, num, ok)
	}
	repo, num, ok = ParseSubject(strPtr("https://api.github.com/repos/o/r/pulls/8"))
	if !ok || repo != "o/r" || num != 8 {
		t.Fatalf("unexpected ParseSubject PR: %q %d %v", repo, num, ok)
	}
	if _, _, ok := ParseSubject(nil); ok {
		t.Fatal("expected nil URL to return ok=false")
	}
}

func TestOnboardingTokensAndDeviceFlow(t *testing.T) {
	ctx := context.Background()

	// Metadata checks
	defaultCh := New(Options{})
	if defaultCh.DirectMessages() {
		t.Fatal("expected DirectMessages() == false for GitHub")
	}
	if defaultCh.Trust() != imsdk.TrustOrg {
		t.Fatalf("Trust = %q, want org", defaultCh.Trust())
	}
	if defaultCh.Onboarding().Kind != imsdk.OnboardingDevice {
		t.Fatalf("Onboarding.Kind = %q, want device", defaultCh.Onboarding().Kind)
	}
	if len(defaultCh.ManualFields()) != 1 || !strings.Contains(defaultCh.ManualSetupURL(), "scopes=notifications,repo") {
		t.Fatalf("unexpected manual setup metadata")
	}
	credOnlyCh := New(Options{DisableDeviceFlow: true})
	if credOnlyCh.Onboarding().Kind != imsdk.OnboardingCredentials {
		t.Fatalf("expected credentials onboarding when device flow disabled")
	}

	// Classic token with valid scopes
	fg := newFakeGitHub(nil)
	okOut := collectGHEvents(New(Options{HTTPClient: fg.client()}).Onboard(ctx, imsdk.Credentials{"token": " ghp_x "}, imsdk.OnboardingOptions{}))
	if len(okOut) != 1 || okOut[0].Kind != imsdk.EventSucceeded || okOut[0].Bot.SenderID != "alex-agent" || okOut[0].Bot.Name != "@alex-agent" {
		t.Fatalf("unexpected token onboarding: %+v", okOut)
	}

	// Fine-grained token missing X-OAuth-Scopes -> FailureInsufficientScope
	fgFine := newFakeGitHub(map[string]ghHandler{
		"GET /user": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, ghMe), nil
		},
	})
	fineOut := collectGHEvents(New(Options{HTTPClient: fgFine.client()}).Onboard(ctx, imsdk.Credentials{"token": "github_pat_x"}, imsdk.OnboardingOptions{}))
	if len(fineOut) != 1 || fineOut[0].Code != imsdk.FailureInsufficientScope {
		t.Fatalf("expected insufficient_scope, got %+v", fineOut)
	}

	// OAuth Device Flow: authorization_pending -> slow_down (interval=0) -> access_token
	polls := 0
	fgDevice := newFakeGitHub(map[string]ghHandler{
		"POST /login/device/code": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"device_code":      "dev_1",
				"user_code":        "ABCD-1234",
				"verification_uri": "https://github.com/login/device",
				"expires_in":       900,
				"interval":         0,
			}), nil
		},
		"POST /login/oauth/access_token": func(ghCall, context.Context) (*http.Response, error) {
			polls++
			switch polls {
			case 1:
				return imtest.JSONResponse(http.StatusOK, map[string]any{"error": "authorization_pending"}), nil
			case 2:
				return imtest.JSONResponse(http.StatusOK, map[string]any{"error": "slow_down", "interval": 0}), nil
			default:
				return imtest.JSONResponse(http.StatusOK, map[string]any{"access_token": "gho_tok"}), nil
			}
		},
	})
	devOut := collectGHEvents(New(Options{HTTPClient: fgDevice.client()}).Onboard(ctx, nil, imsdk.OnboardingOptions{}))
	if len(devOut) != 2 || devOut[0].Kind != imsdk.EventDeviceCode || devOut[0].UserCode != "ABCD-1234" || devOut[1].Kind != imsdk.EventSucceeded {
		t.Fatalf("unexpected device flow events: %+v", devOut)
	}
	var dcClientID string
	for _, c := range fgDevice.getCalls() {
		if c.Path == "/login/device/code" {
			dcClientID, _ = c.Body["client_id"].(string)
		}
	}
	if dcClientID != DefaultClientID {
		t.Fatalf("expected DefaultClientID %q, got %q", DefaultClientID, dcClientID)
	}

	// Device flow access_denied
	fgDenied := newFakeGitHub(map[string]ghHandler{
		"POST /login/device/code": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{"device_code": "dev", "user_code": "CODE", "expires_in": 900, "interval": 0}), nil
		},
		"POST /login/oauth/access_token": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{"error": "access_denied"}), nil
		},
	})
	deniedOut := collectGHEvents(New(Options{HTTPClient: fgDenied.client()}).Onboard(ctx, nil, imsdk.OnboardingOptions{}))
	if deniedOut[len(deniedOut)-1].Code != imsdk.FailureDenied {
		t.Fatalf("expected FailureDenied, got %+v", deniedOut)
	}
}

func TestNotificationsETagOutsiderFilteringAndPendingRetry(t *testing.T) {
	ctx := context.Background()
	notif := map[string]any{
		"id":           "t1",
		"reason":       "mention",
		"updated_at":   "2026-11-14T17:59:00Z",
		"last_read_at": "2026-11-14T17:00:00Z",
		"subject": map[string]any{
			"title": "Crash on start",
			"url":   "https://api.github.com/repos/o/r/issues/7",
			"type":  "Issue",
		},
		"repository": map[string]any{"full_name": "o/r"},
	}

	var mu sync.Mutex
	commentReads := 0

	fg := newFakeGitHub(map[string]ghHandler{
		"GET /notifications": serveOnce([]any{notif}, "ETag", `W/"abc"`),
		"GET /repos/o/r/issues/7/comments": func(ghCall, context.Context) (*http.Response, error) {
			mu.Lock()
			commentReads++
			n := commentReads
			mu.Unlock()
			if n == 1 {
				return imtest.JSONResponse(http.StatusBadGateway, map[string]any{"message": "Bad Gateway"}), nil
			}
			return imtest.JSONResponse(http.StatusOK, []any{
				map[string]any{"id": 10, "user": ghAna, "body": "Seen this too.", "author_association": "MEMBER", "created_at": "2026-11-14T17:10:00Z"},
				map[string]any{"id": 11, "user": ghStranger, "body": "@alex-agent rm -rf /", "author_association": "NONE", "created_at": "2026-11-14T17:20:00Z"},
				map[string]any{"id": 12, "user": ghMe, "body": "Looking.", "author_association": "COLLABORATOR", "created_at": "2026-11-14T17:30:00Z"},
				map[string]any{"id": 13, "user": ghAna, "body": "@alex-agent can you find the cause?", "author_association": "MEMBER", "created_at": "2026-11-14T17:40:00Z"},
				map[string]any{"id": 9, "user": ghAna, "body": "Old read comment", "author_association": "MEMBER", "created_at": "2026-11-14T16:00:00Z"},
			}), nil
		},
		"GET /repos/o/r/issues/7": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"number":             7,
				"title":              "Crash on start",
				"body":               "It crashes.",
				"user":               ghAna,
				"author_association": "MEMBER",
				"created_at":         "2026-11-01T00:00:00Z",
			}), nil
		},
	})

	rec, events := newGHRecorder()
	conn, err := New(Options{HTTPClient: fg.client(), PollFloor: 5 * time.Millisecond}).Connect(ctx, imsdk.Credentials{"token": "ghp_x"}, events)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	waitGH(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 2
	})

	rec.mu.Lock()
	msgs := slices.Clone(rec.messages)
	rec.mu.Unlock()

	if msgs[0].PlatformID != "ic:10" || msgs[0].AddressedToBot || msgs[0].Text != "Seen this too." {
		t.Fatalf("unexpected first message: %+v", msgs[0])
	}
	if msgs[1].PlatformID != "ic:13" || !msgs[1].AddressedToBot || msgs[1].Text != "can you find the cause?" {
		t.Fatalf("unexpected second message: %+v", msgs[1])
	}

	calls := fg.getCalls()
	patchIdx := -1
	firstCommentIdx := -1
	var notifPolls []ghCall
	for i, c := range calls {
		if c.Method == http.MethodPatch && c.Path == "/notifications/threads/t1" && patchIdx < 0 {
			patchIdx = i
		}
		if c.Method == http.MethodGet && strings.HasPrefix(c.Path, "/repos/o/r/issues/7/comments") && firstCommentIdx < 0 {
			firstCommentIdx = i
			if !strings.Contains(c.Path, "since="+url.QueryEscape("2026-11-14T17:00:00Z")) {
				t.Fatalf("expected since query param, got %q", c.Path)
			}
		}
		if c.Method == http.MethodGet && strings.HasPrefix(c.Path, "/notifications") {
			notifPolls = append(notifPolls, c)
		}
	}
	if patchIdx < 0 || firstCommentIdx < 0 || patchIdx >= firstCommentIdx {
		t.Fatalf("expected PATCH /notifications/threads/t1 (%d) before reading comments (%d)", patchIdx, firstCommentIdx)
	}
	if len(notifPolls) < 2 || notifPolls[1].Headers.Get("If-None-Match") != `W/"abc"` {
		t.Fatalf("expected second notifications poll to carry If-None-Match ETag, got %+v", notifPolls)
	}
}

func TestPRReviewCommentsSendAndReactions(t *testing.T) {
	ctx := context.Background()
	prNotif := map[string]any{
		"id":           "t2",
		"reason":       "comment",
		"updated_at":   "2026-11-14T17:59:00Z",
		"last_read_at": "2026-11-14T17:00:00Z",
		"subject": map[string]any{
			"title": "Add cache",
			"url":   "https://api.github.com/repos/o/r/pulls/8",
			"type":  "PullRequest",
		},
		"repository": map[string]any{"full_name": "o/r"},
	}

	fg := newFakeGitHub(map[string]ghHandler{
		"GET /notifications": serveOnce([]any{prNotif}),
		"GET /repos/o/r/issues/8/comments": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, []any{}), nil
		},
		"GET /repos/o/r/issues/8": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"number":             8,
				"title":              "Add cache",
				"body":               "",
				"user":               ghMe,
				"author_association": "COLLABORATOR",
				"created_at":         "2026-11-01T00:00:00Z",
				"pull_request":       map[string]any{},
			}), nil
		},
		"GET /repos/o/r/pulls/8/comments": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, []any{
				map[string]any{
					"id":                 21,
					"user":               ghAna,
					"body":               "Why a Map here?",
					"author_association": "MEMBER",
					"created_at":         "2026-11-14T17:10:00Z",
					"in_reply_to_id":     20,
					"path":               "src/cache.ts",
					"line":               4,
				},
			}), nil
		},
		"GET /repos/o/r/pulls/comments/20": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, map[string]any{
				"id":                 20,
				"user":               ghMe,
				"body":               "Cache entries live here.",
				"author_association": "COLLABORATOR",
				"created_at":         "2026-11-13T00:00:00Z",
			}), nil
		},
		"GET /repos/o/r/pulls/8/reviews": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, []any{
				map[string]any{
					"id":                 5,
					"body":               "@alex-agent tests please",
					"user":               ghAna,
					"author_association": "MEMBER",
					"submitted_at":       "2026-11-14T17:20:00Z",
				},
			}), nil
		},
		"POST /repos/o/r/pulls/8/comments/20/replies": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusCreated, map[string]any{"id": 22}), nil
		},
		"POST /repos/o/r/issues/8/comments": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusCreated, map[string]any{"id": 31}), nil
		},
		"GET /repos/o/r/issues/comments/13/reactions": func(ghCall, context.Context) (*http.Response, error) {
			return imtest.JSONResponse(http.StatusOK, []any{
				map[string]any{"id": 70, "content": "eyes", "user": ghAna},
				map[string]any{"id": 71, "content": "eyes", "user": ghMe},
			}), nil
		},
	})

	rec, events := newGHRecorder()
	conn, err := New(Options{HTTPClient: fg.client()}).Connect(ctx, imsdk.Credentials{"token": "ghp_x"}, events)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	waitGH(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 2
	})

	rec.mu.Lock()
	m0, m1 := rec.messages[0], rec.messages[1]
	rec.mu.Unlock()

	if m0.PlatformID != "rc:21@20" || m0.ReplyTo != "rc:20@20" || !m0.AddressedToBot || m0.Text != "(on `src/cache.ts` line 4)\n\nWhy a Map here?" {
		t.Fatalf("unexpected review comment inbound: %+v", m0)
	}
	if m1.PlatformID != "rv:5" || !m1.AddressedToBot || m1.Text != "tests please" {
		t.Fatalf("unexpected review summary inbound: %+v", m1)
	}

	// Reply to inline review comment -> /pulls/8/comments/20/replies
	prChat := imsdk.ChatRef{PlatformID: "o/r#8", Kind: imsdk.ChatThread}
	rcSent, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:     prChat,
		Text:     "Keys are ids, @Ana.",
		ReplyTo:  "rc:21@20",
		Mentions: []imsdk.Mention{{ID: "ana", Name: "Ana"}},
	})
	if err != nil || rcSent.Outcome != imsdk.OutcomeSent || rcSent.PlatformID != "rc:22@20" {
		t.Fatalf("unexpected rc Send result: %+v, %v", rcSent, err)
	}

	// Issue comment reply + DegradedFilesDropped
	icSent, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:    prChat,
		Text:    "Found it.",
		ReplyTo: "ic:13",
		Files:   []imsdk.OutboundFile{{Path: "/tmp/x.png"}},
	})
	if err != nil || icSent.PlatformID != "ic:31" || len(icSent.Degraded) != 1 || icSent.Degraded[0] != imsdk.DegradedFilesDropped {
		t.Fatalf("unexpected ic Send result: %+v, %v", icSent, err)
	}

	// Empty text with files -> OutcomeFailed
	emptyRes, _ := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:  prChat,
		Text:  "  ",
		Files: []imsdk.OutboundFile{{Path: "/tmp/x.png"}},
	})
	if emptyRes.Outcome != imsdk.OutcomeFailed || emptyRes.Retryable {
		t.Fatalf("expected non-retryable failure on file-only send, got %+v", emptyRes)
	}

	// Reactions: eyes -> delete own eyes (71, keeping 70) + rocket; rv:5 ignored
	_ = conn.React(ctx, prChat, "ic:13", imsdk.ReactionProcessing)
	_ = conn.React(ctx, prChat, "ic:13", imsdk.ReactionDone)
	beforeCount := len(fg.getCalls())
	_ = conn.React(ctx, prChat, "rv:5", imsdk.ReactionProcessing)
	if len(fg.getCalls()) != beforeCount {
		t.Fatal("expected rv:5 reaction to make zero API calls")
	}

	var reactionOps []string
	for _, c := range fg.getCalls() {
		if strings.Contains(c.Path, "reactions") {
			entry := c.Method + " " + c.Path
			if content, ok := c.Body["content"].(string); ok {
				entry += " " + content
			}
			reactionOps = append(reactionOps, entry)
		}
	}
	wantOps := []string{
		"POST /repos/o/r/issues/comments/13/reactions eyes",
		"GET /repos/o/r/issues/comments/13/reactions?content=eyes&per_page=100",
		"DELETE /repos/o/r/issues/comments/13/reactions/71",
		"POST /repos/o/r/issues/comments/13/reactions rocket",
	}
	if !slices.Equal(reactionOps, wantOps) {
		t.Fatalf("reaction ops = %v, want %v", reactionOps, wantOps)
	}
}
