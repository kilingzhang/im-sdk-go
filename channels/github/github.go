// Package github implements the GitHub notifications and comments channel adapter
// using OAuth Device Flow or classic personal access tokens and ETag conditional polling.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

const (
	// DefaultClientID is Mingle's public OAuth App client ID with Device Flow enabled.
	DefaultClientID = "Ov23lio63Bii42lTv6JN"

	defaultAPIBase     = "https://api.github.com"
	defaultWebBase     = "https://github.com"
	defaultPollFloor   = 60 * time.Second
	defaultCallTimeout = 20 * time.Second
	firstLookWindow    = 7 * 24 * time.Hour
	tokenSetupURL      = "https://github.com/settings/tokens/new?scopes=notifications,repo&description=Mingle%20(__MINGLE_AGENT_NAME__)"
)

// GitHubScopes lists the classic OAuth scopes required to poll notifications and reply in repositories.
var GitHubScopes = []string{"notifications", "repo"}

var (
	subjectPattern     = regexp.MustCompile(`/repos/([^/]+/[^/]+)/(?:issues|pulls)/(\d+)$`)
	reviewRootPattern  = regexp.MustCompile(`^rc:\d+@(\d+)$`)
	teamMentionPattern = regexp.MustCompile(`(?:^|[^\w-])@[\w-]+/[\w.-]+`)
	multiSpacePattern  = regexp.MustCompile(`[ \t]{2,}`)
	rateLimitPattern   = regexp.MustCompile(`(?i)rate limit`)
	tooLargePattern    = regexp.MustCompile(`(?i)too long|maximum`)

	allowedReasons = map[string]struct{}{
		"mention":      {},
		"team_mention": {},
		"comment":      {},
		"author":       {},
	}
	trustedAssociations = map[string]struct{}{
		"OWNER":        {},
		"MEMBER":       {},
		"COLLABORATOR": {},
	}
	tokenField = imsdk.CredentialField{
		Key:      "token",
		LabelKey: "github.token",
		Secret:   true,
	}
	capabilities = imsdk.Capabilities{
		QuoteReply:      false,
		Reactions:       true,
		TypingIndicator: false,
		FileUpload:      false,
		FileDownload:    false,
		ListMembers:     false,
		Mentions:        true,
		MaxTextLength:   65536,
	}
)

// Options configures a GitHub channel adapter.
type Options struct {
	HTTPClient *http.Client
	APIBase    string
	WebBase    string
	// ClientID overrides the OAuth App client ID for Device Flow.
	// When empty and DisableDeviceFlow is false, MINGLE_GITHUB_CLIENT_ID or DefaultClientID is used.
	ClientID string
	// DisableDeviceFlow disables OAuth Device Flow so onboarding accepts pasted classic tokens only.
	DisableDeviceFlow bool
	// PollFloor sets the minimum wait between notification polls; X-Poll-Interval can only raise it.
	PollFloor time.Duration
	Now       func() time.Time
}

// Error represents an HTTP error returned by the GitHub REST API.
type Error struct {
	Status int
	Detail string
}

func (e *Error) Error() string {
	return fmt.Sprintf("GitHub %d: %s", e.Status, e.Detail)
}

type ghUser struct {
	Login string `json:"login"`
	ID    int64  `json:"id"`
	Type  string `json:"type,omitempty"`
	Name  string `json:"name,omitempty"`
}

type ghNotification struct {
	ID         string  `json:"id"`
	Reason     string  `json:"reason"`
	UpdatedAt  string  `json:"updated_at"`
	LastReadAt *string `json:"last_read_at"`
	Subject    struct {
		Title string  `json:"title"`
		URL   *string `json:"url"`
		Type  string  `json:"type"`
	} `json:"subject"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type ghIssue struct {
	Number            int     `json:"number"`
	Title             string  `json:"title"`
	Body              *string `json:"body"`
	User              *ghUser `json:"user"`
	AuthorAssociation string  `json:"author_association"`
	CreatedAt         string  `json:"created_at"`
	PullRequest       any     `json:"pull_request,omitempty"`
}

type ghComment struct {
	ID                int64   `json:"id"`
	Body              *string `json:"body"`
	User              *ghUser `json:"user"`
	AuthorAssociation string  `json:"author_association"`
	CreatedAt         string  `json:"created_at"`
}

type ghReviewComment struct {
	ghComment
	InReplyToID  *int64 `json:"in_reply_to_id,omitempty"`
	Path         string `json:"path,omitempty"`
	Line         *int   `json:"line,omitempty"`
	OriginalLine *int   `json:"original_line,omitempty"`
}

type ghReview struct {
	ID                int64   `json:"id"`
	Body              *string `json:"body"`
	User              *ghUser `json:"user"`
	AuthorAssociation string  `json:"author_association"`
	SubmittedAt       *string `json:"submitted_at,omitempty"`
}

type ghReaction struct {
	ID      int64   `json:"id"`
	Content string  `json:"content"`
	User    *ghUser `json:"user"`
}

type channel struct {
	imsdk.BaseChannel
	httpClient *http.Client
	apiBase    string
	webBase    string
	clientID   string
	pollFloor  time.Duration
	now        func() time.Time
}

// New creates a GitHub channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	apiBase := strings.TrimRight(opts.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	webBase := strings.TrimRight(opts.WebBase, "/")
	if webBase == "" {
		webBase = defaultWebBase
	}
	clientID := ""
	if !opts.DisableDeviceFlow {
		clientID = opts.ClientID
		if clientID == "" {
			clientID = os.Getenv("MINGLE_GITHUB_CLIENT_ID")
		}
		if clientID == "" {
			clientID = DefaultClientID
		}
	}
	pollFloor := opts.PollFloor
	if pollFloor <= 0 {
		pollFloor = defaultPollFloor
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	var onboarding imsdk.OnboardingMethod
	var manualFields []imsdk.CredentialField
	var manualSetupURL string
	if clientID != "" {
		onboarding = imsdk.OnboardingMethod{Kind: imsdk.OnboardingDevice}
		manualFields = []imsdk.CredentialField{tokenField}
		manualSetupURL = tokenSetupURL
	} else {
		onboarding = imsdk.OnboardingMethod{
			Kind:     imsdk.OnboardingCredentials,
			Fields:   []imsdk.CredentialField{tokenField},
			SetupURL: tokenSetupURL,
		}
	}

	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:       imsdk.PlatformGitHub,
			TrustValue:          imsdk.TrustOrg,
			NoDirectMessages:    true,
			OnboardingValue:     onboarding,
			ManualFieldsValue:   manualFields,
			ManualSetupURLValue: manualSetupURL,
		},
		httpClient: hc,
		apiBase:    apiBase,
		webBase:    webBase,
		clientID:   clientID,
		pollFloor:  pollFloor,
		now:        nowFn,
	}
}

func (c *channel) Onboard(ctx context.Context, input imsdk.Credentials, _ imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 2)
	go func() {
		defer close(ch)
		token := strings.TrimSpace(input["token"])
		if token != "" {
			ch <- c.onboardToken(ctx, token)
			return
		}
		if len(input) > 0 || c.clientID == "" {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInvalidCredentials}
			return
		}
		c.deviceFlow(ctx, ch)
	}()
	return ch
}

func (c *channel) whoami(ctx context.Context, token string) (ghUser, bool, error) {
	api := c.newClient(token)
	resp, err := api.request(ctx, http.MethodGet, "/user", nil, nil)
	if err != nil {
		return ghUser{}, false, err
	}
	defer resp.Body.Close()

	var user ghUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return ghUser{}, false, err
	}
	rawScopes := resp.Header.Get("X-OAuth-Scopes")
	hasNotifications := false
	hasRepo := false
	for _, part := range strings.Split(rawScopes, ",") {
		switch strings.TrimSpace(part) {
		case "notifications":
			hasNotifications = true
		case "repo", "public_repo":
			hasRepo = true
		}
	}
	return user, hasNotifications && hasRepo, nil
}

func (c *channel) onboardToken(ctx context.Context, token string) imsdk.OnboardingEvent {
	user, scopesOK, err := c.whoami(ctx, token)
	if err != nil {
		code := imsdk.FailureNetwork
		var ge *Error
		if errors.As(err, &ge) && ge.Status == http.StatusUnauthorized {
			code = imsdk.FailureInvalidCredentials
		}
		return imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
	}
	if !scopesOK {
		return imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInsufficientScope}
	}
	return imsdk.OnboardingEvent{
		Kind:        imsdk.EventSucceeded,
		Credentials: imsdk.Credentials{"token": token},
		Bot:         identity(user),
	}
}

func (c *channel) postWebJSON(ctx context.Context, path string, body map[string]string, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()

	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.webBase+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mingle")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(out)
	return nil
}

func (c *channel) deviceFlow(ctx context.Context, ch chan<- imsdk.OnboardingEvent) {
	var device struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		ExpiresIn       *int   `json:"expires_in"`
		Interval        *int   `json:"interval"`
	}
	err := c.postWebJSON(ctx, "/login/device/code", map[string]string{
		"client_id": c.clientID,
		"scope":     strings.Join(GitHubScopes, " "),
	}, &device)
	if err != nil {
		if ctx.Err() == nil {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
		}
		return
	}
	if device.DeviceCode == "" || device.UserCode == "" {
		ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
		return
	}

	expiresSecs := 900
	if device.ExpiresIn != nil {
		expiresSecs = *device.ExpiresIn
	}
	expiresAt := c.now().Add(time.Duration(expiresSecs) * time.Second)
	verifyURI := device.VerificationURI
	if verifyURI == "" {
		verifyURI = c.webBase + "/login/device"
	}
	ch <- imsdk.OnboardingEvent{
		Kind:            imsdk.EventDeviceCode,
		UserCode:        device.UserCode,
		VerificationURI: verifyURI,
		ExpiresAt:       expiresAt,
	}

	interval := 5 * time.Second
	if device.Interval != nil {
		interval = time.Duration(*device.Interval) * time.Second
	}

	for ctx.Err() == nil {
		if !sleepCtx(ctx, interval) {
			return
		}
		if !c.now().Before(expiresAt) {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
			return
		}
		var answer struct {
			AccessToken string `json:"access_token"`
			Error       string `json:"error"`
			Interval    *int   `json:"interval"`
		}
		err := c.postWebJSON(ctx, "/login/oauth/access_token", map[string]string{
			"client_id":   c.clientID,
			"device_code": device.DeviceCode,
			"grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
		}, &answer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if answer.AccessToken != "" {
			ch <- c.onboardToken(ctx, answer.AccessToken)
			return
		}
		switch answer.Error {
		case "authorization_pending":
			continue
		case "slow_down":
			if answer.Interval != nil {
				interval = time.Duration(*answer.Interval) * time.Second
			} else {
				interval += 5 * time.Second
			}
			continue
		case "access_denied":
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureDenied}
			return
		case "expired_token":
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
			return
		default:
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
			return
		}
	}
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	api := c.newClient(creds["token"])
	var me ghUser
	if err := api.call(ctx, http.MethodGet, "/user", nil, &me); err != nil {
		return nil, err
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          identity(me),
			CapabilitiesValue: capabilities,
		},
		api:       api,
		me:        me,
		pollFloor: c.pollFloor,
		events:    events,
		stopCtx:   stopCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go conn.poll()
	return conn, nil
}

type apiClient struct {
	httpClient *http.Client
	apiBase    string
	token      string
}

func (c *channel) newClient(token string) *apiClient {
	return &apiClient{
		httpClient: c.httpClient,
		apiBase:    c.apiBase,
		token:      token,
	}
}

func (a *apiClient) request(ctx context.Context, method, path string, body any, extraHeaders map[string]string) (*http.Response, error) {
	reqCtx, cancel := context.WithTimeout(ctx, defaultCallTimeout)

	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			cancel()
			return nil, err
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(reqCtx, method, a.apiBase+path, bodyReader)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "Mingle")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	rawBody, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	cancel()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode >= 400 {
		var errBody struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(rawBody, &errBody)
		detail := errBody.Message
		if detail == "" {
			detail = resp.Status
		}
		return nil, &Error{Status: resp.StatusCode, Detail: detail}
	}
	resp.Body = io.NopCloser(bytes.NewReader(rawBody))
	return resp, nil
}

func (a *apiClient) call(ctx context.Context, method, path string, body any, out any) error {
	resp, err := a.request(ctx, method, path, body, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusResetContent || out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type connection struct {
	imsdk.BaseConnection
	api       *apiClient
	me        ghUser
	pollFloor time.Duration
	events    imsdk.ChannelEvents

	stopCtx   context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func (c *connection) emitMessage(msg imsdk.InboundMessage) {
	if c.events.OnMessage != nil {
		c.events.OnMessage(msg)
	}
}

func (c *connection) emitState(st imsdk.ConnectionState) {
	if c.events.OnState != nil {
		c.events.OnState(st)
	}
}

func (c *connection) poll() {
	defer close(c.done)
	etag := ""
	pending := make(map[string]ghNotification)
	var pendingOrder []string
	failures := 0

	for c.stopCtx.Err() == nil {
		wait := c.pollFloor
		err := func() error {
			var headers map[string]string
			if etag != "" {
				headers = map[string]string{"If-None-Match": etag}
			}
			resp, err := c.api.request(c.stopCtx, http.MethodGet, "/notifications?participating=true&per_page=50", nil, headers)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if secs, convErr := strconv.Atoi(strings.TrimSpace(resp.Header.Get("X-Poll-Interval"))); convErr == nil && secs > 0 {
				wait = max(c.pollFloor, time.Duration(secs)*time.Second)
			}
			if failures > 0 {
				c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
				failures = 0
			}

			if resp.StatusCode != http.StatusNotModified {
				var notifications []ghNotification
				if err := json.NewDecoder(resp.Body).Decode(&notifications); err != nil {
					return err
				}
				for _, n := range notifications {
					if _, ok := allowedReasons[n.Reason]; !ok {
						continue
					}
					patchResp, err := c.api.request(c.stopCtx, http.MethodPatch, "/notifications/threads/"+n.ID, nil, nil)
					if err != nil {
						return err
					}
					_ = patchResp.Body.Close()

					if earlier, exists := pending[n.ID]; exists {
						n.LastReadAt = earlier.LastReadAt
						pending[n.ID] = n
					} else {
						pending[n.ID] = n
						pendingOrder = append(pendingOrder, n.ID)
					}
				}
				etag = resp.Header.Get("ETag")
			}

			var retryable error
			var nextOrder []string
			for _, id := range pendingOrder {
				n, ok := pending[id]
				if !ok {
					continue
				}
				msgs, err := c.readThread(c.stopCtx, n)
				if err == nil {
					delete(pending, id)
					for _, m := range msgs {
						c.emitMessage(m)
					}
					continue
				}
				if c.stopCtx.Err() != nil {
					return c.stopCtx.Err()
				}
				var ge *Error
				if errors.As(err, &ge) &&
					(ge.Status == http.StatusForbidden || ge.Status == http.StatusNotFound || ge.Status == http.StatusGone) &&
					!rateLimitPattern.MatchString(ge.Detail) {
					delete(pending, id)
					continue
				}
				nextOrder = append(nextOrder, id)
				retryable = err
			}
			pendingOrder = nextOrder
			return retryable
		}()

		if err != nil {
			if c.stopCtx.Err() != nil {
				return
			}
			var ge *Error
			if errors.As(err, &ge) {
				if ge.Status == http.StatusUnauthorized {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrCredentialsRevoked})
					return
				}
				if ge.Status == http.StatusForbidden && !rateLimitPattern.MatchString(ge.Detail) {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrPermissionMissing})
					return
				}
			}
			failures++
			c.emitState(imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: failures})
			shift := min(failures, 10)
			wait = min(15*time.Minute, max(c.pollFloor, time.Second*time.Duration(1<<shift)))
		}

		if !sleepCtx(c.stopCtx, wait) {
			return
		}
	}
}

type timedMessage struct {
	at  time.Time
	msg imsdk.InboundMessage
}

type addExtra struct {
	addressed bool
	replyTo   string
	prefix    string
}

func (c *connection) readThread(ctx context.Context, n ghNotification) ([]imsdk.InboundMessage, error) {
	repo, number, ok := ParseSubject(n.Subject.URL)
	if !ok || (n.Subject.Type != "Issue" && n.Subject.Type != "PullRequest") {
		return nil, nil
	}
	var since string
	var after time.Time
	if n.LastReadAt != nil && *n.LastReadAt != "" {
		since = *n.LastReadAt
		after, _ = parseGitHubTime(since)
	} else {
		updated, _ := parseGitHubTime(n.UpdatedAt)
		after = updated.Add(-firstLookWindow)
		since = after.UTC().Format("2006-01-02T15:04:05.000Z")
	}

	var issue ghIssue
	if err := c.api.call(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d", repo, number), nil, &issue); err != nil {
		return nil, err
	}
	chat := imsdk.ChatRef{
		PlatformID: fmt.Sprintf("%s#%d", repo, number),
		Kind:       imsdk.ChatThread,
		Name:       fmt.Sprintf("%s#%d %s", repo, number, issue.Title),
	}

	mentionsMe := func(text string) bool {
		return Mentions(text, c.me.Login) || (n.Reason == "team_mention" && teamMentionPattern.MatchString(text))
	}

	var collected []timedMessage
	add := func(platformID string, author *ghUser, association, atStr, body string, extra addExtra) {
		at, err := parseGitHubTime(atStr)
		if err != nil || author == nil || !at.After(after) || strings.EqualFold(author.Login, c.me.Login) {
			return
		}
		if _, trusted := trustedAssociations[association]; !trusted {
			return
		}
		cleaned := StripMention(body, c.me.Login)
		text := cleaned
		if extra.prefix != "" {
			text = extra.prefix + "\n\n" + cleaned
		}
		m := mentionsMe(body)
		collected = append(collected, timedMessage{
			at: at,
			msg: imsdk.InboundMessage{
				PlatformID:     platformID,
				Chat:           chat,
				Sender:         imsdk.Sender{ID: author.Login, Name: author.Login, IsBot: author.Type == "Bot"},
				Text:           text,
				Attachments:    []imsdk.InboundAttachment{},
				ReplyTo:        extra.replyTo,
				AddressedToBot: extra.addressed || m,
				Mentioned:      imsdk.BoolPtr(m),
				SentAt:         at,
			},
		})
	}

	add(fmt.Sprintf("i:%d", number), issue.User, issue.AuthorAssociation, issue.CreatedAt, derefStr(issue.Body), addExtra{})

	var comments []ghComment
	commentsPath := fmt.Sprintf("/repos/%s/issues/%d/comments?since=%s&per_page=100", repo, number, url.QueryEscape(since))
	if err := c.api.call(ctx, http.MethodGet, commentsPath, nil, &comments); err != nil {
		return nil, err
	}
	for _, cm := range comments {
		add(fmt.Sprintf("ic:%d", cm.ID), cm.User, cm.AuthorAssociation, cm.CreatedAt, derefStr(cm.Body), addExtra{})
	}

	if issue.PullRequest != nil {
		var reviewComments []ghReviewComment
		rcPath := fmt.Sprintf("/repos/%s/pulls/%d/comments?since=%s&per_page=100", repo, number, url.QueryEscape(since))
		if err := c.api.call(ctx, http.MethodGet, rcPath, nil, &reviewComments); err != nil {
			return nil, err
		}
		authors := make(map[int64]string, len(reviewComments))
		for _, rc := range reviewComments {
			login := ""
			if rc.User != nil {
				login = rc.User.Login
			}
			authors[rc.ID] = login
		}
		for _, rc := range reviewComments {
			root := rc.ID
			if rc.InReplyToID != nil && *rc.InReplyToID != 0 {
				root = *rc.InReplyToID
			}
			rootAuthor, known := authors[root]
			if !known {
				if rcTime, err := parseGitHubTime(rc.CreatedAt); err == nil && rcTime.After(after) {
					var rootComment ghReviewComment
					if err := c.api.call(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/comments/%d", repo, root), nil, &rootComment); err == nil && rootComment.User != nil {
						rootAuthor = rootComment.User.Login
					}
					authors[root] = rootAuthor
				}
			}
			line := 0
			if rc.Line != nil && *rc.Line > 0 {
				line = *rc.Line
			} else if rc.OriginalLine != nil && *rc.OriginalLine > 0 {
				line = *rc.OriginalLine
			}
			var extra addExtra
			extra.addressed = rootAuthor != "" && strings.EqualFold(rootAuthor, c.me.Login)
			if rc.InReplyToID != nil && *rc.InReplyToID != 0 {
				extra.replyTo = fmt.Sprintf("rc:%d@%d", root, root)
			}
			if rc.Path != "" {
				if line > 0 {
					extra.prefix = fmt.Sprintf("(on `%s` line %d)", rc.Path, line)
				} else {
					extra.prefix = fmt.Sprintf("(on `%s`)", rc.Path)
				}
			}
			add(fmt.Sprintf("rc:%d@%d", rc.ID, root), rc.User, rc.AuthorAssociation, rc.CreatedAt, derefStr(rc.Body), extra)
		}

		var reviews []ghReview
		revPath := fmt.Sprintf("/repos/%s/pulls/%d/reviews?per_page=100", repo, number)
		if err := c.api.call(ctx, http.MethodGet, revPath, nil, &reviews); err != nil {
			return nil, err
		}
		for _, rv := range reviews {
			body := derefStr(rv.Body)
			if strings.TrimSpace(body) != "" && rv.SubmittedAt != nil && *rv.SubmittedAt != "" {
				add(fmt.Sprintf("rv:%d", rv.ID), rv.User, rv.AuthorAssociation, *rv.SubmittedAt, body, addExtra{})
			}
		}
	}

	slices.SortStableFunc(collected, func(a, b timedMessage) int {
		return a.at.Compare(b.at)
	})
	out := make([]imsdk.InboundMessage, len(collected))
	for i, item := range collected {
		out[i] = item.msg
	}
	return out, nil
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	repo, number := parseChat(msg.Chat.PlatformID)
	var degraded []imsdk.Degradation
	if len(msg.Files) > 0 {
		degraded = []imsdk.Degradation{imsdk.DegradedFilesDropped}
	}
	if strings.TrimSpace(msg.Text) == "" {
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrUnknown,
			Retryable: false,
			Detail:    "files_unsupported",
		}, nil
	}
	body := mention.Render(msg.Text, msg.Mentions, func(m imsdk.Mention) string {
		return "@" + m.ID
	})
	if msg.ReplyTo != "" {
		if root := reviewRoot(msg.ReplyTo); root != "" {
			var sent ghComment
			err := c.api.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/pulls/%d/comments/%s/replies", repo, number, root), map[string]string{"body": body}, &sent)
			if err != nil {
				return sendFailure(err), nil
			}
			return imsdk.SendResult{
				Outcome:    imsdk.OutcomeSent,
				PlatformID: fmt.Sprintf("rc:%d@%s", sent.ID, root),
				Degraded:   degraded,
			}, nil
		}
	}
	var sent ghComment
	err := c.api.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repo, number), map[string]string{"body": body}, &sent)
	if err != nil {
		return sendFailure(err), nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: fmt.Sprintf("ic:%d", sent.ID),
		Degraded:   degraded,
	}, nil
}

func (c *connection) React(ctx context.Context, chat imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	repo, _ := parseChat(chat.PlatformID)
	target := reactionTarget(repo, platformMsgID)
	if target == "" {
		return nil
	}
	if state == imsdk.ReactionProcessing {
		_ = c.api.call(ctx, http.MethodPost, target, map[string]string{"content": "eyes"}, nil)
		return nil
	}
	c.unreact(ctx, target, "eyes")
	switch state {
	case imsdk.ReactionDone:
		_ = c.api.call(ctx, http.MethodPost, target, map[string]string{"content": "rocket"}, nil)
	case imsdk.ReactionError:
		_ = c.api.call(ctx, http.MethodPost, target, map[string]string{"content": "confused"}, nil)
	}
	return nil
}

func (c *connection) unreact(ctx context.Context, target, content string) {
	var reactions []ghReaction
	if err := c.api.call(ctx, http.MethodGet, fmt.Sprintf("%s?content=%s&per_page=100", target, url.QueryEscape(content)), nil, &reactions); err != nil {
		return
	}
	for _, r := range reactions {
		if r.User != nil && strings.EqualFold(r.User.Login, c.me.Login) {
			_ = c.api.call(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", target, r.ID), nil, nil)
		}
	}
}

func (c *connection) Download(context.Context, imsdk.InboundAttachment, string) error {
	return errors.New("GitHub comments carry files as links; open the link instead")
}

func (c *connection) ListMembers(context.Context, imsdk.ChatRef) ([]imsdk.Sender, error) {
	return []imsdk.Sender{}, nil
}

func (c *connection) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
	})
	<-c.done
	return nil
}

func identity(u ghUser) imsdk.BotIdentity {
	return imsdk.BotIdentity{
		ID:       strconv.FormatInt(u.ID, 10),
		Name:     "@" + u.Login,
		SenderID: u.Login,
	}
}

// ParseSubject extracts owner/repo and issue/PR number from a GitHub notification subject URL.
func ParseSubject(rawURL *string) (repo string, number int, ok bool) {
	if rawURL == nil || *rawURL == "" {
		return "", 0, false
	}
	m := subjectPattern.FindStringSubmatch(*rawURL)
	if len(m) != 3 {
		return "", 0, false
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return m[1], n, true
}

func parseChat(platformID string) (repo string, number int) {
	at := strings.LastIndexByte(platformID, '#')
	if at < 0 {
		return platformID, 0
	}
	n, _ := strconv.Atoi(platformID[at+1:])
	return platformID[:at], n
}

func reviewRoot(platformID string) string {
	m := reviewRootPattern.FindStringSubmatch(platformID)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

func reactionTarget(repo, platformID string) string {
	kind, rest, ok := strings.Cut(platformID, ":")
	if !ok {
		return ""
	}
	id, _, _ := strings.Cut(rest, "@")
	if id == "" {
		return ""
	}
	switch kind {
	case "i":
		return fmt.Sprintf("/repos/%s/issues/%s/reactions", repo, id)
	case "ic":
		return fmt.Sprintf("/repos/%s/issues/comments/%s/reactions", repo, id)
	case "rc":
		return fmt.Sprintf("/repos/%s/pulls/comments/%s/reactions", repo, id)
	default:
		return ""
	}
}

// Mentions reports whether text contains @login as a standalone handle (excluding emails, paths, and longer handles).
func Mentions(text, login string) bool {
	if login == "" || !strings.Contains(text, "@") {
		return false
	}
	for i := strings.IndexByte(text, '@'); i >= 0; {
		if matchesHandleAt(text, i, login) {
			return true
		}
		next := strings.IndexByte(text[i+1:], '@')
		if next < 0 {
			break
		}
		i = i + 1 + next
	}
	return false
}

// StripMention removes standalone @login mentions from text and normalizes surrounding spaces.
func StripMention(text, login string) string {
	if login == "" || !strings.Contains(text, "@") {
		return strings.TrimSpace(multiSpacePattern.ReplaceAllString(text, " "))
	}
	var b strings.Builder
	b.Grow(len(text))
	at := 0
	for i := strings.IndexByte(text, '@'); i >= 0; {
		if matchesHandleAt(text, i, login) {
			b.WriteString(text[at:i])
			at = i + 1 + len(login)
			rel := strings.IndexByte(text[at:], '@')
			if rel < 0 {
				break
			}
			i = at + rel
			continue
		}
		rel := strings.IndexByte(text[i+1:], '@')
		if rel < 0 {
			break
		}
		i = i + 1 + rel
	}
	b.WriteString(text[at:])
	return strings.TrimSpace(multiSpacePattern.ReplaceAllString(b.String(), " "))
}

func matchesHandleAt(text string, atIdx int, login string) bool {
	if atIdx > 0 && isBlockedBeforeMention(text[atIdx-1]) {
		return false
	}
	end := atIdx + 1 + len(login)
	if end > len(text) {
		return false
	}
	if !strings.EqualFold(text[atIdx+1:end], login) {
		return false
	}
	if end < len(text) && isHandleContByte(text[end]) {
		return false
	}
	return true
}

func isHandleContByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}

func isBlockedBeforeMention(b byte) bool {
	return isHandleContByte(b) || b == '@' || b == '/'
}

func sendFailure(err error) imsdk.SendResult {
	var ge *Error
	if errors.As(err, &ge) {
		detail := fmt.Sprintf("%d %s", ge.Status, ge.Detail)
		if len(detail) > 200 {
			detail = detail[:200]
		}
		switch {
		case ge.Status == http.StatusTooManyRequests || (ge.Status == http.StatusForbidden && rateLimitPattern.MatchString(ge.Detail)):
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrRateLimited, Retryable: true, Detail: detail}
		case ge.Status == http.StatusUnauthorized || ge.Status == http.StatusForbidden:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false, Detail: detail}
		case ge.Status == http.StatusNotFound || ge.Status == http.StatusGone:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrChatGone, Retryable: false, Detail: detail}
		case ge.Status == http.StatusUnprocessableEntity && tooLargePattern.MatchString(ge.Detail):
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrTooLarge, Retryable: false, Detail: detail}
		default:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: ge.Status >= 500, Detail: detail}
		}
	}
	if isTimeoutOrCanceled(err) {
		return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}
	}
	return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrNetwork, Retryable: true}
}

func isTimeoutOrCanceled(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func parseGitHubTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
