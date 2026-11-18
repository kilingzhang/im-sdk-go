// Package slack implements the Slack channel adapter using Socket Mode WebSockets
// and Slack Web API HTTP calls without requiring a public webhook endpoint.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

const (
	// AgentNamePlaceholder is substituted with the agent's display name in the Slack app manifest URL.
	AgentNamePlaceholder = "__MINGLE_AGENT_NAME__"
	// AgentHandlePlaceholder is substituted with a Slack-safe handle (a-z, 0-9, -, _, .) in the manifest URL.
	AgentHandlePlaceholder = "__MINGLE_AGENT_HANDLE__"

	defaultAPIBase  = "https://slack.com/api"
	apiTimeout      = 20 * time.Second
	transferTimeout = 120 * time.Second
	maxSeenEntries  = 5000
	wsReadLimit     = 16 << 20
)

// SetupURL opens Slack's app creation page pre-filled with the Socket Mode manifest.
var SetupURL = buildSetupURL()

func buildSetupURL() string {
	type slashCmd struct {
		Command      string `json:"command"`
		Description  string `json:"description"`
		ShouldEscape bool   `json:"should_escape"`
	}
	type manifest struct {
		DisplayInformation struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"display_information"`
		Features struct {
			AppHome struct {
				HomeTabEnabled             bool `json:"home_tab_enabled"`
				MessagesTabEnabled         bool `json:"messages_tab_enabled"`
				MessagesTabReadOnlyEnabled bool `json:"messages_tab_read_only_enabled"`
			} `json:"app_home"`
			BotUser struct {
				DisplayName  string `json:"display_name"`
				AlwaysOnline bool   `json:"always_online"`
			} `json:"bot_user"`
			SlashCommands []slashCmd `json:"slash_commands"`
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
			SocketModeEnabled    bool `json:"socket_mode_enabled"`
			OrgDeployEnabled     bool `json:"org_deploy_enabled"`
			TokenRotationEnabled bool `json:"token_rotation_enabled"`
		} `json:"settings"`
	}

	var m manifest
	m.DisplayInformation.Name = AgentNamePlaceholder
	m.DisplayInformation.Description = "An agent companion, run by Mingle on its owner's computer."
	m.Features.AppHome.HomeTabEnabled = false
	m.Features.AppHome.MessagesTabEnabled = true
	m.Features.AppHome.MessagesTabReadOnlyEnabled = false
	m.Features.BotUser.DisplayName = AgentHandlePlaceholder
	m.Features.BotUser.AlwaysOnline = true
	m.Features.SlashCommands = []slashCmd{
		{
			Command:      "/stop",
			Description:  "Stop the agent's current work",
			ShouldEscape: false,
		},
	}
	m.OAuthConfig.Scopes.Bot = []string{
		"channels:history",
		"groups:history",
		"im:history",
		"chat:write",
		"reactions:write",
		"files:read",
		"files:write",
		"users:read",
	}
	m.Settings.EventSubscriptions.BotEvents = []string{
		"message.im",
		"message.channels",
		"message.groups",
	}
	m.Settings.SocketModeEnabled = true
	m.Settings.OrgDeployEnabled = false
	m.Settings.TokenRotationEnabled = false

	raw, _ := json.Marshal(m)
	escaped := strings.ReplaceAll(url.QueryEscape(string(raw)), "+", "%20")
	return "https://api.slack.com/apps?new_app=1&manifest_json=" + escaped
}

var defaultCapabilities = imsdk.Capabilities{
	QuoteReply:      true,
	Reactions:       true,
	TypingIndicator: false,
	FileUpload:      true,
	FileDownload:    true,
	ListMembers:     false,
	Mentions:        true,
	MaxTextLength:   11000,
}

// DialFunc dials a Socket Mode WebSocket URL.
type DialFunc func(ctx context.Context, wsURL string) (*websocket.Conn, error)

// Options configures a Slack channel adapter.
type Options struct {
	HTTPClient *http.Client
	Dial       DialFunc
	APIBase    string
	Backoff    func(attempt int) time.Duration
}

// APIError represents an error returned by the Slack Web API or upload endpoint.
type APIError struct {
	Code       string
	Status     int
	RetryAfter int
}

func (e *APIError) Error() string {
	return "slack: " + e.Code
}

var (
	revokedCodes = map[string]struct{}{
		"invalid_auth":     {},
		"token_revoked":    {},
		"account_inactive": {},
		"not_authed":       {},
		"token_expired":    {},
	}
	keptSubtypes = map[string]struct{}{
		"file_share":       {},
		"thread_broadcast": {},
		"bot_message":      {},
	}
	chatGoneCodes = map[string]struct{}{
		"channel_not_found": {},
		"is_archived":       {},
		"thread_not_found":  {},
		"invalid_channel":   {},
	}
	forbiddenCodes = map[string]struct{}{
		"not_in_channel":                            {},
		"restricted_action":                         {},
		"cant_update_message":                       {},
		"missing_scope":                             {},
		"no_permission":                             {},
		"access_denied":                             {},
		"posting_to_channel_denied":                 {},
		"file_uploads_disabled":                     {},
		"file_uploads_except_images_disabled":       {},
		"file_type_not_allowed":                     {},
		"blocked_file_type":                         {},
		"slack_connect_blocked_file_type":           {},
		"slack_connect_file_upload_sharing_blocked": {},
	}
	tooLargeCodes = map[string]struct{}{
		"msg_too_long":                {},
		"too_many_attachments":        {},
		"invalid_blocks":              {},
		"file_upload_size_restricted": {},
		"storage_limit_reached":       {},
	}
	transientCodes = map[string]struct{}{
		"internal_error":      {},
		"fatal_error":         {},
		"service_unavailable": {},
		"request_timeout":     {},
	}

	reSlackUserID  = regexp.MustCompile(`^[UW][A-Z0-9]+$`)
	reUserMention  = regexp.MustCompile(`<@([UW][A-Z0-9]+)(?:\|[^>]*)?>`)
	reNamedLink    = regexp.MustCompile(`<(https?:[^|>]+)\|([^>]+)>`)
	reBareLink     = regexp.MustCompile(`<(https?:[^>]+)>`)
	reChannelLink  = regexp.MustCompile(`<#[A-Z0-9]+\|([^>]+)>`)
	reMultiSpaces  = regexp.MustCompile(`[ \t]{2,}`)
	formOnlyMethod = map[string]struct{}{
		"files.getUploadURLExternal": {},
	}
)

// File represents a file attachment carried on an inbound Slack message event.
type File struct {
	ID                 string `json:"id"`
	Name               string `json:"name,omitempty"`
	Title              string `json:"title,omitempty"`
	Size               int64  `json:"size,omitempty"`
	Mimetype           string `json:"mimetype,omitempty"`
	URLPrivateDownload string `json:"url_private_download,omitempty"`
	URLPrivate         string `json:"url_private,omitempty"`
}

// BotProfile holds display metadata on messages posted by Slack bot profiles.
type BotProfile struct {
	Name string `json:"name,omitempty"`
}

// Event represents a Slack Events API message payload.
type Event struct {
	Type         string      `json:"type"`
	Subtype      string      `json:"subtype,omitempty"`
	Channel      string      `json:"channel"`
	ChannelType  string      `json:"channel_type,omitempty"`
	User         string      `json:"user,omitempty"`
	BotID        string      `json:"bot_id,omitempty"`
	BotProfile   *BotProfile `json:"bot_profile,omitempty"`
	Username     string      `json:"username,omitempty"`
	Text         string      `json:"text,omitempty"`
	TS           string      `json:"ts"`
	ThreadTS     string      `json:"thread_ts,omitempty"`
	ParentUserID string      `json:"parent_user_id,omitempty"`
	Files        []File      `json:"files,omitempty"`
}

type channelAdapter struct {
	imsdk.BaseChannel
	httpClient *http.Client
	dial       DialFunc
	apiBase    string
	backoff    func(attempt int) time.Duration
}

// New constructs a Slack Channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	dial := opts.Dial
	if dial == nil {
		dial = func(ctx context.Context, wsURL string) (*websocket.Conn, error) {
			conn, _, err := websocket.Dial(ctx, wsURL, nil)
			if err != nil {
				return nil, err
			}
			conn.SetReadLimit(wsReadLimit)
			return conn, nil
		}
	}
	apiBase := strings.TrimRight(opts.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	backoff := opts.Backoff
	if backoff == nil {
		backoff = defaultBackoff
	}
	return &channelAdapter{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:      imsdk.PlatformSlack,
			TrustValue:         imsdk.TrustOrg,
			GroupMessagesValue: true,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:     imsdk.OnboardingCredentials,
				SetupURL: SetupURL,
				Fields: []imsdk.CredentialField{
					{Key: "botToken", LabelKey: "slack.botToken", Secret: true},
					{Key: "appToken", LabelKey: "slack.appToken", Secret: true},
				},
			},
		},
		httpClient: hc,
		dial:       dial,
		apiBase:    apiBase,
		backoff:    backoff,
	}
}

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		return 30 * time.Second
	}
	d := time.Second * time.Duration(1<<attempt)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (c *channelAdapter) call(ctx context.Context, token, method string, body map[string]any, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	_, isForm := formOnlyMethod[method]
	var rawBody []byte
	var contentType string
	if isForm {
		contentType = "application/x-www-form-urlencoded"
		rawBody = []byte(encodeFormBody(body))
	} else {
		contentType = "application/json; charset=utf-8"
		if body == nil {
			rawBody = []byte("{}")
		} else {
			var err error
			rawBody, err = json.Marshal(body)
			if err != nil {
				return err
			}
		}
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.apiBase+"/"+method, bytes.NewReader(rawBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", contentType)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var base struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &base); err != nil {
		return &APIError{
			Code:   fmt.Sprintf("http_%d", resp.StatusCode),
			Status: resp.StatusCode,
		}
	}
	if !base.OK {
		code := base.Error
		if code == "" {
			code = "unknown"
		}
		retryAfter, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return &APIError{
			Code:       code,
			Status:     resp.StatusCode,
			RetryAfter: retryAfter,
		}
	}
	if out != nil {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return err
		}
	}
	return nil
}

func encodeFormBody(body map[string]any) string {
	vals := url.Values{}
	for k, v := range body {
		if v == nil {
			continue
		}
		switch tv := v.(type) {
		case string:
			vals.Set(k, tv)
		case int:
			vals.Set(k, strconv.Itoa(tv))
		case int64:
			vals.Set(k, strconv.FormatInt(tv, 10))
		case float64:
			vals.Set(k, strconv.FormatFloat(tv, 'f', -1, 64))
		case bool:
			vals.Set(k, strconv.FormatBool(tv))
		default:
			b, err := json.Marshal(v)
			if err == nil {
				vals.Set(k, string(b))
			} else {
				vals.Set(k, fmt.Sprint(v))
			}
		}
	}
	return vals.Encode()
}

type authTestResponse struct {
	UserID string `json:"user_id"`
	User   string `json:"user"`
	BotID  string `json:"bot_id"`
}

func (c *channelAdapter) verify(ctx context.Context, botToken, appToken string) (authTestResponse, error) {
	var auth authTestResponse
	if err := c.call(ctx, botToken, "auth.test", nil, &auth); err != nil {
		return authTestResponse{}, err
	}
	if err := c.call(ctx, appToken, "apps.connections.open", nil, nil); err != nil {
		return authTestResponse{}, err
	}
	return auth, nil
}

func (c *channelAdapter) displayNameOf(ctx context.Context, botToken, userID string) (string, error) {
	var res struct {
		User struct {
			Name     string `json:"name"`
			RealName string `json:"real_name"`
			Profile  struct {
				DisplayName string `json:"display_name"`
				RealName    string `json:"real_name"`
			} `json:"profile"`
		} `json:"user"`
	}
	if err := c.call(ctx, botToken, "users.info", map[string]any{"user": userID}, &res); err != nil {
		return "", err
	}
	for _, candidate := range []string{
		res.User.Profile.DisplayName,
		res.User.Profile.RealName,
		res.User.RealName,
		res.User.Name,
	} {
		if candidate != "" {
			return candidate, nil
		}
	}
	return "", nil
}

func (c *channelAdapter) Onboard(ctx context.Context, input imsdk.Credentials, _ imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 1)
	go func() {
		defer close(ch)
		botToken := strings.TrimSpace(input["botToken"])
		appToken := strings.TrimSpace(input["appToken"])
		if !strings.HasPrefix(botToken, "xoxb-") || !strings.HasPrefix(appToken, "xapp-") {
			ch <- imsdk.OnboardingEvent{
				Kind: imsdk.EventFailed,
				Code: imsdk.FailureInvalidCredentials,
			}
			return
		}
		auth, err := c.verify(ctx, botToken, appToken)
		if err != nil {
			code := imsdk.FailureNetwork
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Code != "unknown" {
				code = imsdk.FailureInvalidCredentials
			}
			ch <- imsdk.OnboardingEvent{
				Kind: imsdk.EventFailed,
				Code: code,
			}
			return
		}
		name, err := c.displayNameOf(ctx, botToken, auth.UserID)
		if err != nil || name == "" {
			name = auth.User
		}
		ch <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"botToken": botToken,
				"appToken": appToken,
				"name":     name,
			},
			Bot: imsdk.BotIdentity{
				ID:   auth.UserID,
				Name: name,
			},
		}
	}()
	return ch
}

func (c *channelAdapter) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	botToken := creds["botToken"]
	appToken := creds["appToken"]

	var auth authTestResponse
	if err := c.call(ctx, botToken, "auth.test", nil, &auth); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && isRevoked(apiErr.Code) && events.OnState != nil {
			events.OnState(imsdk.ConnectionState{
				Kind: imsdk.StateError,
				Code: imsdk.ConnErrCredentialsRevoked,
			})
		}
		return nil, err
	}

	botName := creds["name"]
	if botName == "" {
		botName = auth.User
	}
	bot := imsdk.BotIdentity{ID: auth.UserID, Name: botName}

	connCtx, connCancel := context.WithCancel(context.Background())
	sc := &slackConnection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          bot,
			CapabilitiesValue: defaultCapabilities,
		},
		adapter:    c,
		botToken:   botToken,
		appToken:   appToken,
		botUserID:  auth.UserID,
		ownBotID:   auth.BotID,
		events:     events,
		connCtx:    connCtx,
		connCancel: connCancel,
		names:      make(map[string]string),
		files:      newFIFOMap[File](maxSeenEntries),
		threads:    newThreadSet(),
		seen:       newFIFOSet(maxSeenEntries),
	}

	sc.openSocket(ctx)
	return sc, nil
}

func isRevoked(code string) bool {
	_, ok := revokedCodes[code]
	return ok
}

type slackConnection struct {
	imsdk.BaseConnection
	adapter   *channelAdapter
	botToken  string
	appToken  string
	botUserID string
	ownBotID  string
	events    imsdk.ChannelEvents

	connCtx    context.Context
	connCancel context.CancelFunc

	mu      sync.Mutex
	ws      *websocket.Conn
	closed  bool
	attempt int
	timer   *time.Timer

	namesMu sync.Mutex
	names   map[string]string

	files   *fifoMap[File]
	threads *ThreadSet
	seen    *fifoSet
}

func (c *slackConnection) emitState(st imsdk.ConnectionState) {
	if c.events.OnState != nil {
		c.events.OnState(st)
	}
}

func (c *slackConnection) emitMessage(msg imsdk.InboundMessage) {
	if c.events.OnMessage != nil {
		c.events.OnMessage(msg)
	}
}

func (c *slackConnection) nameOf(ctx context.Context, userID string) string {
	c.namesMu.Lock()
	if known, ok := c.names[userID]; ok {
		c.namesMu.Unlock()
		return known
	}
	c.namesMu.Unlock()

	name, err := c.adapter.displayNameOf(ctx, c.botToken, userID)
	if err != nil {
		name = ""
	}
	c.namesMu.Lock()
	c.names[userID] = name
	c.namesMu.Unlock()
	return name
}

func (c *slackConnection) scheduleReconnect() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.attempt++
	attempt := c.attempt
	delay := c.adapter.backoff(attempt)
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = time.AfterFunc(delay, func() {
		c.openSocket(c.connCtx)
	})
	c.mu.Unlock()

	c.emitState(imsdk.ConnectionState{
		Kind:    imsdk.StateReconnecting,
		Attempt: attempt,
	})
}

func (c *slackConnection) openSocket(dialParentCtx context.Context) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	var opened struct {
		URL string `json:"url"`
	}
	if err := c.adapter.call(dialParentCtx, c.appToken, "apps.connections.open", nil, &opened); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && isRevoked(apiErr.Code) {
			c.emitState(imsdk.ConnectionState{
				Kind: imsdk.StateError,
				Code: imsdk.ConnErrCredentialsRevoked,
			})
			return
		}
		c.scheduleReconnect()
		return
	}

	dialCtx, cancel := context.WithTimeout(dialParentCtx, apiTimeout)
	ws, err := c.adapter.dial(dialCtx, opened.URL)
	cancel()
	if err != nil {
		c.scheduleReconnect()
		return
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = ws.CloseNow()
		return
	}
	old := c.ws
	c.ws = ws
	c.mu.Unlock()
	if old != nil && old != ws {
		_ = old.CloseNow()
	}

	go c.readLoop(ws)
}

type socketEnvelope struct {
	Type       string          `json:"type"`
	EnvelopeID string          `json:"envelope_id"`
	Payload    json.RawMessage `json:"payload"`
}

func (c *slackConnection) readLoop(ws *websocket.Conn) {
	rotated := false
	defer func() {
		_ = ws.CloseNow()
		if rotated {
			return
		}
		c.mu.Lock()
		active := !c.closed && c.ws == ws
		c.mu.Unlock()
		if active {
			c.scheduleReconnect()
		}
	}()

	for {
		_, raw, err := ws.Read(c.connCtx)
		if err != nil {
			return
		}
		var env socketEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		if env.EnvelopeID != "" {
			ack, _ := json.Marshal(map[string]string{"envelope_id": env.EnvelopeID})
			writeCtx, cancel := context.WithTimeout(c.connCtx, 5*time.Second)
			_ = ws.Write(writeCtx, websocket.MessageText, ack)
			cancel()
		}

		switch env.Type {
		case "hello":
			c.mu.Lock()
			wasReconnect := c.attempt > 0
			c.attempt = 0
			c.mu.Unlock()
			if wasReconnect {
				c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			}

		case "disconnect":
			rotated = true
			_ = ws.CloseNow()
			go c.openSocket(c.connCtx)
			return

		case "events_api":
			if len(env.Payload) == 0 {
				continue
			}
			var payload struct {
				EventID string `json:"event_id"`
				Event   *Event `json:"event"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil || payload.Event == nil {
				continue
			}
			dedupID := payload.EventID
			if dedupID == "" {
				dedupID = payload.Event.Channel + ":" + payload.Event.TS
			}
			if !c.seen.Add(dedupID) {
				continue
			}
			ev := *payload.Event
			c.handleEvent(ev)

		case "slash_commands":
			if len(env.Payload) == 0 {
				continue
			}
			c.handleSlash(env.Payload, env.EnvelopeID)
		}
	}
}

func (c *slackConnection) handleEvent(ev Event) {
	inbound := ToInbound(
		ev,
		c.botUserID,
		func(userID string) string { return c.nameOf(c.connCtx, userID) },
		c.threads,
		c.ownBotID,
	)
	if inbound == nil {
		return
	}
	for _, f := range ev.Files {
		if f.ID != "" {
			c.files.Set(f.ID, f)
		}
	}
	c.emitMessage(*inbound)
}

func (c *slackConnection) handleSlash(rawPayload json.RawMessage, envelopeID string) {
	var bodyBytes []byte = rawPayload
	if len(rawPayload) > 0 && rawPayload[0] == '"' {
		var unquoted string
		if err := json.Unmarshal(rawPayload, &unquoted); err != nil {
			return
		}
		bodyBytes = []byte(unquoted)
	}
	var cmd struct {
		Command   string `json:"command"`
		ChannelID string `json:"channel_id"`
		UserID    string `json:"user_id"`
		TriggerID string `json:"trigger_id"`
	}
	if err := json.Unmarshal(bodyBytes, &cmd); err != nil {
		return
	}
	dedupID := cmd.TriggerID
	if dedupID == "" {
		dedupID = envelopeID
	}
	if dedupID != "" && !c.seen.Add(dedupID) {
		return
	}
	if cmd.Command != "/stop" || !strings.HasPrefix(cmd.ChannelID, "D") || cmd.UserID == "" {
		return
	}
	platformID := cmd.TriggerID
	if platformID == "" {
		platformID = fmt.Sprintf("%s:%d", cmd.ChannelID, time.Now().UnixMilli())
	}
	c.emitMessage(imsdk.InboundMessage{
		PlatformID:     platformID,
		Chat:           imsdk.ChatRef{PlatformID: cmd.ChannelID, Kind: imsdk.ChatDM},
		Sender:         imsdk.Sender{ID: cmd.UserID, Name: c.nameOf(c.connCtx, cmd.UserID), IsBot: false},
		Text:           cmd.Command,
		Attachments:    []imsdk.InboundAttachment{},
		AddressedToBot: true,
		SentAt:         time.Now(),
	})
}

func (c *slackConnection) post(ctx context.Context, chat imsdk.ChatRef, text, replyTo string) (string, error) {
	channel, thread := parseChat(chat.PlatformID)
	threadTS := thread
	if threadTS == "" && chat.Kind == imsdk.ChatGroup {
		threadTS = replyTo
	}
	body := slackContent(text)
	body["channel"] = channel
	if threadTS != "" {
		body["thread_ts"] = threadTS
	}
	var sent struct {
		TS string `json:"ts"`
	}
	if err := c.adapter.call(ctx, c.botToken, "chat.postMessage", body, &sent); err != nil {
		return "", err
	}
	if threadTS != "" {
		c.threads.Add(channel + ":" + threadTS)
	}
	return sent.TS, nil
}

type stagedFile struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

func (c *slackConnection) stageUpload(ctx context.Context, file imsdk.OutboundFile) (stagedFile, error) {
	name := file.Name
	if name == "" {
		name = filepath.Base(file.Path)
	}
	info, err := os.Stat(file.Path)
	if err != nil {
		return stagedFile{}, err
	}
	if info.Size() == 0 {
		return stagedFile{}, &APIError{Code: "file_empty", Status: http.StatusOK}
	}
	var reserved struct {
		UploadURL string `json:"upload_url"`
		FileID    string `json:"file_id"`
	}
	if err := c.adapter.call(ctx, c.botToken, "files.getUploadURLExternal", map[string]any{
		"filename": name,
		"length":   info.Size(),
	}, &reserved); err != nil {
		return stagedFile{}, err
	}
	data, err := os.ReadFile(file.Path)
	if err != nil {
		return stagedFile{}, err
	}
	upCtx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(upCtx, http.MethodPost, reserved.UploadURL, bytes.NewReader(data))
	if err != nil {
		return stagedFile{}, errors.New("slack: invalid upload request")
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.adapter.httpClient.Do(req)
	if err != nil {
		return stagedFile{}, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return stagedFile{}, &APIError{
			Code:   fmt.Sprintf("upload_http_%d", resp.StatusCode),
			Status: resp.StatusCode,
		}
	}
	return stagedFile{ID: reserved.FileID, Title: name}, nil
}

func (c *slackConnection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	channel, thread := parseChat(msg.Chat.PlatformID)
	threadTS := thread
	if threadTS == "" && msg.Chat.Kind == imsdk.ChatGroup {
		threadTS = msg.ReplyTo
	}

	var ts string
	staged := make([]stagedFile, 0, len(msg.Files))
	for _, f := range msg.Files {
		sf, err := c.stageUpload(ctx, f)
		if err != nil {
			return imsdk.WithTextSent(sendFailure(ctx, err), ts), nil
		}
		staged = append(staged, sf)
	}
	if strings.TrimSpace(msg.Text) != "" {
		var err error
		ts, err = c.post(ctx, msg.Chat, slackMentions(msg.Text, msg.Mentions), msg.ReplyTo)
		if err != nil {
			return imsdk.WithTextSent(sendFailure(ctx, err), ts), nil
		}
	}
	if len(staged) > 0 {
		completeBody := map[string]any{
			"files":      staged,
			"channel_id": channel,
		}
		if threadTS != "" {
			completeBody["thread_ts"] = threadTS
		}
		if err := c.adapter.call(ctx, c.botToken, "files.completeUploadExternal", completeBody, nil); err != nil {
			return imsdk.WithTextSent(sendFailure(ctx, err), ts), nil
		}
	}
	platformID := ts
	if platformID == "" {
		platformID = fmt.Sprintf("file-%d", time.Now().UnixMilli())
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: platformID,
	}, nil
}

func (c *slackConnection) React(ctx context.Context, chat imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	channel, _ := parseChat(chat.PlatformID)
	set := func(method, name string) {
		_ = c.adapter.call(ctx, c.botToken, method, map[string]any{
			"channel":   channel,
			"timestamp": platformMsgID,
			"name":      name,
		}, nil)
	}
	if state == imsdk.ReactionProcessing {
		set("reactions.add", "eyes")
		return nil
	}
	set("reactions.remove", "eyes")
	switch state {
	case imsdk.ReactionDone:
		set("reactions.add", "white_check_mark")
	case imsdk.ReactionError:
		set("reactions.add", "x")
	}
	return nil
}

type slackLiveReply struct {
	conn    *slackConnection
	channel string
	ts      string
	mention string

	mu   sync.Mutex
	last string
}

func (l *slackLiveReply) Until() time.Time { return time.Time{} }

func (l *slackLiveReply) edit(ctx context.Context, markdown string) error {
	body := slackContent(markdown)
	body["channel"] = l.channel
	body["ts"] = l.ts
	return l.conn.adapter.call(ctx, l.conn.botToken, "chat.update", body, nil)
}

func (l *slackLiveReply) Update(ctx context.Context, markdown string) error {
	l.mu.Lock()
	if markdown == l.last {
		l.mu.Unlock()
		return nil
	}
	l.last = markdown
	l.mu.Unlock()

	_ = l.edit(ctx, markdown)
	return nil
}

func (l *slackLiveReply) Finish(ctx context.Context, markdown string, mentions []imsdk.Mention) (imsdk.SendResult, error) {
	text := slackMentions(markdown, mentions)
	named := slices.ContainsFunc(mentions, func(m imsdk.Mention) bool {
		return m.ID == l.mention
	})
	if l.mention != "" && !named {
		text = "<@" + l.mention + "> " + text
	}
	if err := l.edit(ctx, text); err != nil {
		return sendFailure(ctx, err), nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: l.ts,
	}, nil
}

func (c *slackConnection) StartLive(ctx context.Context, chat imsdk.ChatRef, opts imsdk.LiveOptions) (imsdk.LiveReply, error) {
	channel, _ := parseChat(chat.PlatformID)
	ts, err := c.post(ctx, chat, opts.Initial, opts.ReplyTo)
	if err != nil {
		return nil, err
	}
	return &slackLiveReply{
		conn:    c,
		channel: channel,
		ts:      ts,
		mention: opts.Mention,
		last:    opts.Initial,
	}, nil
}

func (c *slackConnection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	file, ok := c.files.Get(att.ID)
	if !ok {
		return errors.New("slack: this file is no longer available; ask for it to be sent again")
	}
	dlURL := file.URLPrivateDownload
	if dlURL == "" {
		dlURL = file.URLPrivate
	}
	if dlURL == "" {
		return errors.New("slack: this file is no longer available; ask for it to be sent again")
	}

	dlCtx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, dlURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.botToken)

	resp, err := c.adapter.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("slack: download failed: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o644)
}

func (c *slackConnection) OpenDirect(ctx context.Context, userID string) (string, error) {
	var res struct {
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
	}
	if err := c.adapter.call(ctx, c.botToken, "conversations.open", map[string]any{"users": userID}, &res); err != nil {
		return "", err
	}
	return res.Channel.ID, nil
}

func (c *slackConnection) MessageUser(ctx context.Context, userID, text string) (*imsdk.DirectMessageReceipt, error) {
	channelID, err := c.OpenDirect(ctx, userID)
	if err != nil {
		return nil, err
	}
	body := slackContent(text)
	body["channel"] = channelID
	var sent struct {
		TS string `json:"ts"`
	}
	if err := c.adapter.call(ctx, c.botToken, "chat.postMessage", body, &sent); err != nil {
		return nil, err
	}
	return &imsdk.DirectMessageReceipt{
		Chat:       channelID,
		PlatformID: sent.TS,
	}, nil
}

func (c *slackConnection) Close() error {
	c.mu.Lock()
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	ws := c.ws
	c.ws = nil
	c.mu.Unlock()

	c.connCancel()
	if ws != nil {
		_ = ws.CloseNow()
	}
	return nil
}

func slackMentions(markdown string, mentions []imsdk.Mention) string {
	var valid []imsdk.Mention
	for _, m := range mentions {
		if reSlackUserID.MatchString(m.ID) {
			valid = append(valid, m)
		}
	}
	return mention.Render(markdown, valid, func(m imsdk.Mention) string {
		return "<@" + m.ID + ">"
	})
}

func slackContent(markdown string) map[string]any {
	plain := markdown
	if utf8.RuneCountInString(plain) > 3000 {
		plain = string([]rune(plain)[:3000])
	}
	return map[string]any{
		"text": plain,
		"blocks": []map[string]string{
			{"type": "markdown", "text": markdown},
		},
	}
}

func parseChat(platformID string) (channel, thread string) {
	ch, th, ok := strings.Cut(platformID, ":")
	if !ok {
		return platformID, ""
	}
	return ch, th
}

// ThreadSet tracks `channel:thread_ts` threads that the bot has spoken in or been addressed in.
type ThreadSet struct {
	mu  sync.Mutex
	set map[string]struct{}
}

// NewThreadSet creates an empty ThreadSet.
func NewThreadSet() *ThreadSet {
	return newThreadSet()
}

func newThreadSet() *ThreadSet {
	return &ThreadSet{set: make(map[string]struct{})}
}

// Add records a thread key (`C...:ts`).
func (t *ThreadSet) Add(key string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.set[key] = struct{}{}
	t.mu.Unlock()
}

// Has reports whether key is tracked.
func (t *ThreadSet) Has(key string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	_, ok := t.set[key]
	t.mu.Unlock()
	return ok
}

// Len returns the number of tracked threads.
func (t *ThreadSet) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.set)
}

// ToInbound maps a Slack message Event to an InboundMessage, returning nil for ignored events.
func ToInbound(
	ev Event,
	botUserID string,
	nameOf func(userID string) string,
	threads *ThreadSet,
	ownBotID string,
) *imsdk.InboundMessage {
	if ev.Type != "message" {
		return nil
	}
	if ev.Subtype != "" {
		if _, ok := keptSubtypes[ev.Subtype]; !ok {
			return nil
		}
	}
	isBot := ev.BotID != "" || ev.Subtype == "bot_message"
	senderID := ev.User
	if senderID == "" && isBot {
		senderID = ev.BotID
	}
	if senderID == "" || senderID == botUserID || (ownBotID != "" && ev.BotID == ownBotID) {
		return nil
	}

	dm := ev.ChannelType == "im" || strings.HasPrefix(ev.Channel, "D")
	raw := ev.Text
	mentioned := strings.Contains(raw, "<@"+botUserID+">")
	var thread string
	if ev.ThreadTS != "" && ev.ThreadTS != ev.TS {
		thread = ev.ThreadTS
	}
	inThread := thread != "" && threads.Has(ev.Channel+":"+thread)
	addressed := dm || mentioned || inThread || ev.ParentUserID == botUserID

	root := thread
	if root == "" {
		root = ev.TS
	}
	if addressed && !dm {
		threads.Add(ev.Channel + ":" + root)
	}

	var chat imsdk.ChatRef
	switch {
	case dm:
		chat = imsdk.ChatRef{PlatformID: ev.Channel, Kind: imsdk.ChatDM}
	case addressed:
		chat = imsdk.ChatRef{
			PlatformID: ev.Channel + ":" + root,
			Kind:       imsdk.ChatThread,
			Parent:     &imsdk.ParentChat{PlatformID: ev.Channel},
		}
	default:
		chat = imsdk.ChatRef{PlatformID: ev.Channel, Kind: imsdk.ChatGroup}
	}

	text := strings.ReplaceAll(raw, "<@"+botUserID+">", "")
	var others []imsdk.Sender
	matches := reUserMention.FindAllStringSubmatch(text, -1)
	for _, m := range matches {
		full, id := m[0], m[1]
		var name string
		if nameOf != nil {
			name = nameOf(id)
		}
		if !slices.ContainsFunc(others, func(s imsdk.Sender) bool { return s.ID == id }) {
			others = append(others, imsdk.Sender{ID: id, Name: name, IsBot: false})
		}
		label := name
		if label == "" {
			label = id
		}
		text = strings.Replace(text, full, "@"+label, 1)
	}

	text = reNamedLink.ReplaceAllString(text, "$2 ($1)")
	text = reBareLink.ReplaceAllString(text, "$1")
	text = reChannelLink.ReplaceAllString(text, "#$1")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = reMultiSpaces.ReplaceAllString(text, " ")
	text = strings.TrimSpace(text)

	attachments := make([]imsdk.InboundAttachment, 0, len(ev.Files))
	for _, f := range ev.Files {
		name := f.Name
		if name == "" {
			name = f.Title
		}
		if name == "" {
			name = f.ID
		}
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   f.ID,
			Name: name,
			Size: f.Size,
			Mime: f.Mimetype,
		})
	}

	var senderName string
	if isBot {
		if ev.BotProfile != nil && ev.BotProfile.Name != "" {
			senderName = ev.BotProfile.Name
		} else if ev.Username != "" {
			senderName = ev.Username
		} else if ev.User != "" && nameOf != nil {
			senderName = nameOf(ev.User)
		}
	} else if nameOf != nil {
		senderName = nameOf(senderID)
	}

	var replyTo string
	if !addressed && thread != "" {
		replyTo = thread
	}

	var sentAt time.Time
	if sec, err := strconv.ParseFloat(ev.TS, 64); err == nil {
		sentAt = time.UnixMilli(int64(math.Round(sec * 1000)))
	}

	return &imsdk.InboundMessage{
		PlatformID:     ev.TS,
		Chat:           chat,
		Sender:         imsdk.Sender{ID: senderID, Name: senderName, IsBot: isBot},
		Text:           text,
		Attachments:    attachments,
		ReplyTo:        replyTo,
		AddressedToBot: addressed,
		Mentioned:      imsdk.BoolPtr(mentioned),
		Mentions:       others,
		SentAt:         sentAt,
	}
}

func sendFailure(callerCtx context.Context, err error) imsdk.SendResult {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		detail := apiErr.Code
		if apiErr.Code == "ratelimited" || apiErr.Status == http.StatusTooManyRequests {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrRateLimited,
				Retryable: true,
				Detail:    detail,
			}
		}
		if _, ok := chatGoneCodes[apiErr.Code]; ok {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrChatGone,
				Retryable: false,
				Detail:    detail,
			}
		}
		if _, ok := forbiddenCodes[apiErr.Code]; ok {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrForbidden,
				Retryable: false,
				Detail:    detail,
			}
		}
		if _, ok := tooLargeCodes[apiErr.Code]; ok {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrTooLarge,
				Retryable: false,
				Detail:    detail,
			}
		}
		_, transient := transientCodes[apiErr.Code]
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrUnknown,
			Retryable: apiErr.Status >= 500 || transient,
			Detail:    detail,
		}
	}
	if isTimeout(callerCtx, err) {
		return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}
	}
	return imsdk.SendResult{
		Outcome:   imsdk.OutcomeFailed,
		Code:      imsdk.SendErrNetwork,
		Retryable: true,
	}
}

func isTimeout(callerCtx context.Context, err error) bool {
	if callerCtx != nil && errors.Is(callerCtx.Err(), context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

type fifoSet struct {
	mu    sync.Mutex
	max   int
	items map[string]struct{}
	order []string
}

func newFIFOSet(max int) *fifoSet {
	return &fifoSet{
		max:   max,
		items: make(map[string]struct{}),
	}
}

func (s *fifoSet) Add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.items[key]; exists {
		return false
	}
	s.items[key] = struct{}{}
	s.order = append(s.order, key)
	if len(s.items) > s.max {
		oldest := s.order[0]
		delete(s.items, oldest)
		s.order[0] = ""
		s.order = s.order[1:]
		if cap(s.order) > 2*len(s.order) && cap(s.order) > 1024 {
			s.order = slices.Clone(s.order)
		}
	}
	return true
}

type fifoMap[V any] struct {
	mu    sync.Mutex
	max   int
	items map[string]V
	order []string
}

func newFIFOMap[V any](max int) *fifoMap[V] {
	return &fifoMap[V]{
		max:   max,
		items: make(map[string]V),
	}
}

func (m *fifoMap[V]) Set(key string, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.items[key]; !exists {
		m.order = append(m.order, key)
	}
	m.items[key] = val
	if len(m.items) > m.max {
		oldest := m.order[0]
		delete(m.items, oldest)
		m.order[0] = ""
		m.order = m.order[1:]
		if cap(m.order) > 2*len(m.order) && cap(m.order) > 1024 {
			m.order = slices.Clone(m.order)
		}
	}
}

func (m *fifoMap[V]) Get(key string) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	return v, ok
}
