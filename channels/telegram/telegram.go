// Package telegram implements the Telegram Bot API channel adapter over outbound
// getUpdates long polling without requiring a public webhook endpoint.
package telegram

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
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
	"unicode"
	"unicode/utf8"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

const (
	defaultAPIBase     = "https://api.telegram.org"
	defaultPollTimeout = 30 * time.Second
	defaultCallTimeout = 20 * time.Second
	fileCallTimeout    = 120 * time.Second
	permissionsTTL     = 15 * time.Second
	botFatherURL       = "https://t.me/BotFather"
)

var (
	tokenPattern    = regexp.MustCompile(`^\d+:[\w-]{20,}$`)
	digitsPattern   = regexp.MustCompile(`^\d+$`)
	multiSpaceRegex = regexp.MustCompile(`[ \t]{2,}`)
	boldRegex       = regexp.MustCompile(`\*\*(.+?)\*\*`)
	chatNotFoundRe  = regexp.MustCompile(`(?i)chat not found`)

	capabilities = imsdk.Capabilities{
		QuoteReply:      true,
		Reactions:       true,
		TypingIndicator: true,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     false,
		Mentions:        true,
		MaxTextLength:   4096,
	}
)

// Options configures a Telegram channel adapter.
type Options struct {
	HTTPClient  *http.Client
	APIBase     string
	PollTimeout time.Duration
	Now         func() time.Time
}

// User represents a Telegram user or bot object.
type User struct {
	ID                      int64  `json:"id"`
	IsBot                   bool   `json:"is_bot"`
	FirstName               string `json:"first_name"`
	LastName                string `json:"last_name,omitempty"`
	Username                string `json:"username,omitempty"`
	CanReadAllGroupMessages *bool  `json:"can_read_all_group_messages,omitempty"`
}

// Chat represents a Telegram chat object.
type Chat struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title,omitempty"`
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
	IsForum   bool   `json:"is_forum,omitempty"`
}

// Entity represents a Telegram message entity using UTF-16 code unit offsets.
type Entity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
	User   *User  `json:"user,omitempty"`
}

// File represents a Telegram file descriptor on an inbound message.
type File struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

// PhotoSize represents one resolution variant of a Telegram photo.
type PhotoSize struct {
	File
	Width int `json:"width"`
}

// Message represents a Telegram message object.
type Message struct {
	MessageID       int64       `json:"message_id"`
	MessageThreadID int64       `json:"message_thread_id,omitempty"`
	IsTopicMessage  bool        `json:"is_topic_message,omitempty"`
	From            *User       `json:"from,omitempty"`
	Chat            Chat        `json:"chat"`
	Date            int64       `json:"date"`
	Text            string      `json:"text,omitempty"`
	Caption         string      `json:"caption,omitempty"`
	Entities        []Entity    `json:"entities,omitempty"`
	CaptionEntities []Entity    `json:"caption_entities,omitempty"`
	ReplyToMessage  *Message    `json:"reply_to_message,omitempty"`
	Document        *File       `json:"document,omitempty"`
	Photo           []PhotoSize `json:"photo,omitempty"`
	Audio           *File       `json:"audio,omitempty"`
	Voice           *File       `json:"voice,omitempty"`
	Video           *File       `json:"video,omitempty"`
}

type tgUpdate struct {
	UpdateID     int64         `json:"update_id"`
	Message      *Message      `json:"message,omitempty"`
	MyChatMember *tgChatMember `json:"my_chat_member,omitempty"`
}

type tgChatMember struct {
	Chat          Chat `json:"chat"`
	NewChatMember struct {
		Status string `json:"status"`
	} `json:"new_chat_member"`
}

type tgError struct {
	status      int
	description string
	retryAfter  float64
}

func (e *tgError) Error() string {
	return fmt.Sprintf("Telegram %d: %s", e.status, e.description)
}

type channel struct {
	imsdk.BaseChannel
	httpClient  *http.Client
	apiBase     string
	pollTimeout time.Duration
	now         func() time.Time
}

// New creates a Telegram channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	base := strings.TrimRight(opts.APIBase, "/")
	if base == "" {
		base = defaultAPIBase
	}
	pollTimeout := opts.PollTimeout
	if pollTimeout <= 0 {
		pollTimeout = defaultPollTimeout
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:      imsdk.PlatformTelegram,
			TrustValue:         imsdk.TrustPublic,
			GroupMessagesValue: true,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:     imsdk.OnboardingCredentials,
				SetupURL: botFatherURL,
				Fields: []imsdk.CredentialField{
					{Key: "token", LabelKey: "telegram.token", Secret: true},
				},
			},
		},
		httpClient:  hc,
		apiBase:     base,
		pollTimeout: pollTimeout,
		now:         nowFn,
	}
}

func (c *channel) ClaimURL(bot imsdk.BotIdentity, code string) string {
	username := strings.TrimPrefix(bot.Name, "@")
	return fmt.Sprintf("https://t.me/%s?start=%s", url.PathEscape(username), url.QueryEscape(code))
}

func (c *channel) Onboard(ctx context.Context, input imsdk.Credentials, _ imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 1)
	go func() {
		defer close(ch)
		token := strings.TrimSpace(input["token"])
		if token == "" || !tokenPattern.MatchString(token) {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInvalidCredentials}
			return
		}
		api := c.newClient(token)
		var me User
		if err := api.callJSON(ctx, "getMe", nil, defaultCallTimeout, &me); err != nil {
			code := imsdk.FailureNetwork
			var te *tgError
			if errors.As(err, &te) && te.status == http.StatusUnauthorized {
				code = imsdk.FailureInvalidCredentials
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}
		ch <- imsdk.OnboardingEvent{
			Kind:        imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{"token": token},
			Bot:         identity(me),
		}
	}()
	return ch
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	token := creds["token"]
	api := c.newClient(token)
	var me User
	if err := api.callJSON(ctx, "getMe", nil, defaultCallTimeout, &me); err != nil {
		return nil, err
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          identity(me),
			CapabilitiesValue: capabilities,
		},
		api:         api,
		token:       token,
		me:          me,
		pollTimeout: c.pollTimeout,
		now:         c.now,
		events:      events,
		stopCtx:     stopCtx,
		cancel:      cancel,
		done:        make(chan struct{}),
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

type tgEnvelope struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	Parameters  *struct {
		RetryAfter float64 `json:"retry_after"`
	} `json:"parameters,omitempty"`
}

func (a *apiClient) callJSON(ctx context.Context, method string, body any, timeout time.Duration, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var bodyReader io.Reader
	var contentType string
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
		contentType = "application/json"
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, fmt.Sprintf("%s/bot%s/%s", a.apiBase, a.token, method), bodyReader)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return a.doEnvelope(req, out)
}

func (a *apiClient) callMultipart(ctx context.Context, method string, fields map[string]string, fileField, fileName string, fileBytes []byte, timeout time.Duration, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return err
		}
	}
	part, err := mw.CreateFormFile(fileField, fileName)
	if err != nil {
		return err
	}
	if _, err := part.Write(fileBytes); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, fmt.Sprintf("%s/bot%s/%s", a.apiBase, a.token, method), &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return a.doEnvelope(req, out)
}

func (a *apiClient) doEnvelope(req *http.Request, out any) error {
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var env tgEnvelope
	_ = json.NewDecoder(resp.Body).Decode(&env)
	if !env.OK {
		desc := env.Description
		if desc == "" {
			desc = resp.Status
		}
		var retryAfter float64
		if env.Parameters != nil {
			retryAfter = env.Parameters.RetryAfter
		}
		return &tgError{
			status:      resp.StatusCode,
			description: desc,
			retryAfter:  retryAfter,
		}
	}
	if out != nil && len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, out); err != nil {
			return err
		}
	}
	return nil
}

type connection struct {
	imsdk.BaseConnection
	api         *apiClient
	token       string
	me          User
	pollTimeout time.Duration
	now         func() time.Time
	events      imsdk.ChannelEvents

	stopCtx   context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once

	permMu     sync.Mutex
	permAt     time.Time
	permCached []imsdk.BotPermission
	permValid  bool
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

func (c *connection) emitMembership(mc imsdk.MembershipChange) {
	if c.events.OnMembership != nil {
		c.events.OnMembership(mc)
	}
}

func (c *connection) poll() {
	defer close(c.done)
	var offset int64
	failures := 0
	pollSecs := int(c.pollTimeout / time.Second)
	httpTimeout := c.pollTimeout + 10*time.Second

	for c.stopCtx.Err() == nil {
		var updates []tgUpdate
		err := c.api.callJSON(c.stopCtx, "getUpdates", map[string]any{
			"offset":          offset,
			"timeout":         pollSecs,
			"allowed_updates": []string{"message", "my_chat_member"},
		}, httpTimeout, &updates)
		if err != nil {
			if c.stopCtx.Err() != nil {
				return
			}
			var te *tgError
			if errors.As(err, &te) {
				if te.status == http.StatusUnauthorized {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrCredentialsRevoked})
					return
				}
				if te.status == http.StatusConflict {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrUnknown})
					return
				}
			}
			failures++
			c.emitState(imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: failures})
			var wait time.Duration
			if te != nil && te.retryAfter > 0 {
				wait = time.Duration(te.retryAfter * float64(time.Second))
			} else {
				shift := min(failures, 5)
				wait = min(30*time.Second, time.Second*time.Duration(1<<shift))
			}
			if !sleepCtx(c.stopCtx, wait) {
				return
			}
			continue
		}

		if failures > 0 {
			c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			failures = 0
		}
		for _, upd := range updates {
			offset = upd.UpdateID + 1
			if upd.Message != nil {
				if inbound, ok := ToInbound(*upd.Message, c.me); ok {
					c.emitMessage(inbound)
				}
			} else if upd.MyChatMember != nil {
				st := upd.MyChatMember.NewChatMember.Status
				c.emitMembership(imsdk.MembershipChange{
					Chat:   chatRef(upd.MyChatMember.Chat, 0),
					Joined: st == "member" || st == "administrator",
				})
			}
		}
	}
}

func (c *connection) Permissions(ctx context.Context) ([]imsdk.BotPermission, error) {
	c.permMu.Lock()
	defer c.permMu.Unlock()

	now := c.now()
	if c.permValid && now.Sub(c.permAt) <= permissionsTTL {
		return slices.Clone(c.permCached), nil
	}

	var user User
	if err := c.api.callJSON(ctx, "getMe", nil, defaultCallTimeout, &user); err != nil {
		c.permAt = now
		c.permCached = []imsdk.BotPermission{}
		c.permValid = true
		return []imsdk.BotPermission{}, nil
	}
	granted := user.CanReadAllGroupMessages != nil && *user.CanReadAllGroupMessages
	perm := imsdk.BotPermission{
		ID:      imsdk.PermGroupMessages,
		Granted: granted,
	}
	if !granted {
		perm.Grant = &imsdk.PermissionGrant{
			URL:   botFatherURL,
			Steps: "telegram",
		}
	}
	c.permAt = now
	c.permCached = []imsdk.BotPermission{perm}
	c.permValid = true
	return slices.Clone(c.permCached), nil
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	chatID, threadID := parseChat(msg.Chat.PlatformID)
	textID := ""
	lastID := ""

	if msg.Text != "" {
		payload := map[string]any{
			"chat_id": chatID,
			"text":    msg.Text,
		}
		if threadID != 0 {
			payload["message_thread_id"] = threadID
		}
		if msg.ReplyTo != "" {
			if replyID, err := strconv.ParseInt(msg.ReplyTo, 10, 64); err == nil {
				payload["reply_parameters"] = map[string]any{
					"message_id":                  replyID,
					"allow_sending_without_reply": true,
				}
			}
		}
		if ents := MentionEntities(msg.Text, msg.Mentions); len(ents) > 0 {
			payload["entities"] = ents
		}
		var sent Message
		if err := c.api.callJSON(ctx, "sendMessage", payload, defaultCallTimeout, &sent); err != nil {
			return sendFailure(err), nil
		}
		lastID = strconv.FormatInt(sent.MessageID, 10)
		textID = lastID
	}

	for _, f := range msg.Files {
		data, err := os.ReadFile(f.Path)
		if err != nil {
			return imsdk.WithTextSent(imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrUnknown,
				Retryable: false,
				Detail:    err.Error(),
			}, textID), nil
		}
		fields := map[string]string{
			"chat_id": strconv.FormatInt(chatID, 10),
		}
		if threadID != 0 {
			fields["message_thread_id"] = strconv.FormatInt(threadID, 10)
		}
		if msg.ReplyTo != "" {
			if replyID, err := strconv.ParseInt(msg.ReplyTo, 10, 64); err == nil {
				rp, _ := json.Marshal(map[string]any{
					"message_id":                  replyID,
					"allow_sending_without_reply": true,
				})
				fields["reply_parameters"] = string(rp)
			}
		}
		name := f.Name
		if name == "" {
			name = filepath.Base(f.Path)
		}
		var sent Message
		if err := c.api.callMultipart(ctx, "sendDocument", fields, "document", name, data, fileCallTimeout, &sent); err != nil {
			return imsdk.WithTextSent(sendFailure(err), textID), nil
		}
		lastID = strconv.FormatInt(sent.MessageID, 10)
	}

	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: lastID,
		Degraded:   []imsdk.Degradation{imsdk.DegradedMarkdownFlattened},
	}, nil
}

func (c *connection) React(ctx context.Context, chat imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	chatID, _ := parseChat(chat.PlatformID)
	msgID, err := strconv.ParseInt(platformMsgID, 10, 64)
	if err != nil {
		return nil
	}
	var reaction []map[string]string
	switch state {
	case imsdk.ReactionProcessing:
		reaction = []map[string]string{{"type": "emoji", "emoji": "👀"}}
	case imsdk.ReactionError:
		reaction = []map[string]string{{"type": "emoji", "emoji": "👎"}}
	default:
		reaction = []map[string]string{}
	}
	err = c.api.callJSON(ctx, "setMessageReaction", map[string]any{
		"chat_id":    chatID,
		"message_id": msgID,
		"reaction":   reaction,
	}, defaultCallTimeout, nil)
	if err != nil && state == imsdk.ReactionProcessing {
		_ = c.SetTyping(ctx, chat, true)
	}
	return nil
}

func (c *connection) SetTyping(ctx context.Context, chat imsdk.ChatRef, typing bool) error {
	if !typing {
		return nil
	}
	chatID, threadID := parseChat(chat.PlatformID)
	payload := map[string]any{
		"chat_id": chatID,
		"action":  "typing",
	}
	if threadID != 0 {
		payload["message_thread_id"] = threadID
	}
	_ = c.api.callJSON(ctx, "sendChatAction", payload, defaultCallTimeout, nil)
	return nil
}

func (c *connection) StartLive(ctx context.Context, chat imsdk.ChatRef, opts imsdk.LiveOptions) (imsdk.LiveReply, error) {
	chatID, threadID := parseChat(chat.PlatformID)
	initial := plain(opts.Initial)
	payload := map[string]any{
		"chat_id": chatID,
		"text":    initial,
	}
	if threadID != 0 {
		payload["message_thread_id"] = threadID
	}
	if opts.ReplyTo != "" {
		if replyID, err := strconv.ParseInt(opts.ReplyTo, 10, 64); err == nil {
			payload["reply_parameters"] = map[string]any{
				"message_id":                  replyID,
				"allow_sending_without_reply": true,
			}
		}
	}
	var placeholder Message
	if err := c.api.callJSON(ctx, "sendMessage", payload, defaultCallTimeout, &placeholder); err != nil {
		return nil, err
	}
	return &liveReply{
		api:       c.api,
		chatID:    chatID,
		messageID: placeholder.MessageID,
		last:      initial,
	}, nil
}

type liveReply struct {
	mu        sync.Mutex
	api       *apiClient
	chatID    int64
	messageID int64
	last      string
}

func (l *liveReply) Until() time.Time { return time.Time{} }

func (l *liveReply) edit(ctx context.Context, text string, mentions []imsdk.Mention) error {
	payload := map[string]any{
		"chat_id":    l.chatID,
		"message_id": l.messageID,
		"text":       text,
	}
	if ents := MentionEntities(text, mentions); len(ents) > 0 {
		payload["entities"] = ents
	}
	return l.api.callJSON(ctx, "editMessageText", payload, defaultCallTimeout, nil)
}

func (l *liveReply) Update(ctx context.Context, markdown string) error {
	text := plain(markdown)
	l.mu.Lock()
	if text == l.last {
		l.mu.Unlock()
		return nil
	}
	l.last = text
	l.mu.Unlock()
	_ = l.edit(ctx, text, nil)
	return nil
}

func (l *liveReply) Finish(ctx context.Context, markdown string, mentions []imsdk.Mention) (imsdk.SendResult, error) {
	text := plain(markdown)
	l.mu.Lock()
	l.last = text
	l.mu.Unlock()
	if err := l.edit(ctx, text, mentions); err != nil {
		return sendFailure(err), nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: strconv.FormatInt(l.messageID, 10),
		Degraded:   []imsdk.Degradation{imsdk.DegradedMarkdownFlattened},
	}, nil
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	var fileInfo struct {
		FilePath string `json:"file_path"`
	}
	if err := c.api.callJSON(ctx, "getFile", map[string]any{"file_id": att.ID}, defaultCallTimeout, &fileInfo); err != nil {
		return err
	}
	if fileInfo.FilePath == "" {
		return errors.New("Telegram did not return a download path (files over 20 MB cannot be fetched by bots)")
	}
	reqCtx, cancel := context.WithTimeout(ctx, fileCallTimeout)
	defer cancel()

	dlURL := fmt.Sprintf("%s/file/bot%s/%s", c.api.apiBase, c.token, fileInfo.FilePath)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, dlURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.api.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o644)
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

// MentionEntities builds Telegram text_mention entities with UTF-16 code unit offsets and lengths.
func MentionEntities(text string, mentions []imsdk.Mention) []Entity {
	if len(mentions) == 0 {
		return nil
	}
	var numeric []imsdk.Mention
	for _, m := range mentions {
		if digitsPattern.MatchString(m.ID) {
			numeric = append(numeric, m)
		}
	}
	spots := mention.Spots(text, numeric)
	if len(spots) == 0 {
		return nil
	}
	out := make([]Entity, 0, len(spots))
	for _, spot := range spots {
		uid, err := strconv.ParseInt(spot.Mention.ID, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, Entity{
			Type:   "text_mention",
			Offset: utf16Len(text[:spot.Start]),
			Length: utf16Len(text[spot.Start:spot.End]),
			User:   &User{ID: uid},
		})
	}
	return out
}

// ToInbound converts a Telegram Message into an imsdk.InboundMessage.
func ToInbound(msg Message, me User) (imsdk.InboundMessage, bool) {
	if msg.From == nil || msg.Chat.Type == "channel" {
		return imsdk.InboundMessage{}, false
	}
	raw := msg.Text
	if raw == "" {
		raw = msg.Caption
	}
	entities := msg.Entities
	if len(entities) == 0 {
		entities = msg.CaptionEntities
	}
	username := strings.ToLower(me.Username)

	mentioned := false
	type byteSpan struct {
		start int
		end   int
	}
	var removals []byteSpan
	for _, e := range entities {
		startByte, endByte, ok := utf16SpanToBytes(raw, e.Offset, e.Length)
		if !ok {
			continue
		}
		slice := raw[startByte:endByte]
		isMe := (e.Type == "mention" && username != "" && strings.EqualFold(slice, "@"+username)) ||
			(e.Type == "text_mention" && e.User != nil && e.User.ID == me.ID)
		if isMe {
			mentioned = true
			removals = append(removals, byteSpan{start: startByte, end: endByte})
		}
	}
	slices.SortFunc(removals, func(a, b byteSpan) int {
		return cmp.Compare(b.start, a.start)
	})
	text := raw
	prevStart := len(raw) + 1
	for _, rem := range removals {
		if rem.end <= prevStart {
			text = text[:rem.start] + text[rem.end:]
			prevStart = rem.start
		}
	}

	if username != "" {
		trimmed := strings.TrimSpace(text)
		cmdRe := regexp.MustCompile(`(?i)^(/\w+)@` + regexp.QuoteMeta(username) + `\b`)
		if cmdRe.MatchString(trimmed) {
			mentioned = true
			text = cmdRe.ReplaceAllString(trimmed, "$1")
		}
	}
	text = strings.TrimSpace(multiSpaceRegex.ReplaceAllString(text, " "))

	repliedToMe := msg.ReplyToMessage != nil && msg.ReplyToMessage.From != nil && msg.ReplyToMessage.From.ID == me.ID
	var threadID int64
	if msg.IsTopicMessage {
		threadID = msg.MessageThreadID
	}
	chat := chatRef(msg.Chat, threadID)

	var attachments []imsdk.InboundAttachment
	addFile := func(f *File, fallbackName string) {
		if f == nil {
			return
		}
		name := f.FileName
		if name == "" {
			name = fallbackName
		}
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   f.FileID,
			Name: name,
			Size: f.FileSize,
			Mime: f.MimeType,
		})
	}
	addFile(msg.Document, "document")
	if len(msg.Photo) > 0 {
		best := msg.Photo[0]
		for _, p := range msg.Photo[1:] {
			if p.Width > best.Width {
				best = p
			}
		}
		photoFile := best.File
		photoFile.MimeType = "image/jpeg"
		addFile(&photoFile, fmt.Sprintf("photo-%d.jpg", msg.MessageID))
	}
	addFile(msg.Audio, fmt.Sprintf("audio-%d", msg.MessageID))
	addFile(msg.Voice, fmt.Sprintf("voice-%d.ogg", msg.MessageID))
	addFile(msg.Video, fmt.Sprintf("video-%d.mp4", msg.MessageID))

	replyTo := ""
	if msg.ReplyToMessage != nil && !isTopicRoot(msg) {
		replyTo = strconv.FormatInt(msg.ReplyToMessage.MessageID, 10)
	}

	return imsdk.InboundMessage{
		PlatformID:     strconv.FormatInt(msg.MessageID, 10),
		Chat:           chat,
		Sender:         imsdk.Sender{ID: strconv.FormatInt(msg.From.ID, 10), Name: displayName(*msg.From), IsBot: msg.From.IsBot},
		Text:           text,
		Attachments:    attachments,
		ReplyTo:        replyTo,
		AddressedToBot: chat.Kind == imsdk.ChatDM || mentioned || repliedToMe,
		Mentioned:      imsdk.BoolPtr(mentioned),
		SentAt:         time.Unix(msg.Date, 0).UTC(),
	}, true
}

func isTopicRoot(msg Message) bool {
	return msg.IsTopicMessage && msg.ReplyToMessage != nil && msg.ReplyToMessage.MessageID == msg.MessageThreadID
}

func identity(u User) imsdk.BotIdentity {
	name := u.FirstName
	if u.Username != "" {
		name = "@" + u.Username
	}
	return imsdk.BotIdentity{
		ID:   strconv.FormatInt(u.ID, 10),
		Name: name,
	}
}

func displayName(u User) string {
	var parts []string
	if u.FirstName != "" {
		parts = append(parts, u.FirstName)
	}
	if u.LastName != "" {
		parts = append(parts, u.LastName)
	}
	if len(parts) > 0 {
		return strings.Join(parts, " ")
	}
	if u.Username != "" {
		return u.Username
	}
	return strconv.FormatInt(u.ID, 10)
}

func chatRef(c Chat, threadID int64) imsdk.ChatRef {
	kind := imsdk.ChatGroup
	if c.Type == "private" {
		kind = imsdk.ChatDM
	} else if threadID != 0 {
		kind = imsdk.ChatThread
	}
	var name string
	if c.Type == "private" {
		var parts []string
		if c.FirstName != "" {
			parts = append(parts, c.FirstName)
		}
		if c.LastName != "" {
			parts = append(parts, c.LastName)
		}
		name = strings.Join(parts, " ")
	} else {
		name = c.Title
	}
	pid := strconv.FormatInt(c.ID, 10)
	if threadID != 0 {
		pid = fmt.Sprintf("%d:%d", c.ID, threadID)
	}
	return imsdk.ChatRef{
		PlatformID: pid,
		Kind:       kind,
		Name:       name,
	}
}

func parseChat(platformID string) (chatID int64, threadID int64) {
	chatPart, threadPart, hasThread := strings.Cut(platformID, ":")
	chatID, _ = strconv.ParseInt(chatPart, 10, 64)
	if hasThread && threadPart != "" {
		threadID, _ = strconv.ParseInt(threadPart, 10, 64)
	}
	return chatID, threadID
}

func utf16Units(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n += utf16Units(r)
	}
	return n
}

func utf16SpanToBytes(s string, u16Offset, u16Length int) (startByte, endByte int, ok bool) {
	if u16Offset < 0 || u16Length < 0 {
		return 0, 0, false
	}
	targetEnd := u16Offset + u16Length
	u16Pos := 0
	foundStart := false
	foundEnd := false

	for byteIdx, r := range s {
		if !foundStart && u16Pos == u16Offset {
			startByte = byteIdx
			foundStart = true
		}
		if u16Pos == targetEnd {
			endByte = byteIdx
			foundEnd = true
			break
		}
		if !foundStart && u16Pos > u16Offset {
			return 0, 0, false
		}
		if foundStart && u16Pos > targetEnd {
			return 0, 0, false
		}
		u16Pos += utf16Units(r)
	}
	if !foundStart && u16Pos == u16Offset {
		startByte = len(s)
		foundStart = true
	}
	if !foundEnd && u16Pos == targetEnd {
		endByte = len(s)
		foundEnd = true
	}
	return startByte, endByte, foundStart && foundEnd
}

func plain(markdown string) string {
	s := boldRegex.ReplaceAllString(markdown, "$1")
	return stripSingleStarItalic(s)
}

func stripSingleStarItalic(s string) string {
	if !strings.Contains(s, "*") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '*' && isStartOrSpaceBefore(s, i) && i+1 < len(s) && !isSpaceOrNewlineAt(s, i+1) {
			closeIdx := -1
			for j := i + 2; j < len(s); j++ {
				if s[j] == '\n' || s[j] == '\r' {
					break
				}
				if s[j] == '*' && isEndOrSpaceAt(s, j+1) {
					closeIdx = j
					break
				}
			}
			if closeIdx > i+1 {
				b.WriteString(s[i+1 : closeIdx])
				i = closeIdx + 1
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func isStartOrSpaceBefore(s string, idx int) bool {
	if idx == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:idx])
	return unicode.IsSpace(r)
}

func isEndOrSpaceAt(s string, idx int) bool {
	if idx >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[idx:])
	return unicode.IsSpace(r)
}

func isSpaceOrNewlineAt(s string, idx int) bool {
	if idx >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[idx:])
	return unicode.IsSpace(r) || r == '\n' || r == '\r'
}

func sendFailure(err error) imsdk.SendResult {
	var te *tgError
	if errors.As(err, &te) {
		switch {
		case te.status == http.StatusTooManyRequests:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrRateLimited, Retryable: true}
		case te.status == http.StatusForbidden:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false}
		case te.status == http.StatusRequestEntityTooLarge:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrTooLarge, Retryable: false}
		case te.status == http.StatusBadRequest && chatNotFoundRe.MatchString(te.description):
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrChatGone, Retryable: false}
		case te.status >= 500:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: true}
		default:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: false}
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
