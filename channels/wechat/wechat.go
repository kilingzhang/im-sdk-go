// Package wechat implements the Personal WeChat channel adapter over Tencent's iLink 2.4.6 bot service.
package wechat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

var (
	defaultCapabilities = imsdk.Capabilities{
		QuoteReply:      false,
		Reactions:       false,
		TypingIndicator: true,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     false,
		Mentions:        false,
		MaxTextLength:   MaxTextLength,
	}

	degradationOrder = []imsdk.Degradation{
		imsdk.DegradedMarkdownFlattened,
		imsdk.DegradedQuoteDropped,
		imsdk.DegradedFilesDropped,
	}

	imageExtensions = map[string]struct{}{
		".png":  {},
		".jpg":  {},
		".jpeg": {},
		".gif":  {},
		".webp": {},
		".bmp":  {},
	}
)

// Options configures a WeChat iLink channel adapter.
type Options struct {
	HTTPClient      *http.Client
	BotAgent        string
	RetryDelay      func(attempt int) time.Duration
	LongPollTimeout time.Duration
	RequestTimeout  time.Duration
	TransferTimeout time.Duration
	Now             func() time.Time
}

type channel struct {
	imsdk.BaseChannel
	api        *apiClient
	retryDelay func(attempt int) time.Duration
	nowFn      func() time.Time
}

// New creates a stateless Personal WeChat channel adapter.
func New(opts Options) imsdk.Channel {
	retryFn := opts.RetryDelay
	if retryFn == nil {
		retryFn = defaultRetryDelay
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:   imsdk.PlatformWeChat,
			TrustValue:      imsdk.TrustOwner,
			OnboardingValue: imsdk.OnboardingMethod{Kind: imsdk.OnboardingQR},
		},
		api:        newAPIClient(opts),
		retryDelay: retryFn,
		nowFn:      nowFn,
	}
}

func defaultRetryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return 2 * time.Second
	}
	if attempt >= 5 {
		return 30 * time.Second
	}
	d := (2 * time.Second) << (attempt - 1)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (c *channel) Onboard(ctx context.Context, _ imsdk.Credentials, opts imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 8)
	go func() {
		defer close(ch)
		emit := func(ev imsdk.OnboardingEvent) bool {
			select {
			case ch <- ev:
				return true
			case <-ctx.Done():
				return false
			}
		}

		var localTokens []string
		if opts.KnownCredentials != nil {
			if known, err := opts.KnownCredentials(ctx); err == nil {
				for _, cred := range known {
					if tok := strings.TrimSpace(cred["token"]); tok != "" {
						localTokens = append(localTokens, tok)
					}
				}
			}
		}

		login, err := c.api.beginLogin(ctx, localTokens)
		if err != nil {
			code := imsdk.FailureNetwork
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}

		expiresAt := c.nowFn().Add(qrLifetime)
		if !emit(imsdk.OnboardingEvent{
			Kind:      imsdk.EventQRReady,
			QRContent: login.URL,
			ExpiresAt: expiresAt,
		}) {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
			return
		}

		base := QRBaseURL
		verifyCode := ""
		codesTried := 0
		scanned := false

		for ctx.Err() == nil {
			if c.nowFn().After(expiresAt) {
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
				return
			}
			state, err := c.api.pollLogin(ctx, base, login.QRCode, verifyCode)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				if isTimeout(err) {
					continue
				}
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
				return
			}
			verifyCode = ""

			switch state.Status {
			case "wait":
			case "scaned_but_redirect":
				nextBase, err := TrustedBaseURL(state.RedirectHost, base)
				if err != nil {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
					return
				}
				base = nextBase
				if !scanned {
					if !emit(imsdk.OnboardingEvent{Kind: imsdk.EventScanned}) {
						ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
						return
					}
					scanned = true
				}
			case "scaned":
				if !scanned {
					if !emit(imsdk.OnboardingEvent{Kind: imsdk.EventScanned}) {
						ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
						return
					}
					scanned = true
				}
			case "need_verifycode":
				if opts.AwaitCode == nil {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
					return
				}
				if !emit(imsdk.OnboardingEvent{Kind: imsdk.EventNeedsCode, Retry: codesTried > 0}) {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
					return
				}
				codeStr, err := opts.AwaitCode(ctx)
				if err != nil {
					failCode := imsdk.FailureUnknown
					if ctx.Err() != nil {
						failCode = imsdk.FailureCancelled
					}
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: failCode}
					return
				}
				verifyCode = strings.TrimSpace(codeStr)
				codesTried++
			case "verify_code_blocked":
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureDenied}
				return
			case "expired":
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
				return
			case "binded_redirect":
				code := imsdk.FailureAlreadyBound
				if len(localTokens) > 0 {
					code = imsdk.FailureAlreadyConnected
				}
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
				return
			case "confirmed":
				if state.BotToken == "" || state.ILinkBotID == "" || state.ILinkUserID == "" {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
					return
				}
				finalBase, err := TrustedBaseURL(state.BaseURL, base)
				if err != nil {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
					return
				}
				name := opts.AgentName
				if name == "" {
					name = "WeChat"
				}
				ch <- imsdk.OnboardingEvent{
					Kind: imsdk.EventSucceeded,
					Credentials: imsdk.Credentials{
						"token":   state.BotToken,
						"botId":   state.ILinkBotID,
						"baseUrl": finalBase,
						"name":    name,
					},
					Bot:     imsdk.BotIdentity{ID: state.ILinkBotID, Name: name},
					OwnerID: state.ILinkUserID,
				}
				return
			default:
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
				return
			}
		}
		ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
	}()
	return ch
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	token := creds["token"]
	base, err := TrustedBaseURL(creds["baseUrl"], QRBaseURL)
	if err != nil {
		return nil, err
	}
	botID := creds["botId"]
	if botID == "" {
		botID = "wechat"
	}
	botName := creds["name"]
	if botName == "" {
		botName = "WeChat"
	}

	if code, notifyErr := c.api.notify(ctx, base, token, "start"); notifyErr == nil && code == "-14" {
		if events.OnState != nil {
			events.OnState(imsdk.ConnectionState{
				Kind: imsdk.StateError,
				Code: imsdk.ConnErrCredentialsRevoked,
			})
		}
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          imsdk.BotIdentity{ID: botID, Name: botName},
			CapabilitiesValue: defaultCapabilities,
		},
		api:        c.api,
		base:       base,
		token:      token,
		events:     events,
		retryDelay: c.retryDelay,
		nowFn:      c.nowFn,
		stopCancel: stopCancel,
		doneCh:     make(chan struct{}),
		contexts:   make(map[string]string),
		media:      make(map[string]MediaHolder),
	}

	go conn.pollLoop(stopCtx)
	return conn, nil
}

type connection struct {
	imsdk.BaseConnection
	api        *apiClient
	base       string
	token      string
	events     imsdk.ChannelEvents
	retryDelay func(attempt int) time.Duration
	nowFn      func() time.Time

	stopCancel context.CancelFunc
	closeOnce  sync.Once
	doneCh     chan struct{}

	mu         sync.RWMutex
	contexts   map[string]string
	media      map[string]MediaHolder
	mediaOrder []string
}

func (c *connection) contextToken(userID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.contexts[userID]
}

func (c *connection) recordInbound(senderID, ctxToken, platformID string, items []Item) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctxToken != "" {
		c.contexts[senderID] = ctxToken
	}
	for i, it := range items {
		var holder *MediaHolder
		if it.ImageItem != nil {
			holder = &MediaHolder{Media: it.ImageItem.Media, AESKey: it.ImageItem.AESKey}
		} else if it.FileItem != nil {
			holder = &MediaHolder{Media: it.FileItem.Media, AESKey: it.FileItem.AESKey}
		}
		if holder != nil {
			key := fmt.Sprintf("%s#%d", platformID, i)
			if _, exists := c.media[key]; !exists {
				c.mediaOrder = append(c.mediaOrder, key)
			}
			c.media[key] = *holder
		}
	}
	for len(c.mediaOrder) > MaxCachedMedia {
		oldest := c.mediaOrder[0]
		c.mediaOrder = c.mediaOrder[1:]
		delete(c.media, oldest)
	}
}

func (c *connection) getMedia(id string) (MediaHolder, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h, ok := c.media[id]
	return h, ok
}

func (c *connection) pollLoop(ctx context.Context) {
	defer close(c.doneCh)
	failures := 0
	cursor := ""

	for ctx.Err() == nil {
		resp, err := c.api.getUpdates(ctx, c.base, c.token, cursor)
		if err == nil {
			code := rejectedCode(resp.Ret, resp.ErrCode)
			if code == "-14" {
				if c.events.OnState != nil {
					c.events.OnState(imsdk.ConnectionState{
						Kind: imsdk.StateError,
						Code: imsdk.ConnErrCredentialsRevoked,
					})
				}
				return
			}
			if code != "" {
				err = &Error{Code: "updates_rejected", Message: "WeChat refused the update request", ProviderCode: code}
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			if c.events.OnState != nil {
				c.events.OnState(imsdk.ConnectionState{
					Kind:    imsdk.StateReconnecting,
					Attempt: failures,
				})
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.retryDelay(failures)):
			}
			continue
		}

		if failures > 0 && c.events.OnState != nil {
			c.events.OnState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
		}
		failures = 0
		if resp.GetUpdatesBuf != "" {
			cursor = resp.GetUpdatesBuf
		}
		for _, m := range resp.Msgs {
			inbound, ok := toInboundAt(m, c.nowFn)
			if !ok {
				continue
			}
			c.recordInbound(inbound.Sender.ID, m.ContextToken, inbound.PlatformID, m.ItemList)
			if c.events.OnMessage != nil {
				c.events.OnMessage(inbound)
			}
		}
	}
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	if err := ctx.Err(); err != nil {
		return imsdk.SendResult{}, err
	}
	to := msg.Chat.PlatformID
	degradedSet := make(map[imsdk.Degradation]bool, 3)
	if msg.ReplyTo != "" {
		degradedSet[imsdk.DegradedQuoteDropped] = true
	}
	report := func() []imsdk.Degradation {
		var out []imsdk.Degradation
		for _, d := range degradationOrder {
			if degradedSet[d] {
				out = append(out, d)
			}
		}
		return out
	}

	sentAny := false
	handleErr := func(err error) (imsdk.SendResult, error) {
		if isTimeout(err) {
			return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}, nil
		}
		if sentAny {
			degradedSet[imsdk.DegradedFilesDropped] = true
			return imsdk.SendResult{
				Outcome:    imsdk.OutcomeSent,
				PlatformID: "out-" + randomUUID(),
				Degraded:   report(),
			}, nil
		}
		var we *Error
		if errors.As(err, &we) {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrUnknown,
				Retryable: true,
				Detail:    we.ProviderCode,
			}, nil
		}
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrNetwork,
			Retryable: true,
		}, nil
	}

	if strings.TrimSpace(msg.Text) != "" {
		code, err := c.api.sendText(ctx, c.base, c.token, to, msg.Text, c.contextToken(to))
		if err != nil {
			return handleErr(err)
		}
		if code == "-14" {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrForbidden,
				Retryable: false,
				Detail:    code,
			}, nil
		}
		if code != "" {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrUnknown,
				Retryable: false,
				Detail:    code,
			}, nil
		}
		degradedSet[imsdk.DegradedMarkdownFlattened] = true
		sentAny = true
	}

	for _, f := range msg.Files {
		st, err := os.Stat(f.Path)
		if err != nil {
			return handleErr(err)
		}
		if st.Size() == 0 || st.Size() > MaxFileBytes {
			degradedSet[imsdk.DegradedFilesDropped] = true
			continue
		}
		name := f.Name
		if name == "" {
			name = filepath.Base(f.Path)
		}
		rawBytes, err := os.ReadFile(f.Path)
		if err != nil {
			return handleErr(err)
		}
		_, isImg := imageExtensions[strings.ToLower(filepath.Ext(name))]
		item, err := c.api.upload(ctx, c.base, c.token, to, rawBytes, name, isImg)
		if err != nil {
			return handleErr(err)
		}
		code, err := c.api.sendItem(ctx, c.base, c.token, to, item, c.contextToken(to))
		if err != nil {
			return handleErr(err)
		}
		if code == "-14" {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrForbidden,
				Retryable: false,
				Detail:    code,
			}, nil
		}
		if code != "" {
			degradedSet[imsdk.DegradedFilesDropped] = true
			continue
		}
		sentAny = true
	}

	if !sentAny {
		failCode := imsdk.SendErrUnknown
		if len(msg.Files) > 0 {
			failCode = imsdk.SendErrTooLarge
		}
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      failCode,
			Retryable: false,
		}, nil
	}

	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: "out-" + randomUUID(),
		Degraded:   report(),
	}, nil
}

func (c *connection) React(ctx context.Context, chat imsdk.ChatRef, _ string, state imsdk.ReactionState) error {
	on := state == imsdk.ReactionProcessing
	_ = c.api.typing(ctx, c.base, c.token, chat.PlatformID, c.contextToken(chat.PlatformID), on)
	return nil
}

func (c *connection) SetTyping(ctx context.Context, chat imsdk.ChatRef, typing bool) error {
	_ = c.api.typing(ctx, c.base, c.token, chat.PlatformID, c.contextToken(chat.PlatformID), typing)
	return nil
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	item, ok := c.getMedia(att.ID)
	if !ok {
		return errors.New("this file is no longer available; ask for it to be sent again")
	}
	plaintext, err := c.api.downloadMedia(ctx, item)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, plaintext, 0o644)
}

func (c *connection) Close() error {
	c.closeOnce.Do(func() {
		c.stopCancel()
		<-c.doneCh
		stopCtx, cancel := context.WithTimeout(context.Background(), defaultControlTimeout)
		defer cancel()
		_, _ = c.api.notify(stopCtx, c.base, c.token, "stop")
	})
	return nil
}

// ToInbound converts a raw iLink Message into an imsdk.InboundMessage, returning false for self-echoes or invalid messages.
func ToInbound(msg Message) (imsdk.InboundMessage, bool) {
	return toInboundAt(msg, time.Now)
}

func toInboundAt(msg Message, nowFn func() time.Time) (imsdk.InboundMessage, bool) {
	if msg.MessageType == 2 {
		return imsdk.InboundMessage{}, false
	}
	sender := strings.TrimSpace(msg.FromUserID)
	id := msg.messageIDString()
	if sender == "" || id == "" {
		return imsdk.InboundMessage{}, false
	}

	var parts []string
	var attachments []imsdk.InboundAttachment
	for i, item := range msg.ItemList {
		if item.Type == 1 && item.TextItem != nil && strings.TrimSpace(item.TextItem.Text) != "" {
			parts = append(parts, strings.TrimSpace(item.TextItem.Text))
		} else if item.Type == 3 && item.VoiceItem != nil && strings.TrimSpace(item.VoiceItem.Text) != "" {
			parts = append(parts, strings.TrimSpace(item.VoiceItem.Text))
		} else if item.Type == 5 {
			parts = append(parts, "[video]")
		} else if item.Type == 2 && (item.ImageItem == nil || item.ImageItem.Media == nil) {
			parts = append(parts, "[image]")
		} else if item.ImageItem != nil {
			name := "image.jpg"
			if len(attachments) > 0 {
				name = fmt.Sprintf("image-%d.jpg", len(attachments)+1)
			}
			attachments = append(attachments, imsdk.InboundAttachment{
				ID:   fmt.Sprintf("%s#%d", id, i),
				Name: name,
				Mime: "image/jpeg",
			})
		} else if item.FileItem != nil {
			fname := strings.TrimSpace(item.FileItem.FileName)
			if fname == "" {
				fname = "file"
			}
			var size int64
			if parsed, err := strconv.ParseInt(strings.TrimSpace(item.FileItem.Len), 10, 64); err == nil && parsed > 0 {
				size = parsed
			}
			attachments = append(attachments, imsdk.InboundAttachment{
				ID:   fmt.Sprintf("%s#%d", id, i),
				Name: fname,
				Size: size,
			})
		}
	}

	var sentAt time.Time
	if msg.CreateTimeMs > 0 {
		sentAt = time.UnixMilli(msg.CreateTimeMs)
	} else {
		sentAt = nowFn()
	}

	return imsdk.InboundMessage{
		PlatformID:     id,
		Chat:           imsdk.ChatRef{PlatformID: sender, Kind: imsdk.ChatDM},
		Sender:         imsdk.Sender{ID: sender, Name: "", IsBot: false},
		Text:           strings.Join(parts, "\n"),
		Attachments:    attachments,
		AddressedToBot: true,
		SentAt:         sentAt,
	}, true
}
