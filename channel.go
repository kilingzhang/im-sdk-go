// Package imsdk defines the unified, platform-agnostic contract for outbound-only
// chat app adapters, capabilities, onboarding streams, and live replies.
package imsdk

import (
	"context"
	"errors"
	"maps"
	"time"
)

// ErrUnsupported indicates that a channel or live connection does not support an optional capability.
var ErrUnsupported = errors.New("imsdk: capability unsupported")

// Platform identifies a supported chat application.
type Platform string

const (
	PlatformWeChat   Platform = "wechat"
	PlatformWeCom    Platform = "wecom"
	PlatformFeishu   Platform = "feishu"
	PlatformDingTalk Platform = "dingtalk"
	PlatformQQ       Platform = "qq"
	PlatformSlack    Platform = "slack"
	PlatformDiscord  Platform = "discord"
	PlatformTelegram Platform = "telegram"
	PlatformGitHub   Platform = "github"
	PlatformIMessage Platform = "imessage"
)

// ChatKind classifies a conversation container.
type ChatKind string

const (
	ChatDM     ChatKind = "dm"
	ChatGroup  ChatKind = "group"
	ChatThread ChatKind = "thread"
)

// Trust describes who can reach a bot on a platform by default.
type Trust string

const (
	TrustOrg    Trust = "org"
	TrustPublic Trust = "public"
	TrustOwner  Trust = "owner"
)

// Capabilities reports what a connected bot instance can perform right now.
type Capabilities struct {
	QuoteReply      bool
	Reactions       bool
	TypingIndicator bool
	FileUpload      bool
	FileDownload    bool
	ListMembers     bool
	Mentions        bool
	MaxTextLength   int
}

// OnboardingKind describes how a user connects a bot on a platform.
type OnboardingKind string

const (
	OnboardingQR          OnboardingKind = "qr"
	OnboardingDevice      OnboardingKind = "device"
	OnboardingCredentials OnboardingKind = "credentials"
)

// CredentialField describes one user-supplied credential input.
type CredentialField struct {
	Key      string
	LabelKey string
	Secret   bool
}

// OnboardingMethod drives UI or CLI setup flows for a platform.
type OnboardingMethod struct {
	Kind        OnboardingKind
	DesktopLink bool
	Fields      []CredentialField
	SetupURL    string
}

// SetupState reports the status of one local prerequisite check.
type SetupState string

const (
	SetupOK      SetupState = "ok"
	SetupMissing SetupState = "missing"
	SetupDenied  SetupState = "denied"
	SetupUnknown SetupState = "unknown"
)

// SetupCheck reports one prerequisite check without triggering OS prompts.
type SetupCheck struct {
	ID    string
	State SetupState
}

// SetupStatus aggregates all local setup checks and the responsible host application name.
type SetupStatus struct {
	Checks []SetupCheck
	App    string
}

// ChannelSetup checks and triggers local host prerequisites (such as macOS Messages permissions).
type ChannelSetup interface {
	Actions() []string
	Check(ctx context.Context) (SetupStatus, error)
	Act(ctx context.Context, action string) error
}

// OnboardingEventKind identifies a step in an onboarding stream.
type OnboardingEventKind string

const (
	EventQRReady          OnboardingEventKind = "qr_ready"
	EventScanned          OnboardingEventKind = "scanned"
	EventDeviceCode       OnboardingEventKind = "device_code"
	EventNeedsCode        OnboardingEventKind = "needs_code"
	EventAwaitingApproval OnboardingEventKind = "awaiting_approval"
	EventSucceeded        OnboardingEventKind = "succeeded"
	EventFailed           OnboardingEventKind = "failed"
)

// OnboardingFailure classifies why an onboarding attempt ended without connecting.
type OnboardingFailure string

const (
	FailureExpired            OnboardingFailure = "expired"
	FailureDenied             OnboardingFailure = "denied"
	FailureCancelled          OnboardingFailure = "cancelled"
	FailureInvalidCredentials OnboardingFailure = "invalid_credentials"
	FailureInsufficientScope  OnboardingFailure = "insufficient_scope"
	FailureNetwork            OnboardingFailure = "network"
	FailureAlreadyBound       OnboardingFailure = "already_bound"
	FailureAlreadyConnected   OnboardingFailure = "already_connected"
	FailurePermissionMissing  OnboardingFailure = "permission_missing"
	FailureNotSignedIn        OnboardingFailure = "not_signed_in"
	FailureUnknown            OnboardingFailure = "unknown"
)

// Credentials holds opaque per-connection secret key-value pairs.
type Credentials map[string]string

// Clone returns a shallow copy of c.
func (c Credentials) Clone() Credentials {
	if c == nil {
		return nil
	}
	return maps.Clone(c)
}

// BotIdentity identifies a connected bot account on a platform.
type BotIdentity struct {
	ID       string
	Name     string
	SenderID string
}

// OnboardingEvent represents one progress update emitted during Channel.Onboard.
type OnboardingEvent struct {
	Kind            OnboardingEventKind
	QRContent       string
	UserCode        string
	VerificationURI string
	ExpiresAt       time.Time
	Retry           bool
	DetailKey       string
	Credentials     Credentials
	Bot             BotIdentity
	OwnerID         string
	PendingApproval bool
	Code            OnboardingFailure
}

// PermissionID names an optional or platform-toggleable bot permission.
type PermissionID string

const (
	PermGroupMessages  PermissionID = "group_messages"
	PermBotMessages    PermissionID = "bot_messages"
	PermMemberProfiles PermissionID = "member_profiles"
)

// PermissionGrant describes where a user enables a platform-side permission.
type PermissionGrant struct {
	URL   string
	Steps string
}

// BotPermission describes the live grant status of one bot permission.
type BotPermission struct {
	ID       PermissionID
	Granted  bool
	Optional bool
	Grant    *PermissionGrant
}

// OnboardingOptions supplies optional callbacks and context to Channel.Onboard.
type OnboardingOptions struct {
	AgentName        string
	AwaitCode        func(ctx context.Context) (string, error)
	KnownCredentials func(ctx context.Context) ([]Credentials, error)
	Reauthorize      Credentials
	Adding           []PermissionID
}

// ConnectionStateKind classifies the runtime state of a ChannelConnection.
type ConnectionStateKind string

const (
	StateConnecting       ConnectionStateKind = "connecting"
	StateConnected        ConnectionStateKind = "connected"
	StateReconnecting     ConnectionStateKind = "reconnecting"
	StateAwaitingApproval ConnectionStateKind = "awaiting_approval"
	StateError            ConnectionStateKind = "error"
)

// ConnectionErrorCode classifies connection failures into user-actionable vs transient categories.
type ConnectionErrorCode string

const (
	ConnErrCredentialsRevoked ConnectionErrorCode = "credentials_revoked"
	ConnErrPermissionMissing  ConnectionErrorCode = "permission_missing"
	ConnErrCredentialsMissing ConnectionErrorCode = "credentials_missing"
	ConnErrNetwork            ConnectionErrorCode = "network"
	ConnErrUnknown            ConnectionErrorCode = "unknown"
)

// ConnectionState reports a transition in a live connection's health.
type ConnectionState struct {
	Kind    ConnectionStateKind
	Attempt int
	Code    ConnectionErrorCode
}

// ParentChat links a thread back to its parent group or channel.
type ParentChat struct {
	PlatformID string
	Name       string
}

// ChatRef addresses a DM, group, or thread on a platform.
type ChatRef struct {
	PlatformID string
	Kind       ChatKind
	Name       string
	Parent     *ParentChat
	// FollowUp marks a thread that represents a single conversation (Slack/Discord thread):
	// once the bot has spoken in it, subsequent human messages wake the bot without an explicit @.
	FollowUp bool
}

// Sender identifies the author of an inbound message or a chat member.
type Sender struct {
	ID    string
	Name  string
	IsBot bool
}

// InboundAttachment describes a lazy-fetched attachment handle on an inbound message.
type InboundAttachment struct {
	ID   string
	Name string
	Size int64
	Mime string
}

// InboundMessage is the normalized representation of an incoming platform message.
type InboundMessage struct {
	PlatformID     string
	Chat           ChatRef
	Sender         Sender
	Text           string
	Attachments    []InboundAttachment
	ReplyTo        string
	AddressedToBot bool
	// Mentioned is non-nil when the adapter can distinguish an explicit @mention in message text
	// from a reply or DM. When nil, callers treat a group message with AddressedToBot=true as mentioned.
	Mentioned *bool
	Mentions  []Sender
	SentAt    time.Time
	// Earlier marks historical messages backfilled when joining a group (stored as background, never answered).
	Earlier bool
}

// BoolPtr returns a pointer to v.
func BoolPtr(v bool) *bool { return &v }

// Mention maps a written @name to a platform user ID.
type Mention struct {
	ID   string
	Name string
}

// OutboundFile names a local file to attach to an outbound message.
type OutboundFile struct {
	Path string
	Name string
}

// OutboundMessage is a normalized message to deliver through a ChannelConnection.
type OutboundMessage struct {
	Chat     ChatRef
	Text     string
	ReplyTo  string
	Files    []OutboundFile
	Mentions []Mention
}

// Degradation reports a non-fatal capability fallback during Send.
type Degradation string

const (
	DegradedQuoteDropped      Degradation = "quote_dropped"
	DegradedFilesDropped      Degradation = "files_dropped"
	DegradedMarkdownFlattened Degradation = "markdown_flattened"
	DegradedTextSplit         Degradation = "text_split"
	DegradedMentionsPlain     Degradation = "mentions_plain"
)

// SendOutcome classifies the result of an outbound send attempt.
type SendOutcome string

const (
	OutcomeSent      SendOutcome = "sent"
	OutcomeFailed    SendOutcome = "failed"
	OutcomeUncertain SendOutcome = "uncertain"
)

// SendFailureCode classifies why a platform rejected an outbound send.
type SendFailureCode string

const (
	SendErrRateLimited SendFailureCode = "rate_limited"
	SendErrForbidden   SendFailureCode = "forbidden"
	SendErrTooLarge    SendFailureCode = "too_large"
	SendErrChatGone    SendFailureCode = "chat_gone"
	SendErrNetwork     SendFailureCode = "network"
	SendErrUnknown     SendFailureCode = "unknown"
)

// SendResult describes the outcome of sending or finishing a live reply.
type SendResult struct {
	Outcome    SendOutcome
	PlatformID string
	Degraded   []Degradation
	// Chat carries the resolved chat ID when sending to a user ID creates or reveals the DM chat ID.
	Chat string

	Code      SendFailureCode
	Retryable bool
	Detail    string
	// TextSentPlatformID is non-empty when text succeeded before an attachment upload failed,
	// instructing callers to retry only the files so the recipient never sees duplicate text.
	TextSentPlatformID string
}

// WithTextSent annotates a failed SendResult with the already-delivered text message ID.
func WithTextSent(res SendResult, textPlatformID string) SendResult {
	if textPlatformID != "" && res.Outcome == OutcomeFailed {
		res.TextSentPlatformID = textPlatformID
	}
	return res
}

// MembershipChange reports that the bot joined or left a chat.
type MembershipChange struct {
	Chat   ChatRef
	Joined bool
}

// ChannelEvents receives asynchronous inbound events from a live connection.
type ChannelEvents struct {
	OnMessage    func(InboundMessage)
	OnState      func(ConnectionState)
	OnMembership func(MembershipChange)
}

// ReactionState represents the lifecycle emoji/indicator placed on an inbound message.
type ReactionState string

const (
	ReactionProcessing ReactionState = "processing"
	ReactionDone       ReactionState = "done"
	ReactionError      ReactionState = "error"
	ReactionNone       ReactionState = "none"
)

// LiveOptions configures an in-place editable or streaming progress reply.
type LiveOptions struct {
	ReplyTo string
	Mention string
	Initial string
}

// LiveReply controls an open streaming card or editable progress message.
type LiveReply interface {
	// Until returns the deadline at which the platform stops accepting updates, or zero if unbounded.
	Until() time.Time
	Update(ctx context.Context, markdown string) error
	Finish(ctx context.Context, markdown string, mentions []Mention) (SendResult, error)
}

// DirectMessageReceipt reports where a proactive first message to a user landed.
type DirectMessageReceipt struct {
	Chat       string
	PlatformID string
}

// Channel is the stateless per-platform factory for onboarding and connecting bots.
type Channel interface {
	Platform() Platform
	Trust() Trust
	Onboarding() OnboardingMethod
	ManualFields() []CredentialField
	ManualSetupURL() string
	Setup() ChannelSetup
	Reauthorize() bool
	InviteURL(bot BotIdentity) string
	GroupMessages() bool
	DirectMessages() bool
	ClaimURL(bot BotIdentity, code string) string

	Onboard(ctx context.Context, input Credentials, opts OnboardingOptions) <-chan OnboardingEvent
	Connect(ctx context.Context, creds Credentials, events ChannelEvents) (ChannelConnection, error)
}

// ChannelConnection represents one active bot connection on a platform.
type ChannelConnection interface {
	Bot() BotIdentity
	OwnerOnly() bool
	Capabilities() Capabilities

	Send(ctx context.Context, msg OutboundMessage) (SendResult, error)
	React(ctx context.Context, chat ChatRef, platformMsgID string, state ReactionState) error
	StartLive(ctx context.Context, chat ChatRef, opts LiveOptions) (LiveReply, error)
	SetTyping(ctx context.Context, chat ChatRef, typing bool) error
	Download(ctx context.Context, att InboundAttachment, toPath string) error
	MessageUser(ctx context.Context, userID string, text string) (*DirectMessageReceipt, error)
	OpenDirect(ctx context.Context, userID string) (string, error)
	ListMembers(ctx context.Context, chat ChatRef) ([]Sender, error)
	Permissions(ctx context.Context) ([]BotPermission, error)
	Profile(ctx context.Context, userID string) (string, error)
	Close() error
}

// BaseChannel provides sensible default metadata methods so adapters only override what applies.
type BaseChannel struct {
	PlatformValue       Platform
	TrustValue          Trust
	OnboardingValue     OnboardingMethod
	ManualFieldsValue   []CredentialField
	ManualSetupURLValue string
	SetupValue          ChannelSetup
	ReauthorizeValue    bool
	GroupMessagesValue  bool
	NoDirectMessages    bool
}

func (b BaseChannel) Platform() Platform              { return b.PlatformValue }
func (b BaseChannel) Onboarding() OnboardingMethod    { return b.OnboardingValue }
func (b BaseChannel) ManualFields() []CredentialField { return b.ManualFieldsValue }
func (b BaseChannel) ManualSetupURL() string          { return b.ManualSetupURLValue }
func (b BaseChannel) Setup() ChannelSetup             { return b.SetupValue }
func (b BaseChannel) Reauthorize() bool               { return b.ReauthorizeValue }
func (b BaseChannel) InviteURL(BotIdentity) string    { return "" }
func (b BaseChannel) GroupMessages() bool             { return b.GroupMessagesValue }
func (b BaseChannel) DirectMessages() bool            { return !b.NoDirectMessages }
func (b BaseChannel) ClaimURL(BotIdentity, string) string {
	return ""
}
func (b BaseChannel) Trust() Trust {
	if b.TrustValue == "" {
		return TrustPublic
	}
	return b.TrustValue
}

// BaseConnection provides default no-op / ErrUnsupported implementations for optional connection methods.
type BaseConnection struct {
	BotValue          BotIdentity
	OwnerOnlyValue    bool
	CapabilitiesValue Capabilities
}

func (b BaseConnection) Bot() BotIdentity           { return b.BotValue }
func (b BaseConnection) OwnerOnly() bool            { return b.OwnerOnlyValue }
func (b BaseConnection) Capabilities() Capabilities { return b.CapabilitiesValue }
func (b BaseConnection) React(context.Context, ChatRef, string, ReactionState) error {
	return nil
}
func (b BaseConnection) StartLive(context.Context, ChatRef, LiveOptions) (LiveReply, error) {
	return nil, ErrUnsupported
}
func (b BaseConnection) SetTyping(context.Context, ChatRef, bool) error { return nil }
func (b BaseConnection) MessageUser(context.Context, string, string) (*DirectMessageReceipt, error) {
	return nil, ErrUnsupported
}
func (b BaseConnection) OpenDirect(context.Context, string) (string, error) {
	return "", ErrUnsupported
}
func (b BaseConnection) ListMembers(context.Context, ChatRef) ([]Sender, error) {
	return nil, nil
}
func (b BaseConnection) Permissions(context.Context) ([]BotPermission, error) {
	return nil, nil
}
func (b BaseConnection) Profile(context.Context, string) (string, error) {
	return "", nil
}
