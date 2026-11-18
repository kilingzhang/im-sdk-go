// Package imessage implements the macOS Messages self-chat channel adapter.
// On darwin it polls ~/Library/Messages/chat.db read-only via purego libsqlite3
// and sends via AppleScript; on non-darwin platforms New returns an unsupported stub.
package imessage

import (
	"bytes"
	"context"
	"encoding/binary"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	// AgentMark is appended to every message sent by the agent (zero-width space + word joiner)
	// so self-chat polling never mistakes outbound replies for owner input.
	AgentMark = "\u200b\u2060"

	// AppleEpochMs is the Unix millisecond timestamp of 2001-01-01T00:00:00Z.
	AppleEpochMs int64 = 978_307_200_000
)

var (
	accountHandleRe = regexp.MustCompile(`@|^\+?\d{5,}$`)
	prefixHandleRe  = regexp.MustCompile(`(?i)^[ep]:`)
	fencedCodeRe    = regexp.MustCompile("(?s)```[^\\n]*\\n(.*?)```")
	boldRe          = regexp.MustCompile(`\*\*(.+?)\*\*`)
	inlineCodeRe    = regexp.MustCompile("`([^`\\n]+)`")
	linkRe          = regexp.MustCompile(`\[([^\]]+)\]\((\S+?)\)`)
	headingRe       = regexp.MustCompile(`(?m)^#{1,6}\s+`)
)

// ScriptRunner executes an AppleScript with positional arguments and a timeout.
type ScriptRunner func(ctx context.Context, script string, args []string, timeout time.Duration) (string, error)

// ScriptError represents an AppleScript execution or timeout failure.
type ScriptError struct {
	Message  string
	TimedOut bool
}

func (e *ScriptError) Error() string {
	return e.Message
}

// SetupProbe overrides host inspection and system actions for ChannelSetup.
type SetupProbe struct {
	// ActiveAccounts returns the count of enabled iMessage accounts, or nil when unreadable.
	ActiveAccounts func(ctx context.Context) (*int, error)
	// TCCPath overrides ~/Library/Application Support/com.apple.TCC/TCC.db.
	TCCPath string
	// BundleID identifies the host app bundle recorded in TCC.db.
	BundleID string
	// AppPath resolves a bundle identifier to its .app path on disk.
	AppPath func(ctx context.Context, bundleID string) (string, error)
	// Open executes macOS `open` with the given arguments.
	Open func(ctx context.Context, args []string) error
}

// Options configures an iMessage channel adapter.
type Options struct {
	DBPath       string
	RunScript    ScriptRunner
	PollInterval time.Duration
	SentLookup   time.Duration
	Now          func() time.Time
	Setup        SetupProbe
}

// SelfRow represents one message row read from a self-chat in chat.db.
type SelfRow struct {
	RowID          int64
	GUID           string
	Text           *string
	AttributedBody []byte
	// Date is milliseconds since 2001-01-01T00:00:00Z.
	Date           int64
	FromMe         bool
	ChatGUID       string
	ChatIdentifier string
	To             string
}

// AttachmentRow represents one attachment row joined to a message in chat.db.
type AttachmentRow struct {
	Filename string
	Mime     string
	Name     string
	Size     int64
}

// IntPtr returns a pointer to n for configuring SetupProbe.ActiveAccounts.
func IntPtr(n int) *int { return &n }

// DecodeAttributedBody extracts the UTF-8 plain text from an NSAttributedString typedstream blob.
func DecodeAttributedBody(blob []byte) (string, bool) {
	if len(blob) == 0 {
		return "", false
	}
	start := bytes.Index(blob, []byte("NSString"))
	if start < 0 || start+8 >= len(blob) {
		return "", false
	}
	rel := bytes.IndexByte(blob[start+8:], 0x2b)
	if rel < 0 {
		return "", false
	}
	plus := start + 8 + rel
	if plus > start+16 {
		return "", false
	}
	i := plus + 1
	if i >= len(blob) {
		return "", false
	}
	length := int(blob[i])
	i++
	switch length {
	case 0x81:
		if i+2 > len(blob) {
			return "", false
		}
		length = int(binary.LittleEndian.Uint16(blob[i : i+2]))
		i += 2
	case 0x82:
		if i+4 > len(blob) {
			return "", false
		}
		length = int(binary.LittleEndian.Uint32(blob[i : i+4]))
		i += 4
	}
	if length < 0 || i+length > len(blob) {
		return "", false
	}
	return string(blob[i : i+length]), true
}

// ParseAccounts extracts normalized unique iMessage handles from AppleScript output lines ("E:me@icloud.com").
func ParseAccounts(output string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(rawLine, "\r"))
		line = prefixHandleRe.ReplaceAllString(line, "")
		if !accountHandleRe.MatchString(line) {
			continue
		}
		h := normalizeHandle(line)
		if _, dup := seen[h]; !dup {
			seen[h] = struct{}{}
			out = append(out, h)
		}
	}
	return out
}

// CountActiveAccounts counts entries printed by `defaults read com.apple.imservice.ids.iMessage ActiveAccounts`.
func CountActiveAccounts(output string) int {
	cleaned := strings.NewReplacer("(", "", ")", "").Replace(output)
	count := 0
	for _, line := range strings.Split(cleaned, "\n") {
		for _, part := range strings.Split(line, ",") {
			item := strings.TrimSpace(part)
			item = strings.TrimPrefix(item, `"`)
			item = strings.TrimSuffix(item, `"`)
			if item != "" {
				count++
			}
		}
	}
	return count
}

// ToInbound converts one self-chat row into an imsdk.InboundMessage.
func ToInbound(row SelfRow, raw string, attachments []AttachmentRow, owner imsdk.Sender) (imsdk.InboundMessage, bool) {
	text := strings.TrimSpace(strings.ReplaceAll(raw, "\ufffc", ""))
	var files []imsdk.InboundAttachment
	for _, a := range attachments {
		if a.Filename == "" {
			continue
		}
		name := a.Name
		if name == "" {
			if slash := strings.LastIndexByte(a.Filename, '/'); slash >= 0 && slash+1 < len(a.Filename) {
				name = a.Filename[slash+1:]
			} else if a.Filename != "" {
				name = a.Filename
			} else {
				name = "file"
			}
		}
		files = append(files, imsdk.InboundAttachment{
			ID:   a.Filename,
			Name: name,
			Size: a.Size,
			Mime: a.Mime,
		})
	}
	if text == "" && len(files) == 0 {
		return imsdk.InboundMessage{}, false
	}
	return imsdk.InboundMessage{
		PlatformID:     row.GUID,
		Chat:           imsdk.ChatRef{PlatformID: row.ChatGUID, Kind: imsdk.ChatDM, Name: owner.Name},
		Sender:         owner,
		Text:           text,
		Attachments:    files,
		AddressedToBot: true,
		SentAt:         fromAppleDateMs(row.Date),
	}, true
}

// Plain strips common Markdown formatting to plain text suitable for Messages.
func Plain(markdown string) string {
	s := fencedCodeRe.ReplaceAllString(markdown, "$1")
	s = boldRe.ReplaceAllString(s, "$1")
	s = stripSingleStarItalic(s)
	s = inlineCodeRe.ReplaceAllString(s, "$1")
	s = linkRe.ReplaceAllString(s, "$1 ($2)")
	s = headingRe.ReplaceAllString(s, "")
	return s
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

func normalizeHandle(s string) string {
	out := strings.ToLower(strings.TrimSpace(s))
	return prefixHandleRe.ReplaceAllString(out, "")
}

func fromAppleDateMs(appleMs int64) time.Time {
	return time.UnixMilli(AppleEpochMs + appleMs).UTC()
}

func toAppleDateNs(unixMs int64) int64 {
	delta := max(int64(0), unixMs-AppleEpochMs)
	return delta * 1_000_000
}
