// Package outbox handles message chunking, @mention resolution, live-reply handover,
// retryable send backoff, and partial textSent file-only retries.
package outbox

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/live"
	"github.com/kilingzhang/im-sdk-go/mention"
)

// SendError represents an outbound delivery failure with actionable guidance.
type SendError struct {
	Code               string
	Next               string
	TextSentPlatformID string
}

func (e *SendError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Next)
}

// Request describes one outbound message send or reply.
type Request struct {
	Chat              imsdk.ChatRef
	Platform          imsdk.Platform
	Text              string
	ReplyToMessageID  string // internal message ID tracked in live.Manager
	ReplyToPlatformID string // platform-native message ID to quote
	Files             []imsdk.OutboundFile
	Candidates        []mention.Candidate
	Notice            bool
}

// Report summarizes the result of Outbox.Send.
type Report struct {
	PlatformIDs []string
	State       imsdk.SendOutcome
	Chat        string
	Degraded    []imsdk.Degradation
	Mentioned   []mention.MentionedContact
	Unmatched   []string
}

// DefaultRetryDelays defines backoff intervals for retryable outbound failures.
var DefaultRetryDelays = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	9 * time.Second,
}

// Outbox coordinates outbound delivery over a ChannelConnection.
type Outbox struct {
	retryDelays []time.Duration
	live        *live.Manager
}

// New creates an Outbox with the given retry delays (nil uses DefaultRetryDelays) and optional live.Manager.
func New(retryDelays []time.Duration, liveMgr *live.Manager) *Outbox {
	if retryDelays == nil {
		retryDelays = DefaultRetryDelays
	}
	return &Outbox{
		retryDelays: retryDelays,
		live:        liveMgr,
	}
}

// Send splits, resolves @mentions, delivers chunks + attachments, and settles live replies.
func (o *Outbox) Send(ctx context.Context, conn imsdk.ChannelConnection, req Request) (Report, error) {
	if conn == nil {
		return Report{}, &SendError{
			Code: "not_connected",
			Next: "the connection is offline; try again once reconnected",
		}
	}
	text := strings.TrimSpace(req.Text)
	if text == "" && len(req.Files) == 0 {
		return Report{}, &SendError{
			Code: "empty_message",
			Next: "provide non-empty text or at least one file",
		}
	}

	var resolved mention.Resolved
	if req.Chat.Kind != imsdk.ChatDM && !req.Notice && strings.Contains(text, "@") {
		resolved = mention.Resolve(text, req.Candidates)
	}

	caps := conn.Capabilities()
	chunks := Split(text, caps.MaxTextLength)
	if len(chunks) == 0 {
		chunks = []string{""}
	}

	var degraded []imsdk.Degradation
	addDegraded := func(d imsdk.Degradation) {
		if !slices.Contains(degraded, d) {
			degraded = append(degraded, d)
		}
	}
	if len(chunks) > 1 && text != "" {
		addDegraded(imsdk.DegradedTextSplit)
	}
	if len(resolved.Mentions) > 0 && !caps.Mentions {
		addDegraded(imsdk.DegradedMentionsPlain)
	}

	ref := req.Chat
	var sentIDs []string
	var resolvedChat string

	for i, chunk := range chunks {
		last := i == len(chunks)-1
		var files []imsdk.OutboundFile
		if last && len(req.Files) > 0 {
			files = req.Files
		}

		var intoLive *imsdk.SendResult
		if i == 0 && req.ReplyToMessageID != "" && chunk != "" && !req.Notice && o.live != nil {
			if lrRes, ok := o.live.Answer(ctx, req.ReplyToMessageID, chunk, resolved.Mentions); ok {
				intoLive = lrRes
			}
		}

		var res imsdk.SendResult
		var err error
		if intoLive != nil {
			if len(files) > 0 {
				replyTo := ""
				if req.ReplyToPlatformID != "" {
					replyTo = req.ReplyToPlatformID
				}
				res, err = o.attempt(ctx, conn, imsdk.OutboundMessage{
					Chat:    ref,
					Text:    "",
					ReplyTo: replyTo,
					Files:   files,
				})
				if err == nil && res.Outcome == imsdk.OutcomeSent && intoLive.PlatformID != "" {
					res.PlatformID = intoLive.PlatformID
				}
			} else {
				res = *intoLive
			}
		} else {
			replyTo := ""
			if i == 0 {
				replyTo = req.ReplyToPlatformID
			}
			res, err = o.attempt(ctx, conn, imsdk.OutboundMessage{
				Chat:     ref,
				Text:     chunk,
				ReplyTo:  replyTo,
				Files:    files,
				Mentions: resolved.Mentions,
			})
		}
		if err != nil {
			return Report{}, err
		}

		switch res.Outcome {
		case imsdk.OutcomeSent:
			if res.Chat != "" && res.Chat != ref.PlatformID {
				resolvedChat = res.Chat
				ref.PlatformID = res.Chat
			}
			sentIDs = append(sentIDs, res.PlatformID)
			for _, d := range res.Degraded {
				addDegraded(d)
			}
		case imsdk.OutcomeUncertain:
			if !req.Notice && req.ReplyToMessageID != "" && o.live != nil {
				o.live.Settle(ctx, req.ReplyToMessageID, live.OutcomeAnswered, "")
			}
			return Report{
				PlatformIDs: sentIDs,
				State:       imsdk.OutcomeUncertain,
				Chat:        resolvedChat,
				Degraded:    degraded,
				Mentioned:   resolved.Mentioned,
				Unmatched:   resolved.Unmatched,
			}, nil
		default:
			why := ""
			if res.Detail != "" {
				why = fmt.Sprintf(" (%s: %s)", req.Platform, res.Detail)
			}
			if res.TextSentPlatformID != "" {
				if !req.Notice && req.ReplyToMessageID != "" && o.live != nil {
					o.live.Settle(ctx, req.ReplyToMessageID, live.OutcomeAnswered, "")
				}
				return Report{}, &SendError{
					Code:               "send_" + string(res.Code),
					Next:               fmt.Sprintf("the text was delivered but the file was not%s: %s. Do not send the text again; send only the file", why, failureNext(res.Code, "file")),
					TextSentPlatformID: res.TextSentPlatformID,
				}
			}
			return Report{}, &SendError{
				Code: "send_" + string(res.Code),
				Next: failureNext(res.Code, "message") + why,
			}
		}
	}

	if !req.Notice && req.ReplyToMessageID != "" && o.live != nil {
		o.live.Settle(ctx, req.ReplyToMessageID, live.OutcomeAnswered, "")
	}
	return Report{
		PlatformIDs: sentIDs,
		State:       imsdk.OutcomeSent,
		Chat:        resolvedChat,
		Degraded:    degraded,
		Mentioned:   resolved.Mentioned,
		Unmatched:   resolved.Unmatched,
	}, nil
}

func (o *Outbox) attempt(ctx context.Context, conn imsdk.ChannelConnection, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	res, err := conn.Send(ctx, msg)
	if err != nil {
		return imsdk.SendResult{}, err
	}
	textSent := ""
	if res.Outcome == imsdk.OutcomeFailed {
		textSent = res.TextSentPlatformID
	}
	for _, delay := range o.retryDelays {
		if res.Outcome != imsdk.OutcomeFailed || !res.Retryable {
			break
		}
		if res.TextSentPlatformID != "" {
			textSent = res.TextSentPlatformID
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(delay):
		}
		nextMsg := msg
		if textSent != "" {
			nextMsg.Text = ""
		}
		res, err = conn.Send(ctx, nextMsg)
		if err != nil {
			return imsdk.SendResult{}, err
		}
	}
	if textSent != "" {
		if res.Outcome == imsdk.OutcomeSent {
			res.PlatformID = textSent
		} else if res.Outcome == imsdk.OutcomeFailed {
			res.TextSentPlatformID = textSent
		}
	}
	return res, nil
}

func failureNext(code imsdk.SendFailureCode, what string) string {
	switch code {
	case imsdk.SendErrRateLimited:
		return "the chat app is rate limiting; wait a minute before sending again"
	case imsdk.SendErrForbidden:
		if what == "file" {
			return "the bot lacks permission to share files here, or the workspace blocks this kind of file"
		}
		return "the bot may have been removed from this chat or lacks permission to post"
	case imsdk.SendErrTooLarge:
		return "the file is too large for this chat app; send a smaller file or a link"
	case imsdk.SendErrChatGone:
		return "this chat no longer exists for the bot"
	default:
		if what == "file" {
			return "the chat app refused it"
		}
		return "the message was not sent; try again shortly"
	}
}

// Split divides text into pieces of at most maxRunes Unicode code points,
// preferring paragraph (\n\n), then line (\n), then hard rune boundaries.
func Split(text string, maxRunes int) []string {
	if text == "" {
		return nil
	}
	if maxRunes <= 0 || utf8.RuneCountInString(text) <= maxRunes {
		return []string{text}
	}
	runes := []rune(text)
	var out []string
	for len(runes) > maxRunes {
		window := string(runes[:maxRunes])
		cutBytes := max(strings.LastIndex(window, "\n\n"), strings.LastIndex(window, "\n"))
		cutRunes := maxRunes
		if cutBytes > 0 {
			candidate := utf8.RuneCountInString(window[:cutBytes])
			if float64(candidate) > float64(maxRunes)*0.5 {
				cutRunes = candidate
			}
		}
		piece := strings.TrimRight(string(runes[:cutRunes]), " \t\r\n")
		if piece != "" {
			out = append(out, piece)
		}
		rest := strings.TrimLeft(string(runes[cutRunes:]), " \t\r\n")
		runes = []rune(rest)
	}
	if len(runes) > 0 {
		out = append(out, string(runes))
	}
	return out
}
