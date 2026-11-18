package feishu

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

var (
	multiSpaceRe   = regexp.MustCompile(`[ \t]{2,}`)
	multiNewlineRe = regexp.MustCompile(`\n{3,}`)
	fenceLineRe    = regexp.MustCompile("^```(\\w*)$")
	headingLineRe  = regexp.MustCompile(`^#{1,6}\s`)
	h1ToH3Re       = regexp.MustCompile(`(?m)^#{1,3} `)
	h2ToH6LineRe   = regexp.MustCompile(`(?m)^#{2,6} (.+)$`)
	h1LineRe       = regexp.MustCompile(`(?m)^# (.+)$`)
	cardAtTagRe    = regexp.MustCompile(`<at id=[^>]*></at>`)
	anySpaceRe     = regexp.MustCompile(`\s+`)
)

type rawSenderID struct {
	OpenID  string `json:"open_id"`
	UserID  string `json:"user_id"`
	UnionID string `json:"union_id"`
}

type rawSender struct {
	SenderID   rawSenderID `json:"sender_id"`
	SenderType string      `json:"sender_type"`
	TenantKey  string      `json:"tenant_key"`
}

type rawMention struct {
	Key  string      `json:"key"`
	ID   rawSenderID `json:"id"`
	Name string      `json:"name"`
}

type rawMessage struct {
	MessageID   string       `json:"message_id"`
	RootID      string       `json:"root_id"`
	ParentID    string       `json:"parent_id"`
	ThreadID    string       `json:"thread_id"`
	CreateTime  string       `json:"create_time"`
	ChatID      string       `json:"chat_id"`
	ChatType    string       `json:"chat_type"`
	MessageType string       `json:"message_type"`
	Content     string       `json:"content"`
	Mentions    []rawMention `json:"mentions"`
}

type rawMessageEvent struct {
	Sender  rawSender  `json:"sender"`
	Message rawMessage `json:"message"`
}

type listedSender struct {
	ID         string `json:"id"`
	IDType     string `json:"id_type"`
	SenderType string `json:"sender_type"`
	TenantKey  string `json:"tenant_key"`
}

type listedMention struct {
	Key    string `json:"key"`
	ID     string `json:"id"`
	IDType string `json:"id_type"`
	Name   string `json:"name"`
}

type listedBody struct {
	Content string `json:"content"`
}

type listedMessage struct {
	MessageID  string          `json:"message_id"`
	RootID     string          `json:"root_id"`
	ParentID   string          `json:"parent_id"`
	ThreadID   string          `json:"thread_id"`
	CreateTime string          `json:"create_time"`
	ChatID     string          `json:"chat_id"`
	MsgType    string          `json:"msg_type"`
	Deleted    bool            `json:"deleted"`
	Sender     listedSender    `json:"sender"`
	Body       listedBody      `json:"body"`
	Mentions   []listedMention `json:"mentions"`
}

type normalizedResource struct {
	Type     string
	FileKey  string
	FileName string
}

type normalizedMention struct {
	Key    string
	OpenID string
	Name   string
	IsBot  bool
}

type normalizedMessage struct {
	MessageID        string
	ChatID           string
	ChatType         string
	SenderID         string
	SenderName       string
	Content          string
	Resources        []normalizedResource
	Mentions         []normalizedMention
	MentionAll       bool
	MentionedBot     bool
	ReplyToMessageID string
	CreateTimeMs     int64
}

func rawEventFromListed(item listedMessage, chatID string) (rawMessageEvent, bool) {
	if item.Deleted || item.MessageID == "" || item.MsgType == "" || item.Sender.ID == "" {
		return rawMessageEvent{}, false
	}
	if item.MsgType == "system" {
		return rawMessageEvent{}, false
	}
	cid := item.ChatID
	if cid == "" {
		cid = chatID
	}
	mentions := make([]rawMention, 0, len(item.Mentions))
	for _, m := range item.Mentions {
		mentions = append(mentions, rawMention{
			Key:  m.Key,
			ID:   rawSenderID{OpenID: m.ID},
			Name: m.Name,
		})
	}
	return rawMessageEvent{
		Sender: rawSender{
			SenderID:   rawSenderID{OpenID: item.Sender.ID},
			SenderType: item.Sender.SenderType,
			TenantKey:  item.Sender.TenantKey,
		},
		Message: rawMessage{
			MessageID:   item.MessageID,
			RootID:      item.RootID,
			ParentID:    item.ParentID,
			ThreadID:    item.ThreadID,
			CreateTime:  item.CreateTime,
			ChatID:      cid,
			ChatType:    "group",
			MessageType: item.MsgType,
			Content:     item.Body.Content,
			Mentions:    mentions,
		},
	}, true
}

func normalizeRawEvent(ev rawMessageEvent, botOpenID string) normalizedMessage {
	msg := ev.Message
	byKey := make(map[string]normalizedMention, len(msg.Mentions))
	byOpenID := make(map[string]normalizedMention, len(msg.Mentions))
	var mentionOrder []string
	var mentionList []normalizedMention
	mentionAll := false
	mentionedBot := false

	for _, m := range msg.Mentions {
		if m.Key == "@_all" {
			mentionAll = true
			info := normalizedMention{Key: m.Key, Name: m.Name}
			if _, dup := byKey[m.Key]; !dup {
				mentionOrder = append(mentionOrder, m.Key)
			}
			byKey[m.Key] = info
			continue
		}
		openID := m.ID.OpenID
		isBot := botOpenID != "" && openID == botOpenID
		if isBot {
			mentionedBot = true
		}
		info := normalizedMention{
			Key:    m.Key,
			OpenID: openID,
			Name:   m.Name,
			IsBot:  isBot,
		}
		if _, dup := byKey[m.Key]; !dup {
			mentionOrder = append(mentionOrder, m.Key)
		}
		byKey[m.Key] = info
		if openID != "" {
			byOpenID[openID] = info
		}
		mentionList = append(mentionList, info)
	}
	if !mentionAll && strings.Contains(msg.Content, "@_all") {
		mentionAll = true
	}

	rawContent, resources := convertMessageContent(msg.Content, msg.MessageType, byOpenID)
	content := resolveContentMentions(rawContent, mentionOrder, byKey, true)

	senderID := ev.Sender.SenderID.OpenID
	if senderID == "" {
		senderID = ev.Sender.SenderID.UserID
	}
	if senderID == "" {
		senderID = ev.Sender.SenderID.UnionID
	}

	createMs, err := strconv.ParseInt(msg.CreateTime, 10, 64)
	if err != nil {
		createMs = 0
	}

	return normalizedMessage{
		MessageID:        msg.MessageID,
		ChatID:           msg.ChatID,
		ChatType:         msg.ChatType,
		SenderID:         senderID,
		Content:          content,
		Resources:        resources,
		Mentions:         mentionList,
		MentionAll:       mentionAll,
		MentionedBot:     mentionedBot,
		ReplyToMessageID: msg.ParentID,
		CreateTimeMs:     createMs,
	}
}

func resolveContentMentions(content string, keys []string, byKey map[string]normalizedMention, stripBot bool) string {
	if content == "" || len(keys) == 0 {
		return content
	}
	out := content
	for _, key := range keys {
		if key == "" {
			continue
		}
		info := byKey[key]
		if info.IsBot && stripBot {
			re := regexp.MustCompile(`\s?` + regexp.QuoteMeta(key) + `\s?`)
			out = re.ReplaceAllString(out, " ")
			continue
		}
		repl := key
		if info.Name != "" {
			repl = "@" + info.Name
		}
		out = strings.ReplaceAll(out, key, repl)
	}
	return strings.TrimSpace(multiSpaceRe.ReplaceAllString(out, " "))
}

func convertMessageContent(raw, msgType string, byOpenID map[string]normalizedMention) (string, []normalizedResource) {
	switch msgType {
	case "text":
		var p struct {
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil {
			return p.Text, nil
		}
		return "", nil

	case "image":
		var p struct {
			ImageKey string `json:"image_key"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.ImageKey != "" {
			return fmt.Sprintf("![image](%s)", p.ImageKey), []normalizedResource{{Type: "image", FileKey: p.ImageKey}}
		}
		return "[image]", nil

	case "file":
		var p struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.FileKey != "" {
			attr := ""
			if p.FileName != "" {
				attr = fmt.Sprintf(` name="%s"`, strings.ReplaceAll(p.FileName, `"`, "&quot;"))
			}
			return fmt.Sprintf(`<file key="%s"%s/>`, p.FileKey, attr), []normalizedResource{{
				Type:     "file",
				FileKey:  p.FileKey,
				FileName: p.FileName,
			}}
		}
		return "[file]", nil

	case "audio":
		var p struct {
			FileKey string `json:"file_key"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.FileKey != "" {
			return fmt.Sprintf(`<audio key="%s"/>`, p.FileKey), []normalizedResource{{Type: "audio", FileKey: p.FileKey}}
		}
		return "[audio]", nil

	case "video", "media":
		var p struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.FileKey != "" {
			return fmt.Sprintf(`<video key="%s"/>`, p.FileKey), []normalizedResource{{
				Type:     "video",
				FileKey:  p.FileKey,
				FileName: p.FileName,
			}}
		}
		return "[video]", nil

	case "sticker":
		var p struct {
			FileKey string `json:"file_key"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.FileKey != "" {
			return fmt.Sprintf(`<sticker key="%s"/>`, p.FileKey), []normalizedResource{{Type: "sticker", FileKey: p.FileKey}}
		}
		return "[sticker]", nil

	case "post":
		return convertPostContent(raw, byOpenID)

	case "interactive":
		var parsed any
		if json.Unmarshal([]byte(raw), &parsed) != nil || parsed == nil {
			return "[interactive card]", nil
		}
		var pieces []string
		walkInteractiveCard(parsed, &pieces)
		var out []string
		seen := make(map[string]struct{})
		for _, p := range pieces {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			out = append(out, trimmed)
		}
		if len(out) == 0 {
			return "[interactive card]", nil
		}
		return strings.Join(out, "\n"), nil

	default:
		var p struct {
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(raw), &p) == nil && p.Text != "" {
			return p.Text, nil
		}
		return "[unsupported message]", nil
	}
}

type postLocaleBody struct {
	Title   string              `json:"title"`
	Content [][]postElementJSON `json:"content"`
}

type postElementJSON struct {
	Tag      string   `json:"tag"`
	Text     string   `json:"text"`
	Style    []string `json:"style"`
	Href     string   `json:"href"`
	UserID   string   `json:"user_id"`
	UserName string   `json:"user_name"`
	ImageKey string   `json:"image_key"`
	FileKey  string   `json:"file_key"`
	Language string   `json:"language"`
}

func convertPostContent(raw string, byOpenID map[string]normalizedMention) (string, []normalizedResource) {
	var top map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &top) != nil || len(top) == 0 {
		return "[rich text message]", nil
	}
	var body postLocaleBody
	_, hasTitle := top["title"]
	_, hasContent := top["content"]
	if hasTitle || hasContent {
		if json.Unmarshal([]byte(raw), &body) != nil {
			return "[rich text message]", nil
		}
	} else {
		found := false
		for _, loc := range []string{"zh_cn", "en_us", "ja_jp"} {
			if sub, ok := top[loc]; ok {
				if json.Unmarshal(sub, &body) == nil {
					found = true
					break
				}
			}
		}
		if !found {
			for _, sub := range top {
				if json.Unmarshal(sub, &body) == nil {
					found = true
					break
				}
			}
		}
		if !found {
			return "[rich text message]", nil
		}
	}

	var resources []normalizedResource
	var lines []string
	if body.Title != "" {
		lines = append(lines, "**"+body.Title+"**", "")
	}
	for _, para := range body.Content {
		var line strings.Builder
		for _, el := range para {
			switch el.Tag {
			case "text":
				line.WriteString(applyPostStyle(el.Text, el.Style))
			case "a":
				label := el.Text
				if label == "" {
					label = el.Href
				}
				if el.Href != "" {
					line.WriteString(fmt.Sprintf("[%s](%s)", label, el.Href))
				} else {
					line.WriteString(label)
				}
			case "at":
				if el.UserID == "all" || el.UserID == "all_members" {
					line.WriteString("@all")
				} else if info, ok := byOpenID[el.UserID]; ok {
					line.WriteString(info.Key)
				} else if el.UserName != "" {
					line.WriteString("@" + el.UserName)
				} else {
					line.WriteString("@" + el.UserID)
				}
			case "img":
				if el.ImageKey != "" {
					resources = append(resources, normalizedResource{Type: "image", FileKey: el.ImageKey})
					line.WriteString(fmt.Sprintf("![image](%s)", el.ImageKey))
				}
			case "media":
				if el.FileKey != "" {
					resources = append(resources, normalizedResource{Type: "file", FileKey: el.FileKey})
					line.WriteString(fmt.Sprintf(`<file key="%s"/>`, el.FileKey))
				}
			case "code_block":
				line.WriteString(fmt.Sprintf("\n```%s\n%s\n```\n", el.Language, el.Text))
			case "hr":
				line.WriteString("\n---\n")
			default:
				line.WriteString(el.Text)
			}
		}
		lines = append(lines, line.String())
	}
	content := strings.TrimSpace(strings.Join(lines, "\n"))
	if content == "" {
		content = "[rich text message]"
	}
	return content, resources
}

func applyPostStyle(text string, style []string) string {
	if len(style) == 0 || text == "" {
		return text
	}
	out := text
	if slices.Contains(style, "bold") {
		out = "**" + out + "**"
	}
	if slices.Contains(style, "italic") {
		out = "*" + out + "*"
	}
	if slices.Contains(style, "underline") {
		out = "<u>" + out + "</u>"
	}
	if slices.Contains(style, "lineThrough") || slices.Contains(style, "strikethrough") {
		out = "~~" + out + "~~"
	}
	if slices.Contains(style, "codeInline") || slices.Contains(style, "code") {
		out = "`" + out + "`"
	}
	return out
}

func walkInteractiveCard(node any, out *[]string) {
	switch v := node.(type) {
	case []any:
		for _, item := range v {
			walkInteractiveCard(item, out)
		}
	case map[string]any:
		tag, _ := v["tag"].(string)
		if tag == "plain_text" || tag == "lark_md" || tag == "markdown" {
			if s, ok := v["content"].(string); ok {
				*out = append(*out, s)
			}
			return
		}
		if hdr, ok := v["header"].(map[string]any); ok {
			walkInteractiveCard(hdr["title"], out)
		}
		if txt, ok := v["text"]; ok {
			walkInteractiveCard(txt, out)
		}
		for _, key := range []string{"label", "placeholder", "elements", "fields", "actions", "columns", "body"} {
			if sub, ok := v[key]; ok {
				walkInteractiveCard(sub, out)
			}
		}
		if opts, ok := v["options"].([]any); ok {
			for _, o := range opts {
				if om, ok := o.(map[string]any); ok {
					walkInteractiveCard(om["text"], out)
				}
			}
		}
	}
}

func toInbound(msg normalizedMessage, bot imsdk.BotIdentity, chatName string, isBot bool, now time.Time) (*imsdk.InboundMessage, bool) {
	if msg.SenderID == "" || msg.SenderID == bot.ID {
		return nil, false
	}
	dm := msg.ChatType == "p2p"
	text := msg.Content
	if bot.Name != "" {
		text = strings.ReplaceAll(text, "@"+bot.Name, "")
	}
	text = strings.TrimSpace(multiSpaceRe.ReplaceAllString(text, " "))

	attachments := make([]imsdk.InboundAttachment, 0, len(msg.Resources))
	for i, r := range msg.Resources {
		name := r.FileName
		if name == "" {
			suffix := ""
			if r.Type == "image" {
				suffix = ".png"
			}
			name = fmt.Sprintf("%s-%d%s", r.Type, i+1, suffix)
		}
		att := imsdk.InboundAttachment{
			ID:   fmt.Sprintf("%s|%s|%s", msg.MessageID, r.Type, r.FileKey),
			Name: name,
		}
		if r.Type == "image" {
			att.Mime = "image/png"
		}
		attachments = append(attachments, att)
	}

	var others []imsdk.Sender
	for _, m := range msg.Mentions {
		if m.OpenID != "" && !m.IsBot && m.OpenID != bot.ID {
			others = append(others, imsdk.Sender{
				ID:    m.OpenID,
				Name:  m.Name,
				IsBot: false,
			})
		}
	}

	chat := imsdk.ChatRef{
		PlatformID: msg.ChatID,
		Kind:       imsdk.ChatGroup,
	}
	if dm {
		chat.Kind = imsdk.ChatDM
		chat.Name = msg.SenderName
	} else {
		chat.Name = chatName
	}

	sentAt := now
	if msg.CreateTimeMs > 0 {
		sentAt = time.UnixMilli(msg.CreateTimeMs)
	}

	return &imsdk.InboundMessage{
		PlatformID:     msg.MessageID,
		Chat:           chat,
		Sender:         imsdk.Sender{ID: msg.SenderID, Name: msg.SenderName, IsBot: isBot},
		Text:           text,
		Attachments:    attachments,
		ReplyTo:        msg.ReplyToMessageID,
		AddressedToBot: dm || msg.MentionedBot,
		Mentioned:      imsdk.BoolPtr(msg.MentionedBot),
		Mentions:       others,
		SentAt:         sentAt,
	}, true
}

func markdownToPostContent(md string) map[string]any {
	text := optimizePostMarkdown(md)
	return map[string]any{
		"zh_cn": map[string]any{
			"title":   "",
			"content": [][]map[string]any{{{"tag": "md", "text": text}}},
		},
	}
}

func optimizePostMarkdown(text string) string {
	const mark = "___CB_"
	var blocks []string
	lines := strings.Split(text, "\n")
	var rebuilt []string
	inFence := false
	fenceTicks := ""
	var fenceBuf []string

	for _, line := range lines {
		if !inFence {
			trimmed := strings.TrimLeft(line, "")
			if strings.HasPrefix(trimmed, "```") {
				ticks := leadingBackticks(trimmed)
				inFence = true
				fenceTicks = ticks
				fenceBuf = []string{line}
				continue
			}
			rebuilt = append(rebuilt, line)
		} else {
			fenceBuf = append(fenceBuf, line)
			if line == fenceTicks {
				idx := len(blocks)
				blocks = append(blocks, strings.Join(fenceBuf, "\n"))
				rebuilt = append(rebuilt, fmt.Sprintf("%s%d___", mark, idx))
				inFence = false
				fenceBuf = nil
			}
		}
	}
	if inFence && len(fenceBuf) > 0 {
		rebuilt = append(rebuilt, fenceBuf...)
	}

	r := strings.Join(rebuilt, "\n")
	if h1ToH3Re.MatchString(r) {
		r = h2ToH6LineRe.ReplaceAllString(r, "##### $1")
		r = h1LineRe.ReplaceAllString(r, "#### $1")
	}
	for i, b := range blocks {
		r = strings.Replace(r, fmt.Sprintf("%s%d___", mark, i), b, 1)
	}
	return multiNewlineRe.ReplaceAllString(r, "\n\n")
}

func leadingBackticks(s string) string {
	i := 0
	for i < len(s) && s[i] == '`' {
		i++
	}
	return s[:i]
}

func splitWithCodeFences(text string, limit int) []string {
	if len(text) <= limit || limit <= 0 {
		return []string{text}
	}
	lines := strings.Split(text, "\n")
	var out []string
	var buf []string
	bufLen := 0
	var fenceLang *string

	flush := func() {
		if len(buf) == 0 {
			return
		}
		chunk := strings.Join(buf, "\n")
		if fenceLang != nil {
			chunk += "\n```"
		}
		out = append(out, chunk)
		buf = nil
		bufLen = 0
		if fenceLang != nil {
			reopen := "```" + *fenceLang
			buf = append(buf, reopen)
			bufLen = len(reopen)
		}
	}

	for _, line := range lines {
		m := fenceLineRe.FindStringSubmatch(line)
		lineLen := len(line)
		if len(buf) > 0 {
			lineLen++
		}
		isHeading := headingLineRe.MatchString(line)
		nearFull := float64(bufLen) > float64(limit)*0.75
		if bufLen+lineLen > limit || (isHeading && nearFull && len(buf) > 0) {
			flush()
		}
		buf = append(buf, line)
		bufLen += lineLen
		if m != nil {
			if fenceLang == nil {
				lang := m[1]
				fenceLang = &lang
			} else {
				fenceLang = nil
			}
		}
	}
	flush()
	return out
}

func detectReceiveIDType(to string) string {
	switch {
	case strings.HasPrefix(to, "oc_"):
		return "chat_id"
	case strings.HasPrefix(to, "ou_"):
		return "open_id"
	case strings.HasPrefix(to, "on_"):
		return "union_id"
	case strings.Contains(to, "@"):
		return "email"
	default:
		return "user_id"
	}
}
