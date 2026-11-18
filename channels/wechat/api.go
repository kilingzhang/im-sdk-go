package wechat

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	botType                = "3"
	defaultLongPollTimeout = 35 * time.Second
	defaultRequestTimeout  = 15 * time.Second
	defaultControlTimeout  = 10 * time.Second
	defaultTransferTimeout = 120 * time.Second
	qrLifetime             = 8 * time.Minute
)

// TextItem carries plain text inside an iLink message item.
type TextItem struct {
	Text string `json:"text,omitempty"`
}

// VoiceItem carries speech-to-text transcription inside an iLink voice message item.
type VoiceItem struct {
	Text string `json:"text,omitempty"`
}

// ImageItem carries an encrypted image reference.
type ImageItem struct {
	Media   *Media `json:"media,omitempty"`
	AESKey  string `json:"aeskey,omitempty"`
	MidSize int    `json:"mid_size,omitempty"`
}

// FileItem carries an encrypted file reference.
type FileItem struct {
	Media    *Media `json:"media,omitempty"`
	AESKey   string `json:"aeskey,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Len      string `json:"len,omitempty"`
}

// Item represents one element of an iLink item_list.
type Item struct {
	Type      int        `json:"type,omitempty"`
	TextItem  *TextItem  `json:"text_item,omitempty"`
	VoiceItem *VoiceItem `json:"voice_item,omitempty"`
	ImageItem *ImageItem `json:"image_item,omitempty"`
	FileItem  *FileItem  `json:"file_item,omitempty"`
	VideoItem any        `json:"video_item,omitempty"`
}

// Message represents an inbound or echoed iLink message payload.
type Message struct {
	MessageID    json.RawMessage `json:"message_id,omitempty"`
	ClientID     string          `json:"client_id,omitempty"`
	FromUserID   string          `json:"from_user_id,omitempty"`
	MessageType  int             `json:"message_type,omitempty"`
	ContextToken string          `json:"context_token,omitempty"`
	CreateTimeMs int64           `json:"create_time_ms,omitempty"`
	ItemList     []Item          `json:"item_list,omitempty"`
}

func (m Message) messageIDString() string {
	raw := bytes.TrimSpace(m.MessageID)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return m.ClientID
	}
	var s string
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return string(raw)
}

type baseInfo struct {
	ChannelVersion string `json:"channel_version"`
	BotAgent       string `json:"bot_agent"`
}

type statusEnvelope struct {
	Ret     json.RawMessage `json:"ret,omitempty"`
	ErrCode json.RawMessage `json:"errcode,omitempty"`
}

func rejectedCode(ret, errCode json.RawMessage) string {
	for _, field := range []json.RawMessage{ret, errCode} {
		code := parseCodeField(field)
		if code != "" && code != "0" {
			return code
		}
	}
	return ""
}

func parseCodeField(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err == nil {
			return strings.TrimSpace(s)
		}
	}
	return string(trimmed)
}

type timeoutErr struct {
	cause error
}

func (e *timeoutErr) Error() string { return e.cause.Error() }
func (e *timeoutErr) Unwrap() error { return e.cause }

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	var te *timeoutErr
	if errors.As(err, &te) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomWechatUIN() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	n := binary.BigEndian.Uint32(b[:])
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(uint64(n), 10)))
}

type apiClient struct {
	httpClient      *http.Client
	botAgent        string
	longPollTimeout time.Duration
	requestTimeout  time.Duration
	transferTimeout time.Duration
}

func newAPIClient(opts Options) *apiClient {
	var hc http.Client
	if opts.HTTPClient != nil {
		hc = *opts.HTTPClient
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	agent := opts.BotAgent
	if agent == "" {
		agent = "Mingle/0.1"
	}
	longPoll := opts.LongPollTimeout
	if longPoll <= 0 {
		longPoll = defaultLongPollTimeout
	}
	reqTimeout := opts.RequestTimeout
	if reqTimeout <= 0 {
		reqTimeout = defaultRequestTimeout
	}
	xferTimeout := opts.TransferTimeout
	if xferTimeout <= 0 {
		xferTimeout = defaultTransferTimeout
	}
	return &apiClient{
		httpClient:      &hc,
		botAgent:        agent,
		longPollTimeout: longPoll,
		requestTimeout:  reqTimeout,
		transferTimeout: xferTimeout,
	}
}

func (a *apiClient) info() baseInfo {
	return baseInfo{
		ChannelVersion: ProtocolVersion,
		BotAgent:       a.botAgent,
	}
}

func resolveEndpoint(base, endpoint string) (string, error) {
	bu, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	rel, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	return bu.ResolveReference(rel).String(), nil
}

func (a *apiClient) requestJSON(
	ctx context.Context,
	method string,
	base string,
	endpoint string,
	body any,
	token string,
	hasToken bool,
	timeout time.Duration,
	out any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		timeout = a.requestTimeout
	}
	target, err := resolveEndpoint(base, endpoint)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, target, reader)
	if err != nil {
		return err
	}
	req.Header.Set("iLink-App-Id", "bot")
	req.Header.Set("iLink-App-ClientVersion", strconv.Itoa(ClientVersion))
	if body != nil || hasToken {
		req.Header.Set("Content-Type", "application/json")
	}
	if hasToken {
		req.Header.Set("AuthorizationType", "ilink_bot_token")
		req.Header.Set("X-WECHAT-UIN", randomWechatUIN())
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return &timeoutErr{cause: err}
		}
		if isTimeout(err) {
			return &timeoutErr{cause: err}
		}
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return &timeoutErr{cause: err}
		}
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &Error{
			Code:         "http_error",
			Message:      fmt.Sprintf("WeChat HTTP %d", resp.StatusCode),
			ProviderCode: strconv.Itoa(resp.StatusCode),
		}
	}
	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return err
		}
	}
	return nil
}

type qrLoginResult struct {
	QRCode string
	URL    string
}

func (a *apiClient) beginLogin(ctx context.Context, localTokens []string) (qrLoginResult, error) {
	tokens := localTokens
	if len(tokens) > MaxLocalTokens {
		tokens = tokens[:MaxLocalTokens]
	}
	if tokens == nil {
		tokens = []string{}
	}
	var resp struct {
		statusEnvelope
		QRCode           string `json:"qrcode"`
		QRCodeImgContent string `json:"qrcode_img_content"`
	}
	err := a.requestJSON(
		ctx,
		http.MethodPost,
		QRBaseURL,
		"ilink/bot/get_bot_qrcode?bot_type="+botType,
		map[string]any{"local_token_list": tokens},
		"",
		false,
		defaultControlTimeout,
		&resp,
	)
	if err != nil {
		return qrLoginResult{}, err
	}
	if code := rejectedCode(resp.Ret, resp.ErrCode); code != "" {
		return qrLoginResult{}, &Error{Code: "qr_rejected", Message: "WeChat refused the QR request", ProviderCode: code}
	}
	if resp.QRCode == "" || resp.QRCodeImgContent == "" {
		return qrLoginResult{}, &Error{Code: "invalid_qr", Message: "WeChat returned no QR code"}
	}
	trustedURL, err := TrustedQRURL(resp.QRCodeImgContent)
	if err != nil {
		return qrLoginResult{}, err
	}
	return qrLoginResult{QRCode: resp.QRCode, URL: trustedURL}, nil
}

type qrStatusResponse struct {
	Status       string `json:"status"`
	BotToken     string `json:"bot_token"`
	ILinkBotID   string `json:"ilink_bot_id"`
	ILinkUserID  string `json:"ilink_user_id"`
	BaseURL      string `json:"baseurl"`
	RedirectHost string `json:"redirect_host"`
}

func (a *apiClient) pollLogin(ctx context.Context, base, qrcode, verifyCode string) (qrStatusResponse, error) {
	q := url.Values{}
	q.Set("qrcode", qrcode)
	if verifyCode != "" {
		q.Set("verify_code", verifyCode)
	}
	endpoint := "ilink/bot/get_qrcode_status?" + q.Encode()
	var resp qrStatusResponse
	err := a.requestJSON(ctx, http.MethodGet, base, endpoint, nil, "", false, a.longPollTimeout, &resp)
	return resp, err
}

func (a *apiClient) notify(ctx context.Context, base, token, which string) (string, error) {
	var resp statusEnvelope
	err := a.requestJSON(
		ctx,
		http.MethodPost,
		base,
		"ilink/bot/msg/notify"+which,
		map[string]any{"base_info": a.info()},
		token,
		true,
		defaultControlTimeout,
		&resp,
	)
	if err != nil {
		return "", err
	}
	return rejectedCode(resp.Ret, resp.ErrCode), nil
}

type updatesResponse struct {
	statusEnvelope
	Msgs          []Message `json:"msgs"`
	GetUpdatesBuf string    `json:"get_updates_buf"`
}

func (a *apiClient) getUpdates(ctx context.Context, base, token, cursor string) (updatesResponse, error) {
	var resp updatesResponse
	err := a.requestJSON(
		ctx,
		http.MethodPost,
		base,
		"ilink/bot/getupdates",
		map[string]any{
			"get_updates_buf": cursor,
			"base_info":       a.info(),
		},
		token,
		true,
		a.longPollTimeout,
		&resp,
	)
	if err != nil {
		if isTimeout(err) && ctx.Err() == nil {
			return updatesResponse{GetUpdatesBuf: cursor}, nil
		}
		return updatesResponse{}, err
	}
	return resp, nil
}

func (a *apiClient) sendText(ctx context.Context, base, token, to, text, contextToken string) (string, error) {
	return a.sendItem(ctx, base, token, to, Item{
		Type:     1,
		TextItem: &TextItem{Text: text},
	}, contextToken)
}

func (a *apiClient) sendItem(ctx context.Context, base, token, to string, item Item, contextToken string) (string, error) {
	msgBody := map[string]any{
		"from_user_id":  "",
		"to_user_id":    to,
		"client_id":     "mingle-" + randomUUID(),
		"message_type":  2,
		"message_state": 2,
		"item_list":     []Item{item},
	}
	if contextToken != "" {
		msgBody["context_token"] = contextToken
	}
	var resp statusEnvelope
	err := a.requestJSON(
		ctx,
		http.MethodPost,
		base,
		"ilink/bot/sendmessage",
		map[string]any{
			"msg":       msgBody,
			"base_info": a.info(),
		},
		token,
		true,
		a.requestTimeout,
		&resp,
	)
	if err != nil {
		return "", err
	}
	return rejectedCode(resp.Ret, resp.ErrCode), nil
}

func (a *apiClient) upload(ctx context.Context, base, token, to string, rawBytes []byte, name string, isImage bool) (Item, error) {
	var fileKeyBytes [16]byte
	_, _ = rand.Read(fileKeyBytes[:])
	fileKey := hex.EncodeToString(fileKeyBytes[:])

	key := make([]byte, aesBlockSize)
	_, _ = rand.Read(key)

	ciphertext, err := EncryptAES128ECB(rawBytes, key)
	if err != nil {
		return Item{}, err
	}

	sum := md5.Sum(rawBytes)
	mediaType := 3
	if isImage {
		mediaType = 1
	}

	var reserved struct {
		statusEnvelope
		UploadFullURL string `json:"upload_full_url"`
		UploadParam   string `json:"upload_param"`
	}
	err = a.requestJSON(
		ctx,
		http.MethodPost,
		base,
		"ilink/bot/getuploadurl",
		map[string]any{
			"filekey":       fileKey,
			"media_type":    mediaType,
			"to_user_id":    to,
			"rawsize":       len(rawBytes),
			"rawfilemd5":    hex.EncodeToString(sum[:]),
			"filesize":      len(ciphertext),
			"no_need_thumb": true,
			"aeskey":        hex.EncodeToString(key),
			"base_info":     a.info(),
		},
		token,
		true,
		a.requestTimeout,
		&reserved,
	)
	if err != nil {
		return Item{}, err
	}
	if code := rejectedCode(reserved.Ret, reserved.ErrCode); code != "" {
		return Item{}, &Error{Code: "upload_rejected", Message: "WeChat refused the upload", ProviderCode: code}
	}

	targetURL, err := UploadURL(UploadReservation{
		UploadFullURL: reserved.UploadFullURL,
		UploadParam:   reserved.UploadParam,
	}, fileKey)
	if err != nil {
		return Item{}, err
	}

	var downloadParam string
	for attempt := 1; attempt <= 3 && downloadParam == ""; attempt++ {
		if err := ctx.Err(); err != nil {
			return Item{}, err
		}
		upCtx, cancel := context.WithTimeout(ctx, a.transferTimeout)
		req, reqErr := http.NewRequestWithContext(upCtx, http.MethodPost, targetURL, bytes.NewReader(ciphertext))
		if reqErr != nil {
			cancel()
			return Item{}, reqErr
		}
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, doErr := a.httpClient.Do(req)
		if doErr != nil {
			timedOut := upCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil
			cancel()
			if attempt == 3 {
				if timedOut || isTimeout(doErr) {
					return Item{}, &timeoutErr{cause: doErr}
				}
				return Item{}, doErr
			}
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		cancel()

		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			if attempt == 3 {
				return Item{}, &Error{
					Code:         "upload_rejected",
					Message:      fmt.Sprintf("WeChat upload redirected unexpectedly (HTTP %d)", resp.StatusCode),
					ProviderCode: strconv.Itoa(resp.StatusCode),
				}
			}
			continue
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return Item{}, &Error{
				Code:         "upload_rejected",
				Message:      fmt.Sprintf("WeChat refused the upload (HTTP %d)", resp.StatusCode),
				ProviderCode: strconv.Itoa(resp.StatusCode),
			}
		}
		if resp.StatusCode == http.StatusOK {
			downloadParam = strings.TrimSpace(resp.Header.Get("x-encrypted-param"))
			if downloadParam == "" {
				return Item{}, &Error{
					Code:    "upload_failed",
					Message: "WeChat's upload answer had no download parameter",
				}
			}
			break
		}
		if attempt == 3 {
			return Item{}, &Error{
				Code:         "upload_failed",
				Message:      fmt.Sprintf("WeChat upload failed (HTTP %d)", resp.StatusCode),
				ProviderCode: strconv.Itoa(resp.StatusCode),
			}
		}
	}

	media := &Media{
		EncryptQueryParam: downloadParam,
		AESKey:            base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(key))),
		EncryptType:       1,
	}
	if isImage {
		return Item{
			Type: 2,
			ImageItem: &ImageItem{
				Media:   media,
				MidSize: len(ciphertext),
			},
		}, nil
	}
	return Item{
		Type: 4,
		FileItem: &FileItem{
			Media:    media,
			FileName: name,
			Len:      strconv.Itoa(len(rawBytes)),
		},
	}, nil
}

const aesBlockSize = 16

func (a *apiClient) downloadMedia(ctx context.Context, item MediaHolder) ([]byte, error) {
	key, err := MediaKey(item)
	if err != nil {
		return nil, err
	}
	dlURL, err := MediaDownloadURL(item.Media)
	if err != nil {
		return nil, err
	}
	dlCtx, cancel := context.WithTimeout(ctx, a.transferTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, dlURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{
			Code:         "download_failed",
			Message:      fmt.Sprintf("WeChat file download failed (HTTP %d)", resp.StatusCode),
			ProviderCode: strconv.Itoa(resp.StatusCode),
		}
	}
	ciphertext, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return DecryptMedia(ciphertext, key)
}

func (a *apiClient) typing(ctx context.Context, base, token, to, contextToken string, on bool) error {
	cfgBody := map[string]any{
		"ilink_user_id": to,
		"base_info":     a.info(),
	}
	if contextToken != "" {
		cfgBody["context_token"] = contextToken
	}
	var cfg struct {
		statusEnvelope
		TypingTicket string `json:"typing_ticket"`
	}
	if err := a.requestJSON(ctx, http.MethodPost, base, "ilink/bot/getconfig", cfgBody, token, true, defaultControlTimeout, &cfg); err != nil {
		return err
	}
	if cfg.TypingTicket == "" {
		return nil
	}
	status := 2
	if on {
		status = 1
	}
	return a.requestJSON(
		ctx,
		http.MethodPost,
		base,
		"ilink/bot/sendtyping",
		map[string]any{
			"ilink_user_id": to,
			"typing_ticket": cfg.TypingTicket,
			"status":        status,
			"base_info":     a.info(),
		},
		token,
		true,
		defaultControlTimeout,
		nil,
	)
}
