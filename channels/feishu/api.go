package feishu

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	defaultFeishuBaseURL     = "https://open.feishu.cn"
	defaultLarkBaseURL       = "https://open.larksuite.com"
	defaultFeishuAccountsURL = "https://accounts.feishu.cn"
	defaultLarkAccountsURL   = "https://accounts.larksuite.com"
	registrationPath         = "/oauth/v1/app/registration"
	botActiveStatus          = 2
)

type apiError struct {
	Kind       string
	StatusCode int
	FeishuCode int
	Msg        string
	Err        error
}

func (e *apiError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("feishu error %d (http %d)", e.FeishuCode, e.StatusCode)
}

func (e *apiError) Unwrap() error { return e.Err }

func classifyAPIError(err error) *apiError {
	if err == nil {
		return nil
	}
	var ae *apiError
	if errors.As(err, &ae) {
		if ae.Kind == "" {
			ae.Kind = inferErrorKind(ae.StatusCode, ae.FeishuCode, ae.Err)
		}
		return ae
	}
	return &apiError{
		Kind: inferErrorKind(0, 0, err),
		Msg:  err.Error(),
		Err:  err,
	}
}

func inferErrorKind(status, code int, err error) string {
	switch code {
	case 230020, 230017:
		return "target_revoked"
	case 99991400, 99991401:
		return "permission_denied"
	case 230002, 230001:
		return "format_error"
	}
	switch status {
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "permission_denied"
	case http.StatusBadRequest:
		return "format_error"
	case http.StatusNotFound:
		return "target_revoked"
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "send_timeout"
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return "send_timeout"
		}
	}
	return "unknown"
}

func sendResultFromError(err error) imsdk.SendResult {
	ae := classifyAPIError(err)
	switch ae.Kind {
	case "rate_limited":
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrRateLimited, Retryable: true, Detail: ae.Error()}
	case "permission_denied":
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false, Detail: ae.Error()}
	case "target_revoked":
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrChatGone, Retryable: false, Detail: ae.Error()}
	case "upload_failed":
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrTooLarge, Retryable: false, Detail: ae.Error()}
	case "not_connected":
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrNetwork, Retryable: true, Detail: ae.Error()}
	case "send_timeout":
		return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain, Detail: ae.Error()}
	default:
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: false, Detail: ae.Error()}
	}
}

type botInfo struct {
	Name      string
	OpenID    string
	Activated bool
}

type wsClientConfig struct {
	PingInterval      time.Duration
	ReconnectCount    int
	ReconnectInterval time.Duration
	ReconnectNonce    time.Duration
}

type wsEndpointInfo struct {
	URL       string
	ServiceID int32
	Config    wsClientConfig
}

type tokenEntry struct {
	token string
	until time.Time
}

type restClient struct {
	httpClient        *http.Client
	feishuBaseURL     string
	larkBaseURL       string
	feishuAccountsURL string
	larkAccountsURL   string
	requestTimeout    time.Duration
	now               func() time.Time

	mu     sync.Mutex
	tokens map[string]tokenEntry
}

func newRESTClient(opts Options) *restClient {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	feishuBase := strings.TrimRight(opts.FeishuBaseURL, "/")
	if feishuBase == "" {
		feishuBase = defaultFeishuBaseURL
	}
	larkBase := strings.TrimRight(opts.LarkBaseURL, "/")
	if larkBase == "" {
		larkBase = defaultLarkBaseURL
	}
	feishuAcc := strings.TrimRight(opts.FeishuAccountsURL, "/")
	if feishuAcc == "" {
		feishuAcc = defaultFeishuAccountsURL
	}
	larkAcc := strings.TrimRight(opts.LarkAccountsURL, "/")
	if larkAcc == "" {
		larkAcc = defaultLarkAccountsURL
	}
	timeout := opts.RequestTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &restClient{
		httpClient:        hc,
		feishuBaseURL:     feishuBase,
		larkBaseURL:       larkBase,
		feishuAccountsURL: feishuAcc,
		larkAccountsURL:   larkAcc,
		requestTimeout:    timeout,
		now:               nowFn,
		tokens:            make(map[string]tokenEntry),
	}
}

func (c *restClient) origin(domain string) string {
	if domain == "lark" {
		return c.larkBaseURL
	}
	return c.feishuBaseURL
}

func (c *restClient) withBoundedTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.requestTimeout)
}

func (c *restClient) tenantToken(ctx context.Context, appID, appSecret, domain string) (string, error) {
	key := domain + "|" + appID + "|" + appSecret
	now := c.now()

	c.mu.Lock()
	if ent, ok := c.tokens[key]; ok && now.Before(ent.until) {
		c.mu.Unlock()
		return ent.token, nil
	}
	c.mu.Unlock()

	tctx, cancel := c.withBoundedTimeout(ctx)
	defer cancel()

	rawBody, _ := json.Marshal(map[string]string{
		"app_id":     appID,
		"app_secret": appSecret,
	})
	req, err := http.NewRequestWithContext(tctx, http.MethodPost, c.origin(domain)+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(rawBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var parsed struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", &apiError{StatusCode: resp.StatusCode, Err: err}
	}
	if resp.StatusCode >= 400 || parsed.Code != 0 || parsed.TenantAccessToken == "" {
		return "", &apiError{
			StatusCode: resp.StatusCode,
			FeishuCode: parsed.Code,
			Msg:        fmt.Sprintf("feishu auth failed: %s (%d)", parsed.Msg, parsed.Code),
		}
	}

	ttlSec := parsed.Expire - 60
	if ttlSec < 60 {
		ttlSec = 60
	}
	c.mu.Lock()
	c.tokens[key] = tokenEntry{
		token: parsed.TenantAccessToken,
		until: c.now().Add(time.Duration(ttlSec) * time.Second),
	}
	c.mu.Unlock()
	return parsed.TenantAccessToken, nil
}

func (c *restClient) doJSON(ctx context.Context, creds appCreds, method, path string, query url.Values, body any, bounded bool, out any) error {
	tok, err := c.tenantToken(ctx, creds.appID, creds.appSecret, creds.domain)
	if err != nil {
		return err
	}
	rctx := ctx
	cancel := func() {}
	if bounded {
		rctx, cancel = c.withBoundedTimeout(ctx)
	}
	defer cancel()

	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	fullURL := c.origin(creds.domain) + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(rctx, method, fullURL, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		return &apiError{StatusCode: resp.StatusCode, Err: err}
	}
	var env struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	_ = json.Unmarshal(rawResp, &env)
	if resp.StatusCode >= 400 || env.Code != 0 {
		return &apiError{
			StatusCode: resp.StatusCode,
			FeishuCode: env.Code,
			Msg:        env.Msg,
		}
	}
	if out != nil && len(rawResp) > 0 {
		if err := json.Unmarshal(rawResp, out); err != nil {
			return err
		}
	}
	return nil
}

func (c *restClient) botInfo(ctx context.Context, creds appCreds) (botInfo, error) {
	var resp struct {
		Bot struct {
			AppName        string `json:"app_name"`
			OpenID         string `json:"open_id"`
			ActivateStatus int    `json:"activate_status"`
		} `json:"bot"`
	}
	if err := c.doJSON(ctx, creds, http.MethodGet, "/open-apis/bot/v3/info/", nil, nil, true, &resp); err != nil {
		return botInfo{}, err
	}
	name := resp.Bot.AppName
	if name == "" {
		name = "Feishu bot"
	}
	openID := resp.Bot.OpenID
	if openID == "" {
		openID = creds.appID
	}
	return botInfo{
		Name:      name,
		OpenID:    openID,
		Activated: resp.Bot.ActivateStatus == botActiveStatus,
	}, nil
}

func (c *restClient) appScopes(ctx context.Context, creds appCreds) (map[string]bool, error) {
	var resp struct {
		Data struct {
			Scopes []struct {
				ScopeName   string `json:"scope_name"`
				GrantStatus int    `json:"grant_status"`
			} `json:"scopes"`
		} `json:"data"`
	}
	if err := c.doJSON(ctx, creds, http.MethodGet, "/open-apis/application/v6/scopes", nil, nil, true, &resp); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(resp.Data.Scopes))
	for _, s := range resp.Data.Scopes {
		if s.ScopeName != "" {
			out[s.ScopeName] = s.GrantStatus == 1
		}
	}
	return out, nil
}

func (c *restClient) pullWSEndpoint(ctx context.Context, creds appCreds) (wsEndpointInfo, error) {
	rctx, cancel := c.withBoundedTimeout(ctx)
	defer cancel()

	raw, _ := json.Marshal(map[string]string{
		"AppID":     creds.appID,
		"AppSecret": creds.appSecret,
	})
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, c.origin(creds.domain)+"/callback/ws/endpoint", bytes.NewReader(raw))
	if err != nil {
		return wsEndpointInfo{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("locale", "zh")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return wsEndpointInfo{}, err
	}
	defer resp.Body.Close()

	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			URL          string `json:"URL"`
			ClientConfig struct {
				PingInterval      int `json:"PingInterval"`
				ReconnectCount    int `json:"ReconnectCount"`
				ReconnectInterval int `json:"ReconnectInterval"`
				ReconnectNonce    int `json:"ReconnectNonce"`
			} `json:"ClientConfig"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return wsEndpointInfo{}, &apiError{StatusCode: resp.StatusCode, Err: err}
	}
	if resp.StatusCode >= 400 || body.Code != 0 || body.Data.URL == "" {
		return wsEndpointInfo{}, &apiError{
			StatusCode: resp.StatusCode,
			FeishuCode: body.Code,
			Msg:        body.Msg,
		}
	}

	u, err := url.Parse(body.Data.URL)
	if err != nil {
		return wsEndpointInfo{}, err
	}
	var serviceID int32
	if sid := u.Query().Get("service_id"); sid != "" {
		var parsed int
		_, _ = fmt.Sscanf(sid, "%d", &parsed)
		serviceID = int32(parsed)
	}

	pingInt := time.Duration(body.Data.ClientConfig.PingInterval) * time.Second
	if pingInt <= 0 {
		pingInt = 120 * time.Second
	}
	reconnInt := time.Duration(body.Data.ClientConfig.ReconnectInterval) * time.Second
	if reconnInt <= 0 {
		reconnInt = time.Second
	}
	reconnNonce := time.Duration(body.Data.ClientConfig.ReconnectNonce) * time.Second

	return wsEndpointInfo{
		URL:       body.Data.URL,
		ServiceID: serviceID,
		Config: wsClientConfig{
			PingInterval:      pingInt,
			ReconnectCount:    body.Data.ClientConfig.ReconnectCount,
			ReconnectInterval: reconnInt,
			ReconnectNonce:    reconnNonce,
		},
	}, nil
}

func (c *restClient) uploadImage(ctx context.Context, creds appCreds, data []byte, fileName string) (string, error) {
	tok, err := c.tenantToken(ctx, creds.appID, creds.appSecret, creds.domain)
	if err != nil {
		return "", err
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("image_type", "message")
	fw, err := mw.CreateFormFile("image", fileName)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	if _, err := fw.Write(data); err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	_ = mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin(creds.domain)+"/open-apis/im/v1/images", &body)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	defer resp.Body.Close()

	var parsed struct {
		Code     int    `json:"code"`
		Msg      string `json:"msg"`
		ImageKey string `json:"image_key"`
		Data     struct {
			ImageKey string `json:"image_key"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", &apiError{Kind: "upload_failed", StatusCode: resp.StatusCode, Err: err}
	}
	key := parsed.Data.ImageKey
	if key == "" {
		key = parsed.ImageKey
	}
	if resp.StatusCode >= 400 || parsed.Code != 0 || key == "" {
		return "", &apiError{Kind: "upload_failed", StatusCode: resp.StatusCode, FeishuCode: parsed.Code, Msg: parsed.Msg}
	}
	return key, nil
}

func (c *restClient) uploadFile(ctx context.Context, creds appCreds, data []byte, fileName string) (string, error) {
	tok, err := c.tenantToken(ctx, creds.appID, creds.appSecret, creds.domain)
	if err != nil {
		return "", err
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("file_type", "stream")
	_ = mw.WriteField("file_name", fileName)
	fw, err := mw.CreateFormFile("file", fileName)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	if _, err := fw.Write(data); err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	_ = mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin(creds.domain)+"/open-apis/im/v1/files", &body)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", &apiError{Kind: "upload_failed", Err: err}
	}
	defer resp.Body.Close()

	var parsed struct {
		Code    int    `json:"code"`
		Msg     string `json:"msg"`
		FileKey string `json:"file_key"`
		Data    struct {
			FileKey string `json:"file_key"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", &apiError{Kind: "upload_failed", StatusCode: resp.StatusCode, Err: err}
	}
	key := parsed.Data.FileKey
	if key == "" {
		key = parsed.FileKey
	}
	if resp.StatusCode >= 400 || parsed.Code != 0 || key == "" {
		return "", &apiError{Kind: "upload_failed", StatusCode: resp.StatusCode, FeishuCode: parsed.Code, Msg: parsed.Msg}
	}
	return key, nil
}

func (c *restClient) downloadMessageResource(ctx context.Context, creds appCreds, messageID, fileKey, resType string) ([]byte, error) {
	tok, err := c.tenantToken(ctx, creds.appID, creds.appSecret, creds.domain)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if resType == "image" {
		q.Set("type", "image")
	} else {
		q.Set("type", "file")
	}
	fullURL := fmt.Sprintf("%s/open-apis/im/v1/messages/%s/resources/%s?%s",
		c.origin(creds.domain), url.PathEscape(messageID), url.PathEscape(fileKey), q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, &apiError{StatusCode: resp.StatusCode, Msg: string(raw)}
	}
	return io.ReadAll(resp.Body)
}

type registrationResponse struct {
	VerificationURIComplete string  `json:"verification_uri_complete"`
	DeviceCode              string  `json:"device_code"`
	ExpiresIn               float64 `json:"expires_in"`
	Interval                float64 `json:"interval"`
	ClientID                string  `json:"client_id"`
	ClientSecret            string  `json:"client_secret"`
	Error                   string  `json:"error"`
	ErrorDescription        string  `json:"error_description"`
	UserInfo                struct {
		OpenID      string `json:"open_id"`
		TenantBrand string `json:"tenant_brand"`
	} `json:"user_info"`
}

func (c *restClient) postRegistration(ctx context.Context, baseURL string, form url.Values) (registrationResponse, error) {
	rctx, cancel := c.withBoundedTimeout(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodPost, baseURL+registrationPath, strings.NewReader(form.Encode()))
	if err != nil {
		return registrationResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return registrationResponse{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return registrationResponse{}, err
	}
	var out registrationResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		if resp.StatusCode >= 400 {
			return registrationResponse{}, fmt.Errorf("feishu registration HTTP %d", resp.StatusCode)
		}
		return registrationResponse{}, err
	}
	// RFC 8628 returns HTTP 400 for authorization_pending / slow_down / access_denied / expired_token.
	if resp.StatusCode >= 400 && out.Error == "" {
		return registrationResponse{}, fmt.Errorf("feishu registration HTTP %d", resp.StatusCode)
	}
	return out, nil
}

func encodeAddonsParam(scopes []string) (string, error) {
	type addonsPayload struct {
		Scopes struct {
			Tenant []string `json:"tenant"`
		} `json:"scopes"`
		Events struct {
			Items struct {
				Tenant []string `json:"tenant"`
			} `json:"items"`
		} `json:"events"`
	}
	var p addonsPayload
	p.Scopes.Tenant = scopes
	p.Events.Items.Tenant = []string{"im.message.receive_v1", "im.chat.member.bot.added_v1"}

	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(raw); err != nil {
		return "", err
	}
	if err := gw.Close(); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(gzBuf.Bytes()), nil
}
