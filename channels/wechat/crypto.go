package wechat

import (
	"crypto/aes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"strings"
)

const (
	// QRBaseURL is Tencent's default iLink bot API base URL.
	QRBaseURL = "https://ilinkai.weixin.qq.com/"
	// ProtocolVersion is the iLink channel protocol version.
	ProtocolVersion = "2.4.6"
	// ClientVersion encodes 2.4.6 as (2<<16)|(4<<8)|6.
	ClientVersion = (2 << 16) | (4 << 8) | 6
	// CDNBaseURL is Tencent's C2C media transfer prefix.
	CDNBaseURL = "https://novac2c.cdn.weixin.qq.com/c2c"
	// CDNHost is the sole allowed host for encrypted media uploads and downloads.
	CDNHost = "novac2c.cdn.weixin.qq.com"
	// MaxTextLength is the per-message character limit before outbox splitting.
	MaxTextLength = 1800
	// MaxLocalTokens is the maximum number of existing bot tokens sent with a QR login request.
	MaxLocalTokens = 10
	// MaxFileBytes bounds in-memory AES-128-ECB encryption to 100 MiB per attachment.
	MaxFileBytes = 100 * 1024 * 1024
	// MaxCachedMedia bounds cached inbound media descriptors per connection.
	MaxCachedMedia = 2000
)

// Error classifies a protocol, validation, or HTTP error from the WeChat iLink adapter.
type Error struct {
	Code         string
	Message      string
	ProviderCode string
}

func (e *Error) Error() string {
	if e.ProviderCode != "" {
		return fmt.Sprintf("wechat %s (%s): %s", e.Code, e.ProviderCode, e.Message)
	}
	return fmt.Sprintf("wechat %s: %s", e.Code, e.Message)
}

// Media describes an encrypted attachment reference on Tencent's C2C CDN.
type Media struct {
	EncryptQueryParam string `json:"encrypt_query_param,omitempty"`
	FullURL           string `json:"full_url,omitempty"`
	AESKey            string `json:"aes_key,omitempty"`
	EncryptType       int    `json:"encrypt_type,omitempty"`
}

// MediaHolder carries an inbound media descriptor and its hex or base64 AES-128 key.
type MediaHolder struct {
	Media  *Media
	AESKey string
}

// UploadReservation holds the fields returned by ilink/bot/getuploadurl.
type UploadReservation struct {
	Ret           any    `json:"ret,omitempty"`
	ErrCode       any    `json:"errcode,omitempty"`
	UploadFullURL string `json:"upload_full_url,omitempty"`
	UploadParam   string `json:"upload_param,omitempty"`
}

func isTencentHost(host string) bool {
	h := strings.ToLower(host)
	return h == "weixin.qq.com" ||
		strings.HasSuffix(h, ".weixin.qq.com") ||
		h == "wechat.com" ||
		strings.HasSuffix(h, ".wechat.com")
}

func hasAllowedHTTPSPort(u *url.URL) bool {
	port := u.Port()
	if port == "" {
		return !strings.HasSuffix(u.Host, ":")
	}
	return port == "443"
}

// TrustedQRURL validates that a login QR link points to HTTPS port 443 on a Tencent domain.
func TrustedQRURL(value string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", &Error{Code: "invalid_qr", Message: "WeChat returned an invalid QR address"}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return "", &Error{Code: "invalid_qr", Message: "WeChat returned an invalid QR address"}
	}
	if u.Scheme != "https" || u.User != nil || !isTencentHost(u.Hostname()) || !hasAllowedHTTPSPort(u) {
		return "", &Error{Code: "untrusted_qr", Message: "WeChat returned a QR address outside Tencent"}
	}
	u.Host = strings.ToLower(u.Hostname())
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

// TrustedBaseURL validates an API host or URL returned during login/redirect and normalizes a trailing slash.
func TrustedBaseURL(value string, fallback string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return fallback, nil
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return "", &Error{Code: "untrusted_base_url", Message: "WeChat returned an invalid address"}
	}
	if u.Scheme != "https" || u.User != nil || !isTencentHost(u.Hostname()) || !hasAllowedHTTPSPort(u) {
		return "", &Error{Code: "untrusted_base_url", Message: "WeChat returned an address outside Tencent"}
	}
	escapedPath := u.EscapedPath()
	if escapedPath == "" {
		escapedPath = "/"
	} else if !strings.HasSuffix(escapedPath, "/") {
		escapedPath += "/"
	}
	return "https://" + strings.ToLower(u.Hostname()) + escapedPath, nil
}

// MediaDownloadURL builds or validates the HTTPS download URL on novac2c.cdn.weixin.qq.com/c2c/.
func MediaDownloadURL(media *Media) (string, error) {
	if media != nil {
		if q := strings.TrimSpace(media.EncryptQueryParam); q != "" {
			v := url.Values{}
			v.Set("encrypted_query_param", q)
			return CDNBaseURL + "/download?" + v.Encode(), nil
		}
	}
	full := ""
	if media != nil {
		full = strings.TrimSpace(media.FullURL)
	}
	if full == "" {
		return "", &Error{Code: "missing_media_url", Message: "this file has no download address"}
	}
	u, err := url.Parse(full)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return "", &Error{Code: "missing_media_url", Message: "this file has no download address"}
	}
	cleaned := path.Clean(u.Path)
	if u.Scheme != "https" ||
		u.User != nil ||
		strings.ToLower(u.Hostname()) != CDNHost ||
		!hasAllowedHTTPSPort(u) ||
		strings.Contains(u.Path, "..") ||
		!strings.HasPrefix(u.Path, "/c2c/") ||
		!strings.HasPrefix(cleaned, "/c2c/") {
		return "", &Error{Code: "untrusted_media_url", Message: "WeChat sent a file address outside its CDN"}
	}
	u.Host = CDNHost
	u.Fragment = ""
	return u.String(), nil
}

// UploadURL builds or validates the HTTPS upload URL on novac2c.cdn.weixin.qq.com/c2c/upload.
func UploadURL(res UploadReservation, fileKey string) (string, error) {
	var u *url.URL
	var err error
	if full := strings.TrimSpace(res.UploadFullURL); full != "" {
		u, err = url.Parse(full)
		if err != nil || u.Host == "" || u.Hostname() == "" {
			return "", &Error{Code: "invalid_upload_url", Message: "WeChat returned an invalid upload address"}
		}
	} else if param := strings.TrimSpace(res.UploadParam); param != "" {
		u, err = url.Parse(CDNBaseURL + "/upload")
		if err != nil {
			return "", &Error{Code: "invalid_upload_url", Message: "WeChat returned an invalid upload address"}
		}
		q := u.Query()
		q.Set("encrypted_query_param", param)
		q.Set("filekey", fileKey)
		u.RawQuery = q.Encode()
	} else {
		return "", &Error{Code: "missing_upload_url", Message: "WeChat returned no upload address"}
	}

	if u.Scheme != "https" ||
		u.User != nil ||
		strings.ToLower(u.Hostname()) != CDNHost ||
		!hasAllowedHTTPSPort(u) ||
		strings.Contains(u.Path, "..") ||
		u.Path != "/c2c/upload" ||
		path.Clean(u.Path) != "/c2c/upload" {
		return "", &Error{Code: "untrusted_upload_url", Message: "WeChat returned an upload address outside its CDN"}
	}
	u.Host = CDNHost
	u.Fragment = ""
	return u.String(), nil
}

func isHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func isStdBase64(s string) bool {
	if len(s) == 0 || len(s)%4 != 0 {
		return false
	}
	eq := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '=' {
			eq++
			if eq > 2 || i < len(s)-2 {
				return false
			}
			continue
		}
		if eq > 0 {
			return false
		}
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/') {
			return false
		}
	}
	return true
}

// MediaKey extracts the 16-byte AES-128 key from either a 32-char hex aeskey or base64 media.aes_key.
func MediaKey(item MediaHolder) ([]byte, error) {
	if h := strings.TrimSpace(item.AESKey); h != "" {
		if !isHex32(h) {
			return nil, &Error{Code: "invalid_media_key", Message: "WeChat sent an invalid file key"}
		}
		key, err := hex.DecodeString(h)
		if err != nil || len(key) != aes.BlockSize {
			return nil, &Error{Code: "invalid_media_key", Message: "WeChat sent an invalid file key"}
		}
		return key, nil
	}
	encoded := ""
	if item.Media != nil {
		encoded = strings.TrimSpace(item.Media.AESKey)
	}
	if isStdBase64(encoded) {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err == nil {
			if len(raw) == aes.BlockSize {
				return raw, nil
			}
			if len(raw) == 32 && isHex32(string(raw)) {
				key, err := hex.DecodeString(string(raw))
				if err == nil && len(key) == aes.BlockSize {
					return key, nil
				}
			}
		}
	}
	return nil, &Error{Code: "invalid_media_key", Message: "WeChat sent an invalid file key"}
}

// EncryptAES128ECB encrypts plaintext with PKCS#7 padding in 16-byte AES-128-ECB blocks.
func EncryptAES128ECB(plaintext, key []byte) ([]byte, error) {
	if len(key) != aes.BlockSize {
		return nil, &Error{Code: "invalid_media_key", Message: "AES-128 key must be 16 bytes"}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	padLen := aes.BlockSize - (len(plaintext) % aes.BlockSize)
	padded := make([]byte, len(plaintext)+padLen)
	copy(padded, plaintext)
	for i := len(plaintext); i < len(padded); i++ {
		padded[i] = byte(padLen)
	}
	out := make([]byte, len(padded))
	for off := 0; off < len(padded); off += aes.BlockSize {
		block.Encrypt(out[off:off+aes.BlockSize], padded[off:off+aes.BlockSize])
	}
	return out, nil
}

// DecryptMedia decrypts AES-128-ECB ciphertext and validates PKCS#7 padding.
func DecryptMedia(ciphertext, key []byte) ([]byte, error) {
	if len(key) != aes.BlockSize || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, &Error{Code: "invalid_media", Message: "WeChat sent a damaged file"}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, &Error{Code: "invalid_media", Message: "WeChat sent a damaged file"}
	}
	buf := make([]byte, len(ciphertext))
	for off := 0; off < len(ciphertext); off += aes.BlockSize {
		block.Decrypt(buf[off:off+aes.BlockSize], ciphertext[off:off+aes.BlockSize])
	}
	padLen := int(buf[len(buf)-1])
	if padLen < 1 || padLen > aes.BlockSize || padLen > len(buf) {
		return nil, &Error{Code: "invalid_media", Message: "WeChat sent a damaged file"}
	}
	for i := len(buf) - padLen; i < len(buf); i++ {
		if buf[i] != byte(padLen) {
			return nil, &Error{Code: "invalid_media", Message: "WeChat sent a damaged file"}
		}
	}
	return buf[:len(buf)-padLen], nil
}
