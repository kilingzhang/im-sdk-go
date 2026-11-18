package wecom

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	// GenerateURL requests a fresh QR onboarding code from WeCom.
	GenerateURL = "https://work.weixin.qq.com/ai/qc/generate"
	// PollURL polls the scan status of a WeCom QR onboarding code.
	PollURL = "https://work.weixin.qq.com/ai/qc/query_result"
	// DefaultWSURL is the official WeCom intelligent bot WebSocket endpoint.
	DefaultWSURL = "wss://openws.work.weixin.qq.com"

	// PKCS7BlockSize is WeCom's 32-byte PKCS#7 block alignment for AES-256-CBC media.
	PKCS7BlockSize = 32
	// AESKeySize is the 32-byte decoded key length for AES-256-CBC.
	AESKeySize = 32

	// ErrCodeInvalidReqID indicates an unknown or invalid passive reply req_id.
	ErrCodeInvalidReqID = 846605
	// ErrCodeStreamExpired indicates a passive reply stream whose window has expired.
	ErrCodeStreamExpired = 846608
)

// AckError represents a non-zero errcode ack frame returned over the WeCom WebSocket.
type AckError struct {
	ErrCode int
	ErrMsg  string
}

func (e *AckError) Error() string {
	if e.ErrMsg != "" {
		return fmt.Sprintf("wecom ack error %d: %s", e.ErrCode, e.ErrMsg)
	}
	return fmt.Sprintf("wecom ack error %d", e.ErrCode)
}

// AuthError represents a failed aibot_subscribe authentication attempt.
type AuthError struct {
	ErrCode int
	ErrMsg  string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("Authentication failed: %s (code: %d)", e.ErrMsg, e.ErrCode)
}

// ErrCodeOf extracts a numeric WeCom errcode from err if present.
func ErrCodeOf(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var ackErr *AckError
	if errors.As(err, &ackErr) {
		return ackErr.ErrCode, true
	}
	var authErr *AuthError
	if errors.As(err, &authErr) {
		return authErr.ErrCode, true
	}
	return 0, false
}

func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	var authErr *AuthError
	if errors.As(err, &authErr) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "auth")
}

// TrustedAuthURL ensures a QR onboarding URL points strictly to https://work.weixin.qq.com.
func TrustedAuthURL(value string) (string, bool) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || strings.ToLower(u.Hostname()) != "work.weixin.qq.com" {
		return "", false
	}
	if p := u.Port(); p != "" && p != "443" {
		return "", false
	}
	if u.Port() == "" && strings.HasSuffix(u.Host, ":") {
		return "", false
	}
	u.Host = "work.weixin.qq.com"
	return u.String(), true
}

// DecodeAESKey decodes a Base64 WeCom AES-256 key (tolerating omitted trailing '=' padding).
func DecodeAESKey(aesKey string) ([]byte, error) {
	trimmed := strings.TrimSpace(aesKey)
	if trimmed == "" {
		return nil, errors.New("wecom: aesKey must be non-empty")
	}
	if rem := len(trimmed) % 4; rem != 0 {
		trimmed += strings.Repeat("=", 4-rem)
	}
	key, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("wecom: invalid base64 aesKey: %w", err)
	}
	if len(key) != AESKeySize {
		return nil, fmt.Errorf("wecom: expected %d-byte aesKey, got %d", AESKeySize, len(key))
	}
	return key, nil
}

// EncryptFile encrypts plaintext with AES-256-CBC (IV = key[:16]) and 32-byte PKCS#7 padding.
func EncryptFile(plaintext []byte, aesKey string) ([]byte, error) {
	key, err := DecodeAESKey(aesKey)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	mod := len(plaintext) % PKCS7BlockSize
	padLen := PKCS7BlockSize - mod
	padded := make([]byte, len(plaintext)+padLen)
	copy(padded, plaintext)
	for i := len(plaintext); i < len(padded); i++ {
		padded[i] = byte(padLen)
	}
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(out, padded)
	return out, nil
}

// DecryptFile decrypts AES-256-CBC media (IV = key[:16]) and removes 32-byte PKCS#7 padding.
func DecryptFile(encrypted []byte, aesKey string) ([]byte, error) {
	if len(encrypted) == 0 {
		return nil, errors.New("wecom: encrypted payload is empty")
	}
	if len(encrypted)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("wecom: ciphertext length %d is not a multiple of %d", len(encrypted), aes.BlockSize)
	}
	key, err := DecodeAESKey(aesKey)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	decrypted := make([]byte, len(encrypted))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(decrypted, encrypted)

	padLen := int(decrypted[len(decrypted)-1])
	if padLen < 1 || padLen > PKCS7BlockSize || padLen > len(decrypted) {
		return nil, fmt.Errorf("wecom: invalid PKCS#7 padding value %d", padLen)
	}
	for i := len(decrypted) - padLen; i < len(decrypted); i++ {
		if decrypted[i] != byte(padLen) {
			return nil, errors.New("wecom: invalid PKCS#7 padding bytes")
		}
	}
	return decrypted[:len(decrypted)-padLen], nil
}
