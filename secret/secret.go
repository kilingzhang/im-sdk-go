// Package secret stores opaque bot credentials in the operating system's native
// credential vault (macOS Keychain via `security -i` stdin with Base64 chunking and
// read-back verification, Windows CurrentUser DPAPI, Linux Secret Service, or a 0600 file).
package secret

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultService = "app.imsdk"
	keychainChunk  = 3000 // `security -i` caps command lines at 4095 bytes.
)

// Store persists secret values by opaque reference key.
type Store interface {
	Get(ctx context.Context, ref string) (string, bool, error)
	Set(ctx context.Context, ref string, value string) error
	Delete(ctx context.Context, ref string) error
}

// Runner executes a helper program with stdin and returns its exit code and stdout.
type Runner func(ctx context.Context, name string, args []string, stdin string) (code int, stdout string, err error)

// DefaultRunner executes external credential helpers with a 15s default timeout.
func DefaultRunner(ctx context.Context, name string, args []string, stdin string) (int, string, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(stdin)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode(), out.String(), nil
		}
		return 127, "", err
	}
	return 0, out.String(), nil
}

// Default returns the native OS SecretStore for the current runtime.GOOS.
func Default(service string, fallbackPath string) Store {
	if service == "" {
		service = DefaultService
	}
	switch runtime.GOOS {
	case "darwin":
		return NewKeychain(service, DefaultRunner)
	case "windows":
		return NewWindowsDPAPI(fallbackPath, DefaultRunner)
	default:
		return NewLinuxSecretService(service, fallbackPath, DefaultRunner)
	}
}

// MemoryStore is a concurrency-safe in-memory Store for unit tests.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemory creates an empty MemoryStore.
func NewMemory() *MemoryStore {
	return &MemoryStore{data: make(map[string]string)}
}

func (m *MemoryStore) Get(_ context.Context, ref string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[ref]
	return v, ok, nil
}

func (m *MemoryStore) Set(_ context.Context, ref, value string) error {
	m.mu.Lock()
	m.data[ref] = value
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) Delete(_ context.Context, ref string) error {
	m.mu.Lock()
	delete(m.data, ref)
	m.mu.Unlock()
	return nil
}

// FileStore persists secrets in a mode-0600 JSON file.
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFile creates a mode-0600 JSON FileStore at path.
func NewFile(path string) *FileStore {
	return &FileStore{path: path}
}

func (f *FileStore) readLocked() (map[string]string, error) {
	raw, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	if len(bytes.TrimSpace(raw)) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *FileStore) writeLocked(data map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(f.path), 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	_ = os.Chmod(tmp, 0o600)
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	return os.Chmod(f.path, 0o600)
}

func (f *FileStore) Get(_ context.Context, ref string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.readLocked()
	if err != nil {
		return "", false, err
	}
	v, ok := m[ref]
	return v, ok, nil
}

func (f *FileStore) Set(_ context.Context, ref, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.readLocked()
	if err != nil {
		return err
	}
	next := maps.Clone(m)
	next[ref] = value
	return f.writeLocked(next)
}

func (f *FileStore) Delete(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.readLocked()
	if err != nil {
		return err
	}
	if _, ok := m[ref]; !ok {
		return nil
	}
	delete(m, ref)
	return f.writeLocked(m)
}

var (
	reParts = regexp.MustCompile(`^parts:(\d+)$`)
	reHex   = regexp.MustCompile(`^(?:[0-9a-fA-F]{2})+$`)
)

func quoteKeychainArg(s string) (string, error) {
	if strings.ContainsAny(s, "\"\\\r\n") {
		return "", fmt.Errorf("secret: invalid reference %q", s)
	}
	return `"` + s + `"`, nil
}

// KeychainStore writes secrets via `/usr/bin/security -i` stdin (keeping secrets out of argv),
// Base64-encodes values, splits values >3000 chars into `<ref>#1..n`, and verifies every write by reading back.
type KeychainStore struct {
	service string
	run     Runner
}

// NewKeychain creates a macOS KeychainStore.
func NewKeychain(service string, run Runner) *KeychainStore {
	if service == "" {
		service = DefaultService
	}
	if run == nil {
		run = DefaultRunner
	}
	return &KeychainStore{service: service, run: run}
}

func (k *KeychainStore) readItem(ctx context.Context, account string) (string, bool, error) {
	code, stdout, err := k.run(ctx, "/usr/bin/security", []string{"find-generic-password", "-s", k.service, "-a", account, "-w"}, "")
	if err != nil {
		return "", false, err
	}
	if code != 0 {
		return "", false, nil
	}
	return strings.TrimSuffix(stdout, "\n"), true, nil
}

func parsePartCount(stored string) int {
	m := reParts.FindStringSubmatch(stored)
	if len(m) != 2 {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

func (k *KeychainStore) Get(ctx context.Context, ref string) (string, bool, error) {
	if _, err := quoteKeychainArg(ref); err != nil {
		return "", false, err
	}
	stored, ok, err := k.readItem(ctx, ref)
	if err != nil || !ok {
		return "", false, err
	}
	if count := parsePartCount(stored); count > 0 {
		var joined strings.Builder
		for i := 1; i <= count; i++ {
			part, partOK, err := k.readItem(ctx, fmt.Sprintf("%s#%d", ref, i))
			if err != nil || !partOK {
				return "", false, err
			}
			joined.WriteString(part)
		}
		decoded, err := base64.StdEncoding.DecodeString(joined.String())
		if err != nil {
			return "", false, err
		}
		return string(decoded), true, nil
	}
	if after, ok := strings.CutPrefix(stored, "b64:"); ok {
		decoded, err := base64.StdEncoding.DecodeString(after)
		if err != nil {
			return "", false, err
		}
		return string(decoded), true, nil
	}
	if reHex.MatchString(stored) {
		if decoded, err := hex.DecodeString(stored); err == nil {
			return string(decoded), true, nil
		}
	}
	return stored, true, nil
}

func (k *KeychainStore) Set(ctx context.Context, ref, value string) error {
	qService, err := quoteKeychainArg(k.service)
	if err != nil {
		return err
	}
	if _, err := quoteKeychainArg(ref); err != nil {
		return err
	}
	prevStored, _, _ := k.readItem(ctx, ref)
	previous := parsePartCount(prevStored)

	encoded := base64.StdEncoding.EncodeToString([]byte(value))
	addCmd := func(account, data string) (string, error) {
		qAcc, err := quoteKeychainArg(account)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`add-generic-password -s %s -a %s -U -w "%s"`, qService, qAcc, data), nil
	}

	var commands []string
	count := 0
	if len(encoded) <= keychainChunk {
		cmd, err := addCmd(ref, "b64:"+encoded)
		if err != nil {
			return err
		}
		commands = append(commands, cmd)
	} else {
		for at := 0; at < len(encoded); at += keychainChunk {
			count++
			end := min(at+keychainChunk, len(encoded))
			cmd, err := addCmd(fmt.Sprintf("%s#%d", ref, count), encoded[at:end])
			if err != nil {
				return err
			}
			commands = append(commands, cmd)
		}
		cmd, err := addCmd(ref, fmt.Sprintf("parts:%d", count))
		if err != nil {
			return err
		}
		commands = append(commands, cmd)
	}
	for i := count + 1; i <= previous; i++ {
		qAcc, _ := quoteKeychainArg(fmt.Sprintf("%s#%d", ref, i))
		commands = append(commands, fmt.Sprintf(`delete-generic-password -s %s -a %s`, qService, qAcc))
	}

	if _, _, err := k.run(ctx, "/usr/bin/security", []string{"-i"}, strings.Join(commands, "\n")+"\n"); err != nil {
		return err
	}
	readBack, ok, err := k.Get(ctx, ref)
	if err != nil || !ok || readBack != value {
		return errors.New("secret: Keychain read-back verification failed")
	}
	return nil
}

func (k *KeychainStore) Delete(ctx context.Context, ref string) error {
	qService, err := quoteKeychainArg(k.service)
	if err != nil {
		return err
	}
	if _, err := quoteKeychainArg(ref); err != nil {
		return err
	}
	prevStored, _, _ := k.readItem(ctx, ref)
	count := parsePartCount(prevStored)
	var commands []string
	for i := 1; i <= count; i++ {
		qAcc, _ := quoteKeychainArg(fmt.Sprintf("%s#%d", ref, i))
		commands = append(commands, fmt.Sprintf(`delete-generic-password -s %s -a %s`, qService, qAcc))
	}
	qRef, _ := quoteKeychainArg(ref)
	commands = append(commands, fmt.Sprintf(`delete-generic-password -s %s -a %s`, qService, qRef))
	_, _, err = k.run(ctx, "/usr/bin/security", []string{"-i"}, strings.Join(commands, "\n")+"\n")
	return err
}

// WindowsDPAPIStore encrypts secrets with CurrentUser DPAPI via PowerShell and stores ciphertexts in FileStore.
type WindowsDPAPIStore struct {
	file  *FileStore
	run   Runner
	mu    sync.Mutex
	cache map[string]string
}

// NewWindowsDPAPI creates a WindowsDPAPIStore.
func NewWindowsDPAPI(path string, run Runner) *WindowsDPAPIStore {
	if run == nil {
		run = DefaultRunner
	}
	return &WindowsDPAPIStore{
		file:  NewFile(path),
		run:   run,
		cache: make(map[string]string),
	}
}

func (w *WindowsDPAPIStore) dpapi(ctx context.Context, op string, b64Input string) (string, error) {
	script := "Add-Type -AssemblyName System.Security;" +
		"$b=[Convert]::FromBase64String([Console]::In.ReadToEnd().Trim());" +
		fmt.Sprintf("$o=[Security.Cryptography.ProtectedData]::%s($b,$null,'CurrentUser');", op) +
		"[Console]::Out.Write([Convert]::ToBase64String($o))"
	code, stdout, err := w.run(ctx, "powershell.exe", []string{"-NoProfile", "-NonInteractive", "-Command", script}, b64Input)
	if err != nil || code != 0 {
		return "", fmt.Errorf("secret: Windows DPAPI %s failed", op)
	}
	return strings.TrimSpace(stdout), nil
}

func (w *WindowsDPAPIStore) Get(ctx context.Context, ref string) (string, bool, error) {
	w.mu.Lock()
	if v, ok := w.cache[ref]; ok {
		w.mu.Unlock()
		return v, true, nil
	}
	w.mu.Unlock()

	sealed, ok, err := w.file.Get(ctx, ref)
	if err != nil || !ok {
		return "", false, err
	}
	plainB64, err := w.dpapi(ctx, "Unprotect", sealed)
	if err != nil {
		return "", false, err
	}
	raw, err := base64.StdEncoding.DecodeString(plainB64)
	if err != nil {
		return "", false, err
	}
	val := string(raw)
	w.mu.Lock()
	w.cache[ref] = val
	w.mu.Unlock()
	return val, true, nil
}

func (w *WindowsDPAPIStore) Set(ctx context.Context, ref, value string) error {
	sealed, err := w.dpapi(ctx, "Protect", base64.StdEncoding.EncodeToString([]byte(value)))
	if err != nil {
		return err
	}
	if err := w.file.Set(ctx, ref, sealed); err != nil {
		return err
	}
	w.mu.Lock()
	w.cache[ref] = value
	w.mu.Unlock()
	return nil
}

func (w *WindowsDPAPIStore) Delete(ctx context.Context, ref string) error {
	w.mu.Lock()
	delete(w.cache, ref)
	w.mu.Unlock()
	return w.file.Delete(ctx, ref)
}

// LinuxSecretStore uses `secret-tool` when a Secret Service keyring responds, falling back to a 0600 file.
type LinuxSecretStore struct {
	service   string
	file      *FileStore
	run       Runner
	probeOnce sync.Once
	hasRing   bool
}

// NewLinuxSecretService creates a LinuxSecretStore.
func NewLinuxSecretService(service string, fallbackPath string, run Runner) *LinuxSecretStore {
	if service == "" {
		service = DefaultService
	}
	if run == nil {
		run = DefaultRunner
	}
	return &LinuxSecretStore{
		service: service,
		file:    NewFile(fallbackPath),
		run:     run,
	}
}

func (l *LinuxSecretStore) keyringAvailable(ctx context.Context) bool {
	l.probeOnce.Do(func() {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		const probe = "__imsdk_probe__"
		attrs := []string{"service", l.service, "account", probe}
		code, _, err := l.run(pctx, "secret-tool", append([]string{"store", "--label=IMSDK"}, attrs...), "ok")
		if err != nil || code != 0 {
			return
		}
		rCode, stdout, rErr := l.run(pctx, "secret-tool", append([]string{"lookup"}, attrs...), "")
		_, _, _ = l.run(pctx, "secret-tool", append([]string{"clear"}, attrs...), "")
		l.hasRing = rErr == nil && rCode == 0 && stdout == "ok"
	})
	return l.hasRing
}

func (l *LinuxSecretStore) Get(ctx context.Context, ref string) (string, bool, error) {
	if l.keyringAvailable(ctx) {
		code, stdout, err := l.run(ctx, "secret-tool", []string{"lookup", "service", l.service, "account", ref}, "")
		if err == nil && code == 0 {
			return stdout, true, nil
		}
	}
	return l.file.Get(ctx, ref)
}

func (l *LinuxSecretStore) Set(ctx context.Context, ref, value string) error {
	if l.keyringAvailable(ctx) {
		code, _, err := l.run(ctx, "secret-tool", []string{"store", "--label=IMSDK " + ref, "service", l.service, "account", ref}, value)
		if err == nil && code == 0 {
			_ = l.file.Delete(ctx, ref)
			return nil
		}
	}
	return l.file.Set(ctx, ref, value)
}

func (l *LinuxSecretStore) Delete(ctx context.Context, ref string) error {
	if l.keyringAvailable(ctx) {
		_, _, _ = l.run(ctx, "secret-tool", []string{"clear", "service", l.service, "account", ref}, "")
	}
	return l.file.Delete(ctx, ref)
}
