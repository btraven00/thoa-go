// Package thoa is a Go port of the dataset (data lake) parts of the thoa CLI
// (https://pypi.org/project/thoa/, ported from 0.1.8): upload local files as a
// dataset, download a dataset by id.
package thoa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ClientVersion is sent as X-Client-Version; the server answers 426 to clients
// older than its MIN_CLIENT_VERSION. Tracks the python release this ports.
const ClientVersion = "0.1.8"

type Client struct {
	BaseURL string // e.g. https://api.thoa.io (no /api suffix)
	APIKey  string
	HTTP    *http.Client // API calls
	Blob    *http.Client // SAS transfers; no timeout, files can be huge
}

// NewClient resolves settings like the python CLI, plus a gothoa config file.
// Precedence: env (THOA_API_KEY, THOA_API_URL, THOA_API_TIMEOUT), then
// $XDG_CONFIG_HOME/gothoa/config.yaml (api_key, api_url), then the python
// CLI's ~/.thoa/credentials (written by `thoa login`).
func NewClient() (*Client, error) {
	cfg := readConfig()
	base := firstNonEmpty(os.Getenv("THOA_API_URL"), cfg["api_url"], "https://api.thoa.io")
	timeout := 30 * time.Second
	if s := os.Getenv("THOA_API_TIMEOUT"); s != "" {
		var n int
		if _, err := fmt.Sscan(s, &n); err == nil {
			timeout = time.Duration(n) * time.Second
		}
	}
	key := firstNonEmpty(os.Getenv("THOA_API_KEY"), cfg["api_key"], pythonCredentialsKey())
	if key == "" {
		return nil, fmt.Errorf("not authenticated: set THOA_API_KEY or api_key in %s", ConfigPath())
	}
	return &Client{
		BaseURL: strings.TrimRight(base, "/"),
		APIKey:  key,
		HTTP:    &http.Client{Timeout: timeout},
		Blob:    &http.Client{},
	}, nil
}

func ConfigPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "gothoa", "config.yaml")
}

// readConfig parses flat `key: value` lines.
// ponytail: not a YAML parser; swap in gopkg.in/yaml.v3 if the file ever nests.
func readConfig() map[string]string {
	cfg := map[string]string{}
	b, err := os.ReadFile(ConfigPath())
	if err != nil {
		return cfg
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, ":")
		k = strings.TrimSpace(k)
		if !ok || k == "" || strings.HasPrefix(k, "#") {
			continue
		}
		cfg[k] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	return cfg
}

func pythonCredentialsKey() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(home, ".thoa", "credentials"))
	if err != nil {
		return ""
	}
	var creds struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(b, &creds)
	return creds.APIKey
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// APIError is a non-200 answer from the thoa API.
type APIError struct {
	Status int
	Detail string
}

func (e *APIError) Error() string { return fmt.Sprintf("thoa api: %d: %s", e.Status, e.Detail) }

// do calls /api{path}; body (if non-nil) is sent as JSON, out (if non-nil)
// receives the decoded JSON response.
func (c *Client) do(method, path string, q url.Values, body, out any) error {
	u := c.BaseURL + "/api" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, u, r)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Client-Version", ClientVersion)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Detail any `json:"detail"`
		}
		detail := strings.TrimSpace(string(b))
		if json.Unmarshal(b, &e) == nil && e.Detail != nil {
			detail = fmt.Sprint(e.Detail)
		}
		return &APIError{resp.StatusCode, detail}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}
