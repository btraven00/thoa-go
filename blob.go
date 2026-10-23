package thoa

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Azure Blob REST over SAS URLs; replaces azure.storage.blob.BlobClient.

const (
	azureVersion = "2021-08-06"
	minBlock     = 8 << 20
	maxBlocks    = 50000 // Azure's per-blob block limit
)

// Parallel bounds concurrent chunk transfers per Upload/Download call, across
// all files. Upload memory is about Parallel * chunk size (8 MiB for files
// under ~390 GiB).
var Parallel = 16

// Retries is the number of attempts per request; the wait doubles from
// retryBackoff between attempts.
var (
	Retries      = 5
	retryBackoff = time.Second
)

type blobError struct {
	status int
	msg    string
}

func (e *blobError) Error() string { return "blob: " + e.msg }

// retryable: network/io errors, 408, 429 and 5xx. Other 4xx (e.g. an expired
// SAS URL) fail immediately.
func retryable(err error) bool {
	var a *APIError
	var b *blobError
	status := 0
	switch {
	case errors.As(err, &a):
		status = a.Status
	case errors.As(err, &b):
		status = b.status
	default:
		return true
	}
	return status == 408 || status == 429 || status >= 500
}

func retry(fn func() error) error {
	var err error
	for i := 0; i < Retries; i++ {
		if i > 0 {
			time.Sleep(retryBackoff << (i - 1))
		}
		if err = fn(); err == nil || !retryable(err) {
			return err
		}
	}
	return err
}

// forEach runs fn(0..n-1) on up to workers goroutines and returns the first
// error; once one fails, work not yet started is skipped.
func forEach(n, workers int, fn func(i int) error) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	failed := func() bool { mu.Lock(); defer mu.Unlock(); return first != nil }
	next := make(chan int)
	go func() {
		for i := 0; i < n && !failed(); i++ {
			next <- i
		}
		close(next)
	}()
	for w := 0; w < min(max(workers, 1), n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				if err := fn(i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return first
}

// chunkSize is the smallest power-of-two multiple of 8 MiB that fits size in maxBlocks.
func chunkSize(size int64) int64 {
	bs := int64(minBlock)
	for bs*maxBlocks < size {
		bs *= 2
	}
	return bs
}

func md5File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sasURL(raw string, extra url.Values) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range extra {
		q[k] = v
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *Client) blobDo(method, rawURL string, extra url.Values, body []byte, hdr map[string]string, want int) (*http.Response, error) {
	u, err := sasURL(rawURL, extra)
	if err != nil {
		return nil, err
	}
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-ms-version", azureVersion)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.Blob.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != want {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, &blobError{resp.StatusCode, fmt.Sprintf("%s %s: %s", method, resp.Status, b)}
	}
	return resp, nil
}

func (c *Client) blobPut(rawURL string, extra url.Values, body []byte, hdr map[string]string) error {
	return retry(func() error {
		resp, err := c.blobDo(http.MethodPut, rawURL, extra, body, hdr, http.StatusCreated)
		if err != nil {
			return err
		}
		return resp.Body.Close()
	})
}

// uploadBlob stages the file as parallel blocks (each with Content-MD5, like
// the SDK's validate_content=True) and commits them with metadata md5/upload,
// which is what the thoa backend checks. sem is shared across files.
func (c *Client) uploadBlob(rawURL, path string, size int64, md5hex string, sem chan struct{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bs := chunkSize(size)
	ids := make([]string, (size+bs-1)/bs)
	err = forEach(len(ids), Parallel, func(i int) error {
		sem <- struct{}{}
		defer func() { <-sem }()
		off := int64(i) * bs
		buf := make([]byte, min(bs, size-off))
		if n, err := f.ReadAt(buf, off); err != nil && !(err == io.EOF && n == len(buf)) {
			return err
		}
		id := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("block-%08d", i)))
		sum := md5.Sum(buf)
		ids[i] = id
		return c.blobPut(rawURL, url.Values{"comp": {"block"}, "blockid": {id}}, buf,
			map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(sum[:])})
	})
	if err != nil {
		return err
	}

	var list strings.Builder
	list.WriteString(`<?xml version="1.0" encoding="utf-8"?><BlockList>`)
	for _, id := range ids {
		list.WriteString("<Latest>" + id + "</Latest>")
	}
	list.WriteString("</BlockList>")
	return c.blobPut(rawURL, url.Values{"comp": {"blocklist"}}, []byte(list.String()), map[string]string{
		"Content-Type":     "application/xml",
		"x-ms-meta-md5":    md5hex,
		"x-ms-meta-upload": "complete",
	})
}

// normalizeMD5 accepts the hex or base64 forms found in blob metadata.
func normalizeMD5(s string) string {
	s = strings.TrimSpace(s)
	if b, err := hex.DecodeString(s); err == nil && len(b) == 16 {
		return strings.ToLower(s)
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == 16 {
		return hex.EncodeToString(b)
	}
	return ""
}

// downloadBlob fetches the blob into dest.part with parallel ranged GETs, then
// renames it to dest. It skips the download when dest already has the blob's
// size (as the python CLI does) and checks the md5 metadata when present.
func (c *Client) downloadBlob(rawURL, dest string, sem chan struct{}) (skipped bool, err error) {
	var size int64
	var md5meta string
	err = retry(func() error {
		resp, err := c.blobDo(http.MethodHead, rawURL, nil, nil, nil, http.StatusOK)
		if err != nil {
			return err
		}
		resp.Body.Close()
		size, md5meta = resp.ContentLength, resp.Header.Get("x-ms-meta-md5")
		return nil
	})
	if err != nil {
		return false, err
	}
	if size < 0 {
		return false, fmt.Errorf("blob has no Content-Length")
	}
	if st, err := os.Stat(dest); err == nil && st.Size() == size {
		return true, nil
	}

	tmp := dest + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return false, err
	}
	bs := chunkSize(size)
	err = forEach(int((size+bs-1)/bs), Parallel, func(i int) error {
		sem <- struct{}{}
		defer func() { <-sem }()
		off := int64(i) * bs
		end := min(off+bs, size) - 1
		return retry(func() error {
			resp, err := c.blobDo(http.MethodGet, rawURL, nil, nil,
				map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", off, end)}, http.StatusPartialContent)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			// a short body surfaces as io.ErrUnexpectedEOF, which is retried
			_, err = io.Copy(io.NewOffsetWriter(f, off), resp.Body)
			return err
		})
	})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if want := normalizeMD5(md5meta); err == nil && want != "" {
		var got string
		if got, err = md5File(tmp); err == nil && got != want {
			err = fmt.Errorf("md5 mismatch: got %s want %s", got, want)
		}
	}
	if err != nil {
		os.Remove(tmp)
		return false, err
	}
	return false, os.Rename(tmp, dest)
}
