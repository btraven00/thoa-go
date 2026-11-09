package thoa

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type fake struct {
	srv        *httptest.Server
	decrements int
	totalSize  int64
	flaky      int // blob and API GET requests that answered 503
	// noContext answers like the real server for imported datasets: an empty
	// adjusted_context, paths only as /files filenames.
	noContext bool
}

// fakeThoa emulates the API endpoints and Azure SAS blob calls the client
// uses. Every third blob request and API GET fails with 503 to exercise retries.
func fakeThoa(t *testing.T) *fake {
	var mu sync.Mutex
	f := &fake{}
	blocks := map[string][]byte{}
	blobs := map[string][]byte{}
	meta := map[string]string{}
	context := map[string]string{}  // dataset path -> file id (= md5)
	names := map[string]string{}    // file id -> filename sent on complete
	itemFile := map[string]string{} // item public id -> file id
	n := 0

	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		p := r.URL.Path
		if strings.HasPrefix(p, "/api/") && r.Header.Get("X-API-Key") != "k" {
			w.WriteHeader(401)
			return
		}
		if strings.HasPrefix(p, "/blob/") || r.Method == "GET" {
			if n++; n%3 == 0 {
				f.flaky++
				w.WriteHeader(503)
				return
			}
		}
		reply := func(v any) { json.NewEncoder(w).Encode(v) }
		switch {
		case strings.HasPrefix(p, "/blob/"):
			name := strings.TrimPrefix(p, "/blob/")
			if r.URL.Query().Get("sig") != "x" {
				w.WriteHeader(403)
				return
			}
			body, _ := io.ReadAll(r.Body)
			switch {
			case r.Method == "HEAD":
				w.Header().Set("x-ms-meta-md5", meta[name])
				w.Header().Set("Content-Length", fmt.Sprint(len(blobs[name])))
			case r.Method == "GET":
				var from, to int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to); err != nil || to >= len(blobs[name]) {
					w.WriteHeader(416)
					return
				}
				w.WriteHeader(206)
				w.Write(blobs[name][from : to+1])
			case r.URL.Query().Get("comp") == "block":
				sum := md5.Sum(body)
				if r.Header.Get("Content-MD5") != base64.StdEncoding.EncodeToString(sum[:]) {
					w.WriteHeader(400)
					return
				}
				blocks[name+"/"+r.URL.Query().Get("blockid")] = body
				w.WriteHeader(201)
			case r.URL.Query().Get("comp") == "blocklist":
				var data []byte
				for _, m := range regexp.MustCompile(`<Latest>([^<]+)</Latest>`).FindAllStringSubmatch(string(body), -1) {
					data = append(data, blocks[name+"/"+m[1]]...)
				}
				blobs[name] = data
				meta[name] = r.Header.Get("x-ms-meta-md5")
				w.WriteHeader(201)
			}
		case p == "/api/data-transfers":
			clear(context) // the fake's one dataset holds the latest import
			reply(map[string]string{"public_id": "t1"})
		case p == "/api/data-transfers/t1/manifest/unified":
			var req struct{ Items []map[string]any }
			json.NewDecoder(r.Body).Decode(&req)
			var items []map[string]any
			for i, it := range req.Items {
				id := it["md5"].(string)
				context[it["mount_path"].(string)] = id
				itemFile[fmt.Sprint(i)] = id
				items = append(items, map[string]any{
					"provider": "local", "path": it["path"], "upload_required": true,
					"upload_url": f.srv.URL + "/blob/" + id + "?sig=x", "item_public_id": fmt.Sprint(i),
				})
			}
			reply(map[string]any{"items": items})
		case p == "/api/data-transfers/t1/manifest":
			reply(map[string]any{"status": "completed", "total_items": len(context), "completed_paths": []string{}})
		case p == "/api/datasets" && r.URL.Query().Get("public_id") == "": // listing, newest first
			reply([]map[string]any{{"public_id": "other", "number_of_files": len(context), "created_at": "2"},
				{"public_id": "ds1", "number_of_files": len(context), "created_at": "1"}})
		case p == "/api/files" && r.URL.Query().Get("dataset_public_id") == "other": // same names, other content
			var list []map[string]any
			for _, id := range context {
				list = append(list, map[string]any{"public_id": "x", "filename": names[id], "hash": "00"})
			}
			reply(list)
		case p == "/api/datasets":
			ctx := context
			if f.noContext {
				ctx = map[string]string{}
			}
			reply([]map[string]any{{"remaining_downloads": 1, "total_size": f.totalSize, "adjusted_context": ctx}})
		case p == "/api/files":
			var list []map[string]any
			for _, id := range context {
				list = append(list, map[string]any{"public_id": id, "size": len(blobs[id]), "filename": names[id], "hash": id})
			}
			reply(list)
		case strings.HasSuffix(p, "/complete"):
			var body struct{ Filename string }
			json.NewDecoder(r.Body).Decode(&body)
			names[itemFile[strings.Split(p, "/")[6]]] = body.Filename
			reply(map[string]any{})
		case strings.HasPrefix(p, "/api/temporary_links/"):
			id := strings.Split(p, "/")[3]
			reply(map[string]string{"url": f.srv.URL + "/blob/" + id + "?sig=x"})
		case p == "/api/datasets/ds1/decrement_downloads":
			f.decrements++
			reply(map[string]any{})
		default: // complete, start
			reply(map[string]any{})
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestRoundTrip(t *testing.T) {
	f := fakeThoa(t)
	c := &Client{BaseURL: f.srv.URL, APIKey: "k", HTTP: f.srv.Client(), Blob: f.srv.Client()}
	Log, retryBackoff, Parallel = nil, time.Millisecond, 4

	src := t.TempDir()
	big := bytes.Repeat([]byte("0123456789abcdef"), (3*minBlock+12345)/16) // 4 chunks
	want := map[string][]byte{"a.txt": []byte("hello"), "sub/b.bin": big, "sub/empty": {}}
	for p, b := range want {
		os.MkdirAll(filepath.Join(src, "data", filepath.Dir(p)), 0o755)
		os.WriteFile(filepath.Join(src, "data", p), b, 0o644)
	}
	paths, err := DatasetPaths([]string{filepath.Join(src, "data")}) // dataset paths are "data/..."
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.Upload(paths)
	if err != nil || id != "ds1" {
		t.Fatalf("upload: %q %v", id, err)
	}

	files, err := c.Files("ds1")
	if err != nil {
		t.Fatal(err)
	}
	tree := Tree(files, 0)
	if !strings.Contains(tree, "└── data\n") || !strings.Contains(tree, "    └── sub\n") ||
		!strings.Contains(tree, "        ├── b.bin  (file id: ") {
		t.Errorf("tree:\n%s", tree)
	}
	if got := Tree(files, 1); got != "└── data\n" {
		t.Errorf("tree level 1:\n%s", got)
	}

	dst := t.TempDir()
	res, err := c.Download("ds1", dst, nil, []string{"*empty"})
	if err != nil || res.Downloaded != 2 || len(res.Failed) != 0 {
		t.Fatalf("download: %+v %v", res, err)
	}
	for p, b := range want {
		got, err := os.ReadFile(filepath.Join(dst, "data", p))
		if p == "sub/empty" {
			if err == nil {
				t.Errorf("excluded file was downloaded")
			}
			continue
		}
		if !bytes.Equal(got, b) {
			t.Errorf("%s: content mismatch (%d vs %d bytes)", p, len(got), len(b))
		}
	}
	if f.decrements != 1 {
		t.Errorf("decrements = %d", f.decrements)
	}
	if f.flaky == 0 {
		t.Errorf("no 503s injected; retries untested")
	}

	res, err = c.Download("ds1", dst, []string{"data/a.txt"}, nil)
	if err != nil || res.Skipped != 1 || res.Downloaded != 0 || f.decrements != 1 {
		t.Fatalf("re-download should skip and not decrement: %+v %v %d", res, err, f.decrements)
	}

	f.totalSize = 1 << 60 // only used unfiltered; filtered sums /files sizes
	if _, err := c.Download("ds1", t.TempDir(), nil, nil); err == nil || !strings.Contains(err.Error(), "insufficient disk space") {
		t.Errorf("want disk space error, got %v", err)
	}
	if _, err := c.Download("ds1", t.TempDir(), []string{"*a.txt"}, nil); err != nil {
		t.Errorf("filtered download should size by selection: %v", err)
	}
}

func TestFnmatchAndSafeDest(t *testing.T) {
	for _, tc := range []struct {
		pat, name string
		ok        bool
	}{
		{"*/variants.vcf", "a/b/variants.vcf", true}, // '*' crosses '/' as in python
		{"*.bam", "x/y.bam", true},
		{"*.bam", "x/y.bai", false},
		{"f?.txt", "f1.txt", true},
		{"f[!0-9].txt", "f1.txt", false},
		{"f[!0-9].txt", "fa.txt", true},
		{"a.b", "aXb", false},
	} {
		if fnmatch(tc.pat, tc.name) != tc.ok {
			t.Errorf("fnmatch(%q, %q) != %v", tc.pat, tc.name, tc.ok)
		}
	}
	for in, want := range map[string]string{
		"a/b":           "/base/a/b",
		"/etc/passwd":   "/base/etc/passwd",
		"../../etc/x":   "/base/etc/x",
		"a/../../../..": "/base",
	} {
		if got := safeDest("/base", in); got != want {
			t.Errorf("safeDest(%q) = %q, want %q", in, got, want)
		}
	}
	if chunkSize(1) != minBlock || chunkSize(minBlock*maxBlocks+1) != 2*minBlock {
		t.Errorf("chunkSize scaling")
	}
}

func TestDatasetPaths(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "run1", "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "run1", "a.txt"), nil, 0o644)
	os.WriteFile(filepath.Join(src, "run1", "sub", "b.txt"), nil, 0o644)
	os.Symlink(filepath.Join(src, "run1", "a.txt"), filepath.Join(src, "run1", "link.txt"))

	got, err := DatasetPaths([]string{filepath.Join(src, "run1") + "/"})
	if err != nil || len(got) != 3 || got["run1/a.txt"] == "" || got["run1/sub/b.txt"] == "" || got["run1/link.txt"] == "" {
		t.Errorf("dir: %v %v", got, err)
	}
	got, err = DatasetPaths([]string{filepath.Join(src, "run1", "a.txt"), filepath.Join(src, "run1", "sub")})
	if err != nil || len(got) != 2 || got["a.txt"] == "" || got["sub/b.txt"] == "" {
		t.Errorf("file+dir: %v %v", got, err)
	}
	other := t.TempDir()
	os.WriteFile(filepath.Join(other, "a.txt"), nil, 0o644)
	if _, err := DatasetPaths([]string{filepath.Join(src, "run1", "a.txt"), filepath.Join(other, "a.txt")}); err == nil {
		t.Errorf("want collision error")
	}

	for name, ok := range map[string]bool{
		"a.txt": true, "d/a.txt": true,
		"": false, "/etc/x": false, "../x": false, "d/../../x": false, "./a": false, "a//b": false, "..": false,
	} {
		if validName(name) != ok {
			t.Errorf("validName(%q) != %v", name, ok)
		}
	}
	c := &Client{}
	if _, err := c.Upload(map[string]string{"/abs/x": "x"}); err == nil {
		t.Errorf("Upload must reject absolute dataset paths")
	}
}

// Datasets imported before 2026-11 have no adjusted_context: paths come from
// the filenames Upload reported, which must be the dataset paths, not the
// local ones. (Upload itself needs the context to find its dataset.)
func TestRoundTripWithoutContext(t *testing.T) {
	f := fakeThoa(t)
	c := &Client{BaseURL: f.srv.URL, APIKey: "k", HTTP: f.srv.Client(), Blob: f.srv.Client()}
	Log, retryBackoff, Parallel = nil, time.Millisecond, 4

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("world"), 0o644)
	id, err := c.Upload(map[string]string{"a.txt": filepath.Join(src, "a.txt"), "sub/b.txt": filepath.Join(src, "sub", "b.txt")})
	if err != nil {
		t.Fatal(err)
	}
	f.noContext = true

	files, err := c.Files(id)
	if err != nil || len(files) != 2 || files["a.txt"] == "" || files["sub/b.txt"] == "" {
		t.Fatalf("files = %v, %v; want the dataset paths", files, err)
	}
	info, err := c.DatasetFiles(id)
	if err != nil || len(info) != 2 {
		t.Fatalf("DatasetFiles = %v, %v", info, err)
	}
	for _, fi := range info {
		if sum := md5.Sum([]byte(map[string]string{"a.txt": "hello", "sub/b.txt": "world"}[fi.Name])); fi.MD5 != fmt.Sprintf("%x", sum) || fi.Size != 5 {
			t.Errorf("%+v: wrong md5 or size", fi)
		}
	}

	dst := t.TempDir()
	if res, err := c.Download(id, dst, nil, nil); err != nil || res.Downloaded != 2 {
		t.Fatalf("download: %+v %v", res, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "sub", "b.txt")); string(b) != "world" {
		t.Errorf("sub/b.txt = %q", b)
	}

	// By file id, from the import's context: independent of filenames, which
	// keep whatever an earlier upload of the same content named them.
	f.noContext = false
	id2, ctx, err := c.UploadContext(map[string]string{"x/a.txt": filepath.Join(src, "a.txt")})
	if err != nil || ctx["x/a.txt"] == "" {
		t.Fatalf("UploadContext = %q %v %v", id2, ctx, err)
	}
	dst2 := t.TempDir()
	// (the fake keeps one context for all uploads, so pick our path)
	if res, err := c.DownloadFiles(id2, map[string]string{"x/a.txt": ctx["x/a.txt"]}, dst2); err != nil || res.Downloaded != 1 {
		t.Fatalf("DownloadFiles: %+v %v", res, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst2, "x", "a.txt")); string(b) != "hello" {
		t.Errorf("x/a.txt = %q", b)
	}
}
