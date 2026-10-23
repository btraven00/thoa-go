package thoa

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Log receives progress lines; set to nil to silence.
var Log = func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

var logMu sync.Mutex

func logf(format string, args ...any) {
	if Log != nil {
		logMu.Lock()
		defer logMu.Unlock()
		Log(format, args...)
	}
}

type localFile struct {
	name string // path inside the dataset
	path string // local path
	size int64
	md5  string
}

// DatasetPaths maps local files/directories to dataset paths: a file becomes
// its basename, a directory DIR becomes base(DIR)/<path under DIR>. So
// `upload data/run1` yields run1/..., and `upload data/run1/*` yields the
// bare file names. Symlinks to files are followed; symlinked dirs are not.
func DatasetPaths(paths []string) (map[string]string, error) {
	out := map[string]string{} // dataset path -> local path
	for _, root := range paths {
		root = filepath.Clean(root)
		parent := filepath.Dir(root)
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if st, err := os.Stat(p); err != nil || !st.Mode().IsRegular() {
				return err // broken symlink errors; sockets, symlinked dirs etc. skip
			}
			rel, err := filepath.Rel(parent, p)
			if err != nil {
				return err
			}
			name := filepath.ToSlash(rel)
			if prev, ok := out[name]; ok && prev != p {
				return fmt.Errorf("dataset path %q from both %s and %s", name, prev, p)
			}
			out[name] = p
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// validName rejects dataset paths that are absolute, empty, or climb out with "..".
func validName(name string) bool {
	clean := filepath.ToSlash(filepath.Clean(name))
	return name != "" && clean == name && !strings.HasPrefix(name, "/") &&
		clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

// Upload creates a dataset and returns its public id. files maps dataset path
// (relative, slash-separated, e.g. "counts/matrix.h5ad") to local file path;
// DatasetPaths builds it from CLI-style arguments.
func (c *Client) Upload(files map[string]string) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("no files to upload")
	}
	list := make([]localFile, 0, len(files))
	locals := map[string]string{}
	for name, p := range files {
		if !validName(name) {
			return "", fmt.Errorf("invalid dataset path %q: must be relative and clean", name)
		}
		// the manifest echoes items back by local path, so one local file per dataset
		if prev, ok := locals[p]; ok {
			return "", fmt.Errorf("%s mapped to both %q and %q", p, prev, name)
		}
		locals[p] = name
		st, err := os.Stat(p)
		if err != nil {
			return "", err
		}
		if !st.Mode().IsRegular() {
			return "", fmt.Errorf("%s: not a regular file", p)
		}
		list = append(list, localFile{name: name, path: p, size: st.Size()})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].name < list[j].name })

	logf("hashing %d files", len(list))
	err := forEach(len(list), Parallel, func(i int) (err error) {
		list[i].md5, err = md5File(list[i].path)
		return err
	})
	if err != nil {
		return "", err
	}
	byPath := map[string]*localFile{}
	items := make([]map[string]any, len(list))
	for i := range list {
		f := &list[i]
		byPath[f.path] = f
		items[i] = map[string]any{"provider": "local", "path": f.path, "md5": f.md5, "size": f.size, "mount_path": f.name}
	}

	var transfer struct {
		PublicID string `json:"public_id"`
	}
	if err := c.do("POST", "/data-transfers", nil, map[string]any{"direction": "import", "remote_ref": map[string]any{}}, &transfer); err != nil {
		return "", err
	}
	tid := transfer.PublicID

	type item struct {
		Provider       string `json:"provider"`
		Path           string `json:"path"`
		UploadRequired bool   `json:"upload_required"`
		UploadURL      string `json:"upload_url"`
		ItemPublicID   string `json:"item_public_id"`
	}
	var manifest struct {
		Items []item `json:"items"`
	}
	if err := c.do("POST", "/data-transfers/"+tid+"/manifest/unified", nil, map[string]any{"items": items}, &manifest); err != nil {
		return "", err
	}

	var todo []item
	for _, it := range manifest.Items {
		if it.Provider != "local" {
			continue
		}
		if !it.UploadRequired {
			logf("skip (already uploaded) %s", it.Path)
			continue
		}
		if byPath[it.Path] == nil {
			return "", fmt.Errorf("server returned unknown path %q", it.Path)
		}
		todo = append(todo, it)
	}

	sem := make(chan struct{}, max(Parallel, 1))
	err = forEach(len(todo), Parallel, func(i int) error {
		it, f := todo[i], byPath[todo[i].Path]
		logf("upload %s", f.name)
		if err := c.uploadBlob(it.UploadURL, f.path, f.size, f.md5, sem); err != nil {
			return fmt.Errorf("%s: %w", f.path, err)
		}
		// ponytail: not retried, unknown whether the server treats a repeat as idempotent
		return c.do("POST", "/data-transfers/"+tid+"/import/items/"+it.ItemPublicID+"/complete", nil,
			map[string]any{"filename": f.path, "checksum": f.md5, "size": f.size}, nil)
	})
	if err != nil {
		return "", err
	}

	if err := c.do("POST", "/data-transfers/"+tid+"/start", nil, nil, nil); err != nil {
		return "", err
	}
	if err := c.waitTransfer(tid); err != nil {
		return "", err
	}
	var resolved struct {
		DatasetPublicID string `json:"dataset_public_id"`
	}
	err = retry(func() error { return c.do("GET", "/data-transfers/"+tid+"/resolved-context", nil, nil, &resolved) })
	return resolved.DatasetPublicID, err
}

// PollInterval is how often transfer status is polled.
var PollInterval = 2 * time.Second

func (c *Client) waitTransfer(tid string) error {
	last := -1
	for {
		var m struct {
			Status         string   `json:"status"`
			TotalItems     int      `json:"total_items"`
			SkippedItems   int      `json:"skipped_items"`
			CompletedPaths []string `json:"completed_paths"`
		}
		err := retry(func() error { return c.do("GET", "/data-transfers/"+tid+"/manifest", nil, nil, &m) })
		if e, ok := err.(*APIError); ok && e.Status == 404 {
			time.Sleep(PollInterval) // manifest not materialized yet
			continue
		}
		if err != nil {
			return err
		}
		if n := len(m.CompletedPaths); n != last {
			logf("importing %d/%d", n, m.TotalItems-m.SkippedItems)
			last = n
		}
		switch m.Status {
		case "completed":
			return nil
		case "failed":
			var v struct {
				ErrorMessage string `json:"error_message"`
			}
			_ = c.do("GET", "/data-transfers/"+tid, nil, nil, &v)
			return fmt.Errorf("dataset import failed (transfer %s): %s", tid, v.ErrorMessage)
		}
		time.Sleep(PollInterval)
	}
}

// fnmatch translates a python fnmatch pattern ('*' crosses '/') to a regexp.
func fnmatch(pat, name string) bool {
	var re strings.Builder
	re.WriteString("^")
	for i := 0; i < len(pat); i++ {
		switch ch := pat[i]; ch {
		case '*':
			re.WriteString(".*")
		case '?':
			re.WriteString(".")
		case '[':
			j := strings.IndexByte(pat[i+1:], ']')
			if j < 0 {
				re.WriteString(`\[`)
				continue
			}
			class := pat[i+1 : i+1+j]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			re.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i += j + 1
		default:
			re.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	re.WriteString("$")
	ok, _ := regexp.MatchString(re.String(), name)
	return ok
}

// filterFiles applies include/exclude rules; each rule is a glob on the path
// or an exact file id.
func filterFiles(files map[string]string, include, exclude []string) map[string]string {
	hit := func(rules []string, p, id string) bool {
		for _, r := range rules {
			if r == id || fnmatch(r, p) {
				return true
			}
		}
		return false
	}
	out := map[string]string{}
	for p, id := range files {
		if (len(include) == 0 || hit(include, p, id)) && !hit(exclude, p, id) {
			out[p] = id
		}
	}
	return out
}

// safeDest joins a dataset path under base; absolute paths and ".." can't escape it.
func safeDest(base, p string) string {
	return filepath.Join(base, filepath.Clean("/"+p))
}

type datasetInfo struct {
	RemainingDownloads int               `json:"remaining_downloads"`
	TotalSize          int64             `json:"total_size"`
	AdjustedContext    map[string]string `json:"adjusted_context"` // path -> file id
}

func (c *Client) dataset(id string) (*datasetInfo, error) {
	var ds []datasetInfo
	q := url.Values{"public_id": {id}, "include_jobs_as_input": {"false"}, "include_jobs_as_output": {"false"}}
	if err := retry(func() error { return c.do("GET", "/datasets", q, nil, &ds) }); err != nil {
		return nil, err
	}
	if len(ds) == 0 {
		return nil, fmt.Errorf("dataset %s not found", id)
	}
	return &ds[0], nil
}

// Files returns the dataset's files as path -> file id.
func (c *Client) Files(datasetID string) (map[string]string, error) {
	ds, err := c.dataset(datasetID)
	if err != nil {
		return nil, err
	}
	return ds.AdjustedContext, nil
}

// Tree renders path -> file id as a tree, like `thoa dataset ls`; level <= 0
// shows all depths.
func Tree(files map[string]string, level int) string {
	type node struct {
		kids map[string]*node
		id   string
	}
	root := &node{kids: map[string]*node{}}
	for p, id := range files {
		n := root
		for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
			if n.kids[part] == nil {
				n.kids[part] = &node{kids: map[string]*node{}}
			}
			n = n.kids[part]
		}
		n.id = id
	}
	var b strings.Builder
	var walk func(n *node, prefix string, depth int)
	walk = func(n *node, prefix string, depth int) {
		names := make([]string, 0, len(n.kids))
		for k := range n.kids {
			names = append(names, k)
		}
		sort.Strings(names)
		for i, name := range names {
			kid, last := n.kids[name], i == len(names)-1
			conn, ext := "├── ", "│   "
			if last {
				conn, ext = "└── ", "    "
			}
			b.WriteString(prefix + conn + name)
			if kid.id != "" {
				b.WriteString("  (file id: " + kid.id + ")")
			}
			b.WriteString("\n")
			if level <= 0 || depth+1 < level {
				walk(kid, prefix+ext, depth+1)
			}
		}
	}
	walk(root, "", 0)
	return b.String()
}

// downloadSize is the byte count to check free space against: the dataset
// total, or with filters the sum of the selected files when all sizes are known.
func (c *Client) downloadSize(datasetID string, ds *datasetInfo, files map[string]string, filtered bool) int64 {
	if !filtered {
		return ds.TotalSize
	}
	var list []struct {
		PublicID string `json:"public_id"`
		Size     *int64 `json:"size"`
	}
	q := url.Values{"dataset_public_id": {datasetID}}
	if retry(func() error { return c.do("GET", "/files", q, nil, &list) }) != nil {
		return ds.TotalSize
	}
	sizes := map[string]*int64{}
	for _, f := range list {
		sizes[f.PublicID] = f.Size
	}
	var sum int64
	for _, id := range files {
		if sizes[id] == nil {
			return ds.TotalSize
		}
		sum += *sizes[id]
	}
	return sum
}

func fmtBytes(n int64) string {
	f := float64(n)
	for _, u := range []string{"B", "KiB", "MiB", "GiB", "TiB"} {
		if f < 1024 {
			return fmt.Sprintf("%.2f %s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.2f PiB", f)
}

type DownloadResult struct {
	Downloaded, Skipped int
	Failed              map[string]error // dataset path -> error
}

// Download fetches a dataset into dest. include/exclude: see filterFiles.
// It refuses to start without free space for the selection plus headroom
// (max(5%, 200 MiB)). A download is decremented from the dataset's quota only
// if at least one file transferred.
func (c *Client) Download(datasetID, dest string, include, exclude []string) (*DownloadResult, error) {
	ds, err := c.dataset(datasetID)
	if err != nil {
		return nil, err
	}
	if ds.RemainingDownloads <= 0 {
		return nil, fmt.Errorf("dataset %s has no remaining downloads", datasetID)
	}
	files := filterFiles(ds.AdjustedContext, include, exclude)
	if len(files) == 0 {
		return nil, fmt.Errorf("dataset %s: no files (after include/exclude)", datasetID)
	}

	if need := c.downloadSize(datasetID, ds, files, len(include)+len(exclude) > 0); need > 0 {
		need += max(need/20, 200<<20)
		if free, ok := freeBytes(dest); ok && free < need {
			return nil, fmt.Errorf("insufficient disk space at %s: need %s (with headroom), have %s",
				dest, fmtBytes(need), fmtBytes(free))
		}
	}

	ids := map[string]string{} // file id -> SAS url
	for _, id := range files {
		ids[id] = ""
	}
	idList := make([]string, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}
	var mu sync.Mutex
	err = forEach(len(idList), Parallel, func(i int) error {
		var link struct {
			URL    string `json:"url"`
			SASURL string `json:"sas_url"`
		}
		err := retry(func() error {
			return c.do("POST", "/temporary_links/"+idList[i]+"/request-download", nil, nil, &link)
		})
		mu.Lock()
		ids[idList[i]] = firstNonEmpty(link.URL, link.SASURL)
		mu.Unlock()
		return err
	})
	if err != nil {
		return nil, err
	}

	res := &DownloadResult{Failed: map[string]error{}}
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sem := make(chan struct{}, max(Parallel, 1))
	forEach(len(paths), Parallel, func(i int) error {
		p := paths[i]
		skipped, err := func() (bool, error) {
			u := ids[files[p]]
			if u == "" {
				return false, fmt.Errorf("no download url")
			}
			out := safeDest(dest, p)
			if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
				return false, err
			}
			return c.downloadBlob(u, out, sem)
		}()
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err != nil:
			res.Failed[p] = err
			logf("FAIL %s: %v", p, err)
		case skipped:
			res.Skipped++
			logf("skip (exists) %s", p)
		default:
			res.Downloaded++
			logf("got %s", p)
		}
		return nil // keep going; failures are reported per file
	})

	if res.Downloaded > 0 {
		// ponytail: not retried, a lost response would decrement twice
		if err := c.do("PUT", "/datasets/"+datasetID+"/decrement_downloads", nil, nil, nil); err != nil {
			return res, err
		}
	}
	return res, nil
}

type DatasetSummary struct {
	PublicID      string `json:"public_id"`
	CreatedAt     string `json:"created_at"`
	NumberOfFiles int    `json:"number_of_files"`
	TotalSize     int64  `json:"total_size"`
}

// Datasets lists the caller's datasets, newest first.
func (c *Client) Datasets() ([]DatasetSummary, error) {
	var ds []DatasetSummary
	q := url.Values{
		"include_jobs_as_input": {"false"}, "include_jobs_as_output": {"false"},
		"include_context": {"false"}, "include_adjusted_context": {"false"},
	}
	err := retry(func() error { return c.do("GET", "/datasets", q, nil, &ds) })
	// ISO timestamps sort lexically
	sort.Slice(ds, func(i, j int) bool { return ds[i].CreatedAt > ds[j].CreatedAt })
	return ds, err
}
