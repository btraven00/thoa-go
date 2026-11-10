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
	path string // local path; "" for content already on thoa (md5 and size set)
	size int64
	md5  string
}

// manifest key of a file: its local path, or for content already on thoa a
// placeholder the server echoes back like one
func (f *localFile) key() string {
	if f.path == "" {
		return "thoa:" + f.md5 + ":" + f.name
	}
	return f.path
}

type pendingUpload struct {
	url, item string
	f         *localFile
}

// prepareTransfer hashes list's local files, creates an import transfer and
// its manifest (mount path from mount), and returns the uploads it still
// needs. Content thoa already has is skipped; a file without a local path
// must be one of those.
func (c *Client) prepareTransfer(list []localFile, mount func(*localFile) string) (string, []pendingUpload, error) {
	sort.Slice(list, func(i, j int) bool { return list[i].name < list[j].name })
	var local []int
	for i := range list {
		if list[i].path != "" {
			local = append(local, i)
		}
	}
	logf("hashing %d files", len(local))
	err := forEach(len(local), Parallel, func(k int) (err error) {
		f := &list[local[k]]
		f.md5, err = md5File(f.path)
		return err
	})
	if err != nil {
		return "", nil, err
	}
	byKey := map[string]*localFile{}
	items := make([]map[string]any, len(list))
	for i := range list {
		f := &list[i]
		byKey[f.key()] = f
		items[i] = map[string]any{"provider": "local", "path": f.key(), "md5": f.md5, "size": f.size, "mount_path": mount(f)}
	}

	var transfer struct {
		PublicID string `json:"public_id"`
	}
	if err := c.do("POST", "/data-transfers", nil, map[string]any{"direction": "import", "remote_ref": map[string]any{}}, &transfer); err != nil {
		return "", nil, err
	}
	tid := transfer.PublicID
	var manifest struct {
		Items []struct {
			Provider       string `json:"provider"`
			Path           string `json:"path"`
			UploadRequired bool   `json:"upload_required"`
			UploadURL      string `json:"upload_url"`
			ItemPublicID   string `json:"item_public_id"`
		} `json:"items"`
	}
	if err := c.do("POST", "/data-transfers/"+tid+"/manifest/unified", nil, map[string]any{"items": items}, &manifest); err != nil {
		return "", nil, err
	}
	var todo []pendingUpload
	for _, it := range manifest.Items {
		if it.Provider != "local" {
			continue
		}
		f := byKey[it.Path]
		switch {
		case f == nil:
			return "", nil, fmt.Errorf("server returned unknown path %q", it.Path)
		case !it.UploadRequired:
			logf("skip (already on thoa) %s", f.name)
		case f.path == "":
			return "", nil, fmt.Errorf("%s: content %s is not on thoa", f.name, f.md5)
		default:
			todo = append(todo, pendingUpload{it.UploadURL, it.ItemPublicID, f})
		}
	}
	return tid, todo, nil
}

func (c *Client) uploadItems(tid string, todo []pendingUpload) error {
	sem := make(chan struct{}, max(Parallel, 1))
	return forEach(len(todo), Parallel, func(i int) error {
		u := todo[i]
		logf("upload %s", u.f.name)
		if err := c.uploadBlob(u.url, u.f.path, u.f.size, u.f.md5, sem); err != nil {
			return fmt.Errorf("%s: %w", u.f.path, err)
		}
		// ponytail: not retried, unknown whether the server treats a repeat as idempotent
		return c.do("POST", "/data-transfers/"+tid+"/import/items/"+u.item+"/complete", nil,
			// The server names the file after this, not after mount_path.
			map[string]any{"filename": u.f.name, "checksum": u.f.md5, "size": u.f.size}, nil)
	})
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
	id, _, err := c.UploadContext(files)
	return id, err
}

// UploadContext is Upload, also returning the import's input_context:
// dataset path -> file id. Keep it: an imported dataset stores no path map,
// and DownloadFiles needs one.
func (c *Client) UploadContext(files map[string]string) (string, map[string]string, error) {
	if len(files) == 0 {
		return "", nil, fmt.Errorf("no files to upload")
	}
	list := make([]localFile, 0, len(files))
	locals := map[string]string{}
	for name, p := range files {
		if !validName(name) {
			return "", nil, fmt.Errorf("invalid dataset path %q: must be relative and clean", name)
		}
		// the manifest echoes items back by local path, so one local file per dataset
		if prev, ok := locals[p]; ok {
			return "", nil, fmt.Errorf("%s mapped to both %q and %q", p, prev, name)
		}
		locals[p] = name
		st, err := os.Stat(p)
		if err != nil {
			return "", nil, err
		}
		if !st.Mode().IsRegular() {
			return "", nil, fmt.Errorf("%s: not a regular file", p)
		}
		list = append(list, localFile{name: name, path: p, size: st.Size()})
	}
	tid, todo, err := c.prepareTransfer(list, func(f *localFile) string { return f.name })
	if err != nil {
		return "", nil, err
	}
	if err := c.uploadItems(tid, todo); err != nil {
		return "", nil, err
	}

	if err := c.do("POST", "/data-transfers/"+tid+"/start", nil, nil, nil); err != nil {
		return "", nil, err
	}
	if err := c.waitTransfer(tid); err != nil {
		return "", nil, err
	}
	ds, byMD5, err := c.importedDataset(list)
	if err != nil {
		return "", nil, fmt.Errorf("transfer %s: %w", tid, err)
	}
	ctx := make(map[string]string, len(list))
	for _, f := range list {
		ctx[f.name] = byMD5[f.md5]
	}
	return ds, ctx, nil
}

// importedDataset finds the dataset an import just created, and its file ids
// by md5. Since 2026-11 (client 0.1.9) the server links neither way (GET
// .../resolved-context is gone; the transfer's dataset_public_id stays null),
// and its paths can't be trusted: a file that was already on thoa (upload
// skipped) is stored under its basename (BUGS.txt 10). So match by content and
// keep the paths client side, as the job's input_context.
// An import whose files match an existing dataset creates none, so the match
// can be any dataset, however old.
// ponytail: newest dataset holding exactly these md5s, listing files only of
// datasets with a fitting file count; two imports of the same content share
// one, which is harmless.
func (c *Client) importedDataset(list []localFile) (string, map[string]string, error) {
	want := map[string]bool{}
	for _, f := range list {
		want[f.md5] = true
	}
	all, err := c.Datasets()
	if err != nil {
		return "", nil, err
	}
	for _, d := range all {
		if d.NumberOfFiles < len(want) || d.NumberOfFiles > len(list) {
			continue
		}
		files, err := c.DatasetFiles(d.PublicID)
		if err != nil {
			return "", nil, err
		}
		byMD5 := map[string]string{}
		for _, f := range files {
			if want[f.MD5] {
				byMD5[f.MD5] = f.ID
			}
		}
		if len(byMD5) == len(want) && len(files) <= len(list) {
			return d.PublicID, byMD5, nil
		}
	}
	return "", nil, fmt.Errorf("no recent dataset holds the %d uploaded files", len(list))
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
	return c.paths(datasetID, ds)
}

// Select is Files filtered like Download (include/exclude: path globs or file
// ids); a job's InputContext takes the result as is.
func (c *Client) Select(datasetID string, include, exclude []string) (map[string]string, error) {
	files, err := c.Files(datasetID)
	if err != nil {
		return nil, err
	}
	files = filterFiles(files, include, exclude)
	if len(files) == 0 {
		return nil, fmt.Errorf("dataset %s: no files selected", datasetID)
	}
	return files, nil
}

// FileInfo is one file of a dataset as /files reports it.
type FileInfo struct {
	Name string // path inside the dataset (the name it was uploaded under)
	ID   string
	Size int64
	MD5  string // hex
}

// DatasetFiles lists a dataset's files with size and md5, from /files.
func (c *Client) DatasetFiles(datasetID string) ([]FileInfo, error) {
	var list []struct {
		PublicID string `json:"public_id"`
		Filename string `json:"filename"`
		Size     *int64 `json:"size"`
		Hash     string `json:"hash"`
	}
	q := url.Values{"dataset_public_id": {datasetID}}
	if err := retry(func() error { return c.do("GET", "/files", q, nil, &list) }); err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(list))
	for _, f := range list {
		fi := FileInfo{Name: f.Filename, ID: f.PublicID, MD5: normalizeMD5(f.Hash)}
		if f.Size != nil {
			fi.Size = *f.Size
		}
		out = append(out, fi)
	}
	return out, nil
}

// paths maps dataset path -> file id. An imported dataset keeps no path map
// (adjusted_context is empty; the python client only passes the import's
// input_context to its job), so its paths come from the file names /files
// reports, which Upload sets to the dataset paths.
func (c *Client) paths(datasetID string, ds *datasetInfo) (map[string]string, error) {
	if len(ds.AdjustedContext) > 0 {
		return ds.AdjustedContext, nil
	}
	list, err := c.DatasetFiles(datasetID)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(list))
	for _, f := range list {
		m[f.Name] = f.ID
	}
	return m, nil
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
	all, err := c.paths(datasetID, ds)
	if err != nil {
		return nil, err
	}
	files := filterFiles(all, include, exclude)
	if len(files) == 0 {
		return nil, fmt.Errorf("dataset %s: no files (after include/exclude)", datasetID)
	}
	return c.downloadFiles(datasetID, ds, files, dest, len(include)+len(exclude) > 0)
}

// DownloadFiles fetches the given files of a dataset into dest, by file id:
// files maps dataset path -> file id, e.g. the context UploadContext returned.
func (c *Client) DownloadFiles(datasetID string, files map[string]string, dest string) (*DownloadResult, error) {
	ds, err := c.dataset(datasetID)
	if err != nil {
		return nil, err
	}
	if ds.RemainingDownloads <= 0 {
		return nil, fmt.Errorf("dataset %s has no remaining downloads", datasetID)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("dataset %s: no files given", datasetID)
	}
	return c.downloadFiles(datasetID, ds, files, dest, true)
}

func (c *Client) downloadFiles(datasetID string, ds *datasetInfo, files map[string]string, dest string, filtered bool) (*DownloadResult, error) {
	if need := c.downloadSize(datasetID, ds, files, filtered); need > 0 {
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
	err := forEach(len(idList), Parallel, func(i int) error {
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
