# thoa-go

Go client for [thoa](https://pypi.org/project/thoa/) datasets: upload, list, download.
A port of the dataset parts of the python CLI (0.1.8).

```sh
go build -o thoa ./cmd/thoa

thoa upload data/run1                 # prints the new dataset id; files land as run1/...
thoa ls                               # your datasets
thoa ls [-l N] ID                     # files in a dataset, as a tree
thoa download [-i GLOB] [-e GLOB] ID DEST
```

`-j N` sets parallel chunk transfers (default 16). `-q` silences progress.

**Auth** (first match wins): `THOA_API_KEY`, `api_key:` in
`~/.config/gothoa/config.yaml`, or `~/.thoa/credentials` from python's `thoa login`.

**Library:** `thoa.NewClient()`, then `Upload(map[datasetPath]localPath)`,
`Download`, `Datasets`, `Files`.

Known issue: uploads currently fail server-side with
`Provider 'local' is not configured`, with the official client as well.

MIT, see [LICENSE](LICENSE).
