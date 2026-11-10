# thoa-go

Go client for [thoa](https://pypi.org/project/thoa/) datasets and jobs: upload, list, download, run.
A port of the dataset and run parts of the python CLI (0.1.9).

```sh
go build -o thoa ./cmd/thoa

thoa upload data/run1                 # prints the new dataset id; files land as run1/...
thoa ls                               # your datasets
thoa ls [-l N] ID                     # files in a dataset, as a tree
thoa download [-i GLOB] [-e GLOB] ID DEST
thoa files [-i GLOB] [-e GLOB] ID     # "path<TAB>file id" lines
thoa job ID                           # a job's record as JSON, costs included
thoa run -cmd CMD -env env.yml -out DIR [-in PATH]... [-d ID [-i GLOB]...] [-c 4 -ram 16 -disk 20]
                                      # run CMD on a VM; prints the output dataset id
```

`run` is a port of python `thoa run` minus log streaming and Google Drive. One `-d`
stages a chosen subset of an existing dataset's files by id (`input_context`),
which the python client can't do; `-stage FILE` gives the paths instead
(`thoa files` lines). A job takes one dataset: several `-d`, or `-d` with
`-in`, only work for content that was imported, never for job outputs.
A job's output dataset keeps its paths; an import keeps them except for
files thoa already held, which it stores under their base name.

`-j N` sets parallel chunk transfers (default 16). `-q` silences progress.

**Auth** (first match wins): `THOA_API_KEY`, `api_key:` in
`~/.config/gothoa/config.yaml`, or `~/.thoa/credentials` from python's `thoa login`.

**Library:** `thoa.NewClient()`, then `Upload(map[datasetPath]localPath)`,
`Download`, `Datasets`, `Files`, `Select`, `Run`, `Wait`.

MIT, see [LICENSE](LICENSE).
