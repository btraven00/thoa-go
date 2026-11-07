// Command thoa: dataset upload/download against the thoa API.
//
//	thoa ls [-l N] [DATASET_ID]
//	thoa upload PATH...
//	thoa download [-i PATTERN|ID]... [-e PATTERN|ID]... DATASET_ID DEST
//	thoa view [-i X]... [-e X]... DATASET_ID
//	thoa run -cmd CMD (-env FILE | -env-id ID) -out DIR [-in PATH]... [-d DATASET [-i X]... [-e X]...]
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	thoa "github.com/btraven00/thoa-go"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

const usage = `usage:
  thoa ls                                list your datasets
  thoa ls [-l N] ID                      show dataset files as a tree, N levels deep
  thoa upload PATH...                    upload files/dirs as a new dataset, print its id
                                         (a file is stored as its name, a dir DIR as base(DIR)/...)
  thoa download [-i X]... [-e X]... ID DEST
                                         download dataset ID into DEST; X is a path glob or file id
  thoa view [-i X]... [-e X]... ID      new dataset over the selected files of ID (no data moved);
                                         prints its id, then "path<TAB>file id" per file (it keeps no paths)
  thoa run -cmd CMD (-env FILE | -env-id ID) -out DIR [-in PATH]... [-d ID [-i X]... [-e X]...]
                                         run CMD on a VM in a copy of the cwd: -in uploads local files
                                         (mounted at their path under the cwd), -d stages files of dataset
                                         ID at their dataset paths (-i/-e select them); waits, then prints
                                         the output dataset id (DIR's files, with paths)
                                         -c cores (4), -ram GB (16), -disk GB (20)
                                         -stage FILE stages "path<TAB>file id" lines (any datasets' files)
                                         -env reuses an env with the same stored text (-new-env: don't)

flags: -j N parallel chunk transfers (default 16), -q quiet

auth: THOA_API_KEY, or api_key in %s, or ~/.thoa/credentials
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, thoa.ConfigPath())
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	var include, exclude multi
	fs.Var(&include, "i", "include path glob or file id (repeatable)")
	fs.Var(&exclude, "e", "exclude path glob or file id (repeatable)")
	quiet := fs.Bool("q", false, "no progress output")
	level := fs.Int("l", 0, "ls: tree depth (0 = all)")
	fs.IntVar(&thoa.Parallel, "j", thoa.Parallel, "parallel chunk transfers")
	var inputs multi
	fs.Var(&inputs, "in", "run: local file or dir to upload as input (repeatable)")
	cmd := fs.String("cmd", "", "run: command")
	envFile := fs.String("env", "", "run: conda environment YAML")
	envID := fs.String("env-id", "", "run: existing environment id (logged by an earlier run)")
	newEnv := fs.Bool("new-env", false, "run: register -env anew instead of reusing an identical one")
	outDir := fs.String("out", "", "run: output directory, relative to the cwd")
	dataset := fs.String("d", "", "run: dataset whose files to stage")
	var stage multi
	fs.Var(&stage, "stage", "run: file of \"path<TAB>file id\" lines to stage, as thoa view prints (repeatable)")
	cores := fs.Int("c", 4, "run: cores")
	ram := fs.Int("ram", 16, "run: RAM in GB")
	disk := fs.Int("disk", 20, "run: disk in GB")
	fs.Parse(os.Args[2:])
	if *quiet {
		thoa.Log = nil
	}

	c, err := thoa.NewClient()
	if err != nil {
		die(err)
	}

	switch os.Args[1] {
	case "ls":
		if fs.NArg() == 0 {
			ds, err := c.Datasets()
			if err != nil {
				die(err)
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
			fmt.Fprintln(tw, "ID\tCREATED\tFILES\tSIZE (GiB)\t")
			for _, d := range ds {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%.2f\t\n", d.PublicID, d.CreatedAt, d.NumberOfFiles, float64(d.TotalSize)/(1<<30))
			}
			tw.Flush()
			break
		}
		files, err := c.Files(fs.Arg(0))
		if err != nil {
			die(err)
		}
		fmt.Print(thoa.Tree(files, *level))
	case "upload":
		if fs.NArg() == 0 {
			die(fmt.Errorf("upload: need at least one path"))
		}
		files, err := thoa.DatasetPaths(fs.Args())
		if err != nil {
			die(err)
		}
		id, err := c.Upload(files)
		if err != nil {
			die(err)
		}
		fmt.Println(id)
	case "download":
		if fs.NArg() != 2 {
			die(fmt.Errorf("download: need DATASET_ID DEST"))
		}
		res, err := c.Download(fs.Arg(0), fs.Arg(1), include, exclude)
		if res != nil {
			fmt.Fprintf(os.Stderr, "downloaded %d, skipped %d, failed %d\n", res.Downloaded, res.Skipped, len(res.Failed))
		}
		if err != nil {
			die(err)
		}
		if len(res.Failed) > 0 {
			os.Exit(1)
		}
	case "view":
		if fs.NArg() != 1 {
			die(fmt.Errorf("view: need DATASET_ID"))
		}
		files, err := c.Select(fs.Arg(0), include, exclude)
		if err != nil {
			die(err)
		}
		paths := make([]string, 0, len(files))
		for p := range files {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		ids := make([]string, len(paths))
		for i, p := range paths {
			ids[i] = files[p]
		}
		id, err := c.View(ids)
		if err != nil {
			die(err)
		}
		fmt.Println(id)
		for _, p := range paths {
			fmt.Printf("%s\t%s\n", p, files[p])
		}
	case "run":
		j := thoa.Job{Cmd: *cmd, EnvID: *envID, OutputDir: *outDir, Cores: *cores, RAMGB: *ram, DiskGB: *disk,
			InputContext: map[string]string{}}
		if j.EnvID == "" {
			env, err := os.ReadFile(*envFile)
			if err != nil {
				die(fmt.Errorf("run: need -env-id, or -env: %w", err))
			}
			j.Env = string(env)
			if !*newEnv {
				if j.EnvID, err = c.FindEnv(j.Env); err != nil {
					die(err)
				}
				if j.EnvID != "" {
					fmt.Fprintf(os.Stderr, "reusing env %s (same env_string)\n", j.EnvID)
				}
			}
		}
		if *dataset != "" {
			j.InputDataset = *dataset
			// with -stage, the map comes from there; a view has no paths to select by
			if len(stage) == 0 || len(include)+len(exclude) > 0 {
				files, err := c.Select(*dataset, include, exclude)
				if err != nil {
					die(err)
				}
				j.InputContext = files
			}
		}
		for _, f := range stage {
			b, err := os.ReadFile(f)
			if err != nil {
				die(err)
			}
			for _, line := range strings.Split(string(b), "\n") {
				if p, id, ok := strings.Cut(line, "\t"); ok {
					j.InputContext[p] = id
				}
			}
		}
		if len(inputs) > 0 {
			files, err := relToCwd(inputs)
			if err != nil {
				die(err)
			}
			id, ctx, err := c.UploadContext(files)
			if err != nil {
				die(err)
			}
			if j.InputDataset == "" && len(j.InputContext) == 0 {
				j.InputDataset = id
			}
			for p, fid := range ctx {
				j.InputContext[p] = fid
			}
		}
		// thoa stages input_context only for files of the job's input dataset
		// and drops other ids silently, so mixed inputs (-in with -stage or -d,
		// or -stage alone) go through one view over all of them: no data moves.
		if len(j.InputContext) > 0 && (len(stage) > 0 || len(inputs) > 0 && *dataset != "") {
			seen := map[string]bool{} // identical files share an id (md5 dedup)
			ids := make([]string, 0, len(j.InputContext))
			for _, fid := range j.InputContext {
				if !seen[fid] {
					seen[fid] = true
					ids = append(ids, fid)
				}
			}
			v, err := c.View(ids)
			if err != nil {
				die(err)
			}
			fmt.Fprintf(os.Stderr, "input view %s over %d files\n", v, len(ids))
			j.InputDataset = v
		}
		id, err := c.Run(j)
		if err != nil {
			die(err)
		}
		info, err := c.Wait(id)
		if err != nil {
			die(err)
		}
		fmt.Println(info.OutputDataset)
	default:
		fmt.Fprintf(os.Stderr, usage, thoa.ConfigPath())
		os.Exit(2)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "thoa:", err)
	os.Exit(1)
}

// relToCwd maps every file under paths to its path relative to the cwd, so it
// mounts on the VM where it sits locally (thoa mirrors the cwd).
func relToCwd(paths []string) (map[string]string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	files, err := thoa.DatasetPaths(paths)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, local := range files {
		abs, err := filepath.Abs(local)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(wd, abs)
		if err != nil || rel == ".." || len(rel) > 2 && rel[:3] == "../" {
			return nil, fmt.Errorf("%s is outside the cwd", local)
		}
		out[filepath.ToSlash(rel)] = local
	}
	return out, nil
}
