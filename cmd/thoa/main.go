// Command thoa: dataset upload/download against the thoa API.
//
//	thoa ls [-l N] [DATASET_ID]
//	thoa upload PATH...
//	thoa download [-i PATTERN|ID]... [-e PATTERN|ID]... DATASET_ID DEST
package main

import (
	"flag"
	"fmt"
	"os"
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
	default:
		fmt.Fprintf(os.Stderr, usage, thoa.ConfigPath())
		os.Exit(2)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "thoa:", err)
	os.Exit(1)
}
