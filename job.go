package thoa

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Job is one thoa job, as the python client's `thoa run` submits it (0.1.9).
type Job struct {
	Cmd   string // shell command run on the VM, in Cwd
	Env   string // conda environment YAML content, registered anew; or
	EnvID string // an existing environment to reuse (built on first use)

	// Cwd is the working directory on the VM. thoa mirrors the client's
	// layout, so the python client sends its own cwd; empty means os.Getwd().
	Cwd string
	// OutputDir is collected into the job's output dataset when it finishes;
	// relative to Cwd.
	OutputDir string

	Cores, RAMGB, DiskGB int

	// InputDataset and InputContext stage files onto the VM: InputContext
	// maps a mount path (relative paths are taken under Cwd) to a file id.
	// Any subset or renaming of a dataset's files works; the ids stay the same.
	InputDataset string
	InputContext map[string]string

	// Inputs, instead of InputDataset, stage files through one import bound
	// to the job (0.1.9): local files are uploaded unless thoa has their
	// content, files of other datasets are named by content and move no data.
	Inputs []Input
}

// Input is a file staged at Path (relative to Cwd): Local is uploaded, or,
// with Local empty, MD5 (hex) and Size name content already on thoa.
type Input struct {
	Path, Local, MD5 string
	Size             int64
}

// Job statuses after which nothing changes (python thoa core/job_status.py).
var terminal = map[string]bool{
	"completed": true, "failed_execution": true, "failed_validation": true,
	"failed_provisioning": true, "failed_upload": true, "failed_startup": true,
	"cancelled": true, "crashed": true, // thoa doesn't retry a crash during provisioning (job e385a99b)
}

type idOnly struct {
	PublicID string `json:"public_id"`
}

// Run submits j and returns the job id without waiting; see Wait.
func (c *Client) Run(j Job) (string, error) {
	if j.Cmd == "" || j.OutputDir == "" {
		return "", fmt.Errorf("job needs Cmd and OutputDir")
	}
	if j.Cwd == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		j.Cwd = wd
	}
	home, _ := os.UserHomeDir()
	ctx := map[string]string{}
	for p, id := range j.InputContext {
		if !path.IsAbs(p) {
			p = path.Join(filepath.ToSlash(j.Cwd), p)
		}
		ctx[p] = id
	}
	out := j.OutputDir
	if !path.IsAbs(out) {
		out = path.Join(filepath.ToSlash(j.Cwd), out)
	}

	env := idOnly{j.EnvID}
	if env.PublicID == "" {
		id, err := c.NewEnv(j.Env)
		if err != nil {
			return "", err
		}
		env.PublicID = id
	}
	tid, todo := "", []pendingUpload(nil)
	if len(j.Inputs) > 0 {
		// the server stages only the dataset's files when a job has both (2026-11)
		if j.InputDataset != "" || len(ctx) > 0 {
			return "", fmt.Errorf("job: Inputs or InputDataset, not both")
		}
		list := make([]localFile, len(j.Inputs))
		for i, in := range j.Inputs {
			if !validName(in.Path) {
				return "", fmt.Errorf("input path %q: must be relative and clean", in.Path)
			}
			list[i] = localFile{name: in.Path, path: in.Local, md5: in.MD5, size: in.Size}
			if in.Local != "" {
				st, err := os.Stat(in.Local)
				if err != nil {
					return "", err
				}
				list[i].size = st.Size()
			}
		}
		var err error
		tid, todo, err = c.prepareTransfer(list, func(f *localFile) string {
			return path.Join(filepath.ToSlash(j.Cwd), f.name)
		})
		if err != nil {
			return "", err
		}
	}

	var transfer any // null without Inputs
	if tid != "" {
		transfer = tid
	}
	var script, job idOnly
	if err := c.do("POST", "/scripts", nil, map[string]any{
		"name": "thoa-go script", "script_content": j.Cmd,
		"description": "No description provided", "security_status": "pending",
	}, &script); err != nil {
		return "", fmt.Errorf("script: %w", err)
	}
	if err := c.do("POST", "/jobs", nil, map[string]any{
		"requested_ram": j.RAMGB, "requested_cpu": j.Cores, "requested_disk_space": j.DiskGB,
		"has_input_data": j.InputDataset != "" || len(ctx) > 0 || tid != "", "client_home": home,
		"use_existing_input_dataset": false, "input_transfer_public_id": transfer,
		"export_transfer_public_id": nil, "input_mount_root": nil,
		"strict": false, "max_attempts": 3, "disable_preflight": false,
	}, &job); err != nil {
		return "", fmt.Errorf("job: %w", err)
	}
	upd := map[string]any{
		"script_public_id": script.PublicID, "environment_public_id": env.PublicID,
		"current_working_directory": j.Cwd, "download_directory": j.Cwd,
		"output_directory": out,
	}
	if j.InputDataset != "" {
		upd["input_dataset_public_id"] = j.InputDataset
	}
	if len(ctx) > 0 {
		upd["input_context"] = ctx
	}
	if err := c.do("PUT", "/jobs/"+job.PublicID, nil, upd, nil); err != nil {
		c.do("POST", "/jobs/"+job.PublicID+"/cancel", nil, nil, nil)
		return "", fmt.Errorf("job %s: %w", job.PublicID, err)
	}
	if tid != "" {
		if err := c.feed(tid, todo); err != nil {
			c.do("POST", "/jobs/"+job.PublicID+"/cancel", nil, nil, nil)
			return "", fmt.Errorf("job %s inputs: %w", job.PublicID, err)
		}
	}
	logf("job %s submitted (env %s)", job.PublicID, env.PublicID)
	// The python client polls /environments/{id}/validate in the background
	// to get the build going; do it once here, Wait keeps it up.
	c.do("GET", "/environments/"+env.PublicID+"/validate", nil, nil, nil)
	return job.PublicID, nil
}

// feed starts a job's import and uploads what it still needs. The server fails
// the job if the uploader goes quiet for 10 minutes, so beat while uploading.
// The job stages each file as it lands; nothing waits for the whole import.
func (c *Client) feed(tid string, todo []pendingUpload) error {
	if err := c.do("POST", "/data-transfers/"+tid+"/start", nil, nil, nil); err != nil {
		return err
	}
	if len(todo) == 0 {
		return nil
	}
	beat := func() { c.do("POST", "/data-transfers/"+tid+"/client-heartbeat", nil, nil, nil) }
	beat()
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				beat()
			}
		}
	}()
	return c.uploadItems(tid, todo)
}

// JobInfo is the part of GET /jobs that Wait reports.
type JobInfo struct {
	PublicID      string `json:"public_id"`
	Status        string `json:"status"`
	OutputDataset string `json:"output_dataset_public_id"`
	Environment   string `json:"environment_public_id"`
}

func (c *Client) job(id string) (*JobInfo, error) {
	var js []JobInfo
	if err := retry(func() error { return c.do("GET", "/jobs", url.Values{"public_id": {id}}, nil, &js) }); err != nil {
		return nil, err
	}
	if len(js) == 0 {
		return nil, fmt.Errorf("job %s not found", id)
	}
	return &js[0], nil
}

// JobRecord is the job's detail record (status, env, datasets, attempts,
// azure_cost_usd, billed_cost_usd, credits_debited). The costs are only
// filled in here: GET /jobs leaves them null.
func (c *Client) JobRecord(id string) (json.RawMessage, error) {
	var rec json.RawMessage
	err := retry(func() error { return c.do("GET", "/jobs/"+id+"/detail", nil, nil, &rec) })
	return rec, err
}

// JobPoll is how often Wait polls job status.
var JobPoll = 5 * time.Second

// Wait polls job id until it reaches a terminal status and returns its last
// state. A completed job carries its output dataset id; that dataset keeps its
// paths (relative to the output directory's parent), unlike imports.
func (c *Client) Wait(id string) (*JobInfo, error) {
	last := ""
	for {
		j, err := c.job(id)
		if err != nil {
			return nil, err
		}
		if j.Status != last {
			logf("job %s: %s", id, j.Status)
			last = j.Status
		}
		if terminal[j.Status] {
			if j.Status == "failed_validation" && j.Environment != "" {
				return j, fmt.Errorf("job %s: failed_validation; env %s build log:\n%s", id, j.Environment, c.buildLog(j.Environment))
			}
			if j.Status != "completed" {
				return j, fmt.Errorf("job %s: %s", id, j.Status)
			}
			// ponytail: the output dataset can trail "completed"; 12 more polls, then give up
			for i := 0; j.OutputDataset == "" && i < 12; i++ {
				time.Sleep(JobPoll)
				if j, err = c.job(id); err != nil {
					return nil, err
				}
			}
			return j, nil
		}
		if j.Status == "validating" && j.Environment != "" {
			c.do("GET", "/environments/"+j.Environment+"/validate", nil, nil, nil)
		}
		time.Sleep(JobPoll)
	}
}

// NewEnv registers a conda environment YAML and returns its id. It is built
// by the first job that uses it, not before (GET .../validate only reports).
// thoa never dedupes environments and rewrites the YAML it stores, so callers
// that want reuse keep their own YAML -> id map and pass the id as Job.EnvID.
func (c *Client) NewEnv(yml string) (string, error) {
	var env idOnly
	if err := c.do("POST", "/environments", nil, map[string]any{"tools": []string{}, "env_string": yml}, &env); err != nil {
		return "", fmt.Errorf("environment: %w", err)
	}
	return env.PublicID, nil
}

// buildLog returns the tail of environment id's build log.
func (c *Client) buildLog(id string) string {
	var es []struct {
		BuildLogs string `json:"build_logs"`
	}
	c.do("GET", "/environments", url.Values{"public_id": {id}, "include_related_jobs": {"false"}}, nil, &es)
	if len(es) == 0 {
		return ""
	}
	tail := es[0].BuildLogs
	if len(tail) > 3000 {
		tail = "..." + tail[len(tail)-3000:]
	}
	return tail
}

// FindEnv returns the newest environment whose stored env_string is exactly
// yml (trimmed) and whose build hasn't failed, or "" if there is none. thoa
// stores a normalized copy of what it is given, so this only hits for YAML
// already in that form (name "env", bioconda added, channels and dependencies
// sorted, pip block last); anything else just misses and builds anew.
func (c *Client) FindEnv(yml string) (string, error) {
	var es []struct {
		PublicID  string `json:"public_id"`
		EnvString string `json:"env_string"`
		Status    string `json:"env_status"`
		CreatedAt string `json:"created_at"`
	}
	if err := retry(func() error {
		return c.do("GET", "/environments", url.Values{"include_related_jobs": {"false"}}, nil, &es)
	}); err != nil {
		return "", err
	}
	want, id, newest := strings.TrimSpace(yml), "", ""
	for _, e := range es {
		if e.Status != "validation_failed" && strings.TrimSpace(e.EnvString) == want && e.CreatedAt > newest {
			id, newest = e.PublicID, e.CreatedAt
		}
	}
	return id, nil
}
