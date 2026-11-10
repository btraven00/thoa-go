package thoa

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestRunWait checks the payloads Run sends (the python client's, 0.1.9) and
// that Wait follows a job to completion and its output dataset.
func TestRunWait(t *testing.T) {
	got := map[string]map[string]any{} // "METHOD path" -> body
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		got[key] = body
		reply := func(v any) { json.NewEncoder(w).Encode(v) }
		switch key {
		case "POST /api/environments":
			reply(map[string]any{"public_id": "env1"})
		case "POST /api/scripts":
			reply(map[string]any{"public_id": "scr1"})
		case "POST /api/jobs":
			reply(map[string]any{"public_id": "job1"})
		case "PUT /api/jobs/job1", "GET /api/environments/env1/validate":
			reply(map[string]any{})
		case "GET /api/jobs":
			if r.URL.Query().Get("public_id") != "job1" {
				t.Errorf("poll for %q", r.URL.RawQuery)
			}
			polls++
			st := map[int]string{1: "validating", 2: "running"}[polls]
			out := ""
			switch {
			case st != "":
			case polls == 3:
				st = "completed" // output dataset not attached yet
			default:
				st, out = "completed", "ds-out"
			}
			reply([]map[string]any{{"public_id": "job1", "status": st, "output_dataset_public_id": out, "environment_public_id": "env1"}})
		default:
			t.Errorf("unexpected %s", key)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, APIKey: "k", HTTP: srv.Client()}
	JobPoll = time.Millisecond
	Log = nil

	id, err := c.Run(Job{
		Cmd: "ob run benchmark.yaml", Env: "name: x\n", Cwd: "/w", OutputDir: "out",
		Cores: 4, RAMGB: 16, DiskGB: 20,
		InputDataset: "ds-in",
		InputContext: map[string]string{"out/DATA/be1.h5ad": "f1", "/abs/x": "f2"},
	})
	if err != nil || id != "job1" {
		t.Fatalf("Run = %q, %v", id, err)
	}
	if e := got["POST /api/environments"]; e["env_string"] != "name: x\n" {
		t.Errorf("environment body %v", e)
	}
	if s := got["POST /api/scripts"]; s["script_content"] != "ob run benchmark.yaml" {
		t.Errorf("script body %v", s)
	}
	if j := got["POST /api/jobs"]; j["requested_cpu"] != 4.0 || j["has_input_data"] != true {
		t.Errorf("job body %v", j)
	}
	u := got["PUT /api/jobs/job1"]
	ctx, _ := u["input_context"].(map[string]any)
	if u["script_public_id"] != "scr1" || u["environment_public_id"] != "env1" ||
		u["current_working_directory"] != "/w" || u["output_directory"] != "/w/out" ||
		u["input_dataset_public_id"] != "ds-in" ||
		ctx["/w/out/DATA/be1.h5ad"] != "f1" || ctx["/abs/x"] != "f2" || len(ctx) != 2 {
		t.Errorf("job update %v", u)
	}

	j, err := c.Wait(id)
	if err != nil || j.Status != "completed" || j.OutputDataset != "ds-out" {
		t.Fatalf("Wait = %+v, %v", j, err)
	}
	if _, err := c.Run(Job{Cmd: "x"}); err == nil || !strings.Contains(err.Error(), "OutputDir") {
		t.Errorf("Run without OutputDir: %v", err)
	}
}

// TestEnvReuse: Run with EnvID skips creating an environment; a job that
// fails validation reports the environment's build log.
func TestEnvReuse(t *testing.T) {
	created := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reply := func(v any) { json.NewEncoder(w).Encode(v) }
		switch r.Method + " " + r.URL.Path {
		case "POST /api/environments":
			created++
			reply(map[string]any{"public_id": "new"})
		case "POST /api/scripts", "POST /api/jobs":
			reply(map[string]any{"public_id": "x"})
		case "PUT /api/jobs/x", "GET /api/environments/keep/validate":
			reply(map[string]any{})
		case "GET /api/jobs":
			reply([]map[string]any{{"public_id": "x", "status": "failed_validation", "environment_public_id": "keep"}})
		case "GET /api/environments":
			reply([]map[string]any{{"build_logs": "LibMambaUnsatisfiableError: c-ares"}})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, APIKey: "k", HTTP: srv.Client()}
	JobPoll, Log = time.Millisecond, nil

	id, err := c.Run(Job{Cmd: "x", EnvID: "keep", Cwd: "/w", OutputDir: "out"})
	if err != nil || created != 0 {
		t.Fatalf("Run with EnvID: err %v, created %d envs", err, created)
	}
	if _, err := c.Wait(id); err == nil || !strings.Contains(err.Error(), "c-ares") {
		t.Errorf("Wait on failed_validation = %v, want build log", err)
	}
}

// TestFindEnv: exact env_string match, newest first, failed builds skipped.
func TestFindEnv(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{
			{"public_id": "old", "env_string": "name: env\n", "env_status": "validated", "created_at": "2026-10-01"},
			{"public_id": "new", "env_string": "name: env\n", "env_status": "created", "created_at": "2026-11-01"},
			{"public_id": "bad", "env_string": "name: env\n", "env_status": "validation_failed", "created_at": "2026-12-01"},
			{"public_id": "other", "env_string": "name: x\n", "env_status": "validated", "created_at": "2026-12-02"},
		})
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, APIKey: "k", HTTP: srv.Client()}
	if id, err := c.FindEnv("name: env"); err != nil || id != "new" {
		t.Errorf("FindEnv = %q, %v; want new", id, err)
	}
	if id, _ := c.FindEnv("name: none"); id != "" {
		t.Errorf("FindEnv(miss) = %q", id)
	}
}
