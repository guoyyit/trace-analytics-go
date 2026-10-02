package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/server"
	"github.com/guoyyit/trace-analytics-go/service"
)

func TestHTTPEndToEnd(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SyncWAL = false
	svc, err := service.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	h := httptest.NewServer(server.New(svc))
	defer h.Close()

	body := `{"spans":[
	  {"traceId":"t1","spanId":"root","serviceName":"gw","operationName":"req","startTime":"2026-01-01T00:00:00Z","duration":100000000,"status":1},
	  {"traceId":"t1","spanId":"c","parentSpanId":"root","serviceName":"db","operationName":"q","startTime":"2026-01-01T00:00:00.01Z","duration":20000000,"status":2},
	  {"spanId":"broken"}
	]}`
	resp, err := http.Post(h.URL+"/v1/spans", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest status = %d", resp.StatusCode)
	}
	var ingest map[string]any
	json.NewDecoder(resp.Body).Decode(&ingest)
	resp.Body.Close()
	if ingest["accepted"] == nil || ingest["accepted"].(float64) != 2 {
		t.Fatalf("accepted = %v", ingest["accepted"])
	}

	// Malformed JSON -> 400.
	resp, _ = http.Post(h.URL+"/v1/spans", "application/json", bytes.NewBufferString("{bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Filtered search finds the error trace.
	resp, err = http.Get(h.URL + "/v1/traces?error=true&service=db")
	if err != nil {
		t.Fatal(err)
	}
	var search struct {
		Traces []model.Trace `json:"traces"`
	}
	json.NewDecoder(resp.Body).Decode(&search)
	resp.Body.Close()
	if len(search.Traces) != 1 || search.Traces[0].TraceID != "t1" {
		t.Fatalf("search = %+v", search)
	}

	// Trace, span drill-down and critical path.
	resp, _ = http.Get(h.URL + "/v1/traces/t1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get trace = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(h.URL + "/v1/traces/t1/spans/c")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get span = %d", resp.StatusCode)
	}
	var span model.Span
	json.NewDecoder(resp.Body).Decode(&span)
	resp.Body.Close()
	if span.OperationName != "q" {
		t.Fatalf("span = %+v", span)
	}

	resp, _ = http.Get(h.URL + "/v1/traces/t1/critical-path")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("critical path = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Stats and dependencies.
	resp, _ = http.Get(h.URL + "/v1/stats?bucket=1h&groupBy=operation")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(h.URL + "/v1/dependencies")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("deps = %d", resp.StatusCode)
	}
	var deps struct {
		Edges []model.DependencyEdge `json:"edges"`
	}
	json.NewDecoder(resp.Body).Decode(&deps)
	resp.Body.Close()
	if len(deps.Edges) != 1 || deps.Edges[0].ParentService != "gw" || deps.Edges[0].ErrorCount != 1 {
		t.Fatalf("deps = %+v", deps.Edges)
	}

	// Unknown trace -> 404; expired/invalid cursor -> 410.
	resp, _ = http.Get(h.URL + "/v1/traces/nope")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing = %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodGet, h.URL+"/v1/traces?cursor=garbage", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("bad cursor = %d", resp.StatusCode)
	}
	resp.Body.Close()
}
