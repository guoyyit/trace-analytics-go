package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.SyncWAL = false
	s, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return New(ingest.New(s, cfg), query.New(s, cfg), cfg), s
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		rdr = bytes.NewReader(data)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestEndToEnd(t *testing.T) {
	srv, _ := newTestServer(t)

	payload := map[string]any{
		"batchId": "b1",
		"spans": []map[string]any{
			{"traceId": "t1", "spanId": "root", "service": "api", "operation": "GET /x",
				"startUnixNano": 0, "durationNano": 100, "status": "STATUS_ERROR",
				"attributes": map[string]string{"env": "prod"}},
			{"traceId": "t1", "spanId": "db", "parentSpanId": "root", "service": "db",
				"operation": "SELECT", "startUnixNano": 10, "durationNano": 80,
				"status": "STATUS_OK"},
			{"traceId": "", "spanId": "x"}, // malformed, isolated
		},
	}
	code, report := doJSON(t, srv.mux, http.MethodPost, "/api/v1/batches", payload)
	if code != http.StatusOK {
		t.Fatalf("ingest status %d: %v", code, report)
	}
	if acc, _ := report["accepted"].(float64); acc != 2 {
		t.Fatalf("accepted=%v", report["accepted"])
	}
	rejected, _ := report["rejected"].([]any)
	if len(rejected) != 1 {
		t.Fatalf("rejected=%v", report["rejected"])
	}

	code, page := doJSON(t, srv.mux, http.MethodGet,
		"/api/v1/traces?service=db&operation=SELECT", nil)
	if code != http.StatusOK {
		t.Fatalf("search %d", code)
	}
	traces, _ := page["traces"].([]any)
	if len(traces) != 1 {
		t.Fatalf("traces=%v", page)
	}

	code, tr := doJSON(t, srv.mux, http.MethodGet, "/api/v1/traces/t1", nil)
	if code != http.StatusOK || tr["traceId"] != "t1" {
		t.Fatalf("get trace: %d %v", code, tr)
	}

	code, sp := doJSON(t, srv.mux, http.MethodGet, "/api/v1/traces/t1/spans/db", nil)
	if code != http.StatusOK || sp["operation"] != "SELECT" {
		t.Fatalf("get span: %d %v", code, sp)
	}

	code, cp := doJSON(t, srv.mux, http.MethodGet, "/api/v1/traces/t1/critical-path", nil)
	if code != http.StatusOK {
		t.Fatalf("critical path %d", code)
	}
	cpSpans, _ := cp["spans"].([]any)
	if len(cpSpans) != 2 {
		t.Fatalf("critical path spans=%v", cpSpans)
	}

	code, _ = doJSON(t, srv.mux, http.MethodGet,
		"/api/v1/stats?bucketNanos=1000&quantiles=0.5", nil)
	if code != http.StatusOK {
		t.Fatalf("stats %d", code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/dependencies", nil)
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("deps %d", rec.Code)
	}
	var edges []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &edges); err != nil || len(edges) != 1 ||
		edges[0]["fromService"] != "api" || edges[0]["toService"] != "db" {
		t.Fatalf("dependency edge wrong: %s", rec.Body.String())
	}

	// 404 maps to a clear error.
	code, notFound := doJSON(t, srv.mux, http.MethodGet, "/api/v1/traces/nope", nil)
	if code != http.StatusNotFound || !strings.Contains(notFound["error"].(string), "not found") {
		t.Fatalf("not found mapping: %d %v", code, notFound)
	}
}

func TestMalformedJSONRejected(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/batches", strings.NewReader("{nope"))
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
