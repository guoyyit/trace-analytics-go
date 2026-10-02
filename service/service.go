// Package service exposes the analytics engine over HTTP.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
)

// Server is the HTTP API server.
type Server struct {
	ingester *ingest.Ingester
	engine   *query.Engine
	cfg      *config.Config
	http     *http.Server
	mux      *http.ServeMux
}

// New creates a Server.
func New(g *ingest.Ingester, e *query.Engine, cfg *config.Config) *Server {
	if cfg == nil {
		cfg = config.Default()
	}
	s := &Server{ingester: g, engine: e, cfg: cfg, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/v1/batches", s.handleIngest)
	s.mux.HandleFunc("/api/v1/traces", s.handleSearch)
	s.mux.HandleFunc("/api/v1/traces/", s.handleTrace)
	s.mux.HandleFunc("/api/v1/stats", s.handleStats)
	s.mux.HandleFunc("/api/v1/dependencies", s.handleDependencies)
	s.mux.HandleFunc("/api/v1/quarantine", s.handleQuarantine)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// Start begins serving on addr.
func (s *Server) Start(addr string) error {
	s.http = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	if s.http == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.http.Shutdown(ctx)
}

type ingestRequest struct {
	BatchID string       `json:"batchId"`
	Spans   []model.Span `json:"spans"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req ingestRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024*1024)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	report, err := s.ingester.Ingest(req.BatchID, req.Spans)
	if err != nil {
		writeModelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := model.TraceFilter{
		Services:   splitList(q.Get("service")),
		Operations: splitList(q.Get("operation")),
		ErrorState: model.ErrorState(q.Get("error")),
		Cursor:     q.Get("cursor"),
	}
	if err := parseInto(q, "minDurationNano", &f.MinDurationNano); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := parseInto(q, "maxDurationNano", &f.MaxDurationNano); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := parseInto(q, "startUnixNano", &f.StartUnixNano); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := parseInto(q, "endUnixNano", &f.EndUnixNano); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit: "+err.Error())
			return
		}
		f.Limit = n
	}
	f.Attributes = map[string]string{}
	for key, vals := range q {
		if strings.HasPrefix(key, "attr.") && len(vals) > 0 {
			f.Attributes[strings.TrimPrefix(key, "attr.")] = vals[0]
		}
	}
	if len(f.Attributes) == 0 {
		f.Attributes = nil
	}
	if f.ErrorState == "" {
		f.ErrorState = model.ErrorAny
	}

	page, err := s.engine.SearchTraces(r.Context(), f)
	if err != nil {
		writeModelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

// handleTrace serves /traces/{id}, /traces/{id}/spans/{spanId} and
// /traces/{id}/critical-path.
func (s *Server) handleTrace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "traceId required")
		return
	}
	traceID := parts[0]
	switch {
	case len(parts) == 1:
		tr, err := s.engine.GetTrace(r.Context(), traceID)
		if err != nil {
			writeModelError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, tr)
	case len(parts) == 3 && parts[1] == "spans":
		sp, err := s.engine.GetSpan(r.Context(), traceID, parts[2])
		if err != nil {
			writeModelError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, sp)
	case len(parts) == 2 && parts[1] == "critical-path":
		cp, err := s.engine.CriticalPath(r.Context(), traceID)
		if err != nil {
			writeModelError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, cp)
	default:
		writeError(w, http.StatusNotFound, "unknown path")
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := model.StatsFilter{
		Services:   splitList(q.Get("service")),
		Operations: splitList(q.Get("operation")),
	}
	_ = parseInto(q, "startUnixNano", &f.StartUnixNano)
	_ = parseInto(q, "endUnixNano", &f.EndUnixNano)
	bucket, err := parsePositive(q.Get("bucketNanos"), 60_000_000_000)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var quantiles []float64
	if raw := q.Get("quantiles"); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			qv, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil || qv < 0 || qv > 1 {
				writeError(w, http.StatusBadRequest, "invalid quantile: "+p)
				return
			}
			quantiles = append(quantiles, qv)
		}
	}
	out, err := s.engine.Stats(r.Context(), f, bucket, quantiles)
	if err != nil {
		writeModelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDependencies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := model.StatsFilter{
		Services:   splitList(q.Get("service")),
		Operations: splitList(q.Get("operation")),
	}
	_ = parseInto(q, "startUnixNano", &f.StartUnixNano)
	_ = parseInto(q, "endUnixNano", &f.EndUnixNano)
	edges, err := s.engine.Dependencies(r.Context(), f)
	if err != nil {
		writeModelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, edges)
}

func (s *Server) handleQuarantine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, http.StatusOK, map[string]any{"rejected": s.engine.Rejected(limit)})
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseInto(q interface{ Get(string) string }, key string, dst *int64) error {
	raw := q.Get(key)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return errors.New("invalid " + key + ": " + err.Error())
	}
	*dst = v
	return nil
}

func parsePositive(raw string, def int64) (int64, error) {
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, errors.New("value must be a positive integer")
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Headers/body already started; surface the serialization failure in
		// the server log so it cannot silently masquerade as an empty result.
		log.Printf("failed to encode JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeModelError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, model.ErrCursorInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, model.ErrCursorExpired):
		writeError(w, http.StatusGone, err.Error())
	case errors.Is(err, model.ErrLimitExceeded):
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, model.ErrQueryTimeout):
		writeError(w, http.StatusGatewayTimeout, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
