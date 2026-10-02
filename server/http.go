// Package server exposes the trace service over HTTP.
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/guoyyit/trace-analytics-go/analytics"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/service"
)

type Server struct {
	svc *service.Service
	mux *http.ServeMux
}

func New(svc *service.Service) http.Handler {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /v1/spans", s.ingest)
	s.mux.HandleFunc("GET /v1/traces", s.search)
	s.mux.HandleFunc("GET /v1/traces/{traceID}", s.getTrace)
	s.mux.HandleFunc("GET /v1/traces/{traceID}/spans/{spanID}", s.getSpan)
	s.mux.HandleFunc("GET /v1/traces/{traceID}/critical-path", s.criticalPath)
	s.mux.HandleFunc("GET /v1/stats", s.stats)
	s.mux.HandleFunc("GET /v1/dependencies", s.dependencies)
	s.mux.HandleFunc("GET /v1/rejected", s.rejected)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type ingestRequest struct {
	Spans []model.Span `json:"spans"`
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, model.ErrInvalidArgument, "malformed JSON body: "+err.Error())
		return
	}
	res, err := s.svc.Ingest(r.Context(), req.Spans)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	status := http.StatusOK
	if len(res.Rejected) > 0 && res.Accepted == 0 {
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, res)
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r)
	if err != nil {
		writeError(w, model.ErrInvalidArgument, err.Error())
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, model.ErrInvalidArgument, "invalid limit")
			return
		}
		limit = n
	}
	res, err := s.svc.Search(r.Context(), f, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getTrace(w http.ResponseWriter, r *http.Request) {
	tr, err := s.svc.GetTrace(r.Context(), r.PathValue("traceID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

func (s *Server) getSpan(w http.ResponseWriter, r *http.Request) {
	sp, err := s.svc.GetSpan(r.Context(), r.PathValue("traceID"), r.PathValue("spanID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sp)
}

func (s *Server) criticalPath(w http.ResponseWriter, r *http.Request) {
	cp, err := s.svc.TraceCriticalPath(r.Context(), r.PathValue("traceID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cp)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r)
	if err != nil {
		writeError(w, model.ErrInvalidArgument, err.Error())
		return
	}
	opt := analytics.StatsOptions{Filter: f}
	if v := r.URL.Query().Get("bucket"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeError(w, model.ErrInvalidArgument, "invalid bucket")
			return
		}
		opt.Bucket = d
	}
	if v := r.URL.Query().Get("quantiles"); v != "" {
		for _, part := range splitCSV(v) {
			q, err := strconv.ParseFloat(part, 64)
			if err != nil || q < 0 || q > 1 {
				writeError(w, model.ErrInvalidArgument, "invalid quantiles")
				return
			}
			opt.Quantiles = append(opt.Quantiles, q)
		}
	}
	byOp := r.URL.Query().Get("groupBy") == "operation" || r.URL.Query().Get("byOperation") == "true"
	res, err := s.svc.Stats(r.Context(), byOp, opt)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) dependencies(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r)
	if err != nil {
		writeError(w, model.ErrInvalidArgument, err.Error())
		return
	}
	res, err := s.svc.Dependencies(r.Context(), f)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) rejected(w http.ResponseWriter, r *http.Request) {
	list, total, err := s.svc.Rejected(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rejected": list, "total": total})
}

func parseFilter(r *http.Request) (query.Filter, error) {
	var f query.Filter
	q := r.URL.Query()
	f.ServiceName = q.Get("service")
	f.OperationName = q.Get("operation")
	f.TagKey = q.Get("tagKey")
	f.TagValue = q.Get("tagValue")
	f.OnlyErrors = q.Get("error") == "true"
	f.IncludeIncomplete = q.Get("includeIncomplete") == "true"
	if v := q.Get("minDuration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return f, errors.New("invalid minDuration")
		}
		f.MinDuration = d
	}
	if v := q.Get("maxDuration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return f, errors.New("invalid maxDuration")
		}
		f.MaxDuration = d
	}
	if v := q.Get("start"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return f, errors.New("invalid start")
		}
		f.StartTime = t
	}
	if v := q.Get("end"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return f, errors.New("invalid end")
		}
		f.EndTime = t
	}
	return f, nil
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

type errorBody struct {
	Error string `json:"error"`
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrNotFound):
		writeError(w, model.ErrNotFound, err.Error())
	case errors.Is(err, model.ErrInvalidCursor):
		writeError(w, model.ErrInvalidCursor, err.Error())
	case errors.Is(err, model.ErrInvalidArgument):
		writeError(w, model.ErrInvalidArgument, err.Error())
	case errors.Is(err, model.ErrStorageLimit), errors.Is(err, model.ErrTraceTooLarge):
		writeError(w, model.ErrStorageLimit, err.Error())
	case errors.Is(err, model.ErrQueryTimeout):
		writeError(w, model.ErrQueryTimeout, err.Error())
	default:
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: err.Error()})
	}
}

func writeError(w http.ResponseWriter, kind error, msg string) {
	status := http.StatusInternalServerError
	switch kind {
	case model.ErrNotFound:
		status = http.StatusNotFound
	case model.ErrInvalidArgument:
		status = http.StatusBadRequest
	case model.ErrInvalidCursor:
		status = http.StatusGone
	case model.ErrStorageLimit, model.ErrTraceTooLarge:
		status = http.StatusRequestEntityTooLarge
	case model.ErrQueryTimeout:
		status = http.StatusGatewayTimeout
	}
	writeJSON(w, status, errorBody{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
