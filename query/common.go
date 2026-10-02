// Package query provides trace search, aggregation, dependency and
// drill-down reads over immutable store snapshots.
//
// Every read resolves one immutable snapshot (the latest, or the snapshot
// pinned by a pagination cursor), so concurrent writes never cause a query
// to observe half-applied batches or statistics jumps.
package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/store"
)

// encodeKey builds a fixed-width lexicographically ordered key from a
// nanosecond timestamp and trace id. The %020d width keeps unsigned-range
// timestamps sortable as strings.
func encodeKey(startUnixNano int64, traceID string) string {
	return fmt.Sprintf("%020d/%s", startUnixNano, traceID)
}

// Engine executes read queries against the store.
type Engine struct {
	store *store.Store
	cfg   *config.Config
}

// New creates a query Engine.
func New(s *store.Store, cfg *config.Config) *Engine {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Engine{store: s, cfg: cfg}
}

// cursor pins a snapshot sequence and records the last returned trace id.
// It is serialized as url-safe base64 JSON so clients treat it opaquely.
type cursor struct {
	Seq     uint64 `json:"seq"`
	LastKey string `json:"lastKey"`
}

func encodeCursor(seq uint64, lastKey string) (string, error) {
	data, err := json.Marshal(cursor{Seq: seq, LastKey: lastKey})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(s string) (*cursor, error) {
	if s == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, model.ErrCursorInvalid
	}
	var c cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, model.ErrCursorInvalid
	}
	return &c, nil
}

// resolveSnapshot returns the pinned snapshot for the (possibly empty)
// cursor, otherwise the latest committed snapshot.
func (e *Engine) resolveSnapshot(c string) (*store.Snapshot, error) {
	cur, err := decodeCursor(c)
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return e.store.Current(), nil
	}
	return e.store.SnapshotAt(cur.Seq)
}

// runWithTimeout applies the configured query timeout to ctx.
func (e *Engine) runWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.cfg.QueryTimeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			return context.WithTimeout(ctx, e.cfg.QueryTimeout)
		}
	}
	return ctx, func() {}
}

// spanMatches reports whether a single span satisfies every populated
// predicate. List predicates use OR within the list; all predicates are
// ANDed together.
func spanMatches(sp model.Span, f model.TraceFilter) bool {
	if len(f.Services) > 0 && !contains(f.Services, sp.Service) {
		return false
	}
	if len(f.Operations) > 0 && !contains(f.Operations, sp.Operation) {
		return false
	}
	if len(f.Attributes) > 0 {
		for k, v := range f.Attributes {
			got, ok := sp.Attributes[k]
			if !ok || got != v {
				return false
			}
		}
	}
	if f.MinDurationNano > 0 && sp.DurationNano < f.MinDurationNano {
		return false
	}
	if f.MaxDurationNano > 0 && sp.DurationNano > f.MaxDurationNano {
		return false
	}
	switch f.ErrorState {
	case model.ErrorsOnly:
		if sp.Status != model.StatusError {
			return false
		}
	case model.OkOnly:
		if sp.Status == model.StatusError {
			return false
		}
	}
	if f.StartUnixNano > 0 && sp.EndUnixNano() < f.StartUnixNano {
		return false
	}
	if f.EndUnixNano > 0 && sp.StartUnixNano > f.EndUnixNano {
		return false
	}
	return true
}

// traceMatches applies the span predicates at trace level:
// service/operation/tag/duration/error predicates must be satisfied by at
// least one span in the trace; the time window applies to trace bounds.
func traceMatches(tr *model.Trace, f model.TraceFilter) bool {
	if f.StartUnixNano > 0 && tr.EndUnixNano < f.StartUnixNano {
		return false
	}
	if f.EndUnixNano > 0 && tr.StartUnixNano > f.EndUnixNano {
		return false
	}
	if f.MinDurationNano > 0 && (tr.EndUnixNano-tr.StartUnixNano) < f.MinDurationNano {
		return false
	}
	if f.MaxDurationNano > 0 && (tr.EndUnixNano-tr.StartUnixNano) > f.MaxDurationNano {
		return false
	}
	switch f.ErrorState {
	case model.ErrorsOnly:
		if !tr.HasError {
			return false
		}
	case model.OkOnly:
		if tr.HasError {
			return false
		}
	}
	if len(f.Services) == 0 && len(f.Operations) == 0 && len(f.Attributes) == 0 {
		return true
	}
	for _, sp := range tr.Spans {
		if spanMatches(sp, f) {
			return true
		}
	}
	return false
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// summarize projects a trace into a list-page summary.
func summarize(tr *model.Trace) model.TraceSummary {
	services := append([]string(nil), tr.Services...)
	return model.TraceSummary{
		TraceID:       tr.TraceID,
		StartUnixNano: tr.StartUnixNano,
		EndUnixNano:   tr.EndUnixNano,
		DurationNano:  tr.EndUnixNano - tr.StartUnixNano,
		SpanCount:     len(tr.Spans),
		Services:      services,
		HasError:      tr.HasError,
	}
}
