package query

import (
	"context"

	"github.com/guoyyit/trace-analytics-go/model"
)

// searchKey is the deterministic, immutable ordering used for paging:
// (trace start, trace id). Keys are stable because a trace's start timestamp
// can only move to an earlier value when late spans arrive; to guarantee
// stability, pages are served from the snapshot pinned by the first cursor.
func searchKey(tr *model.TraceSummary) string {
	return encodeKey(tr.StartUnixNano, tr.TraceID)
}

// SearchTraces returns one stable page of traces matching f.
//
// Paging semantics: the first response pins the snapshot sequence in the
// opaque cursor. All following pages read that same immutable snapshot, so
// writes during paging cannot duplicate or skip records. If the pinned
// snapshot has already been pruned (SnapshotHistory exceeded), the call
// fails with model.ErrCursorExpired and the client must restart paging.
func (e *Engine) SearchTraces(ctx context.Context, f model.TraceFilter) (*model.TracePage, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()

	limit := f.Limit
	if limit <= 0 {
		limit = e.cfg.DefaultPageLimit
	}
	if e.cfg.MaxPageLimit > 0 && limit > e.cfg.MaxPageLimit {
		return nil, model.ErrLimitExceeded
	}

	snap, err := e.resolveSnapshot(f.Cursor)
	if err != nil {
		return nil, err
	}
	cur, err := decodeCursor(f.Cursor)
	if err != nil {
		return nil, err
	}
	after := ""
	if cur != nil {
		after = cur.LastKey
	}

	ids := e.candidateIDs(snap, f)
	page := &model.TracePage{}
	matched := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return nil, model.ErrQueryTimeout
		}
		tr, ok := snap.Trace(id)
		if !ok || !traceMatches(tr, f) {
			continue
		}
		summary := summarize(tr)
		key := searchKey(&summary)
		if after != "" && key <= after {
			continue
		}
		if matched < limit {
			page.Traces = append(page.Traces, summary)
			matched++
			if matched == limit {
				last := searchKey(&page.Traces[len(page.Traces)-1])
				c, err := encodeCursor(snap.Seq(), last)
				if err != nil {
					return nil, err
				}
				page.NextCursor = c
			}
		}
	}
	return page, nil
}

// candidateIDs restricts the scan using the service index when a single
// service predicate is present; otherwise it scans all trace ids.
func (e *Engine) candidateIDs(snap interface {
	AllTraceIDs() []string
	TraceIDsByService(string) []string
}, f model.TraceFilter) []string {
	if len(f.Services) == 1 {
		return snap.TraceIDsByService(f.Services[0])
	}
	return snap.AllTraceIDs()
}

// GetTrace returns the full assembled trace (aggregation -> trace drill-down).
func (e *Engine) GetTrace(ctx context.Context, traceID string) (*model.Trace, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()
	tr, ok := e.store.Current().Trace(traceID)
	if !ok {
		return nil, model.ErrNotFound
	}
	if ctx.Err() != nil {
		return nil, model.ErrQueryTimeout
	}
	return tr, nil
}

// GetSpan returns a single span detail (trace -> span drill-down).
func (e *Engine) GetSpan(ctx context.Context, traceID, spanID string) (model.Span, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()
	sp, ok := e.store.Current().Span(traceID, spanID)
	if !ok {
		return model.Span{}, model.ErrNotFound
	}
	if ctx.Err() != nil {
		return model.Span{}, model.ErrQueryTimeout
	}
	return sp, nil
}

// Rejected returns recently quarantined spans, isolated from normal results.
func (e *Engine) Rejected(limit int) []model.RejectedSpan {
	return e.store.Current().Rejected(limit)
}
