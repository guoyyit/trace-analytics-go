package query

import (
	"context"

	"github.com/guoyyit/trace-analytics-go/model"
)

// CriticalPath returns the bottleneck span chain of a trace.
//
// The critical path is the longest weighted path through the parent/child
// DAG, where each node's weight is its own exclusive work
// (span duration minus the union of its children's intervals). Parallel
// branches therefore contribute only the slowest branch, while serial
// segments add up. It is the chain that determines the trace total time.
func (e *Engine) CriticalPath(ctx context.Context, traceID string) (*model.CriticalPath, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()

	snap := e.store.Current()
	tr, ok := snap.Trace(traceID)
	if !ok {
		return nil, model.ErrNotFound
	}

	byID := make(map[string]model.Span, len(tr.Spans))
	children := make(map[string][]model.Span, len(tr.Spans))
	var roots []model.Span
	for _, sp := range tr.Spans {
		byID[sp.SpanID] = sp
	}
	for _, sp := range tr.Spans {
		if sp.ParentSpanID == "" {
			roots = append(roots, sp)
			continue
		}
		if _, ok := byID[sp.ParentSpanID]; ok {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp)
		} else {
			// Missing parent: treat as a root so its branch is still analyzed.
			roots = append(roots, sp)
		}
	}

	type dp struct {
		best int64
		next string
		own  int64
	}
	memo := make(map[string]dp, len(tr.Spans))
	var solve func(sp model.Span) dp
	solve = func(sp model.Span) dp {
		if v, ok := memo[sp.SpanID]; ok {
			return v
		}
		exclusive := sp.DurationNano - coveredDuration(sp, children[sp.SpanID])
		bestChild := int64(0)
		bestChildID := ""
		for _, ch := range children[sp.SpanID] {
			v := solve(ch)
			if v.best > bestChild {
				bestChild = v.best
				bestChildID = ch.SpanID
			}
		}
		v := dp{best: exclusive + bestChild, next: bestChildID, own: exclusive}
		memo[sp.SpanID] = v
		return v
	}

	var start model.Span
	best := int64(-1)
	for _, r := range roots {
		if ctx.Err() != nil {
			return nil, model.ErrQueryTimeout
		}
		v := solve(r)
		if v.best > best {
			best = v.best
			start = r
		}
	}

	cp := &model.CriticalPath{
		TraceID:           traceID,
		TotalDurationNano: tr.EndUnixNano - tr.StartUnixNano,
	}
	id := start.SpanID
	for id != "" {
		sp := byID[id]
		cp.Spans = append(cp.Spans, model.CriticalSpan{
			SpanID:        sp.SpanID,
			ParentSpanID:  sp.ParentSpanID,
			Service:       sp.Service,
			Operation:     sp.Operation,
			StartUnixNano: sp.StartUnixNano,
			DurationNano:  sp.DurationNano,
		})
		id = memo[id].next
	}
	return cp, nil
}

// coveredDuration returns the total length of [start,end) covered by the
// union of the children's time intervals.
func coveredDuration(parent model.Span, kids []model.Span) int64 {
	if len(kids) == 0 {
		return 0
	}
	type iv struct{ lo, hi int64 }
	intervals := make([]iv, 0, len(kids))
	for _, ch := range kids {
		lo := ch.StartUnixNano
		hi := ch.EndUnixNano()
		if hi <= lo {
			continue
		}
		intervals = append(intervals, iv{lo, hi})
	}
	if len(intervals) == 0 {
		return 0
	}
	for i := 0; i < len(intervals); i++ {
		for j := i + 1; j < len(intervals); j++ {
			if intervals[j].lo < intervals[i].lo {
				intervals[i], intervals[j] = intervals[j], intervals[i]
			}
		}
	}
	covered := int64(0)
	curLo, curHi := intervals[0].lo, intervals[0].hi
	for _, in := range intervals[1:] {
		if in.lo <= curHi {
			if in.hi > curHi {
				curHi = in.hi
			}
		} else {
			covered += curHi - curLo
			curLo, curHi = in.lo, in.hi
		}
	}
	covered += curHi - curLo
	return covered
}
