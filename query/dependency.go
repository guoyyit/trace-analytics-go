package query

import (
	"context"
	"sort"

	"github.com/guoyyit/trace-analytics-go/model"
)

type edgeKey struct{ from, to string }

type edgeAcc struct {
	count      int64
	errorCount int64
	sum        int64
}

// Dependencies builds the service dependency view from parent/child span
// relations. Only edges whose child span falls in the time filter and whose
// from/to services pass the service filter are included; root spans (no
// parent) and missing parents are ignored at the edge level.
func (e *Engine) Dependencies(ctx context.Context, f model.StatsFilter) ([]model.DependencyEdge, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()

	snap := e.store.Current()
	ids := e.candidateIDs(snap, model.TraceFilter{Services: f.Services})
	accs := map[edgeKey]*edgeAcc{}

	for _, id := range ids {
		if ctx.Err() != nil {
			return nil, model.ErrQueryTimeout
		}
		tr, ok := snap.Trace(id)
		if !ok {
			continue
		}
		byID := make(map[string]model.Span, len(tr.Spans))
		for _, sp := range tr.Spans {
			byID[sp.SpanID] = sp
		}
		for _, child := range tr.Spans {
			if child.ParentSpanID == "" {
				continue
			}
			if f.StartUnixNano > 0 && child.EndUnixNano() < f.StartUnixNano {
				continue
			}
			if f.EndUnixNano > 0 && child.StartUnixNano > f.EndUnixNano {
				continue
			}
			if len(f.Operations) > 0 && !contains(f.Operations, child.Operation) {
				continue
			}
			parent, ok := byID[child.ParentSpanID]
			if !ok || parent.Service == child.Service {
				continue
			}
			if len(f.Services) > 0 &&
				(!contains(f.Services, parent.Service) || !contains(f.Services, child.Service)) {
				continue
			}
			key := edgeKey{parent.Service, child.Service}
			acc := accs[key]
			if acc == nil {
				acc = &edgeAcc{}
				accs[key] = acc
			}
			acc.count++
			acc.sum += child.DurationNano
			if child.Status == model.StatusError || parent.Status == model.StatusError {
				acc.errorCount++
			}
		}
	}

	keys := make([]edgeKey, 0, len(accs))
	for k := range accs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].from != keys[j].from {
			return keys[i].from < keys[j].from
		}
		return keys[i].to < keys[j].to
	})
	edges := make([]model.DependencyEdge, 0, len(keys))
	for _, k := range keys {
		acc := accs[k]
		edges = append(edges, model.DependencyEdge{
			FromService:     k.from,
			ToService:       k.to,
			Count:           acc.count,
			ErrorCount:      acc.errorCount,
			ErrorRate:       float64(acc.errorCount) / float64(acc.count),
			AvgDurationNano: acc.sum / acc.count,
		})
	}
	return edges, nil
}
