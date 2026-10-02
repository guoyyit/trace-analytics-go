package analytics

import (
	"context"
	"sort"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/storage"
)

// DependencyView is the aggregated service-to-service graph.
type DependencyView struct {
	Edges []model.DependencyEdge `json:"edges"`
}

type edgeKey struct {
	parent string
	child  string
}

// Dependencies builds the service dependency graph using parent-child span
// relationships, restricted by the filter.
func Dependencies(v *storage.View, f query.Filter, timeout time.Duration) (*DependencyView, error) {
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	edges := make(map[edgeKey]*model.DependencyEdge)
	for _, id := range v.TraceIDs() {
		select {
		case <-ctx.Done():
			return nil, model.ErrQueryTimeout
		default:
		}
		tr, ok := v.Trace(id)
		if !ok {
			continue
		}
		byID := make(map[string]model.Span, len(tr.Spans))
		for _, sp := range tr.Spans {
			byID[sp.SpanID] = sp
		}
		for _, sp := range tr.Spans {
			if sp.ParentSpanID == "" {
				continue
			}
			parent, ok := byID[sp.ParentSpanID]
			if !ok {
				continue
			}
			if parent.ServiceName == sp.ServiceName {
				continue
			}
			if !spanMatches(sp, f) && !spanMatches(parent, f) {
				continue
			}
			key := edgeKey{parent: parent.ServiceName, child: sp.ServiceName}
			edge, ok := edges[key]
			if !ok {
				edge = &model.DependencyEdge{ParentService: parent.ServiceName, ChildService: sp.ServiceName}
				edges[key] = edge
			}
			edge.CallCount++
			if sp.Status == model.StatusError {
				edge.ErrorCount++
			}
			edge.TotalDuration += sp.Duration
		}
	}

	out := make([]model.DependencyEdge, 0, len(edges))
	for _, e := range edges {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ParentService != out[j].ParentService {
			return out[i].ParentService < out[j].ParentService
		}
		return out[i].ChildService < out[j].ChildService
	})
	return &DependencyView{Edges: out}, nil
}
