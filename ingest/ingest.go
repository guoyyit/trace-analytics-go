// Package ingest handles span validation, dedup/conflict detection and trace
// assembly over out-of-order, duplicate, late and orphan spans.
package ingest

import (
	"sort"

	"github.com/guoyyit/trace-analytics-go/model"
)

// Validate performs structural validation of a single span. It returns a
// non-empty rejection reason when the span is malformed.
func Validate(span model.Span) string {
	if span.TraceID == "" {
		return "missing traceId"
	}
	if span.SpanID == "" {
		return "missing spanId"
	}
	if span.ServiceName == "" {
		return "missing serviceName"
	}
	if span.OperationName == "" {
		return "missing operationName"
	}
	if span.StartTime.IsZero() {
		return "missing startTime"
	}
	if span.Duration < 0 {
		return "negative duration"
	}
	if span.Status > model.StatusError {
		return "invalid status"
	}
	if span.ParentSpanID == span.SpanID {
		return "span is its own parent"
	}
	if len(span.SpanID) > 256 || len(span.TraceID) > 256 || len(span.ParentSpanID) > 256 {
		return "id too long"
	}
	if len(span.ServiceName) > 512 || len(span.OperationName) > 1024 {
		return "name too long"
	}
	for k, v := range span.Tags {
		if k == "" {
			return "empty tag key"
		}
		if len(k) > 512 || len(v) > 8192 {
			return "tag key or value too long"
		}
	}
	return ""
}

// BuildTrace assembles one trace from its complete span set.
func BuildTrace(traceID string, spans []model.Span) *model.Trace {
	if len(spans) == 0 {
		return &model.Trace{TraceID: traceID, Complete: true}
	}

	byID := make(map[string]*model.SpanNode, len(spans))
	for _, sp := range spans {
		if _, ok := byID[sp.SpanID]; ok {
			continue
		}
		byID[sp.SpanID] = &model.SpanNode{Span: sp}
	}

	missing := map[string]struct{}{}
	childrenOf := make(map[string][]*model.SpanNode)
	for _, node := range byID {
		pid := node.Span.ParentSpanID
		if pid == "" {
			continue
		}
		if _, ok := byID[pid]; !ok {
			missing[pid] = struct{}{}
		}
	}

	// Link children, breaking cycle back-edges by lifting the child to a root
	// and recording the broken parent as missing.
	visit := map[string]uint8{} // 0 unseen, 1 on stack, 2 done
	var dfs func(id string)
	dfs = func(id string) {
		if visit[id] == 2 {
			return
		}
		if visit[id] == 1 {
			return
		}
		visit[id] = 1
		node := byID[id]
		pid := node.Span.ParentSpanID
		if pid != "" {
			if _, ok := byID[pid]; ok {
				if visit[pid] == 1 {
					missing[pid] = struct{}{}
				} else {
					dfs(pid)
					if visit[pid] == 2 {
						childrenOf[pid] = append(childrenOf[pid], node)
					} else {
						missing[pid] = struct{}{}
					}
				}
			}
		}
		visit[id] = 2
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		dfs(id)
	}

	for pid, children := range childrenOf {
		byID[pid].Children = children
	}

	var roots []*model.SpanNode
	for _, node := range byID {
		pid := node.Span.ParentSpanID
		if pid == "" {
			roots = append(roots, node)
			continue
		}
		if _, ok := byID[pid]; !ok {
			roots = append(roots, node)
			continue
		}
		linked := false
		for _, ch := range byID[pid].Children {
			if ch == node {
				linked = true
				break
			}
		}
		if !linked {
			roots = append(roots, node)
		}
	}

	start := spans[0].StartTime
	end := spans[0].End()
	unique := make([]model.Span, 0, len(byID))
	for _, node := range byID {
		unique = append(unique, node.Span)
		if node.Span.StartTime.Before(start) {
			start = node.Span.StartTime
		}
		if node.Span.End().After(end) {
			end = node.Span.End()
		}
	}

	less := func(a, b *model.SpanNode) bool {
		if !a.Span.StartTime.Equal(b.Span.StartTime) {
			return a.Span.StartTime.Before(b.Span.StartTime)
		}
		return a.Span.SpanID < b.Span.SpanID
	}
	sortNodes(roots, less)
	sort.Slice(unique, func(i, j int) bool {
		if !unique[i].StartTime.Equal(unique[j].StartTime) {
			return unique[i].StartTime.Before(unique[j].StartTime)
		}
		return unique[i].SpanID < unique[j].SpanID
	})

	missingIDs := make([]string, 0, len(missing))
	for id := range missing {
		missingIDs = append(missingIDs, id)
	}
	sort.Strings(missingIDs)

	return &model.Trace{
		TraceID:         traceID,
		StartTime:       start,
		EndTime:         end,
		Spans:           unique,
		Roots:           roots,
		Complete:        len(missing) == 0,
		MissingParentID: missingIDs,
	}
}

func sortNodes(nodes []*model.SpanNode, less func(a, b *model.SpanNode) bool) {
	sort.Slice(nodes, func(i, j int) bool { return less(nodes[i], nodes[j]) })
	for _, n := range nodes {
		sortNodes(n.Children, less)
	}
}
