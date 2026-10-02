package analytics

import (
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
)

// CriticalPath computes the bottleneck span chain that determines the total
// trace duration. It starts at the root whose subtree ends latest and, at
// each hop, descends into the child whose subtree ends latest.
func CriticalPath(tr *model.Trace) *model.CriticalPath {
	if tr == nil || len(tr.Roots) == 0 {
		return &model.CriticalPath{TraceID: tr.TraceID}
	}

	var root *model.SpanNode
	for _, r := range tr.Roots {
		if root == nil || subtreeEnd(r).After(subtreeEnd(root)) {
			root = r
		}
	}

	chain := []model.CriticalPathHop{}
	seen := map[*model.SpanNode]bool{}
	node := root
	for node != nil {
		if seen[node] {
			break
		}
		seen[node] = true
		chain = append(chain, toHop(node))
		var next *model.SpanNode
		for _, ch := range node.Children {
			if next == nil || subtreeEnd(ch).After(subtreeEnd(next)) {
				next = ch
			}
		}
		node = next
	}

	return &model.CriticalPath{
		TraceID:       tr.TraceID,
		TotalDuration: chain[0].Duration,
		Chain:         chain,
	}
}

func toHop(n *model.SpanNode) model.CriticalPathHop {
	return model.CriticalPathHop{
		SpanID:        n.Span.SpanID,
		ParentSpanID:  n.Span.ParentSpanID,
		ServiceName:   n.Span.ServiceName,
		OperationName: n.Span.OperationName,
		StartTime:     n.Span.StartTime,
		Duration:      n.Span.Duration,
		Status:        n.Span.Status,
	}
}

// subtreeEnd returns the latest end time in a node's subtree, guarding
// against cycles in malformed trace trees.
func subtreeEnd(n *model.SpanNode) time.Time {
	end := n.Span.End()
	seen := map[*model.SpanNode]bool{}
	var walk func(*model.SpanNode)
	walk = func(x *model.SpanNode) {
		if x == nil || seen[x] {
			return
		}
		seen[x] = true
		if x.Span.End().After(end) {
			end = x.Span.End()
		}
		for _, ch := range x.Children {
			walk(ch)
		}
	}
	walk(n)
	return end
}
