// Package service wires storage, ingestion and analytics behind a
// snapshot-isolated, crash-recoverable API.
package service

import (
	"context"
	"errors"
	"time"

	"github.com/guoyyit/trace-analytics-go/analytics"
	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/storage"
)

// IngestResult separates accepted spans from isolated malformed data.
type IngestResult struct {
	Seq        uint64               `json:"seq"`
	Accepted   int                  `json:"accepted"`
	Duplicates int                  `json:"duplicates"`
	Rejected   []model.RejectedSpan `json:"rejected"`
}

// Service is the main application facade.
type Service struct {
	cfg   config.Config
	store *storage.Store
}

func New(ctx context.Context, cfg config.Config) (*Service, error) {
	store, err := storage.OpenStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, store: store}, nil
}

func (s *Service) Close() error { return s.store.Close() }

// Ingest commits one atomic batch: all accepted spans become visible
// together, or none. Rejected spans are isolated and returned.
func (s *Service) Ingest(ctx context.Context, spans []model.Span) (*IngestResult, error) {
	batch, rejected, err := s.store.Commit(ctx, spans)
	if err != nil {
		return nil, err
	}
	accepted, dup := 0, 0
	accepted = len(batch.Spans)
	// Duplicates are already filtered inside Commit; count rejections that
	// came from duplicate conflicts separately for observability.
	for _, r := range rejected {
		if r.Reason == "duplicate" || r.Reason == "conflicting duplicate span" {
			dup++
		}
	}
	return &IngestResult{
		Seq:        batch.Seq,
		Accepted:   accepted,
		Duplicates: dup,
		Rejected:   rejected,
	}, nil
}

func (s *Service) Search(ctx context.Context, f query.Filter, cursor string, limit int) (*query.SearchResult, error) {
	c, err := query.DecodeCursor(cursor)
	if err != nil {
		return nil, err
	}
	v := s.store.Current()
	if c.ViewSeq != 0 {
		if pinned := s.store.ViewFor(c.ViewSeq); pinned != nil {
			v = pinned
		} else {
			return nil, model.ErrInvalidCursor
		}
	}
	return query.Search(v, f, cursor, limit, s.cfg.QueryTimeout)
}

func (s *Service) GetTrace(ctx context.Context, traceID string) (*model.Trace, error) {
	if traceID == "" {
		return nil, errors.Join(model.ErrInvalidArgument, errors.New("empty traceId"))
	}
	tr, ok := s.store.Current().Trace(traceID)
	if !ok {
		return nil, model.ErrNotFound
	}
	return tr, nil
}

func (s *Service) GetSpan(ctx context.Context, traceID, spanID string) (model.Span, error) {
	if traceID == "" || spanID == "" {
		return model.Span{}, errors.Join(model.ErrInvalidArgument, errors.New("empty id"))
	}
	sp, ok := s.store.Current().Span(traceID, spanID)
	if !ok {
		return model.Span{}, model.ErrNotFound
	}
	return sp, nil
}

func (s *Service) Stats(ctx context.Context, byOperation bool, opt analytics.StatsOptions) (map[time.Time]analytics.StatsBucket, error) {
	if opt.Timeout == 0 {
		opt.Timeout = s.cfg.QueryTimeout
	}
	return analytics.ServiceStats(s.store.Current(), byOperation, opt)
}

func (s *Service) Dependencies(ctx context.Context, f query.Filter) (*analytics.DependencyView, error) {
	return analytics.Dependencies(s.store.Current(), f, s.cfg.QueryTimeout)
}

func (s *Service) TraceCriticalPath(ctx context.Context, traceID string) (*model.CriticalPath, error) {
	tr, err := s.GetTrace(ctx, traceID)
	if err != nil {
		return nil, err
	}
	return analytics.CriticalPath(tr), nil
}

func (s *Service) Rejected(ctx context.Context) ([]model.RejectedSpan, int64, error) {
	list, total := s.store.Current().Rejected()
	return list, total, nil
}
