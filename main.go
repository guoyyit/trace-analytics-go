// Command trace-analytics-go runs the local distributed-trace query and
// drill-down analytics service.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/service"
	"github.com/guoyyit/trace-analytics-go/store"
)

func main() {
	addr := flag.String("addr", envOr("ADDR", ":8080"), "HTTP listen address")
	dataDir := flag.String("data-dir", envOr("DATA_DIR", "data"), "persistence directory")
	maxSpans := flag.Int("max-spans", intEnvOr("MAX_SPANS", 1_000_000), "global persisted span cap (0=unlimited)")
	maxSpansPerTrace := flag.Int("max-spans-per-trace", intEnvOr("MAX_SPANS_PER_TRACE", 10_000),
		"per-trace span cap")
	queryTimeout := flag.Duration("query-timeout",
		durationEnvOr("QUERY_TIMEOUT", 5*time.Second), "single query timeout")
	snapshotHistory := flag.Int("snapshot-history", intEnvOr("SNAPSHOT_HISTORY", 32),
		"committed snapshots retained for stable paging")
	flag.Parse()

	cfg := config.Default()
	cfg.DataDir = *dataDir
	cfg.MaxSpans = *maxSpans
	cfg.MaxSpansPerTrace = *maxSpansPerTrace
	cfg.QueryTimeout = *queryTimeout
	cfg.SnapshotHistory = *snapshotHistory

	st, err := store.Open(cfg.DataDir, cfg)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	if dropped := st.ReplayDropped(); dropped > 0 {
		log.Printf("recovery: skipped %d corrupt WAL record(s); committed data intact", dropped)
	}
	ing := ingest.New(st, cfg)
	eng := query.New(st, cfg)
	srv := service.New(ing, eng, cfg)

	go func() {
		log.Printf("trace-analytics listening on %s (data=%s)", *addr, *dataDir)
		if err := srv.Start(*addr); err != nil {
			log.Fatalf("server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	if err := srv.Shutdown(); err != nil {
		log.Printf("shutdown server: %v", err)
	}
	if err := st.Close(); err != nil {
		log.Printf("close store: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnvOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func durationEnvOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
