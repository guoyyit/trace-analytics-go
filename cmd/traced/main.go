// Command traced runs the local distributed trace query and drill-down
// analysis service.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/server"
	"github.com/guoyyit/trace-analytics-go/service"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataDir := flag.String("data", "", "data directory (default: data)")
	queryTimeout := flag.Duration("query-timeout", 0, "per-query timeout")
	maxSpans := flag.Int("max-spans", 0, "global retained span limit")
	flag.Parse()

	cfg := config.Default()
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *queryTimeout > 0 {
		cfg.QueryTimeout = *queryTimeout
	}
	if *maxSpans > 0 {
		cfg.MaxSpans = *maxSpans
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc, err := service.New(ctx, cfg)
	if err != nil {
		log.Fatalf("startup failed: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(svc),
		ReadHeaderTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("traced listening on %s (data=%s)", *addr, cfg.DataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http error: %v", err)
		}
	}()

	<-stop
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	if err := svc.Close(); err != nil {
		log.Printf("close error: %v", err)
	}
}
