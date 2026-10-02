package storage

import (
	"time"

	"github.com/guoyyit/trace-analytics-go/config"
)

func testCfg(dir string) config.Config {
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.SyncWAL = false
	cfg.QueryTimeout = time.Second
	return cfg
}
