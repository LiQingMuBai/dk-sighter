package service

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	tronLoggerOnce sync.Once
	tronLoggerInst *log.Logger

	bscLoggerOnce sync.Once
	bscLoggerInst *log.Logger
)

func tronLogger() *log.Logger {
	tronLoggerOnce.Do(func() {
		tronLoggerInst = buildChainLogger("tron")
	})
	return tronLoggerInst
}

func bscLogger() *log.Logger {
	bscLoggerOnce.Do(func() {
		bscLoggerInst = buildChainLogger("bsc")
	})
	return bscLoggerInst
}

func buildChainLogger(chain string) *log.Logger {
	chain = strings.ToLower(strings.TrimSpace(chain))
	if chain == "" {
		chain = "app"
	}

	flags := log.LstdFlags | log.Lmicroseconds | log.Lshortfile
	prefix := strings.ToUpper(chain) + " "

	if err := os.MkdirAll("logs", 0o755); err != nil {
		return log.New(os.Stdout, prefix, flags)
	}

	filePath := filepath.Join("logs", chain+".log")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return log.New(os.Stdout, prefix, flags)
	}

	return log.New(io.MultiWriter(os.Stdout, f), prefix, flags)
}
