package service

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	baseLogDirOnce sync.Once
	baseLogDirVal  string
)

func resolveBaseLogDir() string {
	baseLogDirOnce.Do(func() {
		if v := strings.TrimSpace(os.Getenv("TRON_WATCHER_LOG_DIR")); v != "" {
			if abs, err := filepath.Abs(v); err == nil {
				baseLogDirVal = abs
				return
			}
			baseLogDirVal = v
			return
		}
		baseLogDirVal = "logs"
	})
	return baseLogDirVal
}

var (
	tronLoggerOnce sync.Once
	tronLoggerInst *log.Logger

	bscLoggerOnce sync.Once
	bscLoggerInst *log.Logger

	tronGRPCBackupLoggerOnce sync.Once
	tronGRPCBackupLoggerInst *log.Logger

	taskLoggerMu sync.Mutex
	taskLoggers  = make(map[string]*log.Logger)
)

type chainDispatchWriter struct {
	name     string
	mainFile *os.File
	mainOut  io.Writer
	tronOut  io.Writer
	bscOut   io.Writer
}

func (w *chainDispatchWriter) Write(p []byte) (int, error) {
	if w == nil || len(p) == 0 {
		return len(p), nil
	}
	lower := bytes.ToLower(p)
	var side io.Writer
	switch {
	case bytes.Contains(lower, []byte("bsc")):
		side = w.bscOut
	case bytes.Contains(lower, []byte("tron")):
		side = w.tronOut
	}
	if w.mainOut != nil {
		if _, err := w.mainOut.Write(p); err != nil {
			return 0, err
		}
	}
	if side != nil {
		if _, err := side.Write(p); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *chainDispatchWriter) Close() error {
	if w == nil {
		return nil
	}
	if w.mainFile != nil {
		return w.mainFile.Close()
	}
	return nil
}

func tronLogger() *log.Logger {
	tronLoggerOnce.Do(func() {
		tronLoggerInst = buildChainLogger("tron")
	})
	return tronLoggerInst
}

func TronLogger() *log.Logger {
	return tronLogger()
}

func bscLogger() *log.Logger {
	bscLoggerOnce.Do(func() {
		bscLoggerInst = buildChainLogger("bsc")
	})
	return bscLoggerInst
}

func BSCLogger() *log.Logger {
	return bscLogger()
}

func tronGRPCBackupLogger() *log.Logger {
	tronGRPCBackupLoggerOnce.Do(func() {
		tronGRPCBackupLoggerInst = buildChainLogger("tron-grpc-backup")
	})
	return tronGRPCBackupLoggerInst
}

func TaskLogger(taskName string) *log.Logger {
	taskLoggerMu.Lock()
	defer taskLoggerMu.Unlock()
	taskName = normalizeTaskName(taskName)
	if l, ok := taskLoggers[taskName]; ok {
		return l
	}
	l := buildTaskLogger(taskName)
	taskLoggers[taskName] = l
	return l
}

func SetupCmdLogger(taskName string) *log.Logger {
	taskName = normalizeTaskName(taskName)
	logger := TaskLogger(taskName)
	log.SetOutput(taskWriter(taskName))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	return logger
}

func SetupTronWatcherCmdLogger() *log.Logger {
	writer := newChainDispatchWriter("tron-watcher")
	flags := log.LstdFlags | log.Lmicroseconds | log.Lshortfile
	logger := log.New(writer, "[tron-watcher] ", flags)
	log.SetOutput(writer)
	log.SetFlags(flags)
	log.SetPrefix("[tron-watcher] ")
	return logger
}

func buildChainLogger(chain string) *log.Logger {
	chain = strings.ToLower(strings.TrimSpace(chain))
	if chain == "" {
		chain = "app"
	}
	return buildLoggerWithPrefix(chain, strings.ToUpper(chain)+" ")
}

func buildTaskLogger(taskName string) *log.Logger {
	taskName = normalizeTaskName(taskName)
	return buildLoggerWithPrefix(taskName, "["+taskName+"] ")
}

func buildLoggerWithPrefix(name, prefix string) *log.Logger {
	flags := log.LstdFlags | log.Lmicroseconds | log.Lshortfile
	writer, err := taskLogWriter(name)
	if err != nil {
		return log.New(os.Stdout, prefix, flags)
	}
	return log.New(writer, prefix, flags)
}

func taskWriter(name string) io.Writer {
	w, err := taskLogWriter(name)
	if err != nil {
		return os.Stdout
	}
	return w
}

func taskLogWriter(name string) (io.Writer, error) {
	dir := resolveBaseLogDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	filePath := filepath.Join(dir, strings.ToLower(name)+".log")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return io.MultiWriter(os.Stdout, f), nil
}

func rawTaskLogWriter(name string) (io.Writer, *os.File, error) {
	dir := resolveBaseLogDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	filePath := filepath.Join(dir, strings.ToLower(name)+".log")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return io.MultiWriter(os.Stdout, f), f, nil
}

func newChainDispatchWriter(taskName string) io.Writer {
	mainOut, mainFile, err := rawTaskLogWriter(taskName)
	if err != nil {
		return os.Stdout
	}
	tronOut := taskWriter("tron")
	bscOut := taskWriter("bsc")
	return &chainDispatchWriter{
		name:     normalizeTaskName(taskName),
		mainFile: mainFile,
		mainOut:  mainOut,
		tronOut:  tronOut,
		bscOut:   bscOut,
	}
}

func normalizeTaskName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "cmd"
	}
	return name
}
