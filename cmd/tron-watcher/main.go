package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"tron_watcher/internal/app"
)

var buildBranch = "unknown"
var buildCommit = "unknown"

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("fatal panic recovered in main: %v\n%s", r, string(debug.Stack()))
			os.Exit(1)
		}
	}()

	cfgPath := os.Getenv("TRON_WATCHER_CONFIG")
	if cfgPath == "" {
		cfgPath = defaultConfigPath()
	}

	log.Printf("startup info: branch=%s commit=%s", resolveBranch(), resolveCommit())
	log.Printf("starting tron watcher, config=%s", cfgPath)

	application, err := app.New(cfgPath)
	if err != nil {
		log.Fatalf("init app failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatalf("run app failed: %v", err)
	}
}

func resolveBranch() string {
	value := strings.TrimSpace(buildBranch)
	if value != "" && !strings.EqualFold(value, "unknown") {
		return value
	}
	value = strings.TrimSpace(runGit("rev-parse", "--abbrev-ref", "HEAD"))
	if value != "" {
		return value
	}
	return "unknown"
}

func resolveCommit() string {
	value := strings.TrimSpace(buildCommit)
	if value != "" && !strings.EqualFold(value, "unknown") {
		return value
	}
	value = strings.TrimSpace(runGit("rev-parse", "--short", "HEAD"))
	if value != "" {
		return value
	}
	return "unknown"
}

func runGit(args ...string) string {
	if _, err := os.Stat(".git"); err != nil {
		return ""
	}
	cmd := exec.Command("git", args...)
	cmd.Stdout = nil
	cmd.Stderr = io.Discard
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func defaultConfigPath() string {
	candidates := []string{
		"configs/config.yaml",
		"config.yaml",
		"configs/config.example.yaml",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "configs/config.example.yaml"
}
