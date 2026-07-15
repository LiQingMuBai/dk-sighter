package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/service"
	"tron_watcher/internal/tron"
)

const (
	defaultGRPCMinRequestDelay = 20 * time.Millisecond
	defaultGRPCTimeout         = 15 * time.Second
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	loadDotEnvIfExists()

	cfgPath := os.Getenv("TRON_WATCHER_CONFIG")
	if cfgPath == "" {
		cfgPath = defaultConfigPath()
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	opts := resolveOptions(cfg)
	if strings.TrimSpace(opts.Address) == "" {
		log.Fatalf("tron grpc address is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := database.NewMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatalf("connect mysql failed: %v", err)
	}
	defer db.Close()

	repo := repository.New(db)
	if err := alignBackupSyncCursor(ctx, repo, opts.MainSyncKey, opts.SyncKey, opts.StartBlock); err != nil {
		log.Fatalf("align backup sync cursor failed: %v", err)
	}

	cache := service.NewAddressCache(repo)
	client := tron.NewGRPCBackupClient(opts.Address, opts.USDTContract, opts.TokenHeader, opts.Token, opts.UseTLS, opts.Timeout, opts.MinRequestInterval)
	if err := client.Start(); err != nil {
		log.Fatalf("start tron grpc client failed: %v", err)
	}
	defer client.Close()

	scanner := service.NewTronGRPCBackupSync(client, repo, cache, opts.MainSyncKey, opts.StartBlock, opts.TXWorkers, opts.BlockSource, opts.SyncKey, opts.MainStaleDuration)
	scanner.SetSkipToLatestOnLag(false)

	log.Printf("starting tron grpc backup block sync: sync_key=%s source=%s grpc=%s start_block=%d tx_workers=%d",
		opts.SyncKey, opts.BlockSource, maskEndpoint(opts.Address), opts.StartBlock, opts.TXWorkers)
	log.Printf("note: this task uses an independent sync cursor and does not change the main service block sync flow")
	log.Printf("note: provider auth header=%s tls=%t timeout=%s min_request_interval=%s", opts.TokenHeader, opts.UseTLS, opts.Timeout, opts.MinRequestInterval)
	log.Printf("note: this grpc task runs in polling mode and refreshes TRX/USDT balances from on-chain current state when a watched transfer is matched")
	log.Printf("note: backup sync is gap-first for chain=tron: when no skipped main gap exists, it stays idle unless the main cursor is missing or stale for longer than %s", opts.MainStaleDuration)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		err := cache.Run(groupCtx, cfg.AddressReloadInterval())
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})
	group.Go(func() error {
		err := scanner.Run(groupCtx, cfg.BlockPollInterval())
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})

	if err := group.Wait(); err != nil && err != context.Canceled {
		log.Fatalf("tron grpc backup block sync stopped with error: %v", err)
	}
	log.Printf("tron grpc backup block sync stopped")
}

type syncOptions struct {
	Address            string
	TokenHeader        string
	Token              string
	UseTLS             bool
	Timeout            time.Duration
	USDTContract       string
	BlockSource        string
	MainSyncKey        string
	SyncKey            string
	StartBlock         int64
	TXWorkers          int
	MainStaleDuration  time.Duration
	MinRequestInterval time.Duration
}

func resolveOptions(cfg *config.Config) syncOptions {
	blockSource := "head"
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_BLOCK_SOURCE")); strings.EqualFold(value, "solid") {
		blockSource = "solid"
	}

	mainSyncKey := "tron_head_scanner"
	if blockSource == "solid" {
		mainSyncKey = "tron_solid_scanner"
	}

	syncKey := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_KEY"))
	if syncKey == "" {
		syncKey = fmt.Sprintf("tron_grpc_backup_%s_scanner", blockSource)
	}

	startBlock := cfg.Watcher.StartBlock
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_START_BLOCK")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			startBlock = parsed
		}
	}

	txWorkers := cfg.Watcher.TXWorkers
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_TX_WORKERS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			txWorkers = parsed
		}
	}

	mainStaleDuration := 60 * time.Second
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_MAIN_STALE_SECONDS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			mainStaleDuration = time.Duration(parsed) * time.Second
		}
	}

	minInterval := cfg.QuickNodeMinRequestInterval()
	if minInterval < defaultGRPCMinRequestDelay {
		minInterval = defaultGRPCMinRequestDelay
	}
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_MIN_REQUEST_INTERVAL_MS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			minInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	timeout := defaultGRPCTimeout
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_TIMEOUT_MS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			timeout = time.Duration(parsed) * time.Millisecond
		}
	}

	useTLS := true
	if value := strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_TLS")); value != "" {
		useTLS = !strings.EqualFold(value, "false") && value != "0"
	}

	return syncOptions{
		Address:            strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_ADDRESS")),
		TokenHeader:        firstNonEmptyEnv("TRON_GRPC_SYNC_TOKEN_HEADER", "X-CATFEE-TOKEN"),
		Token:              strings.TrimSpace(os.Getenv("TRON_GRPC_SYNC_TOKEN")),
		UseTLS:             useTLS,
		Timeout:            timeout,
		USDTContract:       firstNonEmptyEnv("TRON_GRPC_SYNC_USDT_CONTRACT", cfg.QuickNode.USDT),
		BlockSource:        blockSource,
		MainSyncKey:        mainSyncKey,
		SyncKey:            syncKey,
		StartBlock:         startBlock,
		TXWorkers:          txWorkers,
		MainStaleDuration:  mainStaleDuration,
		MinRequestInterval: minInterval,
	}
}

func alignBackupSyncCursor(ctx context.Context, repo *repository.DB, mainSyncKey, backupSyncKey string, startBlock int64) error {
	if repo == nil {
		return fmt.Errorf("repository is nil")
	}
	if strings.TrimSpace(backupSyncKey) == "" {
		return fmt.Errorf("backup sync key is empty")
	}

	backupBlock, backupExists, err := repo.GetLastBlock(ctx, backupSyncKey)
	if err != nil {
		return fmt.Errorf("load backup sync key %s: %w", backupSyncKey, err)
	}

	mainBlock, mainExists, err := repo.GetLastBlock(ctx, mainSyncKey)
	if err != nil {
		return fmt.Errorf("load main sync key %s: %w", mainSyncKey, err)
	}
	if !mainExists {
		if backupExists {
			log.Printf("main sync cursor not found, backup sync keeps current cursor: sync_key=%s block=%d", backupSyncKey, backupBlock)
			return nil
		}
		log.Printf("main sync cursor not found, backup sync will use default init flow: main_sync_key=%s start_block=%d", mainSyncKey, startBlock)
		return nil
	}

	if !backupExists {
		if err := repo.SaveLastBlock(ctx, backupSyncKey, mainBlock); err != nil {
			return fmt.Errorf("init backup sync key %s from %s=%d: %w", backupSyncKey, mainSyncKey, mainBlock, err)
		}
		log.Printf("backup sync cursor initialized from main sync cursor: backup_sync_key=%s main_sync_key=%s main_block=%d init_block=%d", backupSyncKey, mainSyncKey, mainBlock, mainBlock)
		return nil
	}

	log.Printf("backup sync cursor kept on restart: main_sync_key=%s main_block=%d backup_sync_key=%s backup_block=%d reason=gap_first_keep_current_cursor", mainSyncKey, mainBlock, backupSyncKey, backupBlock)
	return nil
}

func firstNonEmptyEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func loadDotEnvIfExists() {
	candidates := []string{
		".env",
		"configs/.env",
	}

	for _, path := range candidates {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if strings.HasPrefix(line, "export ") {
				line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			value = strings.Trim(value, `"'`)
			if key == "" || os.Getenv(key) != "" {
				continue
			}
			_ = os.Setenv(key, value)
		}
	}
}

func defaultConfigPath() string {
	candidates := []string{
		"configs/config.yaml",
		"config.yaml",
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "configs/config.yaml"
}

func maskEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if len(raw) <= 16 {
		return raw
	}
	return raw[:8] + "..." + raw[len(raw)-5:]
}
