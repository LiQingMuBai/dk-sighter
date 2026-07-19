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

const defaultBackupMinRequestDelay = 20 * time.Millisecond

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
	if strings.TrimSpace(opts.HTTPURL) == "" {
		log.Fatalf("tron backup http url is required")
	}
	if strings.TrimSpace(opts.USDTContract) == "" {
		log.Fatalf("tron backup usdt contract is required")
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
	client := tron.NewClient(opts.HTTPURL, "", opts.USDTContract, opts.MinRequestInterval)
	balanceService := service.NewBalanceService(client, repo, cache)
	scanner := service.NewTronBackupSync(client, repo, cache, balanceService, opts.MainSyncKey, opts.StartBlock, opts.TXWorkers, opts.BlockSource, opts.SyncKey, opts.MainStaleDuration)
	scanner.SetSkipToLatestOnLag(false)

	log.Printf("starting tron backup block sync: sync_key=%s source=%s http=%s start_block=%d tx_workers=%d",
		opts.SyncKey, opts.BlockSource, maskEndpoint(opts.HTTPURL), opts.StartBlock, opts.TXWorkers)
	log.Printf("note: this task uses an independent sync cursor and does not change the main service block sync flow")
	log.Printf("note: this backup task is gap-first for chain=tron: when no skipped main gap exists, it stays idle unless the main cursor is missing or stale for longer than %s", opts.MainStaleDuration)
	log.Printf("note: this backup task uses tron http rpc and writes matched TRX/USDT transfers into transfer records")
	log.Printf("note: matched transfers trigger the same delayed on-chain balance refresh queue used by the main tron scanner")
	log.Printf("note: min request interval=%s block_poll_interval=%s", opts.MinRequestInterval, cfg.BlockPollInterval())

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
		log.Fatalf("tron backup block sync stopped with error: %v", err)
	}
	log.Printf("tron backup block sync stopped")
}

type syncOptions struct {
	HTTPURL            string
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
	httpURL := firstNonEmptyEnv("TRON_BACKUP_SYNC_HTTP_URL", cfg.QuickNodeRefreshHTTPURL())
	if httpURL == "" {
		httpURL = strings.TrimSpace(cfg.QuickNode.HTTPURL)
	}

	blockSource := firstNonEmptyEnv("TRON_BACKUP_SYNC_BLOCK_SOURCE", cfg.TronBlockSource())
	if !strings.EqualFold(strings.TrimSpace(blockSource), "solid") {
		blockSource = "head"
	} else {
		blockSource = "solid"
	}

	mainSyncKey := "tron_head_scanner"
	if blockSource == "solid" {
		mainSyncKey = "tron_solid_scanner"
	}

	syncKey := strings.TrimSpace(os.Getenv("TRON_BACKUP_SYNC_KEY"))
	if syncKey == "" {
		syncKey = fmt.Sprintf("tron_backup_%s_scanner", blockSource)
	}

	startBlock := cfg.Watcher.StartBlock
	if value := strings.TrimSpace(os.Getenv("TRON_BACKUP_SYNC_START_BLOCK")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			startBlock = parsed
		}
	}

	txWorkers := cfg.Watcher.TXWorkers
	if value := strings.TrimSpace(os.Getenv("TRON_BACKUP_SYNC_TX_WORKERS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			txWorkers = parsed
		}
	}

	mainStaleDuration := 60 * time.Second
	if value := strings.TrimSpace(os.Getenv("TRON_BACKUP_SYNC_MAIN_STALE_SECONDS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			mainStaleDuration = time.Duration(parsed) * time.Second
		}
	}

	minInterval := cfg.QuickNodeRefreshMinRequestInterval()
	if minInterval <= 0 {
		minInterval = cfg.QuickNodeMinRequestInterval()
	}
	if minInterval < defaultBackupMinRequestDelay {
		minInterval = defaultBackupMinRequestDelay
	}
	if value := strings.TrimSpace(os.Getenv("TRON_BACKUP_SYNC_MIN_REQUEST_INTERVAL_MS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			minInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	return syncOptions{
		HTTPURL:            strings.TrimSpace(httpURL),
		USDTContract:       firstNonEmptyEnv("TRON_BACKUP_SYNC_USDT_CONTRACT", cfg.QuickNode.USDT),
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
