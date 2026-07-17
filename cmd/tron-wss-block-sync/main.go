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
	wsRetryDelay                 = 3 * time.Second
	defaultBackupMinRequestDelay = 20 * time.Millisecond
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
	if strings.TrimSpace(opts.HTTPURL) == "" {
		log.Fatalf("tron http url is required")
	}
	if strings.TrimSpace(opts.WSSURL) == "" {
		log.Printf("tron wss url is empty, backup sync will run in http polling mode only")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := database.NewMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatalf("connect mysql failed: %v", err)
	}
	defer db.Close()

	repo := repository.New(db)
	if err := alignBackupSyncCursor(ctx, repo, opts); err != nil {
		log.Fatalf("align backup sync cursor failed: %v", err)
	}
	cache := service.NewAddressCache(repo)
	tronClient := tron.NewClient(opts.HTTPURL, opts.WSSURL, opts.USDTContract, opts.MinRequestInterval)
	balanceService := service.NewBalanceService(tronClient, repo, cache)
	scanner := service.NewScannerWithSyncKey(
		tronClient,
		repo,
		cache,
		balanceService,
		nil,
		opts.StartBlock,
		opts.TXWorkers,
		opts.BlockSource,
		opts.SyncKey,
	)
	scanner.EnableFullBalanceRefreshOnTransfer(true)

	log.Printf("starting tron wss backup block sync: sync_key=%s source=%s http=%s wss=%s start_block=%d tx_workers=%d",
		opts.SyncKey, opts.BlockSource, opts.HTTPURL, maskEndpoint(opts.WSSURL), opts.StartBlock, opts.TXWorkers)
	log.Printf("note: this task uses an independent sync cursor and does not change the http main service block sync flow")
	log.Printf("note: when a watched address hits any TRX/USDT transfer, this task refreshes both TRX and USDT balances for that address")
	log.Printf("note: backup sync min request interval=%s (target <= 50 RPS)", opts.MinRequestInterval)

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
	if strings.TrimSpace(opts.WSSURL) != "" {
		group.Go(func() error {
			return runWSSLoop(groupCtx, tronClient, scanner)
		})
	}

	if err := group.Wait(); err != nil && err != context.Canceled {
		log.Fatalf("tron wss backup block sync stopped with error: %v", err)
	}
	log.Printf("tron wss backup block sync stopped")
}

type syncOptions struct {
	HTTPURL            string
	WSSURL             string
	USDTContract       string
	BlockSource        string
	MainSyncKey        string
	SyncKey            string
	StartBlock         int64
	TXWorkers          int
	MinRequestInterval time.Duration
}

func resolveOptions(cfg *config.Config) syncOptions {
	httpURL := firstNonEmptyEnv("TRON_WSS_SYNC_HTTP_URL", cfg.QuickNode.HTTPURL)
	wssURL := firstNonEmptyEnv("TRON_WSS_SYNC_WSS_URL", cfg.QuickNode.WSSURL)
	usdtContract := firstNonEmptyEnv("TRON_WSS_SYNC_USDT_CONTRACT", cfg.QuickNode.USDT)
	blockSource := firstNonEmptyEnv("TRON_WSS_SYNC_BLOCK_SOURCE", cfg.TronBlockSource())
	if !strings.EqualFold(strings.TrimSpace(blockSource), "solid") {
		blockSource = "head"
	} else {
		blockSource = "solid"
	}
	mainSyncKey := "tron_head_scanner"
	if blockSource == "solid" {
		mainSyncKey = "tron_solid_scanner"
	}
	syncKey := strings.TrimSpace(os.Getenv("TRON_WSS_SYNC_KEY"))
	if syncKey == "" {
		syncKey = fmt.Sprintf("tron_wss_backup_%s_scanner", blockSource)
	}
	startBlock := cfg.Watcher.StartBlock
	if value := strings.TrimSpace(os.Getenv("TRON_WSS_SYNC_START_BLOCK")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			startBlock = parsed
		}
	}
	txWorkers := cfg.Watcher.TXWorkers
	if value := strings.TrimSpace(os.Getenv("TRON_WSS_SYNC_TX_WORKERS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			txWorkers = parsed
		}
	}
	minInterval := cfg.QuickNodeMinRequestInterval()
	if minInterval < defaultBackupMinRequestDelay {
		minInterval = defaultBackupMinRequestDelay
	}
	if value := strings.TrimSpace(os.Getenv("TRON_WSS_SYNC_MIN_REQUEST_INTERVAL_MS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			minInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	return syncOptions{
		HTTPURL:            strings.TrimSpace(httpURL),
		WSSURL:             strings.TrimSpace(wssURL),
		USDTContract:       strings.TrimSpace(usdtContract),
		BlockSource:        blockSource,
		MainSyncKey:        mainSyncKey,
		SyncKey:            syncKey,
		StartBlock:         startBlock,
		TXWorkers:          txWorkers,
		MinRequestInterval: minInterval,
	}
}

func alignBackupSyncCursor(ctx context.Context, repo *repository.DB, opts syncOptions) error {
	if repo == nil {
		return fmt.Errorf("repository is nil")
	}
	if strings.TrimSpace(opts.SyncKey) == "" {
		return fmt.Errorf("backup sync key is empty")
	}

	backupBlock, exists, err := repo.GetLastBlock(ctx, opts.SyncKey)
	if err != nil {
		return fmt.Errorf("load backup sync key %s: %w", opts.SyncKey, err)
	}
	if exists {
		log.Printf("backup sync cursor already initialized: sync_key=%s block=%d", opts.SyncKey, backupBlock)
		return nil
	}

	mainBlock, mainExists, err := repo.GetLastBlock(ctx, opts.MainSyncKey)
	if err != nil {
		return fmt.Errorf("load main sync key %s: %w", opts.MainSyncKey, err)
	}
	if !mainExists {
		log.Printf("main sync cursor not found, backup sync will use default init flow: main_sync_key=%s start_block=%d", opts.MainSyncKey, opts.StartBlock)
		return nil
	}

	if err := repo.SaveLastBlock(ctx, opts.SyncKey, mainBlock); err != nil {
		return fmt.Errorf("init backup sync key %s from %s=%d: %w", opts.SyncKey, opts.MainSyncKey, mainBlock, err)
	}
	log.Printf("backup sync cursor initialized from main sync cursor: backup_sync_key=%s main_sync_key=%s block=%d", opts.SyncKey, opts.MainSyncKey, mainBlock)
	return nil
}

func runWSSLoop(ctx context.Context, client *tron.Client, scanner *service.Scanner) error {
	for {
		log.Printf("tron wss listener connecting")
		err := client.SubscribeNewHeads(ctx, func() {
			scanner.Trigger()
		})
		if err == nil || err == context.Canceled {
			return err
		}

		log.Printf("tron wss listener stopped, retry after %s: %v", wsRetryDelay, err)
		timer := time.NewTimer(wsRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
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
			key, value, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, exists := os.LookupEnv(key); exists {
				continue
			}
			value = strings.TrimSpace(value)
			value = strings.Trim(value, `"'`)
			_ = os.Setenv(key, value)
		}

		if err := scanner.Err(); err != nil {
			log.Printf("load .env failed: path=%s err=%v", path, err)
			return
		}
		log.Printf("loaded env file: %s", path)
		return
	}
}

func maskEndpoint(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 24 {
		return value
	}
	return value[:20] + "..."
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
