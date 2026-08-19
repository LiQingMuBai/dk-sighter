package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/bsc"
	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/service"
	"tron_watcher/internal/ushield_trace"
)

const (
	wsRetryDelay                 = 3 * time.Second
	defaultBackupMinRequestDelay = 20 * time.Millisecond
	defaultBackupTriggerInterval = 15 * time.Second
)

type syncOptions struct {
	HTTPURL            string
	WSSURL             string
	MainSyncKey        string
	SyncKey            string
	StartBlock         int64
	Confirmations      int
	FollowBehindBlocks int64
	FastCatchUpLag     int64
	MainStaleDuration  time.Duration
	TriggerInterval    time.Duration
	MinRequestInterval time.Duration
}

func main() {
	service.SetupCmdLogger("bsc-backup-block-sync")

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
		log.Fatalf("bsc backup http url is required")
	}
	if strings.TrimSpace(cfg.BSC.USDTContract) == "" {
		log.Fatalf("bsc backup usdt contract is required")
	}
	if strings.TrimSpace(opts.WSSURL) == "" {
		log.Fatalf("bsc backup wss url is required in wss sync mode")
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

	terminalLogger := service.TaskLogger("bsc-backup-block-sync")

	ushieldTraceSvc, err := ushield_trace.NewService(cfg.UShieldMySQL, "bsc")
	if err != nil {
		log.Fatalf("init ushield trace service failed: %v", err)
	}
	defer ushieldTraceSvc.Close()
	ushieldTraceSvc.SetLogger(terminalLogger)

	tgNotifier := service.NewTelegramNotifier(cfg.Telegram)
	tgNotifier.SetLogger(terminalLogger)

	cache := service.NewBSCAddressCache(repo)
	cache.SetLogger(terminalLogger)

	client := bsc.NewClient(opts.HTTPURL, opts.WSSURL, cfg.BSC.USDTContract)
	client.SetMinRequestInterval(opts.MinRequestInterval)

	scanner := service.NewBSCScannerWithSyncKey(
		client,
		repo,
		cache,
		nil,
		opts.StartBlock,
		opts.Confirmations,
		opts.SyncKey,
		true,
	)
	scanner.SetLogger(terminalLogger)
	scanner.SetDeferBalanceRefreshInCatchUp(true)
	scanner.SetFastCatchUpThreshold(opts.FastCatchUpLag)
	scanner.SetDisableBookkeeping(true)
	scanner.SetUShieldTrace(ushieldTraceSvc)
	scanner.SetTelegramDirectSender(tgNotifier)
	repairOwner := buildBSCRepairOwner(opts.SyncKey)
	var (
		modeMu        sync.Mutex
		lastModeLabel string
	)
	scanner.SetMaxScanBlockResolver(func(ctx context.Context, chainLatest int64) (int64, bool, error) {
		for {
			gap, gapExists, err := repo.GetRepairingSyncGapByOwner(ctx, "bsc", repairOwner)
			if err != nil {
				return 0, false, fmt.Errorf("load owned bsc repairing gap: %w", err)
			}
			if !gapExists {
				gap, gapExists, err = repo.ClaimNextPendingSyncGap(ctx, "bsc", repairOwner)
				if err != nil {
					return 0, false, fmt.Errorf("claim pending bsc sync gap: %w", err)
				}
				if !gapExists {
					break
				}
				log.Printf("bsc backup claimed sync gap: owner=%s gap_id=%d gap_from=%d gap_to=%d", repairOwner, gap.ID, gap.FromBlock, gap.ToBlock)
			}

			backupBlock, backupExists, err := repo.GetLastBlock(ctx, opts.SyncKey)
			if err != nil {
				return 0, false, fmt.Errorf("load backup sync state %s for gap repair: %w", opts.SyncKey, err)
			}
			if backupExists && backupBlock >= gap.ToBlock {
				if err := repo.MarkSyncGapDone(ctx, gap.ID); err != nil {
					return 0, false, fmt.Errorf("mark repaired bsc sync gap done id=%d: %w", gap.ID, err)
				}
				log.Printf("bsc backup gap repair finished: owner=%s gap_id=%d gap_from=%d gap_to=%d backup_block=%d", repairOwner, gap.ID, gap.FromBlock, gap.ToBlock, backupBlock)
				continue
			}

			if !backupExists || backupBlock < gap.FromBlock-1 || backupBlock > gap.ToBlock {
				resetTo := gap.FromBlock - 1
				if resetTo < 0 {
					resetTo = 0
				}
				if err := repo.SaveLastBlock(ctx, opts.SyncKey, resetTo); err != nil {
					return 0, false, fmt.Errorf("reset backup cursor for bsc sync gap id=%d to=%d: %w", gap.ID, resetTo, err)
				}
			}

			logBackupModeChange(&modeMu, &lastModeLabel, "repair-gap", "backup sync is repairing owned gap: owner=%s gap_id=%d gap_from=%d gap_to=%d", repairOwner, gap.ID, gap.FromBlock, gap.ToBlock)
			return gap.ToBlock, true, nil
		}

		hasOpenGap, err := repo.HasOpenSyncGap(ctx, "bsc")
		if err != nil {
			return 0, false, fmt.Errorf("check open bsc sync gaps: %w", err)
		}
		if hasOpenGap {
			logBackupModeChange(&modeMu, &lastModeLabel, "idle-other-repairing", "other backup worker owns current bsc sync gap, skip and stay idle: owner=%s", repairOwner)
			return 0, false, nil
		}

		_, updatedAt, exists, err := repo.GetSyncState(ctx, opts.MainSyncKey)
		if err != nil {
			return 0, false, fmt.Errorf("load main sync state %s: %w", opts.MainSyncKey, err)
		}
		if !exists {
			logBackupModeChange(&modeMu, &lastModeLabel, "takeover-missing-main", "main sync cursor missing, backup sync enters takeover mode")
			return chainLatest, true, nil
		}

		if opts.MainStaleDuration > 0 && !updatedAt.IsZero() && time.Since(updatedAt) > opts.MainStaleDuration {
			logBackupModeChange(&modeMu, &lastModeLabel, "takeover-stale-main", "main sync cursor stale for %s, backup sync enters takeover mode", time.Since(updatedAt).Truncate(time.Second))
			return chainLatest, true, nil
		}

		logBackupModeChange(&modeMu, &lastModeLabel, "idle-no-gap", "main sync cursor active and no gap pending, backup sync stays idle")
		return 0, false, nil
	})
	scanner.SetSkipToLatestOnLag(false)

	log.Printf("starting bsc backup block sync: mode=wss sync_key=%s main_sync_key=%s http=%s wss=%s start_block=%d confirmations=%d main_stale_duration=%s trigger_interval=%s min_request_interval=%s",
		opts.SyncKey, opts.MainSyncKey, opts.HTTPURL, maskEndpoint(opts.WSSURL), opts.StartBlock, opts.Confirmations, opts.MainStaleDuration, opts.TriggerInterval, opts.MinRequestInterval)
	log.Printf("note: this task uses an independent sync cursor and does not change the main bsc block sync flow")
	log.Printf("note: backup sync is driven by bsc websocket newHeads events, not by timer polling")
	log.Printf("note: backup sync also uses a small periodic trigger as a safety net when websocket events are delayed or silent")
	log.Printf("note: backup sync is gap-first: when no skipped main gap exists, it will not actively follow the main cursor")
	log.Printf("note: each backup process only continues its own repairing gap and will claim the next pending gap in order")
	log.Printf("note: if main sync cursor is stale for longer than the configured duration, backup sync will switch to takeover mode and catch up to chain latest")
	log.Printf("note: when the main scanner skips lagging block ranges, backup sync will prioritize repairing pending sync_gaps rows before returning to idle mode")
	log.Printf("note: during each catch-up run, backup sync records transfers first and defers matched address balance refresh until the end of the run")
	log.Printf("note: when scan_lag is greater than %d, backup sync marks the run as fast catch-up mode and switches back automatically after catching up", opts.FastCatchUpLag)
	log.Printf("note: matched BNB/USDT transfers will be written into transfer records, duplicate hashes will be skipped, and BNB/USDT balances will only be updated when on-chain current balance differs from mysql")

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		err := cache.Run(groupCtx, cfg.AddressReloadInterval())
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})
	group.Go(func() error {
		err := ushieldTraceSvc.Run(groupCtx)
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})
	group.Go(func() error {
		err := tgNotifier.Run(groupCtx)
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})
	group.Go(func() error {
		err := scanner.RunTriggered(groupCtx)
		if err != nil && err != context.Canceled {
			return err
		}
		return nil
	})
	group.Go(func() error {
		return runTriggerHeartbeat(groupCtx, scanner, opts.TriggerInterval)
	})
	group.Go(func() error {
		return runWSSLoop(groupCtx, client, scanner)
	})

	if err := group.Wait(); err != nil && err != context.Canceled {
		log.Fatalf("bsc backup block sync stopped with error: %v", err)
	}
	log.Printf("bsc backup block sync stopped")
}

func buildBSCRepairOwner(syncKey string) string {
	syncKey = strings.TrimSpace(syncKey)
	if syncKey == "" {
		syncKey = "bsc_backup_scanner"
	}
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s@%s:%d:%d", syncKey, hostname, os.Getpid(), time.Now().UnixNano())
}

func resolveOptions(cfg *config.Config) syncOptions {
	httpURL := firstNonEmptyEnv("BSC_BACKUP_SYNC_HTTP_URL", cfg.BSCRefreshHTTPURL())
	if httpURL == "" {
		httpURL = strings.TrimSpace(cfg.BSC.RPCHTTPURL)
	}
	wssURL := firstNonEmptyEnv("BSC_BACKUP_SYNC_WSS_URL", cfg.BSCRefreshWSSURL())
	if wssURL == "" {
		wssURL = strings.TrimSpace(cfg.BSC.RPCWSSURL)
	}
	syncKey := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_KEY"))
	if syncKey == "" {
		syncKey = "bsc_backup_scanner"
	}
	mainSyncKey := firstNonEmptyEnv("BSC_BACKUP_MAIN_SYNC_KEY", "bsc_scanner")

	startBlock := cfg.BSC.StartBlock
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_START_BLOCK")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			startBlock = parsed
		}
	}

	confirmations := cfg.BSC.Confirmations
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_CONFIRMATIONS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			confirmations = parsed
		}
	}

	followBehindBlocks := int64(10)
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_FOLLOW_BEHIND_BLOCKS")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil && parsed >= 0 {
			followBehindBlocks = parsed
		}
	}

	fastCatchUpLag := int64(20)
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_FAST_CATCH_UP_LAG")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil && parsed >= 0 {
			fastCatchUpLag = parsed
		}
	}

	mainStaleDuration := 60 * time.Second
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_MAIN_STALE_SECONDS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			mainStaleDuration = time.Duration(parsed) * time.Second
		}
	}

	triggerInterval := defaultBackupTriggerInterval
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_TRIGGER_INTERVAL_SECONDS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			triggerInterval = time.Duration(parsed) * time.Second
		}
	}

	minInterval := cfg.BSCRefreshMinRequestInterval()
	if minInterval < defaultBackupMinRequestDelay {
		minInterval = defaultBackupMinRequestDelay
	}
	if value := strings.TrimSpace(os.Getenv("BSC_BACKUP_SYNC_MIN_REQUEST_INTERVAL_MS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			minInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	return syncOptions{
		HTTPURL:            strings.TrimSpace(httpURL),
		WSSURL:             strings.TrimSpace(wssURL),
		MainSyncKey:        strings.TrimSpace(mainSyncKey),
		SyncKey:            strings.TrimSpace(syncKey),
		StartBlock:         startBlock,
		Confirmations:      confirmations,
		FollowBehindBlocks: followBehindBlocks,
		FastCatchUpLag:     fastCatchUpLag,
		MainStaleDuration:  mainStaleDuration,
		TriggerInterval:    triggerInterval,
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

	backupBlock, backupExists, err := repo.GetLastBlock(ctx, opts.SyncKey)
	if err != nil {
		return fmt.Errorf("load backup sync key %s: %w", opts.SyncKey, err)
	}

	mainBlock, mainExists, err := repo.GetLastBlock(ctx, opts.MainSyncKey)
	if err != nil {
		return fmt.Errorf("load main sync key %s: %w", opts.MainSyncKey, err)
	}
	if !mainExists {
		if backupExists {
			log.Printf("main sync cursor not found, backup sync keeps current cursor: sync_key=%s block=%d", opts.SyncKey, backupBlock)
			return nil
		}
		log.Printf("main sync cursor not found, backup sync will use default init flow: main_sync_key=%s start_block=%d", opts.MainSyncKey, opts.StartBlock)
		return nil
	}

	if !backupExists {
		if err := repo.SaveLastBlock(ctx, opts.SyncKey, mainBlock); err != nil {
			return fmt.Errorf("init backup sync key %s from %s=%d: %w", opts.SyncKey, opts.MainSyncKey, mainBlock, err)
		}
		log.Printf("backup sync cursor initialized from main sync cursor: backup_sync_key=%s main_sync_key=%s main_block=%d init_block=%d", opts.SyncKey, opts.MainSyncKey, mainBlock, mainBlock)
		return nil
	}

	log.Printf("backup sync cursor kept on restart: main_sync_key=%s main_block=%d backup_sync_key=%s backup_block=%d reason=gap_first_keep_current_cursor", opts.MainSyncKey, mainBlock, opts.SyncKey, backupBlock)
	return nil
}

func runWSSLoop(ctx context.Context, client *bsc.Client, scanner *service.BSCScanner) error {
	for {
		log.Printf("bsc backup wss listener connecting")
		err := client.SubscribeNewHeads(ctx, func() {
			scanner.Trigger()
		})
		if err == nil || err == context.Canceled {
			return err
		}

		log.Printf("bsc backup wss listener stopped, retry after %s: %v", wsRetryDelay, err)
		timer := time.NewTimer(wsRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func runTriggerHeartbeat(ctx context.Context, scanner *service.BSCScanner, interval time.Duration) error {
	if scanner == nil || interval <= 0 {
		return nil
	}

	log.Printf("bsc backup trigger heartbeat started: interval=%s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			scanner.Trigger()
		}
	}
}

func logBackupModeChange(mu *sync.Mutex, current *string, next string, format string, args ...any) {
	if mu == nil || current == nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()
	if *current == next {
		return
	}

	*current = next
	log.Printf(format, args...)
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
			if key == "" {
				continue
			}
			if _, exists := os.LookupEnv(key); exists {
				continue
			}
			_ = os.Setenv(key, value)
		}
		if err := scanner.Err(); err != nil {
			log.Printf("load dotenv failed from %s: %v", path, err)
		}
		return
	}
}

func defaultConfigPath() string {
	candidates := []string{
		filepath.Join("configs", "config.yaml"),
		"config.yaml",
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return filepath.Join("configs", "config.yaml")
}

func maskEndpoint(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 16 {
		return value
	}
	return value[:10] + "..." + value[len(value)-6:]
}
