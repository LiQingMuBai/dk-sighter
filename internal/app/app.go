package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"tron_watcher/infrastructure"
	"tron_watcher/internal/bsc"
	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
	"tron_watcher/internal/hdwallet"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/service"
	"tron_watcher/internal/tron"
	"tron_watcher/internal/web"
)

type App struct {
	cfg                 *config.Config
	db                  *sql.DB
	cache               *service.AddressCache
	scanner             *service.Scanner
	balances            *service.BalanceService
	scheduledBalances   *service.BalanceService
	notifier            service.TransferNotifier
	tronClient          *tron.Client
	bscClient           *bsc.Client
	bscCache            *service.BSCAddressCache
	bscScanner          *service.BSCScanner
	scheduledBSCScanner *service.BSCScanner
	bscEnabled          bool
	webServer           *web.Server
	wallets             *hdwallet.Service
	activator           *service.TronAddressActivator
}

func New(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	if listen := strings.TrimSpace(os.Getenv("TRON_WATCHER_WEB_LISTEN")); listen != "" {
		cfg.Web.Listen = listen
	}
	if mode := strings.TrimSpace(os.Getenv("TRON_WATCHER_APP_MODE")); mode != "" {
		cfg.App.Mode = mode
	}

	tronClient := tron.NewClient(cfg.QuickNode.HTTPURL, cfg.QuickNode.WSSURL, cfg.QuickNode.USDT, cfg.QuickNodeMinRequestInterval())
	bscEnabled := isBSCEnabled(cfg.BSC.RPCHTTPURL)
	var bscClient *bsc.Client
	if bscEnabled {
		bscClient = bsc.NewClient(cfg.BSC.RPCHTTPURL, cfg.BSC.RPCWSSURL, cfg.BSC.USDTContract)
	}

	if isHDWalletMode(cfg) {
		dataDir := resolveDataDir()
		walletService := hdwallet.NewService(dataDir, cfg.App.HDWalletCount, tronClient, bscClient)
		ctx := context.Background()
		db, err := database.NewMySQL(ctx, cfg.MySQL)
		if err != nil {
			return nil, err
		}
		repo := repository.New(db)
		cache := service.NewAddressCache(repo)
		cache.ConfigureSource(repository.HDWalletSource)
		balanceService := service.NewBalanceService(tronClient, repo, cache)
		notifier := service.NewMultiTransferNotifier(
			service.NewTelegramNotifier(cfg.Telegram),
			service.NewCallbackNotifier(cfg.Callback),
		)
		scanner := service.NewScanner(tronClient, repo, cache, balanceService, notifier, cfg.Watcher.StartBlock, cfg.Watcher.TXWorkers, cfg.TronBlockSource())

		var bscCache *service.BSCAddressCache
		var bscScanner *service.BSCScanner
		if bscEnabled {
			bscCache = service.NewBSCAddressCache(repo)
			bscCache.ConfigureSource(repository.HDWalletSource)
			bscScanner = service.NewBSCScanner(bscClient, repo, bscCache, notifier, cfg.BSC.StartBlock, cfg.BSC.Confirmations)
		}

		energyProviders := buildEnergyProviders(cfg)
		activator, err := service.NewTronAddressActivator(tronClient, repo, cfg.TronActivator)
		if err != nil {
			return nil, err
		}
		walletService.ConfigureEnergyProviders(energyProviders, cfg.Energy.Provider)
		walletService.ConfigureTronActivator(activator)
		walletService.ConfigureBSCGasTopupPrivateKey(cfg.BSC.GasTransferPrivateKey)
		walletService.ConfigureRepository(repo, repository.HDWalletSource)
		webServer, err := web.NewHDWalletServer(cfg.Web, walletService, energyProviders, cfg.Energy.Provider)
		if err != nil {
			return nil, err
		}
		return &App{
			cfg:        cfg,
			db:         db,
			cache:      cache,
			scanner:    scanner,
			balances:   balanceService,
			notifier:   notifier,
			tronClient: tronClient,
			bscClient:  bscClient,
			bscCache:   bscCache,
			bscScanner: bscScanner,
			bscEnabled: bscEnabled,
			webServer:  webServer,
			wallets:    walletService,
			activator:  activator,
		}, nil
	}

	ctx := context.Background()
	db, err := database.NewMySQL(ctx, cfg.MySQL)
	if err != nil {
		return nil, err
	}

	repo := repository.New(db)
	cache := service.NewAddressCache(repo)
	balanceService := service.NewBalanceService(tronClient, repo, cache)
	scheduledRefreshTronClient := tron.NewClient(cfg.QuickNode.HTTPURL, cfg.QuickNode.WSSURL, cfg.QuickNode.USDT, cfg.QuickNodeMinRequestInterval())
	scheduledRefreshBalanceService := service.NewBalanceService(scheduledRefreshTronClient, repo, cache)
	manualRefreshTronClient := tron.NewClient(cfg.QuickNode.HTTPURL, cfg.QuickNode.WSSURL, cfg.QuickNode.USDT, manualRefreshMinInterval(cfg.QuickNodeMinRequestInterval()))
	manualRefreshBalanceService := service.NewBalanceService(manualRefreshTronClient, repo, cache)
	notifier := service.NewMultiTransferNotifier(
		service.NewTelegramNotifier(cfg.Telegram),
		service.NewCallbackNotifier(cfg.Callback),
	)
	scanner := service.NewScanner(tronClient, repo, cache, balanceService, notifier, cfg.Watcher.StartBlock, cfg.Watcher.TXWorkers, cfg.TronBlockSource())

	var bscCache *service.BSCAddressCache
	var bscScanner *service.BSCScanner
	var scheduledRefreshBSCScanner *service.BSCScanner
	var manualRefreshBSCScanner *service.BSCScanner
	if bscEnabled {
		bscCache = service.NewBSCAddressCache(repo)
		bscScanner = service.NewBSCScanner(bscClient, repo, bscCache, notifier, cfg.BSC.StartBlock, cfg.BSC.Confirmations)
		scheduledRefreshBSCClient := bsc.NewClient(cfg.BSC.RPCHTTPURL, cfg.BSC.RPCWSSURL, cfg.BSC.USDTContract)
		scheduledRefreshBSCClient.SetMinRequestInterval(cfg.BSCMinRequestInterval())
		scheduledRefreshBSCScanner = service.NewBSCScanner(scheduledRefreshBSCClient, repo, bscCache, notifier, cfg.BSC.StartBlock, cfg.BSC.Confirmations)
		manualRefreshBSCClient := bsc.NewClient(cfg.BSC.RPCHTTPURL, cfg.BSC.RPCWSSURL, cfg.BSC.USDTContract)
		manualRefreshBSCClient.SetMinRequestInterval(manualRefreshMinInterval(cfg.BSCMinRequestInterval()))
		manualRefreshBSCScanner = service.NewBSCScanner(manualRefreshBSCClient, repo, bscCache, notifier, cfg.BSC.StartBlock, cfg.BSC.Confirmations)
	}

	energyProviders := buildEnergyProviders(cfg)
	activator, err := service.NewTronAddressActivator(tronClient, repo, cfg.TronActivator)
	if err != nil {
		return nil, err
	}
	webServer, err := web.NewServer(
		repo,
		cfg.Web,
		cache,
		balanceService,
		manualRefreshBalanceService,
		bscScanner,
		manualRefreshBSCScanner,
		activator,
		bscClient,
		cfg.BSC.GasTransferPrivateKey,
		energyProviders,
		cfg.Energy.Provider,
	)
	if err != nil {
		return nil, err
	}

	return &App{
		cfg:                 cfg,
		db:                  db,
		cache:               cache,
		scanner:             scanner,
		balances:            balanceService,
		scheduledBalances:   scheduledRefreshBalanceService,
		notifier:            notifier,
		tronClient:          tronClient,
		bscClient:           bscClient,
		bscCache:            bscCache,
		bscScanner:          bscScanner,
		scheduledBSCScanner: scheduledRefreshBSCScanner,
		bscEnabled:          bscEnabled,
		webServer:           webServer,
		activator:           activator,
	}, nil
}

func manualRefreshMinInterval(base time.Duration) time.Duration {
	if base < 50*time.Millisecond {
		return 50 * time.Millisecond
	}
	return base * 2
}

func (a *App) Run(ctx context.Context) error {
	if a.db != nil {
		defer a.db.Close()
	}

	group, groupCtx := errgroup.WithContext(ctx)

	if isHDWalletMode(a.cfg) {
		log.Printf("hd wallet mysql mode enabled: addresses, balances, transfers and sync state are stored in mysql")
	}

	if a.cfg.QuickNode.WSSURL == "" {
		log.Printf("websocket disabled, using http polling only")
	}

	group.Go(a.safeGo("address-cache", func() error {
		err := a.cache.Run(groupCtx, a.cfg.AddressReloadInterval())
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}))

	if a.cfg.App.Local {
		log.Printf("local mode enabled, block sync tasks paused")
	} else {
		group.Go(a.safeGo("scanner", func() error {
			err := a.scanner.Run(groupCtx, a.cfg.BlockPollInterval())
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))
	}

	if a.bscEnabled && a.bscCache != nil && a.bscScanner != nil {
		group.Go(a.safeGo("bsc-address-cache", func() error {
			err := a.bscCache.Run(groupCtx, a.cfg.AddressReloadInterval())
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))

		if a.cfg.App.Local {
			log.Printf("local mode enabled, bsc block sync tasks paused")
		} else {
			if a.cfg.BSC.RPCWSSURL == "" {
				log.Printf("bsc websocket disabled, using http polling only")
			}

			group.Go(a.safeGo("bsc-scanner", func() error {
				err := a.bscScanner.Run(groupCtx, a.cfg.BSCBlockPollInterval())
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			}))

			group.Go(a.safeGo("bsc-new-heads-subscriber", func() error {
				err := a.bscClient.SubscribeNewHeads(groupCtx, func() {
					a.bscScanner.Trigger()
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("bsc websocket listener stopped, fallback to polling only: %v", err)
				}
				<-groupCtx.Done()
				return nil
			}))
		}
	} else {
		log.Printf("bsc scanner disabled: rpc_http_url not configured")
	}

	if !a.cfg.App.Local {
		group.Go(a.safeGo("new-heads-subscriber", func() error {
			err := a.tronClient.SubscribeNewHeads(groupCtx, func() {
				a.scanner.Trigger()
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("websocket listener stopped, fallback to polling only: %v", err)
			}
			<-groupCtx.Done()
			return nil
		}))
	}

	group.Go(a.safeGo("web-server", func() error {
		err := a.webServer.Run(groupCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}))

	if a.activator != nil {
		group.Go(a.safeGo("tron-activator", func() error {
			err := a.activator.Run(groupCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))
	}

	if a.notifier != nil {
		group.Go(a.safeGo("telegram-notifier", func() error {
			err := a.notifier.Run(groupCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))
	}

	if isHDWalletMode(a.cfg) {
		log.Printf("hd wallet mode enabled, scheduled balance refresh disabled")
	} else {
		group.Go(a.safeGo("hourly-balance-refresh", func() error {
			err := service.RunHourlyBalanceRefresh(groupCtx, a.scheduledBalances, a.scheduledBSCScanner, a.cfg.TronBlockSource())
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))
	}

	return group.Wait()
}

func isBSCEnabled(rpcURL string) bool {
	trimmed := strings.TrimSpace(rpcURL)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "your-bsc-rpc") {
		return false
	}
	return true
}

func isHDWalletMode(cfg *config.Config) bool {
	return strings.EqualFold(strings.TrimSpace(cfg.App.Mode), "hd_wallet")
}

func resolveDataDir() string {
	if value := strings.TrimSpace(os.Getenv("TRON_WATCHER_DATA_DIR")); value != "" {
		return value
	}
	return filepath.Join("data", "hd_wallet")
}

func buildEnergyProviders(cfg *config.Config) map[string]infrastructure.EnergyOrderProvider {
	return map[string]infrastructure.EnergyOrderProvider{
		"trxfee": infrastructure.NewTrxfeeClient(cfg.Trxfee.URL, cfg.Trxfee.APIKey, cfg.Trxfee.APISecret),
		"catfee": infrastructure.NewCatfeeSafeClient(cfg.Catfee.URL, cfg.Catfee.APIKey, cfg.Catfee.APISecret),
	}
}

func (a *App) safeGo(component string, fn func() error) func() error {
	return func() error {
		restartDelay := time.Second
		for {
			err := func() (runErr error) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic recovered in %s: %v\n%s", component, r, string(debug.Stack()))
						runErr = fmt.Errorf("%s panic recovered: %v", component, r)
					}
				}()
				return fn()
			}()

			if err == nil || errors.Is(err, context.Canceled) {
				return nil
			}

			log.Printf("%s stopped unexpectedly, restart in %s: %v", component, restartDelay, err)
			timer := time.NewTimer(restartDelay)
			<-timer.C

			if restartDelay < 30*time.Second {
				restartDelay *= 2
				if restartDelay > 30*time.Second {
					restartDelay = 30 * time.Second
				}
			}
		}
	}
}
