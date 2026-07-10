package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shopspring/decimal"

	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/service"
	"tron_watcher/internal/tron"
)

const activateDelay = 5 * time.Second

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	cfgPath := os.Getenv("TRON_WATCHER_CONFIG")
	if cfgPath == "" {
		cfgPath = defaultConfigPath()
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := database.NewMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatalf("connect mysql failed: %v", err)
	}
	defer db.Close()

	repo := repository.New(db)
	tronClient := tron.NewClient(cfg.QuickNode.HTTPURL, cfg.QuickNode.WSSURL, cfg.QuickNode.USDT, cfg.QuickNodeMinRequestInterval())
	activator, err := service.NewTronAddressActivator(tronClient, repo, cfg.TronActivator)
	if err != nil {
		log.Fatalf("init tron activator failed: %v", err)
	}
	if activator == nil {
		log.Fatalf("tron activator is disabled, please set tron_activator.enabled=true")
	}

	addresses, err := repo.LoadActiveAddresses(ctx)
	if err != nil {
		log.Fatalf("load watch addresses failed: %v", err)
	}
	if len(addresses) == 0 {
		log.Printf("no active watch addresses found")
		return
	}

	log.Printf("loaded %d active watch addresses", len(addresses))

	threshold := decimal.NewFromInt(1)
	successCount := 0
	skipCount := 0
	failCount := 0

	for _, item := range addresses {
		select {
		case <-ctx.Done():
			log.Printf("context canceled, stop processing")
			return
		default:
		}

		address := item.AddressBase58
		addressHex, err := tron.Base58ToHex(address)
		if err != nil {
			failCount++
			log.Printf("convert address to hex failed: address=%s err=%v", address, err)
			continue
		}

		balanceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, trxBalance, err := tronClient.GetAccountState(balanceCtx, addressHex)
		cancel()
		if err != nil {
			failCount++
			log.Printf("query trx balance failed: address=%s err=%v", address, err)
			continue
		}

		if trxBalance.GreaterThanOrEqual(threshold) {
			skipCount++
			log.Printf("skip activate: address=%s trx_balance=%s >= 1", address, trxBalance.String())
			continue
		}

		activateCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txID, err := activator.Activate(activateCtx, address)
		cancel()
		if err != nil {
			failCount++
			log.Printf("activate address failed: address=%s err=%v", address, err)
			continue
		}

		successCount++
		log.Printf("activate address success: address=%s txid=%s", address, txID)

		timer := time.NewTimer(activateDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			log.Printf("context canceled during wait, stop processing")
			return
		case <-timer.C:
		}
	}

	log.Printf("activation finished: total=%d success=%d skipped=%d failed=%d", len(addresses), successCount, skipCount, failCount)
}

func defaultConfigPath() string {
	candidates := []string{
		"configs/config.yaml",
		"config.yaml",
		//"configs/config.example.yaml",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "configs/config.example.yaml"
}
