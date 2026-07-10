package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sort"
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
	activatorPrivateKeys := []string{
		// Fill your Tron private keys here. Each record ID is hashed to one fixed signer.
		// "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
	
	}

	activator, err := service.NewTronAddressActivatorWithPrivateKeys(tronClient, repo, activatorPrivateKeys, 64)
	if err != nil {
		log.Fatalf("init tron activator failed: %v", err)
	}

	addresses, err := repo.LoadActiveAddresses(ctx)
	if err != nil {
		log.Fatalf("load watch addresses failed: %v", err)
	}
	if len(addresses) == 0 {
		log.Printf("no active watch addresses found")
		return
	}
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].ID > addresses[j].ID
	})

	log.Printf("loaded %d active watch addresses", len(addresses))

	threshold := decimal.NewFromInt(1)
	successCount := 0
	skipCount := 0
	failCount := 0
	lastActivatedSignerIndex := -1

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

		signerIndex, err := activator.SignerIndexByRecordID(item.ID)
		if err != nil {
			failCount++
			log.Printf("resolve signer failed: id=%d address=%s err=%v", item.ID, address, err)
			continue
		}

		if lastActivatedSignerIndex == signerIndex {
			log.Printf("wait before activate: id=%d address=%s signer_index=%d delay=%s", item.ID, address, signerIndex, activateDelay)
			timer := time.NewTimer(activateDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				log.Printf("context canceled during wait, stop processing")
				return
			case <-timer.C:
			}
		}

		activateCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txID, err := activator.ActivateByRecordID(activateCtx, item.ID, address)
		cancel()
		if err != nil {
			failCount++
			log.Printf("activate address failed: id=%d address=%s err=%v", item.ID, address, err)
			continue
		}

		successCount++
		lastActivatedSignerIndex = signerIndex
		log.Printf("activate address success: id=%d address=%s txid=%s", item.ID, address, txID)
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
