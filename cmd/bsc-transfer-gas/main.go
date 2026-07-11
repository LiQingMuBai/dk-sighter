package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"

	"tron_watcher/internal/bsc"
	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
	"tron_watcher/internal/repository"
)

const (
	defaultPrivateKeyFile = "configs/bsc_gas_private_key.txt"
	transferDelay         = 5 * time.Second
)

var (
	transferAmountBNB = decimal.RequireFromString("0.001")
	skipBalanceBNB    = decimal.RequireFromString("0.001")
	minUSDTBalance    = decimal.RequireFromString("10")
)

type bscWatchAddress struct {
	ID      int64
	Address string
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	cfgPath := os.Getenv("TRON_WATCHER_CONFIG")
	if cfgPath == "" {
		cfgPath = defaultConfigPath()
	}

	privateKeyFile := flag.String("private-key-file", defaultPrivateKeyFile, "path to BSC funding private key file")
	flag.Parse()

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
	bscClient := bsc.NewClient(cfg.BSC.RPCHTTPURL, cfg.BSC.RPCWSSURL, cfg.BSC.USDTContract)
	bscClient.SetMinRequestInterval(cfg.BSCMinRequestInterval())

	privateKey, fromAddress, keySource, err := resolveBSCPrivateKey(cfg.BSC.GasTransferPrivateKey, *privateKeyFile)
	if err != nil {
		log.Fatalf("load bsc private key failed: %v", err)
	}
	log.Printf("bsc gas transfer started: from=%s key_source=%s", fromAddress, keySource)

	addresses, err := loadActiveBSCWatchAddresses(ctx, repo)
	if err != nil {
		log.Fatalf("load bsc watch addresses failed: %v", err)
	}
	if len(addresses) == 0 {
		log.Printf("no active bsc watch addresses found")
		return
	}

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

		balanceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		balance, err := bscClient.GetBNBBalance(balanceCtx, item.Address)
		cancel()
		if err != nil {
			failCount++
			log.Printf("query bnb balance failed: id=%d address=%s err=%v", item.ID, item.Address, err)
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), "", "", "", keySource, "FAILED", map[string]any{
				"id":            item.ID,
				"step":          "get_balance",
				"from_address":  fromAddress,
				"transfer_bnb":  transferAmountBNB.StringFixed(3),
				"current_bnb":   "",
				"key_source":    keySource,
				"error_message": err.Error(),
			}, err.Error())
			continue
		}

		usdtCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		usdtBalance, err := bscClient.GetUSDTBalance(usdtCtx, item.Address)
		cancel()
		if err != nil {
			failCount++
			log.Printf("query usdt balance failed: id=%d address=%s err=%v", item.ID, item.Address, err)
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), balance.StringFixed(18), "", "", keySource, "FAILED", map[string]any{
				"id":            item.ID,
				"step":          "get_usdt_balance",
				"from_address":  fromAddress,
				"transfer_bnb":  transferAmountBNB.StringFixed(3),
				"current_bnb":   balance.StringFixed(18),
				"current_usdt":  "",
				"key_source":    keySource,
				"error_message": err.Error(),
			}, err.Error())
			continue
		}

		if !usdtBalance.GreaterThan(minUSDTBalance) {
			skipCount++
			log.Printf("skip gas transfer: id=%d address=%s usdt_balance=%s <= %s", item.ID, item.Address, usdtBalance.StringFixed(6), minUSDTBalance.StringFixed(0))
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), balance.StringFixed(18), usdtBalance.StringFixed(6), "", keySource, "SKIPPED", map[string]any{
				"id":           item.ID,
				"from_address": fromAddress,
				"transfer_bnb": transferAmountBNB.StringFixed(3),
				"current_bnb":  balance.StringFixed(18),
				"current_usdt": usdtBalance.StringFixed(6),
				"reason":       "usdt_not_above_threshold",
				"key_source":   keySource,
			}, "")
			continue
		}

		if balance.GreaterThan(skipBalanceBNB) {
			skipCount++
			log.Printf("skip gas transfer: id=%d address=%s bnb_balance=%s > %s", item.ID, item.Address, balance.StringFixed(18), skipBalanceBNB.StringFixed(3))
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), balance.StringFixed(18), usdtBalance.StringFixed(6), "", keySource, "SKIPPED", map[string]any{
				"id":           item.ID,
				"from_address": fromAddress,
				"transfer_bnb": transferAmountBNB.StringFixed(3),
				"current_bnb":  balance.StringFixed(18),
				"current_usdt": usdtBalance.StringFixed(6),
				"reason":       "balance_above_threshold",
				"key_source":   keySource,
			}, "")
			continue
		}

		transferCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txHash, err := sendBNB(transferCtx, bscClient, privateKey, fromAddress, item.Address, transferAmountBNB)
		cancel()
		if err != nil {
			failCount++
			log.Printf("transfer gas failed: id=%d address=%s err=%v", item.ID, item.Address, err)
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), balance.StringFixed(18), usdtBalance.StringFixed(6), "", keySource, "FAILED", map[string]any{
				"id":           item.ID,
				"from_address": fromAddress,
				"transfer_bnb": transferAmountBNB.StringFixed(3),
				"current_bnb":  balance.StringFixed(18),
				"current_usdt": usdtBalance.StringFixed(6),
				"key_source":   keySource,
			}, err.Error())
		} else {
			successCount++
			log.Printf("transfer gas success: id=%d address=%s tx_hash=%s amount_bnb=%s", item.ID, item.Address, txHash, transferAmountBNB.StringFixed(3))
			_ = insertBSCGasLog(ctx, repo, item.Address, fromAddress, transferAmountBNB.StringFixed(3), balance.StringFixed(18), usdtBalance.StringFixed(6), txHash, keySource, "SUCCESS", map[string]any{
				"id":           item.ID,
				"from_address": fromAddress,
				"transfer_bnb": transferAmountBNB.StringFixed(3),
				"current_bnb":  balance.StringFixed(18),
				"current_usdt": usdtBalance.StringFixed(6),
				"tx_hash":      txHash,
				"key_source":   keySource,
			}, "")
		}

		timer := time.NewTimer(transferDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			log.Printf("context canceled during transfer delay, stop processing")
			return
		case <-timer.C:
		}
	}

	log.Printf("bsc gas transfer finished: total=%d success=%d skipped=%d failed=%d", len(addresses), successCount, skipCount, failCount)
}

func sendBNB(ctx context.Context, client *bsc.Client, privateKey *ecdsa.PrivateKey, fromAddress string, toAddress string, amount decimal.Decimal) (string, error) {
	if client == nil {
		return "", fmt.Errorf("bsc client is required")
	}

	gasPrice, err := client.GasPrice(ctx)
	if err != nil {
		return "", fmt.Errorf("get gas price: %w", err)
	}
	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return "", fmt.Errorf("get nonce: %w", err)
	}
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return "", fmt.Errorf("get chain id: %w", err)
	}

	amountWei, err := decimalToTokenUnits(amount, 18)
	if err != nil {
		return "", fmt.Errorf("convert amount to wei: %w", err)
	}
	to := common.HexToAddress(toAddress)

	callObj := map[string]any{
		"from":  fromAddress,
		"to":    toAddress,
		"value": "0x" + amountWei.Text(16),
	}
	gasLimit, err := client.EstimateGas(ctx, callObj)
	if err != nil {
		return "", fmt.Errorf("estimate gas: %w", err)
	}
	gasLimit = gasLimit + gasLimit/5 + 5_000

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    amountWei,
		Gas:      gasLimit,
		GasPrice: gasPrice,
	})
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return "", fmt.Errorf("sign bsc tx: %w", err)
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal bsc tx: %w", err)
	}
	txHash, err := client.SendRawTransaction(ctx, hex.EncodeToString(rawTx))
	if err != nil {
		return "", fmt.Errorf("send raw transaction: %w", err)
	}
	return txHash, nil
}

func loadActiveBSCWatchAddresses(ctx context.Context, repo *repository.DB) ([]bscWatchAddress, error) {
	rows, err := repo.QueryContext(ctx, `
		SELECT id, LOWER(address)
		FROM bsc_watch_addresses
		WHERE status = 1
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query bsc_watch_addresses: %w", err)
	}
	defer rows.Close()

	result := make([]bscWatchAddress, 0)
	for rows.Next() {
		var item bscWatchAddress
		if err := rows.Scan(&item.ID, &item.Address); err != nil {
			return nil, fmt.Errorf("scan bsc_watch_address: %w", err)
		}
		item.Address = strings.ToLower(strings.TrimSpace(item.Address))
		if item.Address == "" {
			continue
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func resolveBSCPrivateKey(configValue string, path string) (*ecdsa.PrivateKey, string, string, error) {
	if strings.TrimSpace(configValue) != "" {
		privateKey, fromAddress, err := parseBSCPrivateKey(configValue)
		if err != nil {
			return nil, "", "", fmt.Errorf("parse bsc private key from config: %w", err)
		}
		return privateKey, fromAddress, "config.bsc.gas_transfer_private_key", nil
	}

	privateKey, fromAddress, err := loadBSCPrivateKeyFromFile(path)
	if err != nil {
		return nil, "", "", err
	}
	return privateKey, fromAddress, path, nil
}

func loadBSCPrivateKeyFromFile(path string) (*ecdsa.PrivateKey, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("open private key file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		privateKey, fromAddress, err := parseBSCPrivateKey(line)
		if err != nil {
			return nil, "", fmt.Errorf("parse private key: %w", err)
		}
		return privateKey, fromAddress, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("read private key file: %w", err)
	}
	return nil, "", fmt.Errorf("no private key found in file")
}

func parseBSCPrivateKey(value string) (*ecdsa.PrivateKey, string, error) {
	keyHex := strings.TrimSpace(strings.TrimPrefix(value, "0x"))
	if keyHex == "" {
		return nil, "", fmt.Errorf("empty private key")
	}
	privateKey, err := crypto.HexToECDSA(keyHex)
	if err != nil {
		return nil, "", err
	}
	fromAddress := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	return privateKey, strings.ToLower(fromAddress), nil
}

func insertBSCGasLog(ctx context.Context, repo *repository.DB, address string, fromAddress string, amountBNB string, currentBNB string, currentUSDT string, txHash string, keySource string, status string, payload map[string]any, errMessage string) error {
	if repo == nil {
		return nil
	}
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		body = []byte(fmt.Sprintf(`{"marshal_error":%q}`, marshalErr.Error()))
	}
	return repo.InsertBSCGasTopupLog(ctx, repository.BSCGasTopupLog{
		Address:      strings.ToLower(strings.TrimSpace(address)),
		FromAddress:  strings.ToLower(strings.TrimSpace(fromAddress)),
		AmountBNB:    strings.TrimSpace(amountBNB),
		CurrentBNB:   strings.TrimSpace(currentBNB),
		CurrentUSDT:  strings.TrimSpace(currentUSDT),
		TxHash:       strings.TrimSpace(txHash),
		KeySource:    strings.TrimSpace(keySource),
		Status:       strings.ToUpper(strings.TrimSpace(status)),
		ResponseBody: string(body),
		ErrorMessage: strings.TrimSpace(errMessage),
	})
}

func decimalToTokenUnits(amount decimal.Decimal, decimals int32) (*big.Int, error) {
	if decimals < 0 {
		return nil, fmt.Errorf("invalid decimals")
	}
	if amount.IsNegative() {
		return nil, fmt.Errorf("amount must be positive")
	}
	scale := decimal.NewFromInt(1).Shift(decimals)
	value := amount.Mul(scale)
	if !value.Equal(value.Truncate(0)) {
		return nil, fmt.Errorf("amount has too many decimal places")
	}
	return value.BigInt(), nil
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
