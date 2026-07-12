package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
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
	"tron_watcher/internal/service"
)

const (
	defaultPrivateKeyFile = "configs/bsc_gas_private_key.txt"
	defaultDrawCount      = 250
)

var defaultTransferAmountBNB = decimal.RequireFromString("0.002")

type bscWatchAddress struct {
	ID      int64
	Address string
}

func main() {
	logger := service.BSCLogger()

	cfgPath := os.Getenv("TRON_WATCHER_CONFIG")
	if cfgPath == "" {
		cfgPath = defaultConfigPath()
	}

	privateKeyFile := flag.String("private-key-file", defaultPrivateKeyFile, "path to BSC funding private key file")
	amountFlag := flag.String("amount", defaultTransferAmountBNB.String(), "fixed BNB amount to transfer to one random active bsc_watch_addresses row")
	drawCount := flag.Int("draw-count", defaultDrawCount, "total random draws from active bsc_watch_addresses")
	previewOnly := flag.Bool("preview-only", true, "only print random draw results without sending transactions")
	flag.Parse()

	transferAmountBNB, err := decimal.NewFromString(strings.TrimSpace(*amountFlag))
	if err != nil {
		logger.Fatalf("parse amount failed: %v", err)
	}
	if !transferAmountBNB.IsPositive() {
		logger.Fatalf("amount must be positive")
	}
	if *drawCount <= 0 {
		logger.Fatalf("draw-count must be positive")
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Fatalf("load config failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := database.NewMySQL(ctx, cfg.MySQL)
	if err != nil {
		logger.Fatalf("connect mysql failed: %v", err)
	}
	defer db.Close()

	repo := repository.New(db)

	addresses, err := loadActiveBSCWatchAddresses(ctx, repo)
	if err != nil {
		logger.Fatalf("load bsc watch addresses failed: %v", err)
	}
	if len(addresses) == 0 {
		logger.Printf("no active bsc watch addresses found")
		return
	}

	if *previewOnly {
		logger.Printf("bsc random transfer preview started: mode=preview_only candidates=%d draw_count=%d allow_repeat=true amount_bnb=%s", len(addresses), *drawCount, transferAmountBNB.StringFixed(3))
		for drawIndex := 1; drawIndex <= *drawCount; drawIndex++ {
			select {
			case <-ctx.Done():
				logger.Printf("bsc random transfer preview interrupted: draw=%d/%d err=%v", drawIndex, *drawCount, ctx.Err())
				return
			default:
			}

			target, err := pickRandomAddress(addresses)
			if err != nil {
				logger.Fatalf("pick random bsc watch address failed: draw=%d/%d err=%v", drawIndex, *drawCount, err)
			}
			logger.Printf("bsc random transfer preview draw selected: draw=%d/%d id=%d address=%s", drawIndex, *drawCount, target.ID, target.Address)
		}
		logger.Printf("bsc random transfer preview finished: draw_count=%d candidates=%d", *drawCount, len(addresses))
		return
	}

	bscClient := bsc.NewClient(cfg.BSC.RPCHTTPURL, cfg.BSC.RPCWSSURL, cfg.BSC.USDTContract)
	bscClient.SetMinRequestInterval(cfg.BSCMinRequestInterval())

	privateKey, fromAddress, keySource, err := resolveBSCPrivateKey(cfg.BSC.GasTransferPrivateKey, *privateKeyFile)
	if err != nil {
		logger.Fatalf("load bsc private key failed: %v", err)
	}

	chainIDCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	chainID, err := bscClient.ChainID(chainIDCtx)
	cancel()
	if err != nil {
		logger.Fatalf("load bsc chain id failed: %v", err)
	}

	logger.Printf("bsc random transfer started: from=%s key_source=%s amount_bnb=%s candidates=%d draw_count=%d", fromAddress, keySource, transferAmountBNB.StringFixed(3), len(addresses), *drawCount)

	successCount := 0
	failCount := 0
	for drawIndex := 1; drawIndex <= *drawCount; drawIndex++ {
		select {
		case <-ctx.Done():
			logger.Printf("bsc random transfer interrupted before draw: draw=%d/%d success=%d failed=%d err=%v", drawIndex, *drawCount, successCount, failCount, ctx.Err())
			return
		default:
		}

		target, err := pickRandomAddress(addresses)
		if err != nil {
			logger.Fatalf("pick random bsc watch address failed: draw=%d/%d err=%v", drawIndex, *drawCount, err)
		}

		logger.Printf("bsc random transfer draw selected: draw=%d/%d id=%d address=%s", drawIndex, *drawCount, target.ID, target.Address)
		transferCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txHash, err := sendBNBWithNonce(transferCtx, bscClient, privateKey, fromAddress, target.Address, transferAmountBNB, chainID)
		cancel()
		if err != nil {
			failCount++
			logger.Printf("bsc random transfer failed: draw=%d/%d id=%d address=%s err=%v", drawIndex, *drawCount, target.ID, target.Address, err)
			_ = insertBSCTransferLog(ctx, repo, target.Address, fromAddress, transferAmountBNB.StringFixed(3), "", "", keySource, "FAILED", map[string]any{
				"draw_index":    drawIndex,
				"draw_count":    *drawCount,
				"id":            target.ID,
				"from_address":  fromAddress,
				"transfer_bnb":  transferAmountBNB.StringFixed(3),
				"mode":          "random_draw",
				"key_source":    keySource,
				"error_message": err.Error(),
			}, err.Error())
			continue
		}

		successCount++
		logger.Printf("bsc random transfer success: draw=%d/%d id=%d address=%s tx_hash=%s amount_bnb=%s", drawIndex, *drawCount, target.ID, target.Address, txHash, transferAmountBNB.StringFixed(3))
		_ = insertBSCTransferLog(ctx, repo, target.Address, fromAddress, transferAmountBNB.StringFixed(3), txHash, "", keySource, "SUCCESS", map[string]any{
			"draw_index":   drawIndex,
			"draw_count":   *drawCount,
			"id":           target.ID,
			"from_address": fromAddress,
			"transfer_bnb": transferAmountBNB.StringFixed(3),
			"mode":         "random_draw",
			"tx_hash":      txHash,
			"key_source":   keySource,
		}, "")
		logger.Printf("bsc random transfer cooldown start: draw=%d/%d id=%d address=%s wait=1s", drawIndex, *drawCount, target.ID, target.Address)
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			logger.Printf("bsc random transfer interrupted during cooldown: draw=%d/%d id=%d address=%s success=%d failed=%d err=%v", drawIndex, *drawCount, target.ID, target.Address, successCount, failCount, ctx.Err())
			return
		case <-timer.C:
		}
	}
	logger.Printf("bsc random transfer finished: draw_count=%d success=%d failed=%d amount_bnb=%s", *drawCount, successCount, failCount, transferAmountBNB.StringFixed(3))
}

func sendBNBWithNonce(ctx context.Context, client *bsc.Client, privateKey *ecdsa.PrivateKey, fromAddress string, toAddress string, amount decimal.Decimal, chainID *big.Int) (string, error) {
	if client == nil {
		return "", fmt.Errorf("bsc client is required")
	}

	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return "", fmt.Errorf("get nonce: %w", err)
	}
	gasPrice, err := client.GasPrice(ctx)
	if err != nil {
		return "", fmt.Errorf("get gas price: %w", err)
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

func pickRandomAddress(addresses []bscWatchAddress) (bscWatchAddress, error) {
	if len(addresses) == 0 {
		return bscWatchAddress{}, fmt.Errorf("empty bsc watch addresses")
	}
	n, err := crand.Int(crand.Reader, big.NewInt(int64(len(addresses))))
	if err != nil {
		return bscWatchAddress{}, fmt.Errorf("generate random index: %w", err)
	}
	return addresses[n.Int64()], nil
}

func loadActiveBSCWatchAddresses(ctx context.Context, repo *repository.DB) ([]bscWatchAddress, error) {
	rows, err := repo.QueryContext(ctx, `
		SELECT id, LOWER(address)
		FROM bsc_watch_addresses
		WHERE status = 1
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

func insertBSCTransferLog(ctx context.Context, repo *repository.DB, address string, fromAddress string, amountBNB string, txHash string, responseBody string, keySource string, status string, payload map[string]any, errMessage string) error {
	if repo == nil {
		return nil
	}
	body := []byte(strings.TrimSpace(responseBody))
	if len(body) == 0 {
		marshalBody, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			body = []byte(fmt.Sprintf(`{"marshal_error":%q}`, marshalErr.Error()))
		} else {
			body = marshalBody
		}
	}
	return repo.InsertBSCGasTopupLog(ctx, repository.BSCGasTopupLog{
		Address:      strings.ToLower(strings.TrimSpace(address)),
		FromAddress:  strings.ToLower(strings.TrimSpace(fromAddress)),
		AmountBNB:    strings.TrimSpace(amountBNB),
		CurrentBNB:   "",
		CurrentUSDT:  "",
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
