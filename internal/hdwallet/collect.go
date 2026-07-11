package hdwallet

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	gotronTx "github.com/fbsobreira/gotron-sdk/pkg/client/transaction"
	gotronKeys "github.com/fbsobreira/gotron-sdk/pkg/keys"
	"github.com/shopspring/decimal"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const (
	tronTokenPrecision      = int32(6)
	bscTokenPrecision       = int32(18)
	defaultTronFeeLimitSun  = int64(30_000_000)
	requiredTronSweepEnergy = int64(65_000)
	manualTronSweepEnergy   = int64(10_000)
)

var minimumBSCSweepBNBBalance = decimal.RequireFromString("0.001")

type SweepPreview struct {
	Chain             string           `json:"chain"`
	Destination       string           `json:"destination"`
	SourceAddress     string           `json:"source_address,omitempty"`
	Threshold         string           `json:"threshold"`
	EligibleCount     int              `json:"eligible_count"`
	EligibleTotalUSDT string           `json:"eligible_total_usdt"`
	Candidates        []SweepCandidate `json:"candidates"`
}

type SweepCandidate struct {
	Index       int    `json:"index"`
	Address     string `json:"address"`
	MnemonicTag string `json:"mnemonic_tag,omitempty"`
	TRXBalance  string `json:"trx_balance,omitempty"`
	BNBBalance  string `json:"bnb_balance,omitempty"`
	USDTBalance string `json:"usdt_balance"`
}

func (s *Service) PreviewSweep(chain, destination, sourceAddress string) (SweepPreview, error) {
	normalizedChain, normalizedDestination, err := validateSweepDestination(chain, destination)
	if err != nil {
		return SweepPreview{}, err
	}
	normalizedSourceAddress := strings.TrimSpace(sourceAddress)

	if s.repo != nil {
		return s.previewSweepFromDB(normalizedChain, normalizedDestination, normalizedSourceAddress)
	}
	cfg, file, threshold, err := s.loadSweepContext(normalizedChain)
	if err != nil {
		return SweepPreview{}, err
	}
	currentMnemonicTag := currentSweepMnemonicTag(cfg, normalizedChain)

	candidates, total := collectEligibleCandidates(normalizedChain, file, threshold, normalizedDestination, currentMnemonicTag, normalizedSourceAddress)
	// #region debug-point A:sweep-preview-summary
	debugReport("pre", "A", "hdwallet/collect.go:PreviewSweep", "[DEBUG] sweep preview summary", map[string]any{
		"chain":              normalizedChain,
		"destination":        normalizedDestination,
		"sourceAddress":      normalizedSourceAddress,
		"threshold":          threshold.StringFixed(6),
		"currentMnemonicTag": currentMnemonicTag,
		"fileCount":          len(file.Addresses),
		"eligibleCount":      len(candidates),
		"eligibleTotalUSDT":  total.StringFixed(6),
	})
	// #endregion
	preview := SweepPreview{
		Chain:             normalizedChain,
		Destination:       normalizedDestination,
		SourceAddress:     normalizedSourceAddress,
		Threshold:         threshold.StringFixed(6),
		EligibleCount:     len(candidates),
		EligibleTotalUSDT: total.StringFixed(6),
	}
	limit := len(candidates)
	if limit > 20 {
		limit = 20
	}
	preview.Candidates = append(preview.Candidates, candidates[:limit]...)
	if cfg.Count == 0 {
		preview.Threshold = threshold.StringFixed(6)
	}
	return preview, nil
}

func (s *Service) StartSweep(chain, destination, sourceAddress string) error {
	preview, err := s.PreviewSweep(chain, destination, sourceAddress)
	if err != nil {
		return err
	}
	if preview.EligibleCount == 0 {
		if strings.TrimSpace(preview.SourceAddress) != "" {
			return fmt.Errorf("当前地址不符合归集条件")
		}
		if strings.EqualFold(chain, "tron") {
			return fmt.Errorf("当前链没有 TRX 余额大于等于 1 且 USDT 大于等于阈值的地址")
		}
		return fmt.Errorf("当前链没有 BNB 余额大于等于 0.001 且 USDT 大于等于阈值的地址")
	}
	jobMessage := fmt.Sprintf("开始归集 %s USDT", strings.ToUpper(preview.Chain))
	if preview.SourceAddress != "" {
		jobMessage = fmt.Sprintf("开始归集 %s 单条地址 USDT", strings.ToUpper(preview.Chain))
	}
	if !s.beginJob("sweep-"+preview.Chain, preview.Chain, preview.EligibleCount, jobMessage) {
		return fmt.Errorf("任务正在执行中")
	}
	go s.runSweep(preview.Chain, preview.Destination, preview.SourceAddress)
	return nil
}

func (s *Service) runSweep(chain, destination, sourceAddress string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.finishWithError(fmt.Errorf("panic: %v", recovered))
		}
	}()

	if s.repo != nil {
		s.runSweepFromDB(chain, destination, sourceAddress)
		return
	}

	cfg, file, threshold, err := s.loadSweepContext(chain)
	if err != nil {
		s.finishWithError(err)
		return
	}
	currentMnemonicTag := currentSweepMnemonicTag(cfg, chain)

	candidates, _ := collectEligibleCandidates(chain, file, threshold, destination, currentMnemonicTag, sourceAddress)
	if len(candidates) == 0 {
		s.finishSkipped("没有符合阈值的地址")
		return
	}

	successCount := 0
	failedCount := 0
	skippedCount := 0
	firstFailureReason := ""

	for index, candidate := range candidates {
		message := fmt.Sprintf("%s 归集中 %d/%d", strings.ToUpper(chain), index+1, len(candidates))
		s.setProgress("sweep", chain, index+1, message)

		var (
			txHash     string
			statusNote string
		)
		switch chain {
		case "tron":
			txHash, statusNote, err = s.collectTronUSDT(cfg, file, threshold, destination, candidate, index+1, strings.TrimSpace(sourceAddress) != "")
		case "bsc":
			txHash, err = s.collectBSCUSDT(cfg, file, threshold, destination, candidate)
		default:
			err = fmt.Errorf("unknown chain")
		}

		if err != nil {
			if strings.Contains(err.Error(), "skip:") {
				skippedCount++
			} else {
				failedCount++
				if firstFailureReason == "" {
					firstFailureReason = err.Error()
				}
				s.logSweepAddressError(chain, candidate.Address, index+1, len(candidates), err)
			}
			s.setProgress("sweep", chain, index+1, fmt.Sprintf("%s 地址 %s 归集失败: %v", strings.ToUpper(chain), candidate.Address, err))
			continue
		}

		successCount++
		successMessage := fmt.Sprintf("%s 地址 %s 归集成功: %s", strings.ToUpper(chain), candidate.Address, txHash)
		if statusNote != "" {
			successMessage = fmt.Sprintf("%s，%s", successMessage, statusNote)
		}
		s.setProgress("sweep", chain, index+1, successMessage)
		if (index+1)%10 == 0 || index == len(candidates)-1 {
			if writeErr := s.writeJSON(s.chainPath(chain), file); writeErr != nil {
				s.finishWithError(writeErr)
				return
			}
		}
	}

	if err := s.writeJSON(s.chainPath(chain), file); err != nil {
		s.finishWithError(err)
		return
	}
	message := fmt.Sprintf("%s 归集完成，成功 %d，失败 %d，跳过 %d", strings.ToUpper(chain), successCount, failedCount, skippedCount)
	if firstFailureReason != "" {
		s.finishSuccessWithLastError(message, firstFailureReason)
		return
	}
	s.finishSuccess(message)
}

func (s *Service) collectTronUSDT(cfg ConfigFile, file *ChainFile, threshold decimal.Decimal, destination string, candidate SweepCandidate, progressCurrent int, manualSweep bool) (string, string, error) {
	mnemonic := cfg.TronMnemonic
	var err error
	if s.repo != nil {
		mnemonic, err = s.loadChainMnemonicByTag("tron", candidate.MnemonicTag, cfg.TronMnemonic)
		if err != nil {
			return "", "", err
		}
	}
	wallet, err := DeriveTronWallet(mnemonic, candidate.Index)
	if err != nil {
		return "", "", err
	}
	if wallet.Address == destination {
		return "", "", fmt.Errorf("skip: 目标地址与源地址相同")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	usdtBalance, err := s.tronClient.GetUSDTBalance(ctx, wallet.AddressHex)
	if err != nil {
		return "", "", err
	}
	if usdtBalance.LessThan(threshold) {
		return "", "", fmt.Errorf("skip: 当前 USDT 余额低于阈值")
	}
	trxActive, trxBalance, err := s.tronClient.GetAccountState(ctx, wallet.AddressHex)
	if err != nil {
		return "", "", err
	}
	statusNotes := make([]string, 0, 2)
	if manualSweep && (!trxActive || trxBalance.LessThan(decimal.NewFromInt(1))) {
		if s.tronActivator == nil {
			return "", "", fmt.Errorf("未配置 tron 激活地址功能")
		}
		s.setProgress("sweep", "tron", progressCurrent, fmt.Sprintf("TRON 地址 %s TRX 不足，正在自动激活地址", candidate.Address))
		if _, err := s.tronActivator.Activate(ctx, wallet.Address); err != nil {
			return "", "", fmt.Errorf("激活地址失败: %w", err)
		}
		statusNotes = append(statusNotes, "TRX 不足，已自动激活地址一次")
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return "", "", err
		}
		activationTimer := time.NewTimer(3 * time.Second)
		defer activationTimer.Stop()
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-activationTimer.C:
		}
		trxActive, trxBalance, err = s.tronClient.GetAccountState(ctx, wallet.AddressHex)
		if err != nil {
			return "", "", fmt.Errorf("激活后读取 tron 地址余额失败: %w", err)
		}
	}
	if !trxActive {
		return "", "", fmt.Errorf("skip: tron 地址未激活")
	}
	if trxBalance.LessThan(decimal.NewFromInt(1)) {
		return "", "", fmt.Errorf("skip: trx 余额需大于等于 1")
	}
	availableEnergy, err := s.tronClient.GetAvailableEnergy(ctx, wallet.AddressHex)
	if err != nil {
		return "", "", fmt.Errorf("读取 tron 地址能量失败: %w", err)
	}
	energyThreshold := requiredTronSweepEnergy
	if manualSweep {
		energyThreshold = manualTronSweepEnergy
	}
	energyStatusMessage := "地址能量充足，跳过发能"
	if availableEnergy < energyThreshold {
		providerName, provider := s.resolveEnergyProvider()
		if provider == nil || !provider.IsConfigured() {
			return "", "", fmt.Errorf("未配置发能通道")
		}
		respBody, orderErr := provider.OrderEnergy(wallet.Address, int(requiredTronSweepEnergy))
		if s.repo != nil {
			status := "SUCCESS"
			errorMessage := ""
			if orderErr != nil {
				status = "FAILED"
				errorMessage = orderErr.Error()
			}
			_ = s.repo.InsertEnergyActionLog(ctx, repository.EnergyActionLog{
				ActionName:    "自动发能一次",
				AddressBase58: wallet.Address,
				Provider:      providerName,
				EnergyAmount:  int(requiredTronSweepEnergy),
				ActionScore:   1,
				Status:        status,
				ResponseBody:  respBody,
				ErrorMessage:  errorMessage,
			})
		}
		if orderErr != nil {
			return "", "", fmt.Errorf("发能一次失败(provider=%s): %w", providerName, orderErr)
		}
		energyStatusMessage = "地址能量不足，已自动发能一次"
		statusNotes = append(statusNotes, energyStatusMessage)
		s.setProgress("sweep", "tron", progressCurrent, fmt.Sprintf("TRON 地址 %s %s", candidate.Address, energyStatusMessage))
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return "", "", err
		}
		energyTimer := time.NewTimer(3 * time.Second)
		defer energyTimer.Stop()
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-energyTimer.C:
		}
	} else {
		s.setProgress("sweep", "tron", progressCurrent, fmt.Sprintf("TRON 地址 %s %s", candidate.Address, energyStatusMessage))
	}

	amountUnits, err := decimalToTokenUnits(usdtBalance, tronTokenPrecision)
	if err != nil {
		return "", "", err
	}
	unsignedJSON, err := s.tronClient.CreateUSDTTransferTransaction(ctx, wallet.AddressHex, destination, amountUnits, defaultTronFeeLimitSun)
	if err != nil {
		return "", "", err
	}

	privateKeyBytes, err := hex.DecodeString(wallet.PrivateKeyHex)
	if err != nil {
		return "", "", fmt.Errorf("decode private key: %w", err)
	}
	privateKey, err := gotronKeys.GetPrivateKeyFromBytes(privateKeyBytes)
	if err != nil {
		return "", "", fmt.Errorf("load tron private key: %w", err)
	}
	tx, err := gotronTx.FromJSON(unsignedJSON)
	if err != nil {
		return "", "", fmt.Errorf("decode unsigned transaction: %w", err)
	}
	signedTx, err := gotronTx.SignTransaction(tx, privateKey)
	if err != nil {
		return "", "", fmt.Errorf("sign tron transaction: %w", err)
	}
	signedJSON, err := buildSignedTronTransactionJSON(unsignedJSON, signedTx.GetSignature())
	if err != nil {
		return "", "", fmt.Errorf("encode signed tron transaction: %w", err)
	}
	txHash, err := s.tronClient.BroadcastTransactionJSON(ctx, signedJSON)
	if err != nil {
		return "", "", err
	}

	if s.repo != nil {
		if err := s.repo.UpsertBalance(ctx, wallet.Address, "USDT", decimal.Zero, 0); err != nil {
			return "", "", err
		}
		return txHash, strings.Join(statusNotes, "，"), nil
	}
	if candidate.Index < len(file.Addresses) {
		file.Addresses[candidate.Index].USDTBalance = decimal.Zero.StringFixed(6)
		file.Addresses[candidate.Index].UpdatedAt = nowString()
	}
	file.BalanceUpdatedAt = nowString()
	return txHash, strings.Join(statusNotes, "，"), nil
}

func buildSignedTronTransactionJSON(unsignedJSON []byte, signatures [][]byte) ([]byte, error) {
	if len(unsignedJSON) == 0 {
		return nil, fmt.Errorf("empty unsigned transaction json")
	}
	var payload map[string]any
	if err := json.Unmarshal(unsignedJSON, &payload); err != nil {
		return nil, fmt.Errorf("decode unsigned transaction json: %w", err)
	}
	encodedSignatures := make([]string, 0, len(signatures))
	for _, signature := range signatures {
		if len(signature) == 0 {
			continue
		}
		encodedSignatures = append(encodedSignatures, hex.EncodeToString(signature))
	}
	if len(encodedSignatures) == 0 {
		return nil, fmt.Errorf("missing transaction signature")
	}
	payload["signature"] = encodedSignatures
	return json.Marshal(payload)
}

func (s *Service) collectBSCUSDT(cfg ConfigFile, file *ChainFile, threshold decimal.Decimal, destination string, candidate SweepCandidate) (string, error) {
	mnemonic := cfg.BSCMnemonic
	var err error
	if s.repo != nil {
		mnemonic, err = s.loadChainMnemonicByTag("bsc", candidate.MnemonicTag, cfg.BSCMnemonic)
		if err != nil {
			return "", err
		}
	}
	wallet, err := DeriveBSCWallet(mnemonic, candidate.Index)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(wallet.Address, destination) {
		return "", fmt.Errorf("skip: 目标地址与源地址相同")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	usdtBalance, err := s.bscClient.GetUSDTBalance(ctx, wallet.Address)
	if err != nil {
		return "", err
	}
	if usdtBalance.LessThan(threshold) {
		return "", fmt.Errorf("skip: 当前 USDT 余额低于阈值")
	}

	bnbBalance, err := s.bscClient.GetBNBBalance(ctx, wallet.Address)
	if err != nil {
		return "", err
	}
	if bnbBalance.LessThan(minimumBSCSweepBNBBalance) {
		return "", fmt.Errorf("skip: bnb 余额需大于等于 0.001")
	}
	amountUnits, err := decimalToTokenUnits(usdtBalance, bscTokenPrecision)
	if err != nil {
		return "", err
	}
	gasPrice, err := s.bscClient.GasPrice(ctx)
	if err != nil {
		return "", err
	}
	nonce, err := s.bscClient.PendingNonceAt(ctx, wallet.Address)
	if err != nil {
		return "", err
	}
	chainID, err := s.bscClient.ChainID(ctx)
	if err != nil {
		return "", err
	}
	data, err := buildERC20TransferData(destination, amountUnits)
	if err != nil {
		return "", err
	}
	callObj := map[string]any{
		"from": wallet.Address,
		"to":   s.bscClient.USDTContract(),
		"data": "0x" + hex.EncodeToString(data),
	}
	gasLimit, err := s.bscClient.EstimateGas(ctx, callObj)
	if err != nil {
		return "", err
	}
	gasLimit = gasLimit + gasLimit/5 + 5_000

	bnbBalanceWei, err := decimalToTokenUnits(bnbBalance, 18)
	if err != nil {
		return "", err
	}
	estimatedFeeWei := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit))
	if bnbBalanceWei.Cmp(estimatedFeeWei) < 0 {
		return "", fmt.Errorf("skip: bnb 手续费不足")
	}

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(wallet.PrivateKeyHex, "0x"))
	if err != nil {
		return "", fmt.Errorf("load bsc private key: %w", err)
	}
	tokenAddress := common.HexToAddress(s.bscClient.USDTContract())
	tx := ethTypes.NewTx(&ethTypes.LegacyTx{
		Nonce:    nonce,
		To:       &tokenAddress,
		Value:    big.NewInt(0),
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     data,
	})
	signedTx, err := ethTypes.SignTx(tx, ethTypes.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return "", fmt.Errorf("sign bsc transaction: %w", err)
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal bsc transaction: %w", err)
	}
	txHash, err := s.bscClient.SendRawTransaction(ctx, hex.EncodeToString(rawTx))
	if err != nil {
		return "", err
	}

	if s.repo != nil {
		if err := repository.UpsertBSCBalance(ctx, s.repo, wallet.Address, "USDT", decimal.Zero.StringFixed(6)); err != nil {
			return "", err
		}
		return txHash, nil
	}
	if candidate.Index < len(file.Addresses) {
		file.Addresses[candidate.Index].USDTBalance = decimal.Zero.StringFixed(6)
		file.Addresses[candidate.Index].UpdatedAt = nowString()
	}
	file.BalanceUpdatedAt = nowString()
	return txHash, nil
}

func (s *Service) loadSweepContext(chain string) (ConfigFile, *ChainFile, decimal.Decimal, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return ConfigFile{}, nil, decimal.Zero, err
	}
	file, err := s.loadChainFile(chain)
	if err != nil {
		return ConfigFile{}, nil, decimal.Zero, err
	}

	var threshold decimal.Decimal
	switch chain {
	case "tron":
		if cfg.TronMnemonic == "" {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("tron 助记词未配置")
		}
		if s.tronClient == nil {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("tron rpc 未配置")
		}
		threshold, err = parseThresholdDecimal(cfg.TronUSDTThreshold)
	case "bsc":
		if cfg.BSCMnemonic == "" {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("bsc 助记词未配置")
		}
		if s.bscClient == nil {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("bsc rpc 未配置")
		}
		threshold, err = parseThresholdDecimal(cfg.BSCUSDTThreshold)
	default:
		return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("unsupported chain")
	}
	if err != nil {
		return ConfigFile{}, nil, decimal.Zero, err
	}
	return cfg, &file, threshold, nil
}

func collectEligibleCandidates(chain string, file *ChainFile, threshold decimal.Decimal, destination, currentMnemonicTag, sourceAddress string) ([]SweepCandidate, decimal.Decimal) {
	candidates := make([]SweepCandidate, 0)
	total := decimal.Zero
	for _, record := range file.Addresses {
		isFocus := record.Index == 1
		if isFocus {
			// #region debug-point A:sweep-filter-focus-input
			debugReport("pre", "A", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus input", map[string]any{
				"chain":              chain,
				"destination":        destination,
				"threshold":          threshold.StringFixed(6),
				"currentMnemonicTag": currentMnemonicTag,
				"recordIndex":        record.Index,
				"recordAddress":      record.Address,
				"recordMnemonicTag":  strings.TrimSpace(record.MnemonicTag),
				"recordTRX":          strings.TrimSpace(record.TRXBalance),
				"recordBNB":          strings.TrimSpace(record.BNBBalance),
				"recordUSDT":         strings.TrimSpace(record.USDTBalance),
			})
			// #endregion
		}
		if currentMnemonicTag != "" && strings.TrimSpace(record.MnemonicTag) != currentMnemonicTag {
			if isFocus {
				// #region debug-point A:sweep-filter-focus-skip-tag
				debugReport("pre", "A", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: mnemonic_tag mismatch", map[string]any{
					"currentMnemonicTag": currentMnemonicTag,
					"recordMnemonicTag":  strings.TrimSpace(record.MnemonicTag),
				})
				// #endregion
			}
			continue
		}
		if sourceAddress != "" {
			switch chain {
			case "tron":
				if strings.TrimSpace(record.Address) != sourceAddress {
					continue
				}
			case "bsc":
				if !strings.EqualFold(strings.TrimSpace(record.Address), sourceAddress) {
					continue
				}
			}
		}
		if destination != "" {
			if chain == "tron" && record.Address == destination {
				if isFocus {
					// #region debug-point E:sweep-filter-focus-skip-destination
					debugReport("pre", "E", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: destination equals source", map[string]any{
						"destination": destination,
						"address":     record.Address,
					})
					// #endregion
				}
				continue
			}
			if chain == "bsc" && strings.EqualFold(record.Address, destination) {
				if isFocus {
					// #region debug-point E:sweep-filter-focus-skip-destination
					debugReport("pre", "E", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: destination equals source", map[string]any{
						"destination": destination,
						"address":     record.Address,
					})
					// #endregion
				}
				continue
			}
		}

		usdtBalance, err := decimal.NewFromString(strings.TrimSpace(record.USDTBalance))
		if err != nil {
			if isFocus {
				// #region debug-point B:sweep-filter-focus-parse-usdt-failed
				debugReport("pre", "B", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: parse usdt failed", map[string]any{
					"rawUSDT": strings.TrimSpace(record.USDTBalance),
					"err":     err.Error(),
				})
				// #endregion
			}
			continue
		}
		if usdtBalance.LessThan(threshold) {
			if isFocus {
				// #region debug-point B:sweep-filter-focus-skip-usdt-threshold
				debugReport("pre", "B", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: usdt below threshold", map[string]any{
					"usdt":      usdtBalance.StringFixed(6),
					"threshold": threshold.StringFixed(6),
				})
				// #endregion
			}
			continue
		}
		trxBalance := decimal.Zero
		bnbBalance := decimal.Zero
		if chain == "tron" {
			if strings.TrimSpace(record.TRXBalance) != "" {
				trxBalance, err = decimal.NewFromString(strings.TrimSpace(record.TRXBalance))
				if err != nil {
					if isFocus {
						// #region debug-point D:sweep-filter-focus-parse-trx-failed
						debugReport("pre", "D", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: parse trx failed", map[string]any{
							"rawTRX": strings.TrimSpace(record.TRXBalance),
							"err":    err.Error(),
						})
						// #endregion
					}
					continue
				}
			}
			if sourceAddress == "" && trxBalance.LessThan(decimal.NewFromInt(1)) {
				if isFocus {
					// #region debug-point D:sweep-filter-focus-skip-trx-threshold
					debugReport("pre", "D", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: trx below minimum", map[string]any{
						"trx":           trxBalance.StringFixed(6),
						"sourceAddress": sourceAddress,
					})
					// #endregion
				}
				continue
			}
		} else if chain == "bsc" {
			if strings.TrimSpace(record.BNBBalance) == "" {
				if isFocus {
					// #region debug-point C:sweep-filter-focus-skip-empty-bnb
					debugReport("pre", "C", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: empty bnb balance", nil)
					// #endregion
				}
				continue
			}
			bnbBalance, err = decimal.NewFromString(strings.TrimSpace(record.BNBBalance))
			if err != nil {
				if isFocus {
					// #region debug-point C:sweep-filter-focus-parse-bnb-failed
					debugReport("pre", "C", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: parse bnb failed", map[string]any{
						"rawBNB": strings.TrimSpace(record.BNBBalance),
						"err":    err.Error(),
					})
					// #endregion
				}
				continue
			}
			if bnbBalance.LessThan(minimumBSCSweepBNBBalance) {
				if isFocus {
					// #region debug-point C:sweep-filter-focus-skip-bnb-threshold
					debugReport("pre", "C", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus skip: bnb below minimum", map[string]any{
						"bnb":     bnbBalance.StringFixed(6),
						"minimum": minimumBSCSweepBNBBalance.StringFixed(6),
					})
					// #endregion
				}
				continue
			}
		}
		if isFocus {
			// #region debug-point A:sweep-filter-focus-eligible
			debugReport("pre", "A", "hdwallet/collect.go:collectEligibleCandidates", "[DEBUG] sweep filter focus eligible", map[string]any{
				"trx":  trxBalance.StringFixed(6),
				"bnb":  bnbBalance.StringFixed(6),
				"usdt": usdtBalance.StringFixed(6),
			})
			// #endregion
		}
		candidates = append(candidates, SweepCandidate{
			Index:       record.Index,
			Address:     record.Address,
			MnemonicTag: record.MnemonicTag,
			TRXBalance:  trxBalance.StringFixed(6),
			BNBBalance:  bnbBalance.StringFixed(6),
			USDTBalance: usdtBalance.StringFixed(6),
		})
		total = total.Add(usdtBalance)
	}
	return candidates, total
}

func currentSweepMnemonicTag(cfg ConfigFile, chain string) string {
	switch strings.ToLower(strings.TrimSpace(chain)) {
	case "bsc":
		return mnemonicTagFromValue(cfg.BSCMnemonic)
	default:
		return mnemonicTagFromValue(cfg.TronMnemonic)
	}
}

func debugReport(runID, hypothesisID, location, msg string, data map[string]any) {
	envPath := filepath.Join(".dbg", "sweep-no-candidates.env")
	url := strings.TrimSpace(os.Getenv("DEBUG_SERVER_URL"))
	sessionID := strings.TrimSpace(os.Getenv("DEBUG_SESSION_ID"))
	if url == "" || sessionID == "" {
		content, err := os.ReadFile(envPath)
		if err == nil {
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "DEBUG_SERVER_URL=") && url == "" {
					url = strings.TrimSpace(strings.TrimPrefix(line, "DEBUG_SERVER_URL="))
				}
				if strings.HasPrefix(line, "DEBUG_SESSION_ID=") && sessionID == "" {
					sessionID = strings.TrimSpace(strings.TrimPrefix(line, "DEBUG_SESSION_ID="))
				}
			}
		}
	}
	if url == "" || sessionID == "" {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"sessionId":     sessionID,
		"runId":         runID,
		"hypothesisId":  hypothesisID,
		"location":      location,
		"msg":           msg,
		"data":          data,
		"ts":            time.Now().UnixMilli(),
		"traceId":       "",
		"service":       "tron_watcher",
		"serviceModule": "hdwallet",
	})
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 800 * time.Millisecond}
	_, _ = client.Do(req)
}

func (s *Service) resolveEnergyProvider() (string, interface {
	Name() string
	IsConfigured() bool
	OrderEnergy(string, int) (string, error)
}) {
	providerName := strings.ToLower(strings.TrimSpace(s.defaultEnergyProvider))
	if resolved, ok := resolveEnergyProviderRule(providerName); ok {
		providerName = resolved
	}
	if provider, ok := s.energyProviders[providerName]; ok {
		return providerName, provider
	}
	if provider, ok := s.energyProviders["trxfee"]; ok {
		return "trxfee", provider
	}
	if provider, ok := s.energyProviders["catfee"]; ok {
		return "catfee", provider
	}
	return providerName, nil
}

func resolveEnergyProviderRule(rule string) (string, bool) {
	if rule == "trxfee" || rule == "catfee" {
		return rule, true
	}
	parts := strings.Split(rule, "-")
	if len(parts) != 2 {
		return "", false
	}
	start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return "", false
	}
	end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return "", false
	}
	if start < 0 || start > 24 || end < 0 || end > 24 {
		return "", false
	}
	currentHour := time.Now().UTC().Add(8 * time.Hour).Hour()
	if energyProviderInHourRange(currentHour, start, end) {
		return "trxfee", true
	}
	return "catfee", true
}

func energyProviderInHourRange(currentHour, start, end int) bool {
	if start == 24 {
		start = 0
	}
	if end == 24 {
		end = 23
	}
	if start <= end {
		return currentHour >= start && currentHour <= end
	}
	return currentHour >= start || currentHour <= end
}

func validateSweepDestination(chain, destination string) (string, string, error) {
	normalizedChain := strings.ToLower(strings.TrimSpace(chain))
	if normalizedChain != "bsc" {
		normalizedChain = "tron"
	}
	trimmedDestination := strings.TrimSpace(destination)
	if trimmedDestination == "" {
		return "", "", fmt.Errorf("归集目标地址不能为空")
	}

	if normalizedChain == "tron" {
		if _, err := tron.Base58ToHex(trimmedDestination); err != nil {
			return "", "", fmt.Errorf("tron 归集地址无效")
		}
		return normalizedChain, trimmedDestination, nil
	}
	if !common.IsHexAddress(trimmedDestination) {
		return "", "", fmt.Errorf("bsc 归集地址无效")
	}
	return normalizedChain, common.HexToAddress(trimmedDestination).Hex(), nil
}

func decimalToTokenUnits(amount decimal.Decimal, scale int32) (*big.Int, error) {
	if amount.IsNegative() {
		return nil, fmt.Errorf("amount must be >= 0")
	}
	scaled := amount.Shift(scale).Truncate(0)
	value := new(big.Int)
	if _, ok := value.SetString(scaled.StringFixed(0), 10); !ok {
		return nil, fmt.Errorf("convert decimal to token units failed")
	}
	return value, nil
}

func buildERC20TransferData(destination string, amount *big.Int) ([]byte, error) {
	if !common.IsHexAddress(destination) {
		return nil, fmt.Errorf("invalid bsc destination")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	selector := crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]
	data := make([]byte, 0, 4+32+32)
	data = append(data, selector...)
	data = append(data, common.LeftPadBytes(common.HexToAddress(destination).Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(amount.Bytes(), 32)...)
	return data, nil
}
