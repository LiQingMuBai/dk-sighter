package hdwallet

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const hdWalletConfigSettingKey = "hd_wallet_config_v1"

func (s *Service) ConfigureRepository(repo *repository.DB, source string) {
	s.repo = repo
	if strings.TrimSpace(source) != "" {
		s.hdSource = strings.TrimSpace(source)
	}
}

func (s *Service) saveConfigToDB(plain ConfigFile, persisted ConfigFile) error {
	data, err := json.Marshal(persisted)
	if err != nil {
		return fmt.Errorf("marshal hd wallet config: %w", err)
	}
	ctx := context.Background()
	if err := s.repo.UpsertRuntimeSetting(ctx, hdWalletConfigSettingKey, string(data)); err != nil {
		return err
	}
	if strings.TrimSpace(plain.TronMnemonic) != "" {
		if err := s.repo.UpsertRuntimeSetting(ctx, hdWalletMnemonicSettingKey("tron", mnemonicTagFromValue(plain.TronMnemonic)), persisted.TronMnemonic); err != nil {
			return err
		}
	}
	if strings.TrimSpace(plain.BSCMnemonic) != "" {
		if err := s.repo.UpsertRuntimeSetting(ctx, hdWalletMnemonicSettingKey("bsc", mnemonicTagFromValue(plain.BSCMnemonic)), persisted.BSCMnemonic); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) loadConfigFromDB() (ConfigFile, error) {
	value, exists, err := s.repo.GetRuntimeSetting(context.Background(), hdWalletConfigSettingKey)
	if err != nil {
		return ConfigFile{}, err
	}
	if !exists || strings.TrimSpace(value) == "" {
		return defaultConfigFile(s.count), nil
	}

	var cfg ConfigFile
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return ConfigFile{}, fmt.Errorf("unmarshal hd wallet config: %w", err)
	}
	cfg, err = s.decryptConfig(cfg)
	if err != nil {
		return ConfigFile{}, err
	}
	return applyConfigDefaults(cfg, s.count), nil
}

func (s *Service) runFullSyncToDB(cfg ConfigFile) error {
	tronMnemonicTag := mnemonicTagFromValue(cfg.TronMnemonic)
	tronExistingIndexes, err := s.repo.ListHDTronWalletIndexes(context.Background(), s.hdSource, tronMnemonicTag)
	if err != nil {
		return err
	}
	tronIndexes := collectMissingWalletIndexes(tronExistingIndexes, s.count, hdWalletGenerateBatchSize)
	tronItems := make([]repository.HDWatchAddressInput, 0, len(tronIndexes))
	for _, i := range tronIndexes {
		item, err := DeriveTronWallet(cfg.TronMnemonic, i)
		if err != nil {
			return err
		}
		tronItems = append(tronItems, repository.HDWatchAddressInput{
			WalletIndex: i,
			MnemonicTag: tronMnemonicTag,
			Address:     item.Address,
		})
	}
	if err := s.repo.InsertHDTronWatchAddresses(context.Background(), s.hdSource, tronItems); err != nil {
		return err
	}
	s.setProgress("generate", "tron", len(tronItems), fmt.Sprintf("Tron 本次新增 %d 个地址", len(tronItems)))

	bscMnemonicTag := mnemonicTagFromValue(cfg.BSCMnemonic)
	bscExistingIndexes, err := s.repo.ListHDBSCWalletIndexes(context.Background(), s.hdSource, bscMnemonicTag)
	if err != nil {
		return err
	}
	bscIndexes := collectMissingWalletIndexes(bscExistingIndexes, s.count, hdWalletGenerateBatchSize)
	bscItems := make([]repository.HDWatchAddressInput, 0, len(bscIndexes))
	for _, i := range bscIndexes {
		item, err := DeriveBSCWallet(cfg.BSCMnemonic, i)
		if err != nil {
			return err
		}
		bscItems = append(bscItems, repository.HDWatchAddressInput{
			WalletIndex: i,
			MnemonicTag: bscMnemonicTag,
			Address:     item.Address,
		})
	}
	if err := s.repo.InsertHDBSCWatchAddresses(context.Background(), s.hdSource, bscItems); err != nil {
		return err
	}
	s.setProgress("generate", "bsc", len(tronItems)+len(bscItems), fmt.Sprintf("BSC 本次新增 %d 个地址", len(bscItems)))
	if len(tronItems) == 0 && len(bscItems) == 0 {
		return fmt.Errorf("当前助记词派生地址已达到上限 %d", s.count)
	}
	return nil
}

func collectMissingWalletIndexes(existingIndexes []int, maxCount, batchSize int) []int {
	if maxCount <= 0 || batchSize <= 0 {
		return nil
	}
	existing := make(map[int]struct{}, len(existingIndexes))
	for _, idx := range existingIndexes {
		if idx < 0 || idx >= maxCount {
			continue
		}
		existing[idx] = struct{}{}
	}
	indexes := make([]int, 0, batchSize)
	for i := 0; i < maxCount && len(indexes) < batchSize; i++ {
		if _, ok := existing[i]; ok {
			continue
		}
		indexes = append(indexes, i)
	}
	return indexes
}

func (s *Service) stateFromDB(chain string, page, pageSize int) (State, error) {
	cfg, _ := s.loadConfig()

	s.mu.Lock()
	job := s.job
	tronSyncRunning := s.tronSyncRunning
	bscSyncRunning := s.bscSyncRunning
	tronLastScheduledSyncAt := s.tronLastScheduledSyncAt
	bscLastScheduledSyncAt := s.bscLastScheduledSyncAt
	s.mu.Unlock()

	normalizedChain := strings.ToLower(strings.TrimSpace(chain))
	if normalizedChain != "bsc" {
		normalizedChain = "tron"
	}

	tronSummary, err := s.repo.GetHDTronSummary(context.Background(), s.hdSource)
	if err != nil {
		return State{}, err
	}
	bscSummary, err := s.repo.GetHDBSCSummary(context.Background(), s.hdSource)
	if err != nil {
		return State{}, err
	}

	tronLastBlock, _, _ := s.repo.GetLastBlock(context.Background(), "tron_solid_scanner")
	bscLastBlock, _, _ := s.repo.GetLastBlock(context.Background(), "bsc_scanner")

	pageData, err := s.pageDataFromDB(normalizedChain, page, pageSize)
	if err != nil {
		return State{}, err
	}

	return State{
		Configured: cfg.TronMnemonic != "" && cfg.BSCMnemonic != "",
		Config:     cfg,
		Job:        job,
		Tron: Summary{
			Count:               tronSummary.Count,
			TRXTotal:            tronSummary.TRXTotal.StringFixed(6),
			USDTTotal:           tronSummary.USDTTotal.StringFixed(6),
			LastUpdatedAt:       formatNullTime(tronSummary.LastUpdated),
			ScheduledRunning:    tronSyncRunning,
			LastScheduledSyncAt: tronLastScheduledSyncAt,
			LastScannedBlock:    tronLastBlock,
		},
		BSC: Summary{
			Count:               bscSummary.Count,
			BNBTotal:            bscSummary.BNBTotal.StringFixed(6),
			USDTTotal:           bscSummary.USDTTotal.StringFixed(6),
			LastUpdatedAt:       formatNullTime(bscSummary.LastUpdated),
			ScheduledRunning:    bscSyncRunning,
			LastScheduledSyncAt: bscLastScheduledSyncAt,
			LastScannedBlock:    bscLastBlock,
		},
		Page: pageData,
	}, nil
}

func (s *Service) pageDataFromDB(chain string, page, pageSize int) (PageData, error) {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if page <= 0 {
		page = 1
	}
	offset := (page - 1) * pageSize

	switch chain {
	case "bsc":
		rows, totalCount, err := s.repo.ListHDBSCDashboardRows(context.Background(), s.hdSource, pageSize, offset)
		if err != nil {
			return PageData{}, err
		}
		totalPages := 1
		if totalCount > 0 {
			totalPages = (totalCount + pageSize - 1) / pageSize
		}
		items := make([]AddressRecord, 0, len(rows))
		for _, row := range rows {
			items = append(items, AddressRecord{
				Index:       row.WalletIndex,
				Address:     row.Address,
				MnemonicTag: row.MnemonicTag,
				BNBBalance:  row.BNBBalance.StringFixed(6),
				USDTBalance: row.USDTBalance.StringFixed(6),
				UpdatedAt:   formatNullTime(row.UpdatedAt),
			})
		}
		return PageData{
			Chain:      chain,
			Items:      items,
			Page:       page,
			PageSize:   pageSize,
			TotalCount: totalCount,
			TotalPages: totalPages,
		}, nil
	default:
		rows, totalCount, err := s.repo.ListHDTronDashboardRows(context.Background(), s.hdSource, pageSize, offset)
		if err != nil {
			return PageData{}, err
		}
		totalPages := 1
		if totalCount > 0 {
			totalPages = (totalCount + pageSize - 1) / pageSize
		}
		items := make([]AddressRecord, 0, len(rows))
		for _, row := range rows {
			items = append(items, AddressRecord{
				Index:       row.WalletIndex,
				Address:     row.Address,
				MnemonicTag: row.MnemonicTag,
				TRXBalance:  row.TRXBalance.StringFixed(6),
				USDTBalance: row.USDTBalance.StringFixed(6),
				UpdatedAt:   formatNullTime(row.UpdatedAt),
			})
		}
		return PageData{
			Chain:      "tron",
			Items:      items,
			Page:       page,
			PageSize:   pageSize,
			TotalCount: totalCount,
			TotalPages: totalPages,
		}, nil
	}
}

func (s *Service) refreshAddressFromDB(chain, address string) (AddressRecord, error) {
	return s.refreshAddressFromDBWithContext(context.Background(), chain, address)
}

func (s *Service) refreshAddressFromDBWithContext(ctx context.Context, chain, address string) (AddressRecord, error) {
	normalizedChain := strings.ToLower(strings.TrimSpace(chain))
	switch normalizedChain {
	case "tron":
		if s.tronClient == nil {
			return AddressRecord{}, fmt.Errorf("tron client not configured")
		}
		row, exists, err := s.repo.GetHDTronDashboardRowByAddress(context.Background(), s.hdSource, address)
		if err != nil {
			return AddressRecord{}, err
		}
		if !exists {
			return AddressRecord{}, fmt.Errorf("address not found")
		}
		addressHex, err := tron.Base58ToHex(row.Address)
		if err != nil {
			return AddressRecord{}, err
		}
		active, trxBalance, err := s.tronClient.GetAccountState(ctx, addressHex)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 tron 地址余额失败: %w", err)
		}
		if err := waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		if err := s.repo.UpsertBalance(ctx, row.Address, "TRX", trxBalance, 0); err != nil {
			return AddressRecord{}, err
		}
		usdtBalance := decimalZeroString()
		if active {
			value, err := s.tronClient.GetUSDTBalance(ctx, addressHex)
			if err != nil {
				return AddressRecord{}, fmt.Errorf("读取 tron 地址 usdt 余额失败: %w", err)
			}
			if err := waitForBalanceThrottle(ctx); err != nil {
				return AddressRecord{}, err
			}
			if err := s.repo.UpsertBalance(ctx, row.Address, "USDT", value, 0); err != nil {
				return AddressRecord{}, err
			}
			usdtBalance = value.StringFixed(6)
		}
		return AddressRecord{
			Index:       row.WalletIndex,
			Address:     row.Address,
			MnemonicTag: row.MnemonicTag,
			TRXBalance:  trxBalance.StringFixed(6),
			USDTBalance: usdtBalance,
			UpdatedAt:   nowString(),
		}, nil
	case "bsc":
		if s.bscClient == nil {
			return AddressRecord{}, fmt.Errorf("bsc client not configured")
		}
		row, exists, err := s.repo.GetHDBSCDashboardRowByAddress(context.Background(), s.hdSource, address)
		if err != nil {
			return AddressRecord{}, err
		}
		if !exists {
			return AddressRecord{}, fmt.Errorf("address not found")
		}
		bnbBalance, err := s.bscClient.GetBNBBalance(ctx, row.Address)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 bsc 地址 bnb 余额失败: %w", err)
		}
		if err := waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		if err := repository.UpsertBSCBalance(ctx, s.repo, row.Address, "BNB", bnbBalance.StringFixed(6)); err != nil {
			return AddressRecord{}, err
		}
		usdtBalance, err := s.bscClient.GetUSDTBalance(ctx, row.Address)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 bsc 地址 usdt 余额失败: %w", err)
		}
		if err := waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		if err := repository.UpsertBSCBalance(ctx, s.repo, row.Address, "USDT", usdtBalance.StringFixed(6)); err != nil {
			return AddressRecord{}, err
		}
		return AddressRecord{
			Index:       row.WalletIndex,
			Address:     row.Address,
			MnemonicTag: row.MnemonicTag,
			BNBBalance:  bnbBalance.StringFixed(6),
			USDTBalance: usdtBalance.StringFixed(6),
			UpdatedAt:   nowString(),
		}, nil
	default:
		return AddressRecord{}, fmt.Errorf("unsupported chain")
	}
}

func (s *Service) runScheduledSyncFromDB(ctx context.Context, chain string) error {
	cfg, err := s.loadConfig()
	if err != nil {
		return err
	}

	switch chain {
	case "tron":
		if cfg.TronMnemonic == "" || s.tronClient == nil {
			return nil
		}
		s.setChainSyncRunning("tron", true)
		defer s.setChainSyncRunning("tron", false)

		summary, err := s.repo.GetHDTronSummary(ctx, s.hdSource)
		if err != nil {
			return err
		}
		if summary.Count == 0 {
			s.setChainLastScheduledSyncAt("tron", nowString())
			return nil
		}
		rows, _, err := s.repo.ListHDTronDashboardRows(ctx, s.hdSource, summary.Count, 0)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := s.refreshAddressFromDBWithContext(ctx, "tron", row.Address); err != nil {
				return err
			}
		}
		s.setChainLastScheduledSyncAt("tron", nowString())
		return nil
	case "bsc":
		if cfg.BSCMnemonic == "" || s.bscClient == nil {
			return nil
		}
		s.setChainSyncRunning("bsc", true)
		defer s.setChainSyncRunning("bsc", false)

		summary, err := s.repo.GetHDBSCSummary(ctx, s.hdSource)
		if err != nil {
			return err
		}
		if summary.Count == 0 {
			s.setChainLastScheduledSyncAt("bsc", nowString())
			return nil
		}
		rows, _, err := s.repo.ListHDBSCDashboardRows(ctx, s.hdSource, summary.Count, 0)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := s.refreshAddressFromDBWithContext(ctx, "bsc", row.Address); err != nil {
				return err
			}
		}
		s.setChainLastScheduledSyncAt("bsc", nowString())
		return nil
	default:
		return nil
	}
}

func (s *Service) loadSweepContextFromDB(chain string) (ConfigFile, *ChainFile, decimal.Decimal, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return ConfigFile{}, nil, decimal.Zero, err
	}

	file := &ChainFile{Chain: chain}
	switch chain {
	case "tron":
		if cfg.TronMnemonic == "" {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("tron 助记词未配置")
		}
		if s.tronClient == nil {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("tron rpc 未配置")
		}
		threshold, err := parseThresholdDecimal(cfg.TronUSDTThreshold)
		if err != nil {
			return ConfigFile{}, nil, decimal.Zero, err
		}
		summary, err := s.repo.GetHDTronSummary(context.Background(), s.hdSource)
		if err != nil {
			return ConfigFile{}, nil, decimal.Zero, err
		}
		file.Count = summary.Count
		if summary.Count > 0 {
			rows, _, err := s.repo.ListHDTronDashboardRows(context.Background(), s.hdSource, summary.Count, 0)
			if err != nil {
				return ConfigFile{}, nil, decimal.Zero, err
			}
			file.Addresses = make([]AddressRecord, 0, len(rows))
			for _, row := range rows {
				file.Addresses = append(file.Addresses, AddressRecord{
					Index:       row.WalletIndex,
					Address:     row.Address,
					MnemonicTag: row.MnemonicTag,
					TRXBalance:  row.TRXBalance.StringFixed(6),
					USDTBalance: row.USDTBalance.StringFixed(6),
					UpdatedAt:   formatNullTime(row.UpdatedAt),
				})
			}
		}
		return cfg, file, threshold, nil
	case "bsc":
		if cfg.BSCMnemonic == "" {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("bsc 助记词未配置")
		}
		if s.bscClient == nil {
			return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("bsc rpc 未配置")
		}
		threshold, err := parseThresholdDecimal(cfg.BSCUSDTThreshold)
		if err != nil {
			return ConfigFile{}, nil, decimal.Zero, err
		}
		summary, err := s.repo.GetHDBSCSummary(context.Background(), s.hdSource)
		if err != nil {
			return ConfigFile{}, nil, decimal.Zero, err
		}
		file.Count = summary.Count
		if summary.Count > 0 {
			rows, _, err := s.repo.ListHDBSCDashboardRows(context.Background(), s.hdSource, summary.Count, 0)
			if err != nil {
				return ConfigFile{}, nil, decimal.Zero, err
			}
			file.Addresses = make([]AddressRecord, 0, len(rows))
			for _, row := range rows {
				file.Addresses = append(file.Addresses, AddressRecord{
					Index:       row.WalletIndex,
					Address:     row.Address,
					MnemonicTag: row.MnemonicTag,
					BNBBalance:  row.BNBBalance.StringFixed(6),
					USDTBalance: row.USDTBalance.StringFixed(6),
					UpdatedAt:   formatNullTime(row.UpdatedAt),
				})
			}
		}
		return cfg, file, threshold, nil
	default:
		return ConfigFile{}, nil, decimal.Zero, fmt.Errorf("unsupported chain")
	}
}

func (s *Service) previewSweepFromDB(chain, destination string) (SweepPreview, error) {
	cfg, file, threshold, err := s.loadSweepContextFromDB(chain)
	if err != nil {
		return SweepPreview{}, err
	}
	currentMnemonicTag := currentSweepMnemonicTag(cfg, chain)

	candidates, total := collectEligibleCandidates(chain, file, threshold, destination, currentMnemonicTag)
	preview := SweepPreview{
		Chain:             chain,
		Destination:       destination,
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

func (s *Service) runSweepFromDB(chain, destination string) {
	cfg, file, threshold, err := s.loadSweepContextFromDB(chain)
	if err != nil {
		s.finishWithError(err)
		return
	}
	currentMnemonicTag := currentSweepMnemonicTag(cfg, chain)

	candidates, _ := collectEligibleCandidates(chain, file, threshold, destination, currentMnemonicTag)
	if len(candidates) == 0 {
		s.finishSkipped("没有符合阈值的地址")
		return
	}

	successCount := 0
	failedCount := 0
	skippedCount := 0
	firstFailureReason := ""

	for index, candidate := range candidates {
		s.setProgress("sweep", chain, index+1, fmt.Sprintf("%s 归集中 %d/%d", strings.ToUpper(chain), index+1, len(candidates)))

		var (
			txHash     string
			statusNote string
		)
		switch chain {
		case "tron":
			txHash, statusNote, err = s.collectTronUSDT(cfg, nil, threshold, destination, candidate, index+1)
		case "bsc":
			txHash, err = s.collectBSCUSDT(cfg, nil, threshold, destination, candidate)
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
	}

	message := fmt.Sprintf("%s 归集完成，成功 %d，失败 %d，跳过 %d", strings.ToUpper(chain), successCount, failedCount, skippedCount)
	if firstFailureReason != "" {
		s.finishSuccessWithLastError(message, firstFailureReason)
		return
	}
	s.finishSuccess(message)
}

func formatNullTime(value sql.NullTime) string {
	if !value.Valid {
		return ""
	}
	return value.Time.In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
}

func decimalZeroString() string {
	return "0.000000"
}

func hdWalletMnemonicSettingKey(chain, mnemonicTag string) string {
	return fmt.Sprintf("hd_wallet_mnemonic_%s_%s", strings.ToLower(strings.TrimSpace(chain)), strings.TrimSpace(mnemonicTag))
}

func mnemonicTagFromValue(mnemonic string) string {
	normalized := normalizeMnemonic(mnemonic)
	sum := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("m-%x", sum[:4])
}

func (s *Service) loadChainMnemonicByTag(chain, mnemonicTag, fallback string) (string, error) {
	if strings.TrimSpace(mnemonicTag) == "" {
		return fallback, nil
	}
	value, exists, err := s.repo.GetRuntimeSetting(context.Background(), hdWalletMnemonicSettingKey(chain, mnemonicTag))
	if err != nil {
		return "", err
	}
	if exists && strings.TrimSpace(value) != "" {
		return s.decryptString(value)
	}
	if mnemonicTagFromValue(fallback) == strings.TrimSpace(mnemonicTag) {
		return fallback, nil
	}
	return "", fmt.Errorf("%s 助记词标识 %s 不存在", strings.ToUpper(chain), mnemonicTag)
}
