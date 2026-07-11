package hdwallet

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tyler-smith/go-bip39"

	"tron_watcher/infrastructure"
	"tron_watcher/internal/bsc"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const (
	defaultPageSize            = 50
	maxPageSize                = 200
	hdWalletGenerateBatchSize  = 200
	encryptedPrefix            = "enc:v1:"
	defaultBalanceRequestDelay = 10 * time.Millisecond
)

type AddressRecord struct {
	Index       int    `json:"index"`
	Address     string `json:"address"`
	MnemonicTag string `json:"mnemonic_tag,omitempty"`
	AddressHex  string `json:"address_hex,omitempty"`
	TRXBalance  string `json:"trx_balance,omitempty"`
	USDTBalance string `json:"usdt_balance,omitempty"`
	BNBBalance  string `json:"bnb_balance,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type ConfigFile struct {
	TronMnemonic      string `json:"tron_mnemonic"`
	BSCMnemonic       string `json:"bsc_mnemonic"`
	Count             int    `json:"count"`
	TronUSDTThreshold string `json:"tron_usdt_threshold"`
	BSCUSDTThreshold  string `json:"bsc_usdt_threshold"`
	UpdatedAt         string `json:"updated_at"`
}

type ChainFile struct {
	Chain               string          `json:"chain"`
	Count               int             `json:"count"`
	GeneratedAt         string          `json:"generated_at"`
	BalanceUpdatedAt    string          `json:"balance_updated_at,omitempty"`
	LastScheduledSyncAt string          `json:"last_scheduled_sync_at,omitempty"`
	LastScannedBlock    int64           `json:"last_scanned_block,omitempty"`
	LatestChainBlock    int64           `json:"latest_chain_block,omitempty"`
	Addresses           []AddressRecord `json:"addresses"`
}

type JobState struct {
	Running    bool   `json:"running"`
	Stage      string `json:"stage"`
	Chain      string `json:"chain,omitempty"`
	Message    string `json:"message,omitempty"`
	Current    int    `json:"current"`
	Total      int    `json:"total"`
	LastError  string `json:"last_error,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
}

type Summary struct {
	Count               int    `json:"count"`
	TRXTotal            string `json:"trx_total,omitempty"`
	USDTTotal           string `json:"usdt_total,omitempty"`
	BNBTotal            string `json:"bnb_total,omitempty"`
	LastUpdatedAt       string `json:"last_updated_at,omitempty"`
	ScheduledRunning    bool   `json:"scheduled_running"`
	LastScheduledSyncAt string `json:"last_scheduled_sync_at,omitempty"`
	LastScannedBlock    int64  `json:"last_scanned_block,omitempty"`
	LatestChainBlock    int64  `json:"latest_chain_block,omitempty"`
	SyncLag             int64  `json:"sync_lag,omitempty"`
}

type PageData struct {
	Chain      string          `json:"chain"`
	Items      []AddressRecord `json:"items"`
	Page       int             `json:"page"`
	PageSize   int             `json:"page_size"`
	TotalCount int             `json:"total_count"`
	TotalPages int             `json:"total_pages"`
}

type State struct {
	Configured bool       `json:"configured"`
	Config     ConfigFile `json:"config"`
	Job        JobState   `json:"job"`
	Tron       Summary    `json:"tron"`
	BSC        Summary    `json:"bsc"`
	Page       PageData   `json:"page"`
}

type tronSweepActivator interface {
	Activate(context.Context, string) (string, error)
}

type Service struct {
	dataDir               string
	count                 int
	tronClient            *tron.Client
	bscClient             *bsc.Client
	repo                  *repository.DB
	hdSource              string
	tronActivator         tronSweepActivator
	bscGasTopupPrivateKey string
	energyProviders       map[string]infrastructure.EnergyOrderProvider
	defaultEnergyProvider string

	mu                      sync.Mutex
	ioMu                    sync.Mutex
	job                     JobState
	tronSyncRunning         bool
	bscSyncRunning          bool
	tronLastScheduledSyncAt string
	bscLastScheduledSyncAt  string
	balanceRequestDelay     time.Duration
}

func NewService(dataDir string, count int, tronClient *tron.Client, bscClient *bsc.Client) *Service {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = filepath.Join("data", "hd_wallet")
	}
	if count <= 0 {
		count = 10000
	}
	return &Service{
		dataDir:             dataDir,
		count:               count,
		tronClient:          tronClient,
		bscClient:           bscClient,
		hdSource:            repository.HDWalletSource,
		balanceRequestDelay: defaultBalanceRequestDelay,
	}
}

func (s *Service) ConfigureBalanceRequestDelay(delay time.Duration) {
	if s == nil || delay <= 0 {
		return
	}
	s.balanceRequestDelay = delay
}

func (s *Service) ConfigureEnergyProviders(providers map[string]infrastructure.EnergyOrderProvider, defaultProvider string) {
	s.energyProviders = providers
	s.defaultEnergyProvider = strings.ToLower(strings.TrimSpace(defaultProvider))
}

func (s *Service) ConfigureTronActivator(activator tronSweepActivator) {
	s.tronActivator = activator
}

func (s *Service) ConfigureBSCGasTopupPrivateKey(privateKey string) {
	s.bscGasTopupPrivateKey = strings.TrimSpace(privateKey)
}

func (s *Service) SaveConfig(tronMnemonic, bscMnemonic, tronUSDTThreshold, bscUSDTThreshold string) (ConfigFile, error) {
	cfg := ConfigFile{
		TronMnemonic:      normalizeMnemonic(tronMnemonic),
		BSCMnemonic:       normalizeMnemonic(bscMnemonic),
		Count:             s.count,
		TronUSDTThreshold: normalizeThreshold(tronUSDTThreshold),
		BSCUSDTThreshold:  normalizeThreshold(bscUSDTThreshold),
		UpdatedAt:         nowString(),
	}
	if !bip39.IsMnemonicValid(cfg.TronMnemonic) {
		return ConfigFile{}, fmt.Errorf("tron 助记词无效")
	}
	if !bip39.IsMnemonicValid(cfg.BSCMnemonic) {
		return ConfigFile{}, fmt.Errorf("bsc 助记词无效")
	}
	if _, err := parseThresholdDecimal(cfg.TronUSDTThreshold); err != nil {
		return ConfigFile{}, fmt.Errorf("tron usdt 阈值无效: %w", err)
	}
	if _, err := parseThresholdDecimal(cfg.BSCUSDTThreshold); err != nil {
		return ConfigFile{}, fmt.Errorf("bsc usdt 阈值无效: %w", err)
	}
	persisted, err := s.encryptConfig(cfg)
	if err != nil {
		return ConfigFile{}, err
	}
	if s.repo != nil {
		if err := s.saveConfigToDB(cfg, persisted); err != nil {
			return ConfigFile{}, err
		}
		return cfg, nil
	}
	if err := s.writeJSON(s.configPath(), persisted); err != nil {
		return ConfigFile{}, err
	}
	return cfg, nil
}

func (s *Service) StartSync() error {
	if !s.beginJob("generate-addresses", "all", hdWalletGenerateBatchSize*2, "开始分批生成地址") {
		return fmt.Errorf("任务正在执行中")
	}
	go s.runFullSync()
	return nil
}

func (s *Service) RefreshAddress(chain, address string) (AddressRecord, error) {
	if s.repo != nil {
		return s.refreshAddressFromDB(chain, address)
	}
	normalizedChain := strings.ToLower(strings.TrimSpace(chain))
	normalizedAddress := strings.TrimSpace(address)
	if normalizedChain != "tron" && normalizedChain != "bsc" {
		return AddressRecord{}, fmt.Errorf("unsupported chain")
	}
	if normalizedAddress == "" {
		return AddressRecord{}, fmt.Errorf("address is required")
	}

	file, err := s.loadChainFile(normalizedChain)
	if err != nil {
		return AddressRecord{}, err
	}

	idx := -1
	for i, item := range file.Addresses {
		switch normalizedChain {
		case "tron":
			if strings.EqualFold(strings.TrimSpace(item.Address), normalizedAddress) {
				idx = i
			}
		case "bsc":
			if strings.EqualFold(strings.TrimSpace(item.Address), normalizedAddress) {
				idx = i
			}
		}
		if idx >= 0 {
			break
		}
	}
	if idx < 0 {
		return AddressRecord{}, fmt.Errorf("address not found")
	}

	record := &file.Addresses[idx]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch normalizedChain {
	case "tron":
		if s.tronClient == nil {
			return AddressRecord{}, fmt.Errorf("tron client not configured")
		}
		active, trxBalance, err := s.tronClient.GetAccountState(ctx, record.AddressHex)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 tron 地址余额失败: %w", err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		usdtBalance := decimal.Zero
		if active {
			usdtBalance, err = s.tronClient.GetUSDTBalance(ctx, record.AddressHex)
			if err != nil {
				return AddressRecord{}, fmt.Errorf("读取 tron 地址 usdt 余额失败: %w", err)
			}
			if err := s.waitForBalanceThrottle(ctx); err != nil {
				return AddressRecord{}, err
			}
		}
		record.TRXBalance = trxBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
	case "bsc":
		if s.bscClient == nil {
			return AddressRecord{}, fmt.Errorf("bsc client not configured")
		}
		bnbBalance, err := s.bscClient.GetBNBBalance(ctx, record.Address)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 bsc 地址 bnb 余额失败: %w", err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		usdtBalance, err := s.bscClient.GetUSDTBalance(ctx, record.Address)
		if err != nil {
			return AddressRecord{}, fmt.Errorf("读取 bsc 地址 usdt 余额失败: %w", err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return AddressRecord{}, err
		}
		record.BNBBalance = bnbBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
	}

	record.UpdatedAt = nowString()
	file.BalanceUpdatedAt = record.UpdatedAt
	if err := s.writeJSON(s.chainPath(normalizedChain), file); err != nil {
		return AddressRecord{}, err
	}
	return *record, nil
}

func (s *Service) State(chain string, page, pageSize int) (State, error) {
	if s.repo != nil {
		return s.stateFromDB(chain, page, pageSize)
	}
	cfg, _ := s.loadConfig()
	tronFile, _ := s.loadChainFile("tron")
	bscFile, _ := s.loadChainFile("bsc")

	s.mu.Lock()
	job := s.job
	tronSyncRunning := s.tronSyncRunning
	bscSyncRunning := s.bscSyncRunning
	s.mu.Unlock()

	normalizedChain := strings.ToLower(strings.TrimSpace(chain))
	if normalizedChain != "bsc" {
		normalizedChain = "tron"
	}
	pageData := paginate(normalizedChain, page, pageSize, tronFile, bscFile)

	return State{
		Configured: cfg.TronMnemonic != "" && cfg.BSCMnemonic != "",
		Config:     cfg,
		Job:        job,
		Tron:       buildSummary(tronFile, job, tronSyncRunning),
		BSC:        buildSummary(bscFile, job, bscSyncRunning),
		Page:       pageData,
	}, nil
}

func (s *Service) runFullSync() {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.finishWithError(fmt.Errorf("panic: %v", recovered))
		}
	}()

	cfg, err := s.loadConfig()
	if err != nil {
		s.finishWithError(err)
		return
	}
	if cfg.TronMnemonic == "" || cfg.BSCMnemonic == "" {
		s.finishWithError(fmt.Errorf("请先保存 tron 和 bsc 助记词"))
		return
	}
	if s.repo != nil {
		if err := s.runFullSyncToDB(cfg); err != nil {
			s.finishWithError(err)
			return
		}
		s.mu.Lock()
		s.job.Running = false
		s.job.Stage = "done"
		s.job.Message = "本次分批地址生成完成"
		s.job.Current = s.job.Total
		s.job.UpdatedAt = nowString()
		s.job.FinishedAt = nowString()
		s.mu.Unlock()
		return
	}

	tronFile, err := s.ensureTronFile(cfg)
	if err != nil {
		s.finishWithError(err)
		return
	}
	s.setProgress("generate", "tron", len(tronFile.Addresses), "Tron 地址生成完成")

	bscFile, err := s.ensureBSCFile(cfg)
	if err != nil {
		s.finishWithError(err)
		return
	}
	s.setProgress("generate", "bsc", len(tronFile.Addresses)+len(bscFile.Addresses), "BSC 地址生成完成")

	s.mu.Lock()
	s.job.Running = false
	s.job.Stage = "done"
	s.job.Message = "地址文件生成完成"
	s.job.Current = s.job.Total
	s.job.UpdatedAt = nowString()
	s.job.FinishedAt = nowString()
	s.mu.Unlock()
}

func (s *Service) runScheduledSync(ctx context.Context, chain string) error {
	defer func() {
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
	}()

	if s.repo != nil {
		return s.runScheduledSyncFromDB(ctx, chain)
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return err
	}

	switch chain {
	case "tron":
		if cfg.TronMnemonic == "" {
			return nil
		}
		if s.tronClient == nil {
			return nil
		}

		hdBlockSyncState.tronMu.Lock()
		defer hdBlockSyncState.tronMu.Unlock()

		file, err := s.ensureTronFile(cfg)
		if err != nil {
			return err
		}
		if err := s.refreshTronBalances(ctx, file, 0, true); err != nil {
			return err
		}
		file.LastScheduledSyncAt = nowString()
		if err := s.writeJSON(s.chainPath("tron"), file); err != nil {
			return err
		}
	case "bsc":
		if cfg.BSCMnemonic == "" {
			return nil
		}
		if s.bscClient == nil {
			return nil
		}

		hdBlockSyncState.bscMu.Lock()
		defer hdBlockSyncState.bscMu.Unlock()

		file, err := s.ensureBSCFile(cfg)
		if err != nil {
			return err
		}
		if err := s.refreshBSCBalances(ctx, file, 0); err != nil {
			return err
		}
		file.LastScheduledSyncAt = nowString()
		if err := s.writeJSON(s.chainPath("bsc"), file); err != nil {
			return err
		}
	default:
		return nil
	}

	return nil
}

func (s *Service) refreshTronBalances(ctx context.Context, file *ChainFile, progressBase int, activatedOnly bool) error {
	for i := range file.Addresses {
		record := &file.Addresses[i]

		active, trxBalance, err := s.tronClient.GetAccountState(ctx, record.AddressHex)
		if err != nil {
			return fmt.Errorf("读取 tron 第 %d 个地址 trx 余额失败: %w", i, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}
		usdtBalance := decimal.Zero
		if active {
			usdtBalance, err = s.tronClient.GetUSDTBalance(ctx, record.AddressHex)
			if err != nil {
				return fmt.Errorf("读取 tron 第 %d 个地址 usdt 余额失败: %w", i, err)
			}
			if err := s.waitForBalanceThrottle(ctx); err != nil {
				return err
			}
		}

		record.TRXBalance = trxBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
		if !activatedOnly || active || record.TRXBalance != "" || record.USDTBalance != "" {
			record.UpdatedAt = nowString()
		}

		current := progressBase + i + 1
		s.setProgress("balance", "tron", current, fmt.Sprintf("Tron 余额刷新中 %d/%d", i+1, len(file.Addresses)))
		if (i+1)%50 == 0 || i == len(file.Addresses)-1 {
			file.BalanceUpdatedAt = nowString()
			if err := s.writeJSON(s.chainPath("tron"), file); err != nil {
				return err
			}
		}
	}
	file.BalanceUpdatedAt = nowString()
	return s.writeJSON(s.chainPath("tron"), file)
}

func (s *Service) refreshBSCBalances(ctx context.Context, file *ChainFile, progressBase int) error {
	for i := range file.Addresses {
		record := &file.Addresses[i]

		bnbBalance, err := s.bscClient.GetBNBBalance(ctx, record.Address)
		if err != nil {
			return fmt.Errorf("读取 bsc 第 %d 个地址 bnb 余额失败: %w", i, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}
		usdtBalance, err := s.bscClient.GetUSDTBalance(ctx, record.Address)
		if err != nil {
			return fmt.Errorf("读取 bsc 第 %d 个地址 usdt 余额失败: %w", i, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}

		record.BNBBalance = bnbBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
		record.UpdatedAt = nowString()

		current := progressBase + i + 1
		s.setProgress("balance", "bsc", current, fmt.Sprintf("BSC 余额刷新中 %d/%d", i+1, len(file.Addresses)))
		if (i+1)%50 == 0 || i == len(file.Addresses)-1 {
			file.BalanceUpdatedAt = nowString()
			if err := s.writeJSON(s.chainPath("bsc"), file); err != nil {
				return err
			}
		}
	}
	file.BalanceUpdatedAt = nowString()
	return s.writeJSON(s.chainPath("bsc"), file)
}

func (s *Service) loadConfig() (ConfigFile, error) {
	if s.repo != nil {
		return s.loadConfigFromDB()
	}
	var cfg ConfigFile
	err := s.readJSON(s.configPath(), &cfg)
	if errors.Is(err, os.ErrNotExist) {
		return defaultConfigFile(s.count), nil
	}
	if err != nil {
		return cfg, err
	}
	cfg, err = s.decryptConfig(cfg)
	if err != nil {
		return cfg, err
	}
	cfg = applyConfigDefaults(cfg, s.count)
	return cfg, nil
}

func (s *Service) loadChainFile(chain string) (ChainFile, error) {
	var data ChainFile
	err := s.readJSON(s.chainPath(chain), &data)
	if errors.Is(err, os.ErrNotExist) {
		return ChainFile{Chain: chain}, nil
	}
	return data, err
}

func (s *Service) beginJob(stage, chain string, total int, message string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.Running {
		return false
	}
	s.job = JobState{
		Running:   true,
		Stage:     stage,
		Chain:     chain,
		Message:   message,
		StartedAt: nowString(),
		UpdatedAt: nowString(),
		Total:     total,
	}
	return true
}

func (s *Service) setProgress(stage, chain string, current int, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.job.Running {
		return
	}
	if s.job.Total > 0 && current > s.job.Total {
		current = s.job.Total
	}
	s.job.Stage = stage
	s.job.Chain = chain
	s.job.Current = current
	s.job.Message = message
	s.job.UpdatedAt = nowString()
}

func (s *Service) finishWithError(err error) {
	s.mu.Lock()
	stage := s.job.Stage
	chain := s.job.Chain
	current := s.job.Current
	total := s.job.Total
	s.job.Running = false
	s.job.Stage = "failed"
	s.job.LastError = err.Error()
	s.job.Message = err.Error()
	s.job.UpdatedAt = nowString()
	s.job.FinishedAt = nowString()
	s.mu.Unlock()
	log.Printf("hd wallet job failed: stage=%s chain=%s progress=%d/%d err=%v", stage, chain, current, total, err)
}

func (s *Service) finishSuccess(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job.Running = false
	s.job.Stage = "done"
	s.job.LastError = ""
	s.job.Message = message
	s.job.Current = s.job.Total
	s.job.UpdatedAt = nowString()
	s.job.FinishedAt = nowString()
}

func (s *Service) finishSuccessWithLastError(message, lastError string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job.Running = false
	s.job.Stage = "done"
	s.job.LastError = strings.TrimSpace(lastError)
	s.job.Message = message
	s.job.Current = s.job.Total
	s.job.UpdatedAt = nowString()
	s.job.FinishedAt = nowString()
}

func (s *Service) finishSkipped(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job.Running = false
	s.job.Stage = "skipped"
	s.job.Message = message
	s.job.UpdatedAt = nowString()
	s.job.FinishedAt = nowString()
}

func (s *Service) logSweepAddressError(chain, address string, current, total int, err error) {
	log.Printf("hd wallet sweep address failed: chain=%s address=%s progress=%d/%d err=%v", chain, address, current, total, err)
}

func (s *Service) configPath() string {
	return filepath.Join(s.dataDir, "config.json")
}

func (s *Service) chainPath(chain string) string {
	return filepath.Join(s.dataDir, chain+".json")
}

func (s *Service) ensureTronFile(cfg ConfigFile) (*ChainFile, error) {
	file, err := s.loadChainFile("tron")
	if err != nil {
		return nil, err
	}
	if len(file.Addresses) == cfg.Count && cfg.Count > 0 {
		return &file, nil
	}
	records, err := DeriveTronAddresses(cfg.TronMnemonic, cfg.Count)
	if err != nil {
		return nil, err
	}
	file = ChainFile{
		Chain:       "tron",
		Count:       len(records),
		GeneratedAt: nowString(),
		Addresses:   records,
	}
	if err := s.writeJSON(s.chainPath("tron"), file); err != nil {
		return nil, err
	}
	return &file, nil
}

func (s *Service) ensureBSCFile(cfg ConfigFile) (*ChainFile, error) {
	file, err := s.loadChainFile("bsc")
	if err != nil {
		return nil, err
	}
	if len(file.Addresses) == cfg.Count && cfg.Count > 0 {
		return &file, nil
	}
	records, err := DeriveBSCAddresses(cfg.BSCMnemonic, cfg.Count)
	if err != nil {
		return nil, err
	}
	file = ChainFile{
		Chain:       "bsc",
		Count:       len(records),
		GeneratedAt: nowString(),
		Addresses:   records,
	}
	if err := s.writeJSON(s.chainPath("bsc"), file); err != nil {
		return nil, err
	}
	return &file, nil
}

func (s *Service) keyPath() string {
	return filepath.Join(s.dataDir, ".config.key")
}

func (s *Service) writeJSON(path string, payload any) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func (s *Service) readJSON(path string, out any) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("unmarshal %s: %w", path, err)
	}
	return nil
}

func (s *Service) encryptConfig(cfg ConfigFile) (ConfigFile, error) {
	tronMnemonic, err := s.encryptString(cfg.TronMnemonic)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("encrypt tron mnemonic: %w", err)
	}
	bscMnemonic, err := s.encryptString(cfg.BSCMnemonic)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("encrypt bsc mnemonic: %w", err)
	}
	cfg.TronMnemonic = tronMnemonic
	cfg.BSCMnemonic = bscMnemonic
	return cfg, nil
}

func (s *Service) decryptConfig(cfg ConfigFile) (ConfigFile, error) {
	var err error
	cfg.TronMnemonic, err = s.decryptString(cfg.TronMnemonic)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("decrypt tron mnemonic: %w", err)
	}
	cfg.BSCMnemonic, err = s.decryptString(cfg.BSCMnemonic)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("decrypt bsc mnemonic: %w", err)
	}
	return cfg, nil
}

func (s *Service) encryptString(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	if strings.HasPrefix(value, encryptedPrefix) {
		return value, nil
	}

	key, err := s.loadOrCreateKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	cipherText := gcm.Seal(nil, nonce, []byte(value), nil)
	payload := append(nonce, cipherText...)
	return encryptedPrefix + base64.StdEncoding.EncodeToString(payload), nil
}

func (s *Service) decryptString(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, encryptedPrefix) {
		return value, nil
	}

	key, err := s.loadOrCreateKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, encryptedPrefix))
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("encrypted payload too short")
	}
	nonce := raw[:gcm.NonceSize()]
	cipherText := raw[gcm.NonceSize():]
	plainText, err := gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return "", err
	}
	return string(plainText), nil
}

func (s *Service) loadOrCreateKey() ([]byte, error) {
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	data, err := os.ReadFile(s.keyPath())
	if err == nil {
		trimmed := strings.TrimSpace(string(data))
		decoded, err := base64.StdEncoding.DecodeString(trimmed)
		if err != nil {
			return nil, fmt.Errorf("decode key file: %w", err)
		}
		if len(decoded) != 32 {
			return nil, fmt.Errorf("invalid key length")
		}
		return decoded, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	seed := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, seed); err != nil {
		return nil, fmt.Errorf("generate key seed: %w", err)
	}
	sum := sha256.Sum256(seed)
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	if err := os.WriteFile(s.keyPath(), []byte(encoded), 0o600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}
	return sum[:], nil
}

func paginate(chain string, page, pageSize int, tronFile, bscFile ChainFile) PageData {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if page <= 0 {
		page = 1
	}

	source := tronFile
	if chain == "bsc" {
		source = bscFile
	}
	totalCount := len(source.Addresses)
	totalPages := 0
	if totalCount > 0 {
		totalPages = (totalCount + pageSize - 1) / pageSize
	}
	if totalPages == 0 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * pageSize
	if start > totalCount {
		start = totalCount
	}
	end := start + pageSize
	if end > totalCount {
		end = totalCount
	}
	items := make([]AddressRecord, 0, end-start)
	if start < end {
		items = append(items, source.Addresses[start:end]...)
	}

	return PageData{
		Chain:      chain,
		Items:      items,
		Page:       page,
		PageSize:   pageSize,
		TotalCount: totalCount,
		TotalPages: totalPages,
	}
}

func defaultConfigFile(count int) ConfigFile {
	return ConfigFile{
		Count:             count,
		TronUSDTThreshold: "10",
		BSCUSDTThreshold:  "10",
	}
}

func applyConfigDefaults(cfg ConfigFile, count int) ConfigFile {
	defaults := defaultConfigFile(count)
	if cfg.Count == 0 {
		cfg.Count = defaults.Count
	}
	if strings.TrimSpace(cfg.TronUSDTThreshold) == "" {
		cfg.TronUSDTThreshold = defaults.TronUSDTThreshold
	}
	if strings.TrimSpace(cfg.BSCUSDTThreshold) == "" {
		cfg.BSCUSDTThreshold = defaults.BSCUSDTThreshold
	}
	return cfg
}

func normalizeThreshold(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "10"
	}
	return trimmed
}

func parseThresholdDecimal(value string) (decimal.Decimal, error) {
	parsed, err := decimal.NewFromString(normalizeThreshold(value))
	if err != nil {
		return decimal.Zero, err
	}
	if parsed.IsNegative() {
		return decimal.Zero, fmt.Errorf("threshold must be >= 0")
	}
	return parsed, nil
}

func (s *Service) waitForBalanceThrottle(ctx context.Context) error {
	delay := s.balanceRequestDelay
	if delay <= 0 {
		delay = defaultBalanceRequestDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func buildSummary(file ChainFile, job JobState, chainSyncRunning bool) Summary {
	summary := Summary{
		Count:               len(file.Addresses),
		LastScheduledSyncAt: file.LastScheduledSyncAt,
		LastScannedBlock:    file.LastScannedBlock,
		LatestChainBlock:    file.LatestChainBlock,
	}
	var trxTotal decimal.Decimal
	var usdtTotal decimal.Decimal
	var bnbTotal decimal.Decimal

	for _, item := range file.Addresses {
		if item.TRXBalance != "" {
			if value, err := decimal.NewFromString(item.TRXBalance); err == nil {
				trxTotal = trxTotal.Add(value)
			}
		}
		if item.USDTBalance != "" {
			if value, err := decimal.NewFromString(item.USDTBalance); err == nil {
				usdtTotal = usdtTotal.Add(value)
			}
		}
		if item.BNBBalance != "" {
			if value, err := decimal.NewFromString(item.BNBBalance); err == nil {
				bnbTotal = bnbTotal.Add(value)
			}
		}
		if item.UpdatedAt > summary.LastUpdatedAt {
			summary.LastUpdatedAt = item.UpdatedAt
		}
	}

	summary.TRXTotal = trxTotal.StringFixed(6)
	summary.USDTTotal = usdtTotal.StringFixed(6)
	summary.BNBTotal = bnbTotal.StringFixed(6)
	summary.ScheduledRunning = chainSyncRunning
	summary.SyncLag = file.LatestChainBlock - file.LastScannedBlock
	if summary.SyncLag < 0 {
		summary.SyncLag = 0
	}
	return summary
}

func nowString() string {
	return time.Now().UTC().Add(8 * time.Hour).Format("2006-01-02 15:04:05")
}

func scheduledMessage(chain string) string {
	switch chain {
	case "tron":
		return "定时同步 Tron TRX/USDT 余额"
	case "bsc":
		return "定时同步 BSC BNB/USDT 余额"
	default:
		return "定时同步余额"
	}
}

func scheduledDoneMessage(chain string) string {
	switch chain {
	case "tron":
		return "Tron TRX/USDT 定时同步完成"
	case "bsc":
		return "BSC BNB/USDT 定时同步完成"
	default:
		return "定时同步完成"
	}
}

func (s *Service) setChainSyncRunning(chain string, running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch chain {
	case "tron":
		s.tronSyncRunning = running
	case "bsc":
		s.bscSyncRunning = running
	}
}

func (s *Service) setChainLastScheduledSyncAt(chain, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch chain {
	case "tron":
		s.tronLastScheduledSyncAt = value
	case "bsc":
		s.bscLastScheduledSyncAt = value
	}
}
