package web

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"

	"tron_watcher/internal/repository"
)

var manualBSCGasTransferAmount = decimal.RequireFromString("0.001")

type bscDashboardPageData struct {
	GeneratedAt  string
	Records      []bscDashboardRecordView
	Page         int
	PageSize     int
	Total        int
	TotalPages   int
	HasPrev      bool
	HasNext      bool
	PrevPage     int
	NextPage     int
	Sort         string
	AddressQuery string
}

type bscDashboardRecordView struct {
	Address   string
	BNB       string
	USDT      string
	UpdatedAt string
}

type bscDeleteAddressesRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type bscAddWatchAddressesRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type bscAddWatchAddressesResponse struct {
	Success            bool     `json:"success"`
	Message            string   `json:"message"`
	Count              int      `json:"count"`
	Addresses          []string `json:"addresses,omitempty"`
	DuplicateAddresses []string `json:"duplicate_addresses,omitempty"`
	InvalidAddresses   []string `json:"invalid_addresses,omitempty"`
}

type bscDeleteAddressesResponse struct {
	Success      bool     `json:"success"`
	Message      string   `json:"message"`
	Count        int64    `json:"count"`
	Addresses    []string `json:"addresses,omitempty"`
	DeletedCount int64    `json:"deleted_count"`
}

type bscTransferGasResponse struct {
	Success         bool     `json:"success"`
	Message         string   `json:"message"`
	Address         string   `json:"address,omitempty"`
	Addresses       []string `json:"addresses,omitempty"`
	TxHash          string   `json:"tx_hash,omitempty"`
	TotalCount      int      `json:"total_count,omitempty"`
	SuccessCount    int      `json:"success_count,omitempty"`
	FailedCount     int      `json:"failed_count,omitempty"`
	FailedAddresses []string `json:"failed_addresses,omitempty"`
}

type bscTransferGasRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

func (s *Server) handleBSCDeleteAddresses(w http.ResponseWriter, r *http.Request) {
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req bscDeleteAddressesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, bscDeleteAddressesResponse{
			Success: false,
			Message: "请求参数格式错误",
		})
		return
	}

	addresses := make([]string, 0, len(req.Addresses)+1)
	if req.Address != "" {
		addresses = append(addresses, req.Address)
	}
	addresses = append(addresses, req.Addresses...)
	addresses = uniqueNonEmptyBSCStrings(addresses)
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, bscDeleteAddressesResponse{
			Success: false,
			Message: "请先选择要删除的地址",
		})
		return
	}

	affected, err := repository.SoftDeleteBSCWatchAddresses(r.Context(), s.repo, addresses)
	if err != nil {
		log.Printf("bsc delete addresses failed: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, bscDeleteAddressesResponse{
			Success:   false,
			Message:   "删除地址失败",
			Addresses: addresses,
		})
		return
	}

	log.Printf("bsc delete addresses: total=%d affected=%d", len(addresses), affected)
	s.writeJSON(w, http.StatusOK, bscDeleteAddressesResponse{
		Success:      true,
		Message:      "删除成功",
		Count:        affected,
		Addresses:    addresses,
		DeletedCount: affected,
	})
}

func (s *Server) handleBSCAddWatchAddresses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, bscAddWatchAddressesResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}

	var req bscAddWatchAddressesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, bscAddWatchAddressesResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	addresses, invalid := normalizeBSCWatchAddresses(req)
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, bscAddWatchAddressesResponse{
			Success:          false,
			Message:          "no valid addresses",
			InvalidAddresses: invalid,
		})
		return
	}

	existing, err := repository.FindExistingBSCWatchAddresses(r.Context(), s.repo, addresses)
	if err != nil {
		log.Printf("find existing bsc watch addresses failed: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, bscAddWatchAddressesResponse{
			Success:          false,
			Message:          "check addresses failed",
			Addresses:        addresses,
			InvalidAddresses: invalid,
		})
		return
	}

	toInsert := make([]string, 0, len(addresses))
	duplicates := make([]string, 0)
	for _, address := range addresses {
		if _, ok := existing[address]; ok {
			duplicates = append(duplicates, address)
			continue
		}
		toInsert = append(toInsert, address)
	}

	if len(duplicates) > 0 {
		log.Printf("duplicate bsc watch addresses ignored: %s", strings.Join(duplicates, ","))
	}

	if err := repository.InsertBSCWatchAddresses(r.Context(), s.repo, toInsert); err != nil {
		log.Printf("insert bsc watch addresses failed: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, bscAddWatchAddressesResponse{
			Success:            false,
			Message:            "save addresses failed",
			Addresses:          toInsert,
			DuplicateAddresses: duplicates,
			InvalidAddresses:   invalid,
		})
		return
	}

	if s.reloader != nil && len(toInsert) > 0 {
		if err := s.reloader.Reload(r.Context()); err != nil {
			log.Printf("reload bsc address cache failed after api insert: %v", err)
			s.writeJSON(w, http.StatusInternalServerError, bscAddWatchAddressesResponse{
				Success:            false,
				Message:            "addresses saved but cache reload failed",
				Count:              len(toInsert),
				Addresses:          toInsert,
				DuplicateAddresses: duplicates,
				InvalidAddresses:   invalid,
			})
			return
		}
	}

	if s.bscBalances != nil && len(toInsert) > 0 {
		refreshCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		s.bscBalances.RefreshAddresses(refreshCtx, toInsert)
	}

	s.writeJSON(w, http.StatusOK, bscAddWatchAddressesResponse{
		Success:            true,
		Message:            "ok",
		Count:              len(toInsert),
		Addresses:          toInsert,
		DuplicateAddresses: duplicates,
		InvalidAddresses:   invalid,
	})
}

func (s *Server) handleBSCRefreshAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, refreshAddressResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}
	if s.bscBalances == nil {
		s.writeJSON(w, http.StatusInternalServerError, refreshAddressResponse{
			Success: false,
			Message: "bsc balance refresher not configured",
		})
		return
	}

	var req refreshAddressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, refreshAddressResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	address := strings.TrimSpace(req.Address)
	if address == "" {
		s.writeJSON(w, http.StatusBadRequest, refreshAddressResponse{
			Success: false,
			Message: "address is required",
		})
		return
	}

	refreshCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	s.bscBalances.RefreshAddresses(refreshCtx, []string{address})

	s.writeJSON(w, http.StatusOK, refreshAddressResponse{
		Success: true,
		Message: "BSC 地址余额更新成功",
		Address: address,
	})
}

func (s *Server) handleBSCTransferGas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, bscTransferGasResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}
	if s.bscClient == nil {
		s.writeJSON(w, http.StatusInternalServerError, bscTransferGasResponse{
			Success: false,
			Message: "bsc rpc 未配置",
		})
		return
	}
	if strings.TrimSpace(s.bscGasTopupPrivateKey) == "" {
		s.writeJSON(w, http.StatusInternalServerError, bscTransferGasResponse{
			Success: false,
			Message: "未配置 bsc gas 补充私钥",
		})
		return
	}

	var req bscTransferGasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, bscTransferGasResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	addresses := uniqueNonEmptyBSCStrings(append(append(make([]string, 0, len(req.Addresses)+1), req.Address), req.Addresses...))
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusNotFound, bscTransferGasResponse{
			Success: false,
			Message: "address is required",
		})
		return
	}
	for _, address := range addresses {
		if !isValidBSCAddress(address) {
			s.writeJSON(w, http.StatusBadRequest, bscTransferGasResponse{
				Success: false,
				Message: "invalid bsc address",
				Address: address,
			})
			return
		}
	}

	if len(addresses) == 1 {
		txHash, err := s.transferBSCGasToAddress(r.Context(), addresses[0])
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, bscTransferGasResponse{
				Success: false,
				Message: "转手续费失败: " + err.Error(),
				Address: addresses[0],
			})
			return
		}

		if s.bscBalances != nil {
			refreshCtx, refreshCancel := context.WithTimeout(r.Context(), 20*time.Second)
			defer refreshCancel()
			s.bscBalances.RefreshAddresses(refreshCtx, []string{addresses[0]})
		}

		s.writeJSON(w, http.StatusOK, bscTransferGasResponse{
			Success: true,
			Message: "转手续费成功",
			Address: addresses[0],
			TxHash:  txHash,
		})
		return
	}

	successAddresses := make([]string, 0, len(addresses))
	failedAddresses := make([]string, 0)
	for idx, address := range addresses {
		if _, err := s.transferBSCGasToAddress(r.Context(), address); err != nil {
			failedAddresses = append(failedAddresses, address)
			log.Printf("batch transfer bsc gas failed: address=%s err=%v", address, err)
		} else {
			successAddresses = append(successAddresses, address)
		}

		if idx >= len(addresses)-1 {
			continue
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-r.Context().Done():
			timer.Stop()
			s.writeJSON(w, http.StatusRequestTimeout, bscTransferGasResponse{
				Success:         len(successAddresses) > 0,
				Message:         "批量转手续费已中断",
				Addresses:       successAddresses,
				TotalCount:      len(addresses),
				SuccessCount:    len(successAddresses),
				FailedCount:     len(failedAddresses),
				FailedAddresses: failedAddresses,
			})
			return
		case <-timer.C:
		}
	}

	if len(successAddresses) > 0 && s.bscBalances != nil {
		refreshCtx, refreshCancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer refreshCancel()
		s.bscBalances.RefreshAddresses(refreshCtx, successAddresses)
	}

	message := fmt.Sprintf("批量转手续费完成，成功 %d 个，失败 %d 个", len(successAddresses), len(failedAddresses))
	statusCode := http.StatusOK
	if len(successAddresses) == 0 {
		statusCode = http.StatusInternalServerError
		message = "批量转手续费失败"
	}
	s.writeJSON(w, statusCode, bscTransferGasResponse{
		Success:         len(successAddresses) > 0,
		Message:         message,
		Addresses:       successAddresses,
		TotalCount:      len(addresses),
		SuccessCount:    len(successAddresses),
		FailedCount:     len(failedAddresses),
		FailedAddresses: failedAddresses,
	})
}

func (s *Server) transferBSCGasToAddress(ctx context.Context, address string) (string, error) {
	record, ok, err := repository.GetBSCDashboardRecordByAddress(ctx, s.repo, address)
	if err != nil {
		log.Printf("get bsc dashboard record failed: address=%s err=%v", address, err)
		return "", fmt.Errorf("读取地址信息失败")
	}
	if !ok {
		return "", fmt.Errorf("地址不存在或未启用")
	}

	transferCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	fromAddress, keySource, txHash, err := s.sendBSCGasTopup(transferCtx, address, manualBSCGasTransferAmount)
	if err != nil {
		log.Printf("transfer bsc gas failed: address=%s err=%v", address, err)
		_ = insertWebBSCGasLog(ctx, s.repo, address, fromAddress, manualBSCGasTransferAmount.StringFixed(3), record.BNB, record.USDT, "", keySource, "FAILED", map[string]any{
			"address":      address,
			"from_address": fromAddress,
			"transfer_bnb": manualBSCGasTransferAmount.StringFixed(3),
			"current_bnb":  record.BNB,
			"current_usdt": record.USDT,
			"key_source":   keySource,
		}, err.Error())
		return "", err
	}

	_ = insertWebBSCGasLog(ctx, s.repo, address, fromAddress, manualBSCGasTransferAmount.StringFixed(3), record.BNB, record.USDT, txHash, keySource, "SUCCESS", map[string]any{
		"address":      address,
		"from_address": fromAddress,
		"transfer_bnb": manualBSCGasTransferAmount.StringFixed(3),
		"current_bnb":  record.BNB,
		"current_usdt": record.USDT,
		"tx_hash":      txHash,
		"key_source":   keySource,
	}, "")
	return txHash, nil
}

func buildBSCDashboardPageData(records []bscDashboardRecordView, page, pageSize, total int) bscDashboardPageData {
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if page < 1 {
		page = 1
	}
	if totalPages > 0 && page > totalPages {
		page = totalPages
	}
	return bscDashboardPageData{
		Records:    records,
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		HasPrev:    page > 1,
		HasNext:    page < totalPages,
		PrevPage:   maxBSCInt(1, page-1),
		NextPage:   page + 1,
	}
}

func (s *Server) countActiveBSCWatchAddresses(ctx context.Context, addressQuery string) (int, error) {
	return repository.CountActiveBSCWatchAddressesByQuery(ctx, s.repo, addressQuery)
}

func (s *Server) listBSCDashboardRecords(ctx context.Context, limit, offset int, sort repository.BSCDashboardSort, addressQuery string) ([]repository.BSCDashboardRecord, error) {
	return repository.ListBSCDashboardRecordsByQuery(ctx, s.repo, limit, offset, sort, addressQuery)
}

func parsePositiveBSCPage(raw string) int {
	page, err := strconv.Atoi(raw)
	if err != nil || page < 1 {
		return 1
	}
	return page
}

func parseBSCDashboardSort(raw string) repository.BSCDashboardSort {
	switch repository.BSCDashboardSort(strings.TrimSpace(raw)) {
	case repository.BSCDashboardSortUSDTAsc,
		repository.BSCDashboardSortBNBDesc,
		repository.BSCDashboardSortBNBAsc:
		return repository.BSCDashboardSort(strings.TrimSpace(raw))
	default:
		return repository.BSCDashboardSortUSDTDesc
	}
}

func maxBSCInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func uniqueNonEmptyBSCStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeBSCWatchAddresses(req bscAddWatchAddressesRequest) ([]string, []string) {
	raw := make([]string, 0, 1+len(req.Addresses))
	if strings.TrimSpace(req.Address) != "" {
		raw = append(raw, req.Address)
	}
	raw = append(raw, req.Addresses...)

	result := make([]string, 0, len(raw))
	invalid := make([]string, 0)
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		address := strings.ToLower(strings.TrimSpace(item))
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}

		if !isValidBSCAddress(address) {
			invalid = append(invalid, item)
			continue
		}

		seen[address] = struct{}{}
		result = append(result, address)
	}
	return result, invalid
}

func isValidBSCAddress(address string) bool {
	if len(address) != 42 {
		return false
	}
	if !strings.HasPrefix(address, "0x") {
		return false
	}
	for i := 2; i < len(address); i++ {
		c := address[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func (s *Server) sendBSCGasTopup(ctx context.Context, toAddress string, amount decimal.Decimal) (string, string, string, error) {
	privateKey, fromAddress, err := parseWebBSCPrivateKey(s.bscGasTopupPrivateKey)
	if err != nil {
		return "", "config.bsc.gas_transfer_private_key", "", err
	}
	gasPrice, err := s.bscClient.GasPrice(ctx)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("get gas price: %w", err)
	}
	nonce, err := s.bscClient.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("get nonce: %w", err)
	}
	chainID, err := s.bscClient.ChainID(ctx)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("get chain id: %w", err)
	}
	amountWei, err := webDecimalToTokenUnits(amount, 18)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("convert amount to wei: %w", err)
	}
	to := common.HexToAddress(toAddress)
	callObj := map[string]any{
		"from":  fromAddress,
		"to":    toAddress,
		"value": "0x" + amountWei.Text(16),
	}
	gasLimit, err := s.bscClient.EstimateGas(ctx, callObj)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("estimate gas: %w", err)
	}
	gasLimit = gasLimit + gasLimit/5 + 5_000

	tx := ethTypes.NewTx(&ethTypes.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    amountWei,
		Gas:      gasLimit,
		GasPrice: gasPrice,
	})
	signedTx, err := ethTypes.SignTx(tx, ethTypes.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("sign bsc tx: %w", err)
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("marshal bsc tx: %w", err)
	}
	txHash, err := s.bscClient.SendRawTransaction(ctx, hex.EncodeToString(rawTx))
	if err != nil {
		return fromAddress, "config.bsc.gas_transfer_private_key", "", fmt.Errorf("send raw transaction: %w", err)
	}
	return fromAddress, "config.bsc.gas_transfer_private_key", txHash, nil
}

func parseWebBSCPrivateKey(value string) (*ecdsa.PrivateKey, string, error) {
	keyHex := strings.TrimSpace(strings.TrimPrefix(value, "0x"))
	if keyHex == "" {
		return nil, "", fmt.Errorf("empty private key")
	}
	privateKey, err := crypto.HexToECDSA(keyHex)
	if err != nil {
		return nil, "", fmt.Errorf("parse private key: %w", err)
	}
	return privateKey, strings.ToLower(crypto.PubkeyToAddress(privateKey.PublicKey).Hex()), nil
}

func insertWebBSCGasLog(ctx context.Context, repo *repository.DB, address string, fromAddress string, amountBNB string, currentBNB string, currentUSDT string, txHash string, keySource string, status string, payload map[string]any, errMessage string) error {
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

func webDecimalToTokenUnits(amount decimal.Decimal, decimals int32) (*big.Int, error) {
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

func formatBSCDisplayBalance(value string) string {
	parsed, err := decimal.NewFromString(strings.TrimSpace(value))
	if err != nil {
		return strings.TrimSpace(value)
	}
	return parsed.StringFixed(6)
}
