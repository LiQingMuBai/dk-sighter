package web

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

type listBalancesResponse struct {
	Success    bool              `json:"success"`
	Message    string            `json:"message"`
	TotalCount int               `json:"total_count,omitempty"`
	Records    []balanceRecordVM `json:"records,omitempty"`
}

type balanceRecordVM struct {
	Address   string `json:"address"`
	TRX       string `json:"trx,omitempty"`
	BNB       string `json:"bnb,omitempty"`
	USDT      string `json:"usdt,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type listTransfersResponse struct {
	Success    bool               `json:"success"`
	Message    string             `json:"message"`
	TotalCount int                `json:"total_count,omitempty"`
	Records    []transferRecordVM `json:"records,omitempty"`
}

type transferRecordVM struct {
	TxHash          string `json:"tx_hash"`
	BlockNumber     int64  `json:"block_number"`
	BlockTime       int64  `json:"block_time"`
	AssetCode       string `json:"asset_code"`
	ContractAddress string `json:"contract_address,omitempty"`
	WatchAddress    string `json:"watch_address"`
	FromAddress     string `json:"from_address"`
	ToAddress       string `json:"to_address"`
	Amount          string `json:"amount"`
	LogIndex        int    `json:"log_index"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
}

func (s *Server) handleTronBalancesAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, listBalancesResponse{Success: false, Message: "unauthorized"})
		return
	}

	address := strings.TrimSpace(r.URL.Query().Get("address"))
	if address != "" {
		if _, err := tron.Base58ToHex(address); err != nil {
			s.writeJSON(w, http.StatusBadRequest, listBalancesResponse{Success: false, Message: "invalid address"})
			return
		}

		row, ok, err := s.repo.GetDashboardRowByAddress(r.Context(), address)
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, listBalancesResponse{Success: false, Message: "load balances failed"})
			return
		}
		if !ok {
			s.writeJSON(w, http.StatusOK, listBalancesResponse{Success: true, Message: "ok", TotalCount: 0, Records: []balanceRecordVM{}})
			return
		}

		updated := ""
		if row.LastUpdatedAt.Valid {
			updated = row.LastUpdatedAt.Time.Format(time.RFC3339)
		}
		s.writeJSON(w, http.StatusOK, listBalancesResponse{
			Success:    true,
			Message:    "ok",
			TotalCount: 1,
			Records: []balanceRecordVM{{
				Address:   row.AddressBase58,
				TRX:       row.TRXBalance.String(),
				USDT:      row.USDTBalance.String(),
				UpdatedAt: updated,
			}},
		})
		return
	}

	limit := parseAPIInt(r.URL.Query().Get("limit"), 20, 1, 200)
	offset := parseAPIInt(r.URL.Query().Get("offset"), 0, 0, 1<<30)
	sort := parseDashboardSort(r.URL.Query().Get("sort"))

	result, err := s.repo.ListDashboardRows(r.Context(), offset, limit, sort)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, listBalancesResponse{Success: false, Message: "load balances failed"})
		return
	}

	records := make([]balanceRecordVM, 0, len(result.Rows))
	for _, row := range result.Rows {
		updated := ""
		if row.LastUpdatedAt.Valid {
			updated = row.LastUpdatedAt.Time.Format(time.RFC3339)
		}
		records = append(records, balanceRecordVM{
			Address:   row.AddressBase58,
			TRX:       row.TRXBalance.String(),
			USDT:      row.USDTBalance.String(),
			UpdatedAt: updated,
		})
	}

	s.writeJSON(w, http.StatusOK, listBalancesResponse{
		Success:    true,
		Message:    "ok",
		TotalCount: result.TotalCount,
		Records:    records,
	})
}

func (s *Server) handleBSCDashboardBalancesAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, listBalancesResponse{Success: false, Message: "unauthorized"})
		return
	}

	address := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("address")))
	if address != "" {
		if !isValidBSCAddress(address) {
			s.writeJSON(w, http.StatusBadRequest, listBalancesResponse{Success: false, Message: "invalid address"})
			return
		}

		row, ok, err := repository.GetBSCDashboardRecordByAddress(r.Context(), s.repo, address)
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, listBalancesResponse{Success: false, Message: "load balances failed"})
			return
		}
		if !ok {
			s.writeJSON(w, http.StatusOK, listBalancesResponse{Success: true, Message: "ok", TotalCount: 0, Records: []balanceRecordVM{}})
			return
		}

		updated := ""
		if !row.UpdatedAt.IsZero() {
			updated = row.UpdatedAt.Format(time.RFC3339)
		}
		s.writeJSON(w, http.StatusOK, listBalancesResponse{
			Success:    true,
			Message:    "ok",
			TotalCount: 1,
			Records: []balanceRecordVM{{
				Address:   row.Address,
				BNB:       row.BNB,
				USDT:      row.USDT,
				UpdatedAt: updated,
			}},
		})
		return
	}

	limit := parseAPIInt(r.URL.Query().Get("limit"), 20, 1, 200)
	offset := parseAPIInt(r.URL.Query().Get("offset"), 0, 0, 1<<30)
	sort := parseBSCDashboardSort(r.URL.Query().Get("sort"))

	total, err := repository.CountActiveBSCWatchAddresses(r.Context(), s.repo)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, listBalancesResponse{Success: false, Message: "load balances failed"})
		return
	}

	rows, err := repository.ListBSCDashboardRecords(r.Context(), s.repo, limit, offset, sort)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, listBalancesResponse{Success: false, Message: "load balances failed"})
		return
	}

	records := make([]balanceRecordVM, 0, len(rows))
	for _, row := range rows {
		updated := ""
		if !row.UpdatedAt.IsZero() {
			updated = row.UpdatedAt.Format(time.RFC3339)
		}
		records = append(records, balanceRecordVM{
			Address:   row.Address,
			BNB:       row.BNB,
			USDT:      row.USDT,
			UpdatedAt: updated,
		})
	}

	s.writeJSON(w, http.StatusOK, listBalancesResponse{
		Success:    true,
		Message:    "ok",
		TotalCount: total,
		Records:    records,
	})
}

func (s *Server) handleTronTransfersInAPI(w http.ResponseWriter, r *http.Request) {
	s.handleTronTransfersAPI(w, r, true)
}

func (s *Server) handleTronTransfersOutAPI(w http.ResponseWriter, r *http.Request) {
	s.handleTronTransfersAPI(w, r, false)
}

func (s *Server) handleTronTransfersAPI(w http.ResponseWriter, r *http.Request, inbound bool) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, listTransfersResponse{Success: false, Message: "unauthorized"})
		return
	}

	watchAddress := strings.TrimSpace(r.URL.Query().Get("watch_address"))
	if watchAddress == "" {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "watch_address required"})
		return
	}
	if _, err := tron.Base58ToHex(watchAddress); err != nil {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid watch_address"})
		return
	}

	assetCode := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asset_code")))
	limit := parseAPIInt(r.URL.Query().Get("limit"), 20, 1, 200)
	offset := parseAPIInt(r.URL.Query().Get("offset"), 0, 0, 1<<30)
	startTimeMs, err := parseAPITimeMillis(r.URL.Query().Get("start_time"))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid start_time"})
		return
	}
	endTimeMs, err := parseAPITimeMillis(r.URL.Query().Get("end_time"))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid end_time"})
		return
	}
	if startTimeMs > 0 && endTimeMs > 0 && endTimeMs < startTimeMs {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "end_time must be >= start_time"})
		return
	}

	var result *repository.TransferListResult
	var queryErr error
	if inbound {
		result, queryErr = s.repo.ListTransferInRecords(r.Context(), watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
	} else {
		result, queryErr = s.repo.ListTransferOutRecords(r.Context(), watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
	}
	if queryErr != nil {
		s.writeJSON(w, http.StatusInternalServerError, listTransfersResponse{Success: false, Message: "load transfers failed"})
		return
	}

	records := make([]transferRecordVM, 0, len(result.Records))
	for _, rec := range result.Records {
		records = append(records, convertTransferRecordVM(rec))
	}

	s.writeJSON(w, http.StatusOK, listTransfersResponse{
		Success:    true,
		Message:    "ok",
		TotalCount: result.TotalCount,
		Records:    records,
	})
}

func (s *Server) handleBSCTransfersInAPI(w http.ResponseWriter, r *http.Request) {
	s.handleBSCTransfersAPI(w, r, true)
}

func (s *Server) handleBSCTransfersOutAPI(w http.ResponseWriter, r *http.Request) {
	s.handleBSCTransfersAPI(w, r, false)
}

func (s *Server) handleBSCTransfersAPI(w http.ResponseWriter, r *http.Request, inbound bool) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, listTransfersResponse{Success: false, Message: "unauthorized"})
		return
	}

	watchAddress := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("watch_address")))
	if watchAddress == "" {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "watch_address required"})
		return
	}
	if !isValidBSCAddress(watchAddress) {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid watch_address"})
		return
	}

	assetCode := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asset_code")))
	limit := parseAPIInt(r.URL.Query().Get("limit"), 20, 1, 200)
	offset := parseAPIInt(r.URL.Query().Get("offset"), 0, 0, 1<<30)
	direction := "out"
	if inbound {
		direction = "in"
	}
	log.Printf("bsc transfer records api request: direction=%s watch_address=%s asset_code=%s limit=%d offset=%d", direction, watchAddress, assetCode, limit, offset)
	startTimeMs, err := parseAPITimeMillis(r.URL.Query().Get("start_time"))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid start_time"})
		return
	}
	endTimeMs, err := parseAPITimeMillis(r.URL.Query().Get("end_time"))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "invalid end_time"})
		return
	}
	if startTimeMs > 0 && endTimeMs > 0 && endTimeMs < startTimeMs {
		s.writeJSON(w, http.StatusBadRequest, listTransfersResponse{Success: false, Message: "end_time must be >= start_time"})
		return
	}

	var result *repository.TransferListResult
	var queryErr error
	if inbound {
		result, queryErr = s.repo.ListBSCTransferInRecords(r.Context(), watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
	} else {
		result, queryErr = s.repo.ListBSCTransferOutRecords(r.Context(), watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
	}
	if queryErr != nil {
		log.Printf("bsc transfer records api query failed: direction=%s watch_address=%s asset_code=%s err=%v", direction, watchAddress, assetCode, queryErr)
		s.writeJSON(w, http.StatusInternalServerError, listTransfersResponse{Success: false, Message: "load transfers failed"})
		return
	}
	log.Printf("bsc transfer records api result: direction=%s watch_address=%s asset_code=%s total=%d returned=%d", direction, watchAddress, assetCode, result.TotalCount, len(result.Records))

	records := make([]transferRecordVM, 0, len(result.Records))
	for _, rec := range result.Records {
		records = append(records, convertTransferRecordVM(rec))
	}

	s.writeJSON(w, http.StatusOK, listTransfersResponse{
		Success:    true,
		Message:    "ok",
		TotalCount: result.TotalCount,
		Records:    records,
	})
}

func convertTransferRecordVM(rec repository.TransferListRecord) transferRecordVM {
	contract := ""
	if rec.ContractAddress.Valid {
		contract = rec.ContractAddress.String
	}
	return transferRecordVM{
		TxHash:          rec.TxHash,
		BlockNumber:     rec.BlockNumber,
		BlockTime:       rec.BlockTime,
		AssetCode:       rec.AssetCode,
		ContractAddress: contract,
		WatchAddress:    rec.WatchAddress,
		FromAddress:     rec.FromAddress,
		ToAddress:       rec.ToAddress,
		Amount:          rec.Amount.String(),
		LogIndex:        rec.LogIndex,
		Status:          rec.Status,
		CreatedAt:       rec.CreatedAt.Format(time.RFC3339),
	}
}

func parseAPIInt(input string, fallback, min, max int) int {
	value, err := strconv.Atoi(strings.TrimSpace(input))
	if err != nil {
		return fallback
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func parseAPITimeMillis(input string) (int64, error) {
	text := strings.TrimSpace(input)
	if text == "" {
		return 0, nil
	}
	if strings.Contains(text, "T") || strings.Contains(text, "-") {
		value, err := time.Parse(time.RFC3339, text)
		if err != nil {
			return 0, err
		}
		return value.UnixMilli(), nil
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}
