package web

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tron_watcher/internal/repository"
)

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
