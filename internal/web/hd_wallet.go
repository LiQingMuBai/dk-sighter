package web

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

type walletDashboardPageData struct {
	GeneratedAt string
	DesktopMode bool
}

type saveHDWalletConfigRequest struct {
	TronMnemonic      string `json:"tron_mnemonic"`
	BSCMnemonic       string `json:"bsc_mnemonic"`
	TronUSDTThreshold string `json:"tron_usdt_threshold"`
	BSCUSDTThreshold  string `json:"bsc_usdt_threshold"`
}

type hdWalletSweepRequest struct {
	Chain       string `json:"chain"`
	Destination string `json:"destination"`
	Address     string `json:"address"`
}

type hdWalletRefreshAddressRequest struct {
	Chain   string `json:"chain"`
	Address string `json:"address"`
}

type hdWalletBSCGasTopupRequest struct {
	Address string `json:"address"`
}

func (s *Server) handleHDWalletDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.templates.ExecuteTemplate(w, "wallet_dashboard.html", walletDashboardPageData{
		GeneratedAt: formatBeijingTime(time.Now()),
		DesktopMode: s.desktopMode,
	}); err != nil {
		http.Error(w, "render wallet dashboard failed", http.StatusInternalServerError)
		log.Printf("render wallet dashboard failed: %v", err)
	}
}

func (s *Server) handleHDWalletState(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	page := parsePositiveInt(r.URL.Query().Get("page"), 1)
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50)
	chain := strings.TrimSpace(r.URL.Query().Get("chain"))
	if chain == "" {
		chain = "tron"
	}

	state, err := s.walletService.State(chain, page, pageSize)
	if err != nil {
		http.Error(w, "load wallet state failed", http.StatusInternalServerError)
		log.Printf("load wallet state failed: %v", err)
		return
	}
	s.writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleHDWalletConfig(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req saveHDWalletConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	cfg, err := s.walletService.SaveConfig(req.TronMnemonic, req.BSCMnemonic, req.TronUSDTThreshold, req.BSCUSDTThreshold)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "已加密保存在本地文件",
		"config":  cfg,
	})
}

func (s *Server) handleHDWalletSync(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.walletService.StartSync(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.writeJSON(w, http.StatusAccepted, map[string]any{
		"success": true,
		"message": "地址生成任务已启动",
	})
}

func (s *Server) handleHDWalletSweepPreview(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req hdWalletSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	sourceAddress := ""
	if s.desktopMode {
		sourceAddress = req.Address
	}
	preview, err := s.walletService.PreviewSweep(req.Chain, req.Destination, sourceAddress)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"preview": preview,
	})
}

func (s *Server) handleHDWalletSweepExecute(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req hdWalletSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	sourceAddress := ""
	if s.desktopMode {
		sourceAddress = req.Address
	}
	if err := s.walletService.StartSweep(req.Chain, req.Destination, sourceAddress); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusAccepted, map[string]any{
		"success": true,
		"message": "请耐心等待，正在归集中",
	})
}

func (s *Server) handleHDWalletRefreshAddress(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req hdWalletRefreshAddressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	record, err := s.walletService.RefreshAddress(req.Chain, req.Address)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "地址余额更新成功",
		"item":    record,
	})
}

func (s *Server) handleHDWalletBSCGasTopup(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.desktopMode {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req hdWalletBSCGasTopupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	txHash, err := s.walletService.TopUpBSCGas(req.Address)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "已向该地址补充 0.001 BNB 手续费",
		"tx_hash": txHash,
		"address": strings.TrimSpace(req.Address),
	})
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if s.walletMode {
		s.handleHDWalletDashboard(w, r)
		return
	}
	if !s.isAuthenticated(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	page := parsePositiveInt(r.URL.Query().Get("page"), 1)
	sort := parseDashboardSort(r.URL.Query().Get("sort"))
	pageSize := defaultDashboardPageSize
	offset := (page - 1) * pageSize

	result, err := s.repo.ListDashboardRows(r.Context(), offset, pageSize, sort)
	if err != nil {
		http.Error(w, "load dashboard failed", http.StatusInternalServerError)
		log.Printf("load dashboard failed: %v", err)
		return
	}
	chartPoints, err := s.repo.ListDailyEnergyChart(r.Context(), 30)
	if err != nil {
		http.Error(w, "load dashboard failed", http.StatusInternalServerError)
		log.Printf("load energy chart failed: %v", err)
		return
	}

	viewRows := make([]dashboardRowView, 0, len(result.Rows))
	for _, row := range result.Rows {
		lastUpdated := "-"
		if row.LastUpdatedAt.Valid {
			lastUpdated = formatBeijingTime(row.LastUpdatedAt.Time)
		}

		viewRows = append(viewRows, dashboardRowView{
			Address:       row.AddressBase58,
			TRXBalance:    row.TRXBalance.StringFixed(6),
			USDTBalance:   row.USDTBalance.StringFixed(6),
			LastUpdatedAt: lastUpdated,
		})
	}

	totalPages := 0
	if result.TotalCount > 0 {
		totalPages = (result.TotalCount + pageSize - 1) / pageSize
	}
	if totalPages > 0 && page > totalPages {
		page = totalPages
		offset = (page - 1) * pageSize
		result, err = s.repo.ListDashboardRows(r.Context(), offset, pageSize, sort)
		if err != nil {
			http.Error(w, "load dashboard failed", http.StatusInternalServerError)
			log.Printf("reload dashboard failed: %v", err)
			return
		}
		viewRows = make([]dashboardRowView, 0, len(result.Rows))
		for _, row := range result.Rows {
			lastUpdated := "-"
			if row.LastUpdatedAt.Valid {
				lastUpdated = formatBeijingTime(row.LastUpdatedAt.Time)
			}

			viewRows = append(viewRows, dashboardRowView{
				Address:       row.AddressBase58,
				TRXBalance:    row.TRXBalance.StringFixed(6),
				USDTBalance:   row.USDTBalance.StringFixed(6),
				LastUpdatedAt: lastUpdated,
			})
		}
	}

	data := dashboardPageData{
		GeneratedAt:     formatBeijingTime(time.Now()),
		Rows:            viewRows,
		TotalCount:      result.TotalCount,
		Page:            page,
		PageSize:        pageSize,
		HasPrev:         page > 1,
		HasNext:         totalPages > 0 && page < totalPages,
		PrevPage:        maxInt(page-1, 1),
		NextPage:        page + 1,
		TotalPages:      totalPages,
		ChartLabelsJSON: toJSONString(chartLabels(chartPoints)),
		ChartValuesJSON: toJSONString(chartValues(chartPoints)),
		Sort:            string(sort),
	}
	if totalPages > 0 && data.NextPage > totalPages {
		data.NextPage = totalPages
	}

	if err := s.templates.ExecuteTemplate(w, "dashboard.html", data); err != nil {
		http.Error(w, "render dashboard failed", http.StatusInternalServerError)
		log.Printf("render dashboard failed: %v", err)
	}
}
