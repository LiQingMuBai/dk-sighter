package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"tron_watcher/infrastructure"
	"tron_watcher/internal/config"
	"tron_watcher/internal/hdwallet"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

type addressReloader interface {
	Reload(context.Context) error
}

type tronBalanceRefresher interface {
	RefreshAddresses(context.Context, []string) error
}

type bscBalanceRefresher interface {
	RefreshAddresses(context.Context, []string)
}

type tronAddressActivator interface {
	Activate(context.Context, string) (string, error)
	EnqueueBatch([]string) (string, int, error)
}

type Server struct {
	repo                  *repository.DB
	reloader              addressReloader
	tronBalances          tronBalanceRefresher
	bscBalances           bscBalanceRefresher
	tronActivator         tronAddressActivator
	listen                string
	templates             *template.Template
	username              string
	password              string
	sessionName           string
	sessionToken          string
	apiKey                string
	energyProviders       map[string]infrastructure.EnergyOrderProvider
	defaultEnergyProvider string
	mnemonicStore         *mnemonicStore
	desktopMode           bool
	walletMode            bool
	walletService         *hdwallet.Service
}

type dashboardPageData struct {
	GeneratedAt             string
	Rows                    []dashboardRowView
	TotalCount              int
	Page                    int
	PageSize                int
	HasPrev                 bool
	HasNext                 bool
	PrevPage                int
	NextPage                int
	TotalPages              int
	ChartLabelsJSON         string
	ChartValuesJSON         string
	ChartActivateValuesJSON string
	Sort                    string
	AddressQuery            string
}

type apiDocsPageData struct {
	BaseURL  string
	HTTPURL  string
	HTTPSURL string
}

type dashboardRowView struct {
	Address       string
	TRXBalance    string
	USDTBalance   string
	LastUpdatedAt string
}

const defaultDashboardPageSize = 20

type loginPageData struct {
	Error           string
	CaptchaQuestion string
	CaptchaToken    string
}

type addWatchAddressesRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type addWatchAddressesResponse struct {
	Success            bool     `json:"success"`
	Message            string   `json:"message"`
	Count              int      `json:"count"`
	Addresses          []string `json:"addresses,omitempty"`
	DuplicateAddresses []string `json:"duplicate_addresses,omitempty"`
	InvalidAddresses   []string `json:"invalid_addresses,omitempty"`
}

type refreshAddressRequest struct {
	Address string `json:"address"`
}

type refreshAddressResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Address string `json:"address,omitempty"`
}

type actionPreviewRequest struct {
	Action    string   `json:"action"`
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type actionPreviewResponse struct {
	Success         bool     `json:"success"`
	Message         string   `json:"message"`
	Action          string   `json:"action,omitempty"`
	Address         string   `json:"address,omitempty"`
	Addresses       []string `json:"addresses,omitempty"`
	TotalCount      int      `json:"total_count,omitempty"`
	SuccessCount    int      `json:"success_count,omitempty"`
	FailedCount     int      `json:"failed_count,omitempty"`
	FailedAddresses []string `json:"failed_addresses,omitempty"`
}

type deleteWatchAddressRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type deleteWatchAddressResponse struct {
	Success         bool     `json:"success"`
	Message         string   `json:"message"`
	Address         string   `json:"address,omitempty"`
	Addresses       []string `json:"addresses,omitempty"`
	TotalCount      int      `json:"total_count,omitempty"`
	SuccessCount    int      `json:"success_count,omitempty"`
	FailedCount     int      `json:"failed_count,omitempty"`
	FailedAddresses []string `json:"failed_addresses,omitempty"`
}

type activateAddressRequest struct {
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

type activateAddressResponse struct {
	Success      bool     `json:"success"`
	Message      string   `json:"message"`
	Address      string   `json:"address,omitempty"`
	Addresses    []string `json:"addresses,omitempty"`
	TotalCount   int      `json:"total_count,omitempty"`
	SuccessCount int      `json:"success_count,omitempty"`
	TxID         string   `json:"txid,omitempty"`
	JobID        string   `json:"job_id,omitempty"`
}

type cacheMnemonicRequest struct {
	Mnemonic string `json:"mnemonic"`
}

type cacheMnemonicResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ExpiresIn int    `json:"expires_in,omitempty"`
}

func NewServer(
	repo *repository.DB,
	cfg config.WebConfig,
	reloader addressReloader,
	tronBalances tronBalanceRefresher,
	bscBalances bscBalanceRefresher,
	tronActivator tronAddressActivator,
	energyProviders map[string]infrastructure.EnergyOrderProvider,
	defaultEnergyProvider string,
) (*Server, error) {
	tmpl, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Username) == "" || strings.TrimSpace(cfg.Password) == "" {
		return nil, fmt.Errorf("web username/password is required")
	}

	return &Server{
		repo:                  repo,
		reloader:              reloader,
		tronBalances:          tronBalances,
		bscBalances:           bscBalances,
		tronActivator:         tronActivator,
		listen:                cfg.Listen,
		templates:             tmpl,
		username:              cfg.Username,
		password:              cfg.Password,
		sessionName:           cfg.SessionName,
		sessionToken:          buildSessionToken(cfg.Username, cfg.Password),
		apiKey:                strings.TrimSpace(cfg.APIKey),
		energyProviders:       energyProviders,
		defaultEnergyProvider: strings.ToLower(strings.TrimSpace(defaultEnergyProvider)),
		mnemonicStore:         newMnemonicStore(),
	}, nil
}

func NewHDWalletServer(
	cfg config.WebConfig,
	walletService *hdwallet.Service,
	energyProviders map[string]infrastructure.EnergyOrderProvider,
	defaultEnergyProvider string,
) (*Server, error) {
	tmpl, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Username) == "" || strings.TrimSpace(cfg.Password) == "" {
		return nil, fmt.Errorf("web username/password is required")
	}

	return &Server{
		listen:                cfg.Listen,
		templates:             tmpl,
		username:              cfg.Username,
		password:              cfg.Password,
		sessionName:           cfg.SessionName,
		sessionToken:          buildSessionToken(cfg.Username, cfg.Password),
		apiKey:                strings.TrimSpace(cfg.APIKey),
		energyProviders:       energyProviders,
		defaultEnergyProvider: strings.ToLower(strings.TrimSpace(defaultEnergyProvider)),
		mnemonicStore:         newMnemonicStore(),
		desktopMode:           strings.TrimSpace(os.Getenv("TRON_WATCHER_DESKTOP")) == "1",
		walletMode:            true,
		walletService:         walletService,
	}, nil
}

func loadTemplates() (*template.Template, error) {
	templateDir := strings.TrimSpace(os.Getenv("TRON_WATCHER_TEMPLATE_DIR"))
	if templateDir == "" {
		templateDir = filepath.Join("web", "templates")
	}

	files := []string{
		filepath.Join(templateDir, "dashboard.html"),
		filepath.Join(templateDir, "login.html"),
		filepath.Join(templateDir, "api_docs.html"),
		filepath.Join(templateDir, "openapi.json"),
		filepath.Join(templateDir, "bsc_dashboard.html"),
		filepath.Join(templateDir, "wallet_dashboard.html"),
	}
	return template.ParseFiles(files...)
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/login", s.handleLogin)
	mux.HandleFunc("/logout", s.handleLogout)
	mux.HandleFunc("/api/action-preview", s.handleActionPreview)
	if s.walletMode {
		mux.HandleFunc("/api/hd-wallet/state", s.handleHDWalletState)
		mux.HandleFunc("/api/hd-wallet/config", s.handleHDWalletConfig)
		mux.HandleFunc("/api/hd-wallet/sync", s.handleHDWalletSync)
		mux.HandleFunc("/api/hd-wallet/refresh-address", s.handleHDWalletRefreshAddress)
		mux.HandleFunc("/api/hd-wallet/sweep/preview", s.handleHDWalletSweepPreview)
		mux.HandleFunc("/api/hd-wallet/sweep/execute", s.handleHDWalletSweepExecute)
	} else {
		mux.HandleFunc("/bsc", s.handleBSCDashboard)
		mux.HandleFunc("/docs", s.handleAPIDocs)
		mux.HandleFunc("/openapi.json", s.handleOpenAPI)
		mux.HandleFunc("/api/bsc/watch-addresses", s.handleBSCAddWatchAddresses)
		mux.HandleFunc("/api/bsc/delete-addresses", s.handleBSCDeleteAddresses)
		mux.HandleFunc("/api/bsc/refresh-address", s.handleBSCRefreshAddress)
		mux.HandleFunc("/api/bsc/balances", s.handleBSCDashboardBalancesAPI)
		mux.HandleFunc("/api/bsc/transfers/in", s.handleBSCTransfersInAPI)
		mux.HandleFunc("/api/bsc/transfers/out", s.handleBSCTransfersOutAPI)
		mux.HandleFunc("/api/mnemonic/cache", s.handleCacheMnemonic)
		mux.HandleFunc("/api/watch-addresses", s.handleAddWatchAddresses)
		mux.HandleFunc("/api/tron/watch-addresses", s.handleAddWatchAddresses)
		mux.HandleFunc("/api/tron/refresh-address", s.handleRefreshAddress)
		mux.HandleFunc("/api/tron/balances", s.handleTronBalancesAPI)
		mux.HandleFunc("/api/tron/transfers/in", s.handleTronTransfersInAPI)
		mux.HandleFunc("/api/tron/transfers/out", s.handleTronTransfersOutAPI)
		mux.HandleFunc("/api/watch-address/delete", s.handleDeleteWatchAddress)
		mux.HandleFunc("/api/tron/delete-addresses", s.handleDeleteWatchAddress)
		mux.HandleFunc("/api/tron/activate-address", s.handleActivateAddress)
	}

	server := &http.Server{
		Addr:    s.listen,
		Handler: s.recoverMiddleware(mux),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown web server failed: %v", err)
		}
	}()

	log.Printf("web dashboard listening on %s", s.listen)
	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleAPIDocs(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	data := apiDocsPageData{
		BaseURL:  scheme + "://" + r.Host,
		HTTPURL:  "http://" + r.Host,
		HTTPSURL: "https://" + r.Host,
	}
	if err := s.templates.ExecuteTemplate(w, "api_docs.html", data); err != nil {
		http.Error(w, "render api docs failed", http.StatusInternalServerError)
		log.Printf("render api docs failed: %v", err)
	}
}

func (s *Server) handleBSCDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	page := parsePositiveBSCPage(r.URL.Query().Get("page"))
	sort := parseBSCDashboardSort(r.URL.Query().Get("sort"))
	addressQuery := strings.TrimSpace(r.URL.Query().Get("address"))
	pageSize := 20

	total, err := s.countActiveBSCWatchAddresses(r.Context(), addressQuery)
	if err != nil {
		http.Error(w, "load bsc total failed", http.StatusInternalServerError)
		log.Printf("load bsc total failed: %v", err)
		return
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}

	offset := (page - 1) * pageSize
	records, err := s.listBSCDashboardRecords(r.Context(), pageSize, offset, sort, addressQuery)
	if err != nil {
		http.Error(w, "load bsc dashboard failed", http.StatusInternalServerError)
		log.Printf("load bsc dashboard failed: %v", err)
		return
	}

	recordViews := make([]bscDashboardRecordView, 0, len(records))
	for _, record := range records {
		updatedAt := "-"
		if !record.UpdatedAt.IsZero() {
			updatedAt = formatBeijingTime(record.UpdatedAt)
		}
		recordViews = append(recordViews, bscDashboardRecordView{
			Address:   record.Address,
			BNB:       record.BNB,
			USDT:      record.USDT,
			UpdatedAt: updatedAt,
		})
	}

	data := buildBSCDashboardPageData(recordViews, page, pageSize, total)
	data.GeneratedAt = formatBeijingTime(time.Now())
	data.Sort = string(sort)
	data.AddressQuery = addressQuery
	if err := s.templates.ExecuteTemplate(w, "bsc_dashboard.html", data); err != nil {
		http.Error(w, "render bsc dashboard failed", http.StatusInternalServerError)
		log.Printf("render bsc dashboard failed: %v", err)
	}
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data := apiDocsPageData{
		BaseURL:  scheme + "://" + r.Host,
		HTTPURL:  "http://" + r.Host,
		HTTPSURL: "https://" + r.Host,
	}
	if err := s.templates.ExecuteTemplate(w, "openapi.json", data); err != nil {
		http.Error(w, "render openapi failed", http.StatusInternalServerError)
		log.Printf("render openapi failed: %v", err)
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.isAuthenticated(r) {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		s.renderLogin(w, "")
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if !s.validateCaptcha(r) {
			s.renderLogin(w, "验证码错误")
			return
		}
		username := strings.TrimSpace(r.FormValue("username"))
		password := r.FormValue("password")
		if username != s.username || password != s.password {
			s.renderLogin(w, "账号或密码错误")
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name:     s.sessionName,
			Value:    s.sessionToken,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			MaxAge:   86400 * 7,
		})
		http.Redirect(w, r, "/", http.StatusFound)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     s.sessionName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s *Server) handleAddWatchAddresses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, addWatchAddressesResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}

	var req addWatchAddressesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, addWatchAddressesResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	addresses, invalid := normalizeWatchAddresses(req)
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, addWatchAddressesResponse{
			Success:          false,
			Message:          "no valid addresses",
			InvalidAddresses: invalid,
		})
		return
	}

	existing, err := s.repo.FindExistingWatchAddresses(r.Context(), addresses)
	if err != nil {
		log.Printf("find existing watch addresses failed: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, addWatchAddressesResponse{
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
		log.Printf("duplicate watch addresses ignored: %s", strings.Join(duplicates, ","))
	}

	if err := s.repo.InsertWatchAddresses(r.Context(), toInsert); err != nil {
		log.Printf("upsert watch addresses failed: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, addWatchAddressesResponse{
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
			log.Printf("reload address cache failed after api insert: %v", err)
			s.writeJSON(w, http.StatusInternalServerError, addWatchAddressesResponse{
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

	if s.tronBalances != nil && len(toInsert) > 0 {
		if err := s.tronBalances.RefreshAddresses(r.Context(), toInsert); err != nil {
			log.Printf("refresh tron balances after api insert failed: %v", err)
			s.writeJSON(w, http.StatusInternalServerError, addWatchAddressesResponse{
				Success:            false,
				Message:            "addresses saved but balance sync failed",
				Count:              len(toInsert),
				Addresses:          toInsert,
				DuplicateAddresses: duplicates,
				InvalidAddresses:   invalid,
			})
			return
		}
	}

	s.writeJSON(w, http.StatusOK, addWatchAddressesResponse{
		Success:            true,
		Message:            "ok",
		Count:              len(toInsert),
		Addresses:          toInsert,
		DuplicateAddresses: duplicates,
		InvalidAddresses:   invalid,
	})
}

func (s *Server) handleActionPreview(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, actionPreviewResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req actionPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, actionPreviewResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	action := strings.TrimSpace(req.Action)
	addresses := normalizeActionAddresses(req.Address, req.Addresses)
	if action == "" || len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, actionPreviewResponse{
			Success: false,
			Message: "action and addresses are required",
		})
		return
	}

	log.Printf("action clicked: action=%s addresses=%d", action, len(addresses))
	if action == "一键归集" {
		for _, address := range addresses {
			log.Printf("collect action clicked: action=%s address=%s", action, address)
		}
		s.writeJSON(w, http.StatusBadRequest, actionPreviewResponse{
			Success:    false,
			Message:    "一键归集功能暂未开发",
			Action:     action,
			Address:    firstAddress(addresses),
			Addresses:  addresses,
			TotalCount: len(addresses),
		})
		return
	}

	if action == "发能一次" || action == "发能两次" {
		providerName, provider := s.resolveEnergyProvider(r.Context())
		if provider == nil || !provider.IsConfigured() {
			s.writeJSON(w, http.StatusBadRequest, actionPreviewResponse{
				Success:   false,
				Message:   "未配置发能通道",
				Action:    action,
				Addresses: addresses,
			})
			return
		}

		energyAmount := 65000
		successMessage := "发能一次成功"
		failMessage := "发能一次失败"
		logLabel := "energy once"
		if action == "发能两次" {
			energyAmount = 130000
			successMessage = "发能两次成功"
			failMessage = "发能两次失败"
			logLabel = "energy twice"
		}

		successCount := 0
		failedAddresses := make([]string, 0)
		for _, address := range addresses {
			respBody, err := provider.OrderEnergy(address, energyAmount)
			if err != nil {
				if s.repo != nil {
					_ = s.repo.InsertEnergyActionLog(r.Context(), repository.EnergyActionLog{
						ActionName:    action,
						AddressBase58: address,
						Provider:      providerName,
						EnergyAmount:  energyAmount,
						ActionScore:   scoreByAction(action),
						Status:        "FAILED",
						ResponseBody:  respBody,
						ErrorMessage:  err.Error(),
					})
				}
				log.Printf("%s failed: provider=%s action=%s address=%s energy_amount=%d err=%v body=%s",
					logLabel, providerName, action, address, energyAmount, err, respBody)
				failedAddresses = append(failedAddresses, address)
				continue
			}

			successCount++
			if s.repo != nil {
				if err := s.repo.InsertEnergyActionLog(r.Context(), repository.EnergyActionLog{
					ActionName:    action,
					AddressBase58: address,
					Provider:      providerName,
					EnergyAmount:  energyAmount,
					ActionScore:   scoreByAction(action),
					Status:        "SUCCESS",
					ResponseBody:  respBody,
				}); err != nil {
					log.Printf("insert energy action log failed: action=%s address=%s err=%v", action, address, err)
				}
			}
			log.Printf("%s success: provider=%s action=%s address=%s energy_amount=%d resp=%s",
				logLabel, providerName, action, address, energyAmount, respBody)
		}

		failedCount := len(failedAddresses)
		if successCount == 0 {
			s.writeJSON(w, http.StatusInternalServerError, actionPreviewResponse{
				Success:         false,
				Message:         failMessage,
				Action:          action,
				Addresses:       addresses,
				TotalCount:      len(addresses),
				SuccessCount:    successCount,
				FailedCount:     failedCount,
				FailedAddresses: failedAddresses,
			})
			return
		}

		message := successMessage
		if len(addresses) > 1 {
			message = fmt.Sprintf("%s：成功 %d / %d", action, successCount, len(addresses))
		}
		s.writeJSON(w, http.StatusOK, actionPreviewResponse{
			Success:         true,
			Message:         message,
			Action:          action,
			Address:         firstAddress(addresses),
			Addresses:       addresses,
			TotalCount:      len(addresses),
			SuccessCount:    successCount,
			FailedCount:     failedCount,
			FailedAddresses: failedAddresses,
		})
		return
	}

	s.writeJSON(w, http.StatusOK, actionPreviewResponse{
		Success:      true,
		Message:      fmt.Sprintf("已点击：%s（%d 个地址）", action, len(addresses)),
		Action:       action,
		Address:      firstAddress(addresses),
		Addresses:    addresses,
		TotalCount:   len(addresses),
		SuccessCount: len(addresses),
	})
}

func (s *Server) handleCacheMnemonic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, cacheMnemonicResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}

	var req cacheMnemonicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, cacheMnemonicResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	mnemonic := normalizeMnemonic(req.Mnemonic)
	if !isValidMnemonic(mnemonic) {
		s.writeJSON(w, http.StatusBadRequest, cacheMnemonicResponse{
			Success: false,
			Message: "助记词必须是 12 或 24 个单词",
		})
		return
	}

	s.mnemonicStore.Set(mnemonic, time.Minute)
	log.Printf("mnemonic cached in memory for 1 minute")
	s.writeJSON(w, http.StatusOK, cacheMnemonicResponse{
		Success:   true,
		Message:   "助记词已暂存 1 分钟",
		ExpiresIn: 60,
	})
}

func (s *Server) handleRefreshAddress(w http.ResponseWriter, r *http.Request) {
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
	if s.tronBalances == nil {
		s.writeJSON(w, http.StatusInternalServerError, refreshAddressResponse{
			Success: false,
			Message: "tron balance refresher not configured",
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

	if err := s.tronBalances.RefreshAddresses(r.Context(), []string{address}); err != nil {
		log.Printf("refresh tron address balance failed: address=%s err=%v", address, err)
		s.writeJSON(w, http.StatusInternalServerError, refreshAddressResponse{
			Success: false,
			Message: "更新 Tron 余额失败",
			Address: address,
		})
		return
	}

	s.writeJSON(w, http.StatusOK, refreshAddressResponse{
		Success: true,
		Message: "Tron 地址余额更新成功",
		Address: address,
	})
}
func (s *Server) handleDeleteWatchAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, deleteWatchAddressResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}

	var req deleteWatchAddressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, deleteWatchAddressResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	addresses := normalizeActionAddresses(req.Address, req.Addresses)
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, deleteWatchAddressResponse{
			Success: false,
			Message: "address or addresses is required",
		})
		return
	}

	successCount := 0
	failedAddresses := make([]string, 0)
	for _, address := range addresses {
		if err := s.repo.DisableWatchAddress(r.Context(), address); err != nil {
			log.Printf("disable watch address failed: address=%s err=%v", address, err)
			failedAddresses = append(failedAddresses, address)
			continue
		}
		successCount++
		log.Printf("watch address disabled: address=%s", address)
	}

	if s.reloader != nil {
		if err := s.reloader.Reload(r.Context()); err != nil {
			log.Printf("reload address cache failed after delete: err=%v", err)
		}
	}

	failedCount := len(failedAddresses)
	if successCount == 0 {
		s.writeJSON(w, http.StatusInternalServerError, deleteWatchAddressResponse{
			Success:         false,
			Message:         "删除地址失败",
			Address:         firstAddress(addresses),
			Addresses:       addresses,
			TotalCount:      len(addresses),
			SuccessCount:    0,
			FailedCount:     failedCount,
			FailedAddresses: failedAddresses,
		})
		return
	}

	message := "删除地址成功"
	if len(addresses) > 1 {
		message = fmt.Sprintf("批量删除成功 %d / %d", successCount, len(addresses))
	}
	s.writeJSON(w, http.StatusOK, deleteWatchAddressResponse{
		Success:         true,
		Message:         message,
		Address:         firstAddress(addresses),
		Addresses:       addresses,
		TotalCount:      len(addresses),
		SuccessCount:    successCount,
		FailedCount:     failedCount,
		FailedAddresses: failedAddresses,
	})
}

func (s *Server) handleActivateAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.isAPIAuthorized(r) && !s.isAuthenticated(r) {
		s.writeJSON(w, http.StatusUnauthorized, activateAddressResponse{
			Success: false,
			Message: "unauthorized",
		})
		return
	}

	var req activateAddressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, activateAddressResponse{
			Success: false,
			Message: "invalid json body",
		})
		return
	}

	addresses := normalizeActionAddresses(req.Address, req.Addresses)
	if len(addresses) == 0 {
		s.writeJSON(w, http.StatusBadRequest, activateAddressResponse{
			Success: false,
			Message: "address or addresses is required",
		})
		return
	}

	if s.tronActivator == nil {
		s.writeJSON(w, http.StatusInternalServerError, activateAddressResponse{
			Success:    false,
			Message:    "tron activator not configured",
			Address:    firstAddress(addresses),
			Addresses:  addresses,
			TotalCount: len(addresses),
		})
		return
	}

	if len(addresses) == 1 {
		log.Printf("tron activate address requested: address=%s", addresses[0])
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		txID, err := s.tronActivator.Activate(ctx, addresses[0])
		cancel()
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, activateAddressResponse{
				Success:    false,
				Message:    err.Error(),
				Address:    addresses[0],
				TotalCount: 1,
			})
			return
		}
		s.writeJSON(w, http.StatusOK, activateAddressResponse{
			Success:      true,
			Message:      "激活地址已发送",
			Address:      addresses[0],
			TotalCount:   1,
			SuccessCount: 1,
			TxID:         txID,
		})
		return
	}

	log.Printf("tron batch activate requested: total=%d", len(addresses))
	jobID, queued, err := s.tronActivator.EnqueueBatch(addresses)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, activateAddressResponse{
			Success:    false,
			Message:    err.Error(),
			Address:    firstAddress(addresses),
			Addresses:  addresses,
			TotalCount: len(addresses),
		})
		return
	}
	s.writeJSON(w, http.StatusOK, activateAddressResponse{
		Success:      true,
		Message:      fmt.Sprintf("准备批量激活地址 %d 个，后台异步执行", queued),
		Address:      firstAddress(addresses),
		Addresses:    addresses,
		TotalCount:   len(addresses),
		SuccessCount: 0,
		JobID:        jobID,
	})
}

func (s *Server) renderLogin(w http.ResponseWriter, errMsg string) {
	question, answer, err := generateCaptcha()
	if err != nil {
		http.Error(w, "render login failed", http.StatusInternalServerError)
		log.Printf("generate captcha failed: %v", err)
		return
	}
	if err := s.templates.ExecuteTemplate(w, "login.html", loginPageData{
		Error:           errMsg,
		CaptchaQuestion: question,
		CaptchaToken:    buildCaptchaToken(answer, s.sessionToken),
	}); err != nil {
		http.Error(w, "render login failed", http.StatusInternalServerError)
		log.Printf("render login failed: %v", err)
	}
}

func (s *Server) isAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie(s.sessionName)
	if err != nil {
		return false
	}
	return cookie.Value == s.sessionToken
}

func buildSessionToken(username, password string) string {
	sum := sha256.Sum256([]byte(username + ":" + password))
	return hex.EncodeToString(sum[:])
}

func (s *Server) isAPIAuthorized(r *http.Request) bool {
	if s.apiKey == "" {
		return true
	}
	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if apiKey == "" {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			apiKey = strings.TrimSpace(auth[7:])
		}
	}
	return apiKey == s.apiKey
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json response failed: %v", err)
	}
}

func buildCaptchaToken(answer int, sessionToken string) string {
	sum := sha256.Sum256([]byte(strconv.Itoa(answer) + ":" + sessionToken))
	return hex.EncodeToString(sum[:])
}

func (s *Server) validateCaptcha(r *http.Request) bool {
	answer := strings.TrimSpace(r.FormValue("captcha"))
	token := strings.TrimSpace(r.FormValue("captcha_token"))
	if answer == "" || token == "" {
		return false
	}
	return token == buildCaptchaToken(mustAtoi(answer), s.sessionToken)
}

func generateCaptcha() (string, int, error) {
	left, err := cryptoRandInt(9)
	if err != nil {
		return "", 0, err
	}
	right, err := cryptoRandInt(9)
	if err != nil {
		return "", 0, err
	}
	a := left + 1
	b := right + 1
	return fmt.Sprintf("%d + %d = ?", a, b), a + b, nil
}

func cryptoRandInt(max int64) (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()), nil
}

func mustAtoi(input string) int {
	value, err := strconv.Atoi(input)
	if err != nil {
		return -1
	}
	return value
}

func parsePositiveInt(input string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(input))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func parseDashboardSort(input string) repository.DashboardSort {
	switch repository.DashboardSort(strings.TrimSpace(input)) {
	case repository.DashboardSortUSDTAsc,
		repository.DashboardSortTRXDesc,
		repository.DashboardSortTRXAsc:
		return repository.DashboardSort(strings.TrimSpace(input))
	default:
		return repository.DashboardSortUSDTDesc
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func formatBeijingTime(t time.Time) string {
	return t.UTC().Add(8 * time.Hour).Format("2006-01-02 15:04:05")
}

func normalizeActionAddresses(single string, batch []string) []string {
	raw := make([]string, 0, 1+len(batch))
	if strings.TrimSpace(single) != "" {
		raw = append(raw, single)
	}
	raw = append(raw, batch...)

	result := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		address := strings.TrimSpace(item)
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, address)
	}
	return result
}

func firstAddress(addresses []string) string {
	if len(addresses) == 0 {
		return ""
	}
	return addresses[0]
}

func scoreByAction(action string) int {
	switch action {
	case "发能两次":
		return 2
	case "发能一次":
		return 1
	default:
		return 0
	}
}

func chartLabels(points []repository.EnergyChartPoint) []string {
	result := make([]string, 0, len(points))
	for _, point := range points {
		result = append(result, point.Day)
	}
	return result
}

func chartValues(points []repository.EnergyChartPoint) []int {
	result := make([]int, 0, len(points))
	for _, point := range points {
		result = append(result, point.Count)
	}
	return result
}

func toJSONString(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func (s *Server) resolveEnergyProvider(ctx context.Context) (string, infrastructure.EnergyOrderProvider) {
	providerName := strings.ToLower(strings.TrimSpace(s.defaultEnergyProvider))
	if resolved, ok := resolveProviderByRule(providerName); ok {
		providerName = resolved
	}
	if s.repo != nil {
		value, exists, err := s.repo.GetRuntimeSetting(ctx, "energy_provider")
		if err != nil {
			log.Printf("load runtime energy provider failed, fallback to default: %v", err)
		} else if exists {
			trimmed := strings.ToLower(strings.TrimSpace(value))
			if trimmed != "" {
				if resolved, ok := resolveProviderByRule(trimmed); ok {
					providerName = resolved
				} else {
					providerName = trimmed
				}
			}
		}
	}

	provider, ok := s.energyProviders[providerName]
	if ok {
		return providerName, provider
	}

	fallbackName := strings.ToLower(strings.TrimSpace(s.defaultEnergyProvider))
	if resolved, ok := resolveProviderByRule(fallbackName); ok {
		fallbackName = resolved
	}
	fallback, ok := s.energyProviders[fallbackName]
	if ok {
		return fallbackName, fallback
	}
	return providerName, nil
}

func resolveProviderByRule(rule string) (string, bool) {
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
	if inHourRange(currentHour, start, end) {
		return "trxfee", true
	}
	return "catfee", true
}

func inHourRange(currentHour, start, end int) bool {
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

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("panic recovered in http handler: method=%s path=%s panic=%v\n%s",
					r.Method, r.URL.Path, recovered, string(debug.Stack()))
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func normalizeWatchAddresses(req addWatchAddressesRequest) ([]string, []string) {
	raw := make([]string, 0, 1+len(req.Addresses))
	if strings.TrimSpace(req.Address) != "" {
		raw = append(raw, req.Address)
	}
	raw = append(raw, req.Addresses...)

	valid := make([]string, 0, len(raw))
	invalid := make([]string, 0)
	seen := make(map[string]struct{}, len(raw))

	for _, item := range raw {
		address := strings.TrimSpace(item)
		if address == "" {
			continue
		}
		if _, err := tron.Base58ToHex(address); err != nil {
			invalid = append(invalid, address)
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		valid = append(valid, address)
	}
	return valid, invalid
}
