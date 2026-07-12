package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	gotronAddress "github.com/fbsobreira/gotron-sdk/pkg/address"
	gotronTx "github.com/fbsobreira/gotron-sdk/pkg/client/transaction"
	gotronKeys "github.com/fbsobreira/gotron-sdk/pkg/keys"

	"tron_watcher/internal/config"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const (
	activateTRXAmountSun = int64(1_000_000)
	activateInterval     = 3 * time.Second
)

type TronAddressActivator struct {
	tronClient *tron.Client
	repo       *repository.DB
	signers    []tronActivatorSigner
	nextSigner uint64
	jobs       chan activateJob
	jobMu      sync.RWMutex
	jobStatus  map[string]ActivateJobStatus
	logger     *log.Logger
}

type activateJob struct {
	id        string
	addresses []string
}

type tronActivatorSigner struct {
	privateKey *btcec.PrivateKey
	fromHex    string
	fromBase58 string
}

type ActivateJobStatus struct {
	JobID        string
	TotalCount   int
	SuccessCount int
	FailedCount  int
	Finished     bool
	UpdatedAt    time.Time
}

func NewTronAddressActivator(tronClient *tron.Client, repo *repository.DB, cfg config.TronActivatorConfig) (*TronAddressActivator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	return NewTronAddressActivatorWithPrivateKeys(tronClient, repo, normalizeActivatorPrivateKeys(cfg), cfg.QueueSize)
}

func NewTronAddressActivatorWithPrivateKeys(tronClient *tron.Client, repo *repository.DB, privateKeyHexes []string, queueSize int) (*TronAddressActivator, error) {
	if tronClient == nil {
		return nil, fmt.Errorf("tron client is required")
	}

	if len(privateKeyHexes) == 0 {
		return nil, fmt.Errorf("tron_activator.private_key or private_keys is required")
	}

	signers := make([]tronActivatorSigner, 0, len(privateKeyHexes))
	for i, privateKeyHex := range privateKeyHexes {
		privateKey, err := gotronKeys.GetPrivateKeyFromHex(privateKeyHex)
		if err != nil {
			return nil, fmt.Errorf("load tron activator private key %d: %w", i, err)
		}
		fromAddr := gotronAddress.BTCECPrivkeyToAddress(privateKey)
		fromHex := strings.ToUpper(hex.EncodeToString(fromAddr.Bytes()))
		if fromHex == "" {
			return nil, fmt.Errorf("derive tron activator address failed for private key %d", i)
		}
		signers = append(signers, tronActivatorSigner{
			privateKey: privateKey,
			fromHex:    tronHexOrPanic(fromHex),
			fromBase58: fromAddr.String(),
		})
	}

	if queueSize <= 0 {
		queueSize = 64
	}

	return &TronAddressActivator{
		tronClient: tronClient,
		repo:       repo,
		signers:    signers,
		jobs:       make(chan activateJob, queueSize),
		jobStatus:  make(map[string]ActivateJobStatus),
		logger:     tronLogger(),
	}, nil
}

func (a *TronAddressActivator) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job := <-a.jobs:
			a.processJob(ctx, job)
		}
	}
}

func (a *TronAddressActivator) Activate(ctx context.Context, address string) (string, error) {
	if a == nil || a.tronClient == nil || len(a.signers) == 0 {
		return "", fmt.Errorf("tron activator not configured")
	}
	to := strings.TrimSpace(address)
	if to == "" {
		return "", fmt.Errorf("empty address")
	}
	return a.sendOneWithLog(ctx, "", to, a.pickSigner)
}

func (a *TronAddressActivator) ActivateByRecordID(ctx context.Context, recordID int64, address string) (string, error) {
	if a == nil || a.tronClient == nil || len(a.signers) == 0 {
		return "", fmt.Errorf("tron activator not configured")
	}
	to := strings.TrimSpace(address)
	if to == "" {
		return "", fmt.Errorf("empty address")
	}
	return a.sendOneWithLog(ctx, "", to, func() (tronActivatorSigner, error) {
		return a.pickSignerByRecordID(recordID)
	})
}

func (a *TronAddressActivator) SignerIndexByRecordID(recordID int64) (int, error) {
	if a == nil || len(a.signers) == 0 {
		return 0, fmt.Errorf("tron activator not configured")
	}
	return a.signerIndexByRecordID(recordID), nil
}

func (a *TronAddressActivator) EnqueueBatch(addresses []string) (string, int, error) {
	if a == nil || a.tronClient == nil || len(a.signers) == 0 {
		return "", 0, fmt.Errorf("tron activator not configured")
	}
	normalized := normalizeBatchAddresses(addresses)
	if len(normalized) == 0 {
		return "", 0, fmt.Errorf("addresses is required")
	}

	jobID, err := newJobID()
	if err != nil {
		return "", 0, err
	}

	select {
	case a.jobs <- activateJob{id: jobID, addresses: normalized}:
		a.setJobStatus(ActivateJobStatus{
			JobID:      jobID,
			TotalCount: len(normalized),
			Finished:   false,
			UpdatedAt:  time.Now(),
		})
		return jobID, len(normalized), nil
	default:
		return "", 0, fmt.Errorf("activate queue is full")
	}
}

func (a *TronAddressActivator) processJob(ctx context.Context, job activateJob) {
	if len(job.addresses) == 0 {
		return
	}
	a.setJobStatus(ActivateJobStatus{
		JobID:      job.id,
		TotalCount: len(job.addresses),
		Finished:   false,
		UpdatedAt:  time.Now(),
	})
	a.logger.Printf("tron activate job started: job_id=%s total=%d", job.id, len(job.addresses))

	successCount := 0
	failedCount := 0
	for i, address := range job.addresses {
		select {
		case <-ctx.Done():
			a.setJobStatus(ActivateJobStatus{
				JobID:        job.id,
				TotalCount:   len(job.addresses),
				SuccessCount: successCount,
				FailedCount:  failedCount,
				Finished:     true,
				UpdatedAt:    time.Now(),
			})
			log.Printf("tron activate job canceled: job_id=%s", job.id)
			return
		default:
		}

		taskCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txID, err := a.sendOneWithLog(taskCtx, job.id, address, a.pickSigner)
		cancel()
		if err != nil {
			failedCount++
			a.logger.Printf("tron activate failed: job_id=%s address=%s err=%v", job.id, address, err)
		} else {
			successCount++
			a.logger.Printf("tron activate sent: job_id=%s address=%s txid=%s", job.id, address, txID)
		}
		a.setJobStatus(ActivateJobStatus{
			JobID:        job.id,
			TotalCount:   len(job.addresses),
			SuccessCount: successCount,
			FailedCount:  failedCount,
			Finished:     false,
			UpdatedAt:    time.Now(),
		})

		if i < len(job.addresses)-1 {
			timer := time.NewTimer(activateInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				a.setJobStatus(ActivateJobStatus{
					JobID:        job.id,
					TotalCount:   len(job.addresses),
					SuccessCount: successCount,
					FailedCount:  failedCount,
					Finished:     true,
					UpdatedAt:    time.Now(),
				})
				a.logger.Printf("tron activate job canceled: job_id=%s", job.id)
				return
			case <-timer.C:
			}
		}
	}

	a.setJobStatus(ActivateJobStatus{
		JobID:        job.id,
		TotalCount:   len(job.addresses),
		SuccessCount: successCount,
		FailedCount:  failedCount,
		Finished:     true,
		UpdatedAt:    time.Now(),
	})
	a.logger.Printf("tron activate job finished: job_id=%s total=%d", job.id, len(job.addresses))
}

func (a *TronAddressActivator) GetJobStatus(jobID string) (int, int, int, bool, bool) {
	if a == nil {
		return 0, 0, 0, false, false
	}
	a.jobMu.RLock()
	defer a.jobMu.RUnlock()
	status, ok := a.jobStatus[strings.TrimSpace(jobID)]
	if !ok {
		return 0, 0, 0, false, false
	}
	return status.TotalCount, status.SuccessCount, status.FailedCount, status.Finished, true
}

func (a *TronAddressActivator) setJobStatus(status ActivateJobStatus) {
	if a == nil || strings.TrimSpace(status.JobID) == "" {
		return
	}
	a.jobMu.Lock()
	defer a.jobMu.Unlock()
	a.jobStatus[strings.TrimSpace(status.JobID)] = status
}

func (a *TronAddressActivator) sendOneWithLog(ctx context.Context, jobID string, toAddress string, picker func() (tronActivatorSigner, error)) (string, error) {
	signer, err := picker()
	if err != nil {
		return "", err
	}
	txID, err := a.sendOne(ctx, signer, toAddress)

	if a.repo != nil {
		addressBase58 := strings.TrimSpace(toAddress)
		if normalized, normalizeErr := normalizeTronAddressToBase58(toAddress); normalizeErr == nil {
			addressBase58 = normalized
		}

		item := repository.TronActivationLog{
			JobID:             strings.TrimSpace(jobID),
			AddressBase58:     addressBase58,
			FromAddressBase58: signer.fromBase58,
			AmountSun:         activateTRXAmountSun,
			TxID:              "",
			Status:            "FAILED",
			ErrorMessage:      "",
		}
		if err != nil {
			item.ErrorMessage = err.Error()
		} else {
			item.TxID = txID
			item.Status = "SUCCESS"
		}

		if logErr := a.repo.InsertTronActivationLog(ctx, item); logErr != nil {
			a.logger.Printf("insert tron activation log failed: err=%v", logErr)
		}
	}

	return txID, err
}

func (a *TronAddressActivator) sendOne(ctx context.Context, signer tronActivatorSigner, toAddress string) (string, error) {
	unsignedJSON, err := a.tronClient.CreateTRXTransferTransaction(ctx, signer.fromHex, toAddress, activateTRXAmountSun)
	if err != nil {
		return "", err
	}

	tx, err := gotronTx.FromJSON(unsignedJSON)
	if err != nil {
		return "", fmt.Errorf("decode unsigned transaction: %w", err)
	}
	signedTx, err := gotronTx.SignTransaction(tx, signer.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign tron transaction: %w", err)
	}
	signedJSON, err := buildSignedTronTransactionJSON(unsignedJSON, signedTx.GetSignature())
	if err != nil {
		return "", fmt.Errorf("encode signed tron transaction: %w", err)
	}
	return a.tronClient.BroadcastTransactionJSON(ctx, signedJSON)
}

func (a *TronAddressActivator) pickSigner() (tronActivatorSigner, error) {
	if a == nil || len(a.signers) == 0 {
		return tronActivatorSigner{}, fmt.Errorf("tron activator not configured")
	}
	if len(a.signers) == 1 {
		return a.signers[0], nil
	}
	index := atomic.AddUint64(&a.nextSigner, 1) - 1
	return a.signers[int(index%uint64(len(a.signers)))], nil
}

func (a *TronAddressActivator) pickSignerByRecordID(recordID int64) (tronActivatorSigner, error) {
	if a == nil || len(a.signers) == 0 {
		return tronActivatorSigner{}, fmt.Errorf("tron activator not configured")
	}
	return a.signers[a.signerIndexByRecordID(recordID)], nil
}

func (a *TronAddressActivator) signerIndexByRecordID(recordID int64) int {
	if len(a.signers) <= 1 {
		return 0
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(fmt.Sprintf("%d", recordID)))
	index := int(hasher.Sum32() % uint32(len(a.signers)))
	if index < 0 || index >= len(a.signers) {
		return 0
	}
	return index
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

func normalizeBatchAddresses(addresses []string) []string {
	result := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, item := range addresses {
		addr := strings.TrimSpace(item)
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		result = append(result, addr)
	}
	return result
}

func newJobID() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func normalizeActivatorPrivateKeys(cfg config.TronActivatorConfig) []string {
	candidates := make([]string, 0, len(cfg.PrivateKeys)+1)
	if strings.TrimSpace(cfg.PrivateKey) != "" {
		candidates = append(candidates, cfg.PrivateKey)
	}
	candidates = append(candidates, cfg.PrivateKeys...)

	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		normalized := strings.TrimPrefix(strings.TrimSpace(candidate), "0x")
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

func tronHexOrPanic(value string) string {
	normalized := tron.NormalizeHexAddress(value)
	if normalized == "" {
		return value
	}
	return normalized
}

func normalizeTronAddressToBase58(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", fmt.Errorf("empty address")
	}
	if strings.HasPrefix(trimmed, "T") {
		addr, err := gotronAddress.Base58ToAddress(trimmed)
		if err != nil {
			return "", err
		}
		return addr.String(), nil
	}
	return tron.HexToBase58(tron.NormalizeHexAddress(trimmed))
}
