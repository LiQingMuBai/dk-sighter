package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
	privateKey *btcec.PrivateKey
	fromHex    string
	fromBase58 string
	jobs       chan activateJob
}

type activateJob struct {
	id        string
	addresses []string
}

func NewTronAddressActivator(tronClient *tron.Client, repo *repository.DB, cfg config.TronActivatorConfig) (*TronAddressActivator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	privateKeyHex := strings.TrimPrefix(strings.TrimSpace(cfg.PrivateKey), "0x")
	if privateKeyHex == "" {
		return nil, fmt.Errorf("tron_activator.private_key is required")
	}
	if tronClient == nil {
		return nil, fmt.Errorf("tron client is required")
	}

	privateKey, err := gotronKeys.GetPrivateKeyFromHex(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("load tron activator private key: %w", err)
	}
	fromAddr := gotronAddress.BTCECPrivkeyToAddress(privateKey)
	fromHex := strings.ToUpper(hex.EncodeToString(fromAddr.Bytes()))
	if fromHex == "" {
		return nil, fmt.Errorf("derive tron activator address failed")
	}

	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 64
	}

	return &TronAddressActivator{
		tronClient: tronClient,
		repo:       repo,
		privateKey: privateKey,
		fromHex:    tronHexOrPanic(fromHex),
		fromBase58: fromAddr.String(),
		jobs:       make(chan activateJob, queueSize),
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
	if a == nil || a.tronClient == nil || a.privateKey == nil {
		return "", fmt.Errorf("tron activator not configured")
	}
	to := strings.TrimSpace(address)
	if to == "" {
		return "", fmt.Errorf("empty address")
	}
	return a.sendOneWithLog(ctx, "", to)
}

func (a *TronAddressActivator) EnqueueBatch(addresses []string) (string, int, error) {
	if a == nil || a.tronClient == nil || a.privateKey == nil {
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
		return jobID, len(normalized), nil
	default:
		return "", 0, fmt.Errorf("activate queue is full")
	}
}

func (a *TronAddressActivator) processJob(ctx context.Context, job activateJob) {
	if len(job.addresses) == 0 {
		return
	}
	log.Printf("tron activate job started: job_id=%s total=%d", job.id, len(job.addresses))

	for i, address := range job.addresses {
		select {
		case <-ctx.Done():
			log.Printf("tron activate job canceled: job_id=%s", job.id)
			return
		default:
		}

		taskCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		txID, err := a.sendOneWithLog(taskCtx, job.id, address)
		cancel()
		if err != nil {
			log.Printf("tron activate failed: job_id=%s address=%s err=%v", job.id, address, err)
		} else {
			log.Printf("tron activate sent: job_id=%s address=%s txid=%s", job.id, address, txID)
		}

		if i < len(job.addresses)-1 {
			timer := time.NewTimer(activateInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				log.Printf("tron activate job canceled: job_id=%s", job.id)
				return
			case <-timer.C:
			}
		}
	}

	log.Printf("tron activate job finished: job_id=%s total=%d", job.id, len(job.addresses))
}

func (a *TronAddressActivator) sendOneWithLog(ctx context.Context, jobID string, toAddress string) (string, error) {
	txID, err := a.sendOne(ctx, toAddress)

	if a.repo != nil {
		addressBase58 := strings.TrimSpace(toAddress)
		if normalized, normalizeErr := normalizeTronAddressToBase58(toAddress); normalizeErr == nil {
			addressBase58 = normalized
		}

		item := repository.TronActivationLog{
			JobID:             strings.TrimSpace(jobID),
			AddressBase58:     addressBase58,
			FromAddressBase58: a.fromBase58,
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
			log.Printf("insert tron activation log failed: err=%v", logErr)
		}
	}

	return txID, err
}

func (a *TronAddressActivator) sendOne(ctx context.Context, toAddress string) (string, error) {
	unsignedJSON, err := a.tronClient.CreateTRXTransferTransaction(ctx, a.fromHex, toAddress, activateTRXAmountSun)
	if err != nil {
		return "", err
	}

	tx, err := gotronTx.FromJSON(unsignedJSON)
	if err != nil {
		return "", fmt.Errorf("decode unsigned transaction: %w", err)
	}
	signedTx, err := gotronTx.SignTransaction(tx, a.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign tron transaction: %w", err)
	}
	signedJSON, err := buildSignedTronTransactionJSON(unsignedJSON, signedTx.GetSignature())
	if err != nil {
		return "", fmt.Errorf("encode signed tron transaction: %w", err)
	}
	return a.tronClient.BroadcastTransactionJSON(ctx, signedJSON)
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
