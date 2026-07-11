package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcec/v2"
	gotronAddress "github.com/fbsobreira/gotron-sdk/pkg/address"
	gotronTx "github.com/fbsobreira/gotron-sdk/pkg/client/transaction"
	gotronKeys "github.com/fbsobreira/gotron-sdk/pkg/keys"

	"tron_watcher/internal/config"
	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const activateTRXAmountSun = int64(1_000_000)

type TronAddressActivator struct {
	tronClient *tron.Client
	signers    []tronActivatorSigner
}

type tronActivatorSigner struct {
	privateKey *btcec.PrivateKey
	fromHex    string
}

func NewTronAddressActivator(tronClient *tron.Client, _ *repository.DB, cfg config.TronActivatorConfig) (*TronAddressActivator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	return NewTronAddressActivatorWithPrivateKeys(tronClient, normalizeActivatorPrivateKeys(cfg))
}

func NewTronAddressActivatorWithPrivateKeys(tronClient *tron.Client, privateKeyHexes []string) (*TronAddressActivator, error) {
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
		})
	}
	return &TronAddressActivator{
		tronClient: tronClient,
		signers:    signers,
	}, nil
}

func (a *TronAddressActivator) Activate(ctx context.Context, address string) (string, error) {
	if a == nil || a.tronClient == nil || len(a.signers) == 0 {
		return "", fmt.Errorf("tron activator not configured")
	}
	to := strings.TrimSpace(address)
	if to == "" {
		return "", fmt.Errorf("empty address")
	}
	return a.sendOne(ctx, a.signers[0], to)
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
	signedJSON, err := buildSignedTronTransactionPayload(unsignedJSON, signedTx.GetSignature())
	if err != nil {
		return "", fmt.Errorf("encode signed tron transaction: %w", err)
	}
	return a.tronClient.BroadcastTransactionJSON(ctx, signedJSON)
}

func normalizeActivatorPrivateKeys(cfg config.TronActivatorConfig) []string {
	values := make([]string, 0, len(cfg.PrivateKeys)+1)
	if trimmed := strings.TrimSpace(cfg.PrivateKey); trimmed != "" {
		values = append(values, trimmed)
	}
	for _, item := range cfg.PrivateKeys {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

func tronHexOrPanic(value string) string {
	if strings.HasPrefix(strings.ToUpper(value), "41") {
		return strings.ToUpper(value)
	}
	return "41" + strings.ToUpper(strings.TrimPrefix(value, "0x"))
}

func buildSignedTronTransactionPayload(unsignedJSON []byte, signatures [][]byte) ([]byte, error) {
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
