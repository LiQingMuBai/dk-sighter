package tron

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/btcsuite/btcutil/base58"
)

func Base58ToHex(address string) (string, error) {
	raw := base58.Decode(address)
	if len(raw) != 25 {
		return "", fmt.Errorf("invalid tron address length")
	}

	payload := raw[:21]
	checksum := raw[21:]
	hash1 := sha256.Sum256(payload)
	hash2 := sha256.Sum256(hash1[:])
	if !equalBytes(checksum, hash2[:4]) {
		return "", fmt.Errorf("invalid tron address checksum")
	}

	return strings.ToUpper(hex.EncodeToString(payload)), nil
}

func HexToBase58(hexAddr string) (string, error) {
	hexAddr = NormalizeHexAddress(hexAddr)
	payload, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", fmt.Errorf("decode hex address: %w", err)
	}
	if len(payload) != 21 {
		return "", fmt.Errorf("invalid tron hex payload")
	}

	hash1 := sha256.Sum256(payload)
	hash2 := sha256.Sum256(hash1[:])
	full := append(payload, hash2[:4]...)
	return base58.Encode(full), nil
}

func NormalizeHexAddress(hexAddr string) string {
	hexAddr = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(hexAddr)), "0x")
	switch len(hexAddr) {
	case 40:
		hexAddr = "41" + hexAddr
	case 42:
	default:
	}
	return strings.ToUpper(hexAddr)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
