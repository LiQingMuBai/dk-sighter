package hdwallet

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/tyler-smith/go-bip39"
	"golang.org/x/crypto/sha3"

	"tron_watcher/internal/tron"
)

const (
	tronCoinType = 195
	evmCoinType  = 60
)

type DerivedWallet struct {
	Index         int
	Address       string
	AddressHex    string
	PrivateKeyHex string
}

func DeriveTronAddresses(mnemonic string, count int) ([]AddressRecord, error) {
	return deriveAddresses(mnemonic, count, tronCoinType, true)
}

func DeriveBSCAddresses(mnemonic string, count int) ([]AddressRecord, error) {
	return deriveAddresses(mnemonic, count, evmCoinType, false)
}

func DeriveTronWallet(mnemonic string, index int) (DerivedWallet, error) {
	return deriveWallet(mnemonic, index, tronCoinType, true)
}

func DeriveBSCWallet(mnemonic string, index int) (DerivedWallet, error) {
	return deriveWallet(mnemonic, index, evmCoinType, false)
}

func deriveAddresses(mnemonic string, count, coinType int, tronMode bool) ([]AddressRecord, error) {
	normalized := normalizeMnemonic(mnemonic)
	if !bip39.IsMnemonicValid(normalized) {
		return nil, fmt.Errorf("invalid mnemonic")
	}
	if count <= 0 {
		return nil, fmt.Errorf("count must be positive")
	}

	seed := bip39.NewSeed(normalized, "")
	rootKey, err := hdkeychain.NewMaster(seed, &chaincfg.MainNetParams)
	if err != nil {
		return nil, fmt.Errorf("new hd wallet: %w", err)
	}

	records := make([]AddressRecord, 0, count)
	for i := 0; i < count; i++ {
		childKey, err := deriveKey(rootKey, uint32(coinType), uint32(i))
		if err != nil {
			return nil, fmt.Errorf("derive wallet index %d: %w", i, err)
		}
		privateKey, err := childKey.ECPrivKey()
		if err != nil {
			return nil, fmt.Errorf("load private key index %d: %w", i, err)
		}
		addressBytes := publicKeyToAddress(privateKey.PubKey().SerializeUncompressed())

		record := AddressRecord{Index: i}
		if tronMode {
			record.AddressHex = "41" + strings.ToUpper(hex.EncodeToString(addressBytes))
			record.Address, err = tron.HexToBase58(record.AddressHex)
			if err != nil {
				return nil, fmt.Errorf("convert tron address index %d: %w", i, err)
			}
		} else {
			record.Address = "0x" + strings.ToLower(hex.EncodeToString(addressBytes))
		}
		records = append(records, record)
	}

	return records, nil
}

func deriveWallet(mnemonic string, index, coinType int, tronMode bool) (DerivedWallet, error) {
	normalized := normalizeMnemonic(mnemonic)
	if !bip39.IsMnemonicValid(normalized) {
		return DerivedWallet{}, fmt.Errorf("invalid mnemonic")
	}
	if index < 0 {
		return DerivedWallet{}, fmt.Errorf("index must be non-negative")
	}

	seed := bip39.NewSeed(normalized, "")
	rootKey, err := hdkeychain.NewMaster(seed, &chaincfg.MainNetParams)
	if err != nil {
		return DerivedWallet{}, fmt.Errorf("new hd wallet: %w", err)
	}

	childKey, err := deriveKey(rootKey, uint32(coinType), uint32(index))
	if err != nil {
		return DerivedWallet{}, fmt.Errorf("derive wallet index %d: %w", index, err)
	}
	privateKey, err := childKey.ECPrivKey()
	if err != nil {
		return DerivedWallet{}, fmt.Errorf("load private key index %d: %w", index, err)
	}
	addressBytes := publicKeyToAddress(privateKey.PubKey().SerializeUncompressed())

	wallet := DerivedWallet{
		Index:         index,
		PrivateKeyHex: hex.EncodeToString(privateKey.Serialize()),
	}
	if tronMode {
		wallet.AddressHex = "41" + strings.ToUpper(hex.EncodeToString(addressBytes))
		wallet.Address, err = tron.HexToBase58(wallet.AddressHex)
		if err != nil {
			return DerivedWallet{}, fmt.Errorf("convert tron address index %d: %w", index, err)
		}
		return wallet, nil
	}

	wallet.Address = "0x" + strings.ToLower(hex.EncodeToString(addressBytes))
	return wallet, nil
}

func normalizeMnemonic(input string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(input)), " ")
}

func deriveKey(rootKey *hdkeychain.ExtendedKey, coinType, index uint32) (*hdkeychain.ExtendedKey, error) {
	segments := []uint32{
		hdkeychain.HardenedKeyStart + 44,
		hdkeychain.HardenedKeyStart + coinType,
		hdkeychain.HardenedKeyStart + 0,
		0,
		index,
	}

	current := rootKey
	var err error
	for _, segment := range segments {
		current, err = current.Derive(segment)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

func publicKeyToAddress(uncompressedPubKey []byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write(uncompressedPubKey[1:])
	sum := hash.Sum(nil)
	return sum[len(sum)-20:]
}
