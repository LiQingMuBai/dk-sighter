package tron

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	gotronAddress "github.com/fbsobreira/gotron-sdk/pkg/address"
	gotronAPI "github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	gotronCore "github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	grpcDefaultTimeout            = 15 * time.Second
	grpcDefaultMinRequestInterval = 20 * time.Millisecond
	grpcTransferTopic             = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	grpcMaxRecvMsgSize            = 32 * 10e6
)

type GRPCBackupClient struct {
	address            string
	usdtContractHex    string
	usdtContractBase58 string
	tokenHeader        string
	token              string
	useTLS             bool
	timeout            time.Duration
	minInterval        time.Duration
	conn               *grpc.ClientConn
	wallet             gotronAPI.WalletClient
	walletSolidity     gotronAPI.WalletSolidityClient
	rateMu             sync.Mutex
	nextRequestTime    time.Time
}

func NewGRPCBackupClient(address, usdtContract, tokenHeader, token string, useTLS bool, timeout, minRequestInterval time.Duration) *GRPCBackupClient {
	if timeout <= 0 {
		timeout = grpcDefaultTimeout
	}
	if minRequestInterval <= 0 {
		minRequestInterval = grpcDefaultMinRequestInterval
	}
	tokenHeader = strings.ToLower(strings.TrimSpace(tokenHeader))
	if tokenHeader == "" {
		tokenHeader = "x-catfee-token"
	}
	usdtContract = strings.TrimSpace(usdtContract)
	usdtContractHex := normalizeContractAddress(usdtContract)
	usdtContractBase58 := strings.TrimSpace(usdtContract)
	if strings.HasPrefix(usdtContractHex, "41") && !strings.HasPrefix(usdtContractBase58, "T") {
		if converted, err := HexToBase58(usdtContractHex); err == nil {
			usdtContractBase58 = converted
		}
	}
	return &GRPCBackupClient{
		address:            strings.TrimSpace(address),
		usdtContractHex:    usdtContractHex,
		usdtContractBase58: usdtContractBase58,
		tokenHeader:        tokenHeader,
		token:              strings.TrimSpace(token),
		useTLS:             useTLS,
		timeout:            timeout,
		minInterval:        minRequestInterval,
	}
}

func (c *GRPCBackupClient) Start() error {
	if strings.TrimSpace(c.address) == "" {
		return fmt.Errorf("empty grpc address")
	}

	var transport grpc.DialOption
	if c.useTLS {
		transport = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
	} else {
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	conn, err := grpc.NewClient(c.address, transport)
	if err != nil {
		return fmt.Errorf("connect grpc client: %w", err)
	}

	c.conn = conn
	c.wallet = gotronAPI.NewWalletClient(conn)
	c.walletSolidity = gotronAPI.NewWalletSolidityClient(conn)
	return nil
}

func (c *GRPCBackupClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *GRPCBackupClient) GetHeadBlockNumber(ctx context.Context) (int64, error) {
	if err := c.waitTurn(ctx); err != nil {
		return 0, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	block, err := c.wallet.GetNowBlock2(callCtx, new(gotronAPI.EmptyMessage))
	if err != nil {
		return 0, fmt.Errorf("get head block: %w", err)
	}
	return block.GetBlockHeader().GetRawData().GetNumber(), nil
}

func (c *GRPCBackupClient) GetSolidBlockNumber(ctx context.Context) (int64, error) {
	if err := c.waitTurn(ctx); err != nil {
		return 0, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	block, err := c.walletSolidity.GetNowBlock2(callCtx, new(gotronAPI.EmptyMessage))
	if err != nil {
		return 0, fmt.Errorf("get solid block: %w", err)
	}
	return block.GetBlockHeader().GetRawData().GetNumber(), nil
}

func (c *GRPCBackupClient) GetBlockByNum(ctx context.Context, blockNum int64, source string) (*gotronAPI.BlockExtention, error) {
	if err := c.waitTurn(ctx); err != nil {
		return nil, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	request := &gotronAPI.NumberMessage{Num: blockNum}
	option := grpc.MaxCallRecvMsgSize(grpcMaxRecvMsgSize)
	if strings.EqualFold(strings.TrimSpace(source), "solid") {
		block, err := c.walletSolidity.GetBlockByNum2(callCtx, request, option)
		if err != nil {
			return nil, fmt.Errorf("get solid block by num %d: %w", blockNum, err)
		}
		return block, nil
	}

	block, err := c.wallet.GetBlockByNum2(callCtx, request, option)
	if err != nil {
		return nil, fmt.Errorf("get head block by num %d: %w", blockNum, err)
	}
	return block, nil
}

func (c *GRPCBackupClient) GetTransactionInfoByID(ctx context.Context, txID string, source string) (*gotronCore.TransactionInfo, error) {
	if err := c.waitTurn(ctx); err != nil {
		return nil, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	txID = strings.TrimPrefix(strings.TrimSpace(txID), "0x")
	txBytes, err := hex.DecodeString(txID)
	if err != nil {
		return nil, fmt.Errorf("decode tx id: %w", err)
	}
	request := &gotronAPI.BytesMessage{Value: txBytes}
	if strings.EqualFold(strings.TrimSpace(source), "solid") {
		info, err := c.walletSolidity.GetTransactionInfoById(callCtx, request)
		if err != nil {
			fallbackCtx, fallbackCancel := c.callContext(ctx)
			defer fallbackCancel()

			fallbackInfo, fallbackErr := c.wallet.GetTransactionInfoById(fallbackCtx, request)
			if fallbackErr != nil {
				return nil, fmt.Errorf("get solid tx info %s: %w; fallback get head tx info: %v", txID, err, fallbackErr)
			}
			return fallbackInfo, nil
		}
		return info, nil
	}

	info, err := c.wallet.GetTransactionInfoById(callCtx, request)
	if err != nil {
		return nil, fmt.Errorf("get head tx info %s: %w", txID, err)
	}
	return info, nil
}

func (c *GRPCBackupClient) GetAccountState(ctx context.Context, addressBase58 string) (bool, decimal.Decimal, error) {
	addressBytes, err := decodeBase58Address(addressBase58)
	if err != nil {
		return false, decimal.Zero, err
	}
	if err := c.waitTurn(ctx); err != nil {
		return false, decimal.Zero, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	account, err := c.walletSolidity.GetAccount(callCtx, &gotronCore.Account{Address: addressBytes})
	if err != nil {
		return false, decimal.Zero, fmt.Errorf("get account: %w", err)
	}
	if len(account.GetAddress()) == 0 {
		return false, decimal.Zero, nil
	}
	balance := decimal.NewFromInt(account.GetBalance()).Div(decimal.NewFromInt(trxPrecision))
	return true, balance, nil
}

func (c *GRPCBackupClient) GetUSDTBalance(ctx context.Context, addressBase58 string) (decimal.Decimal, error) {
	if strings.TrimSpace(c.usdtContractHex) == "" {
		return decimal.Zero, fmt.Errorf("empty usdt contract")
	}

	ownerAddress, err := gotronAddress.HexToAddress("410000000000000000000000000000000000000000")
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid zero owner address: %w", err)
	}
	contractAddress, err := gotronAddress.Base58ToAddress(c.usdtContractBase58)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid usdt contract address: %w", err)
	}
	targetAddress, err := gotronAddress.Base58ToAddress(strings.TrimSpace(addressBase58))
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid target address: %w", err)
	}

	dataHex := "0x70a08231" + strings.Repeat("0", 64-len(targetAddress.Hex()[2:])) + targetAddress.Hex()[2:]
	dataBytes, err := hex.DecodeString(strings.TrimPrefix(dataHex, "0x"))
	if err != nil {
		return decimal.Zero, fmt.Errorf("decode balanceOf data: %w", err)
	}

	if err := c.waitTurn(ctx); err != nil {
		return decimal.Zero, err
	}
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp, err := c.walletSolidity.TriggerConstantContract(callCtx, &gotronCore.TriggerSmartContract{
		OwnerAddress:    ownerAddress.Bytes(),
		ContractAddress: contractAddress.Bytes(),
		Data:            dataBytes,
	})
	if err != nil {
		return decimal.Zero, fmt.Errorf("trigger balanceOf: %w", err)
	}
	if len(resp.GetConstantResult()) == 0 {
		return decimal.Zero, fmt.Errorf("empty balanceOf result")
	}

	value := new(big.Int).SetBytes(resp.GetConstantResult()[0])
	return decimal.NewFromBigInt(value, 0).Div(decimal.NewFromInt(usdtPrecision)), nil
}

func (c *GRPCBackupClient) IsUSDTTransferLog(logItem *gotronCore.TransactionInfo_Log) bool {
	if logItem == nil {
		return false
	}
	if NormalizeHexAddress(hex.EncodeToString(logItem.GetAddress())) != c.usdtContractHex {
		return false
	}
	topics := logItem.GetTopics()
	if len(topics) == 0 {
		return false
	}
	return strings.EqualFold(hex.EncodeToString(topics[0]), grpcTransferTopic)
}

func (c *GRPCBackupClient) DecodeTransferLog(logItem *gotronCore.TransactionInfo_Log) (fromHex string, toHex string, amount decimal.Decimal, err error) {
	if logItem == nil || len(logItem.GetTopics()) < 3 {
		return "", "", decimal.Zero, fmt.Errorf("insufficient topics")
	}

	fromHex = NormalizeHexAddress(hex.EncodeToString(logItem.GetTopics()[1]))
	if len(fromHex) > 42 {
		fromHex = NormalizeHexAddress(fromHex[len(fromHex)-40:])
	}
	toHex = NormalizeHexAddress(hex.EncodeToString(logItem.GetTopics()[2]))
	if len(toHex) > 42 {
		toHex = NormalizeHexAddress(toHex[len(toHex)-40:])
	}

	value := new(big.Int).SetBytes(logItem.GetData())
	return fromHex, toHex, decimal.NewFromBigInt(value, 0).Div(decimal.NewFromInt(usdtPrecision)), nil
}

func (c *GRPCBackupClient) DecodeTransferContract(contract *gotronCore.Transaction_Contract) (*gotronCore.TransferContract, error) {
	if contract == nil || contract.GetParameter() == nil {
		return nil, fmt.Errorf("empty transfer contract")
	}
	message := new(gotronCore.TransferContract)
	if err := proto.Unmarshal(contract.GetParameter().GetValue(), message); err != nil {
		return nil, fmt.Errorf("unmarshal transfer contract: %w", err)
	}
	return message, nil
}

func (c *GRPCBackupClient) waitTurn(ctx context.Context) error {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()

	now := time.Now()
	wait := time.Until(c.nextRequestTime)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	now = time.Now()
	c.nextRequestTime = now.Add(c.minInterval)
	return nil
}

func (c *GRPCBackupClient) callContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	if strings.TrimSpace(c.token) != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, c.tokenHeader, c.token)
	}
	return ctx, cancel
}

func decodeBase58Address(addressBase58 string) ([]byte, error) {
	addressHex, err := Base58ToHex(strings.TrimSpace(addressBase58))
	if err != nil {
		return nil, fmt.Errorf("decode base58 address: %w", err)
	}
	addressBytes, err := hex.DecodeString(addressHex)
	if err != nil {
		return nil, fmt.Errorf("decode hex address: %w", err)
	}
	return addressBytes, nil
}
