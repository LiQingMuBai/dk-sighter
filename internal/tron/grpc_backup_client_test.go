package tron

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	gotronCore "github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestShouldInspectUSDTTriggerContract(t *testing.T) {
	client := NewGRPCBackupClient("grpc.example.com:443", "412222222222222222222222222222222222222222", "x-token", "secret", true, 15*time.Second, 20*time.Millisecond)
	watched := map[string]struct{}{
		NormalizeHexAddress("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"): {},
		NormalizeHexAddress("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"): {},
	}
	isWatchedHex := func(hexAddr string) bool {
		_, ok := watched[NormalizeHexAddress(hexAddr)]
		return ok
	}

	tests := []struct {
		name     string
		contract *gotronCore.Transaction_Contract
		want     bool
	}{
		{
			name: "transfer to watched address",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
				trc20TransferMethodID+encodeAddressArg("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")+repeatZeroWord(),
			),
			want: true,
		},
		{
			name: "transfer from watched owner",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				trc20TransferMethodID+encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+repeatZeroWord(),
			),
			want: true,
		},
		{
			name: "transfer without watched addresses",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
				trc20TransferMethodID+encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+repeatZeroWord(),
			),
			want: false,
		},
		{
			name: "transferFrom with watched from",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
				trc20TransferFromMethodID+
					encodeAddressArg("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")+
					encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+
					repeatZeroWord(),
			),
			want: true,
		},
		{
			name: "non usdt contract skipped",
			contract: mustBuildTriggerContract(t,
				"413333333333333333333333333333333333333333",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				trc20TransferMethodID+encodeAddressArg("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")+repeatZeroWord(),
			),
			want: false,
		},
		{
			name: "unknown method skipped",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"095ea7b3"+encodeAddressArg("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")+repeatZeroWord(),
			),
			want: false,
		},
		{
			name: "malformed payload stays conservative",
			contract: mustBuildTriggerContract(t,
				"412222222222222222222222222222222222222222",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"abcd",
			),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := client.ShouldInspectUSDTTriggerContract(tt.contract, isWatchedHex)
			if got != tt.want {
				t.Fatalf("ShouldInspectUSDTTriggerContract() = %v, want %v", got, tt.want)
			}
		})
	}
}

func mustBuildTriggerContract(t *testing.T, contractHex, ownerHex, dataHex string) *gotronCore.Transaction_Contract {
	t.Helper()

	contractAddr, err := hex.DecodeString(NormalizeHexAddress(contractHex))
	if err != nil {
		t.Fatalf("decode contract address: %v", err)
	}
	ownerAddr, err := hex.DecodeString(NormalizeHexAddress(ownerHex))
	if err != nil {
		t.Fatalf("decode owner address: %v", err)
	}
	dataBytes, err := hex.DecodeString(dataHex)
	if err != nil {
		t.Fatalf("decode data: %v", err)
	}

	parameterAny, err := anypb.New(&gotronCore.TriggerSmartContract{
		OwnerAddress:    ownerAddr,
		ContractAddress: contractAddr,
		Data:            dataBytes,
	})
	if err != nil {
		t.Fatalf("build trigger contract any: %v", err)
	}

	return &gotronCore.Transaction_Contract{
		Type:      gotronCore.Transaction_Contract_TriggerSmartContract,
		Parameter: parameterAny,
	}
}

func repeatZeroWord() string {
	return strings.Repeat("0", 64)
}
