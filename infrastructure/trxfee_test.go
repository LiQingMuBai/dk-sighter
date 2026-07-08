package infrastructure

import (
	"testing"
)

//
//func TestTrxfeeClient_EnableTimesOrder(t *testing.T) {
//
//	trxfee := NewTrxfeeClient("https://trxfee.io/", "CC4F20ACDB45AFA10A22D6BDA2AE9F3F", "99144B2AC7ED7F73ECFF59144D46E321F1DC83B373DE2FF6A367423F4CF61FB5")
//
//	trxfee.Order("12321321", "TS4WHd3PyEiYXDxRZbmofj1zugudW6Dior", 65_000*1)
//
//}
//func TestTrxfeeClient_DisableTimesOrder(t *testing.T) {
//
//	username := "@12"
//
//	if len(username) < 4 || !strings.Contains(username, "@") {
//		fmt.Println("没包括")
//	} else {
//		fmt.Println(username)
//	}
//
//}

func TestActivationAddress(t *testing.T) {
	trxfee := NewTrxfeeClient("https://trxfee.io/", "CC4F20ACDB45AFA10A22D6BDA2AE9F3F", "99144B2AC7ED7F73ECFF59144D46E321F1DC83B373DE2FF6A367423F4CF61FB5", "")
	trxfee.Activation("TBCG8qr7TSLZqYLYsf8UB3uoKSMWJ9qo94")
}
