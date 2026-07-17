package infrastructure

import (
	"fmt"
	"testing"
)

func TestCatfeeService_Order(t *testing.T) {
	t.Skip("skip network test")

	catfee, err := NewCatfeeService("72a74f5a-2f63-407b-bfc5-1c5f790334ca", "3b4855a097034cdae525b4d123b2b61a", "https://api.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	catfee.Order("TAPH2hzc29WZPpnsfVjnFVGc1YDJs2Audi", "65000")

}

func TestCatfeeService_Premium(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("72a74f5a-2f63-407b-bfc5-1c5f790334ca", "3b4855a097034cdae525b4d123b2b61a", "https://api.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	data, err := catfee.Premium("vip664", "3")

	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("data: %v\n", data)
}

// ; catfee:
// ;     catfee-apikey: 4beb7017-ef60-4bdb-b880-20880af87ed3
// ;     catfee-apisecret: b24ae29d03f61fbecb5381ee41b412bc
// ;     catfee-apiurl: https://nile.catfee.io
func TestCatfeeService_MateOpenBasicAdd(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	status, err := catfee.MateOpenBasicAdd("TAPH2hzc29WZPpnsfVjnFVGc1YDJs2Audi", "5788852829")

	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(status)
}

func TestCatfeeService_MateOpenBasicDelete(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	code, err := catfee.MateOpenBasicDelete("TY5t9HdU3h5ZT4LAw6E8ZN2jK297VL9999")

	if err != nil {
		fmt.Println("失败", err)
		return
	}

	fmt.Println(code)
}
func TestCatfeeService_MateOpenBasicGet(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	response, err := catfee.MateOpenBasicGet("TY5t9HdU3h5ZT4LAw6E8ZN2jK297VL9999")

	if err != nil {
		t.Fatal(err)
	}
	//fmt.Printf("%#v\n", response)

	fmt.Printf("地址：%s\n", response.Data.Address)
	fmt.Printf("已用次数：%d\n", response.Data.UsedCount)
	fmt.Printf("用户：%s\n", response.Data.Remark)
	fmt.Printf("状态：%s\n", response.Data.Status)

}
func TestCatfeeService_MateOpenBasicEnable(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	status, err := catfee.MateOpenBasicEnable("TXLEwaBgvDqwWTcHxScqhE5Jz8Ak3n2222")

	if err != nil {

	}

	fmt.Println(status)
}

func TestCatfeeService_MateOpenBasicDisable(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	status, err := catfee.MateOpenBasicDisable("TXLEwaBgvDqwWTcHxScqhE5Jz8Ak3n2222")

	if err != nil {

	}

	fmt.Println(status)
}
func TestCatfeeService_MateOpenBasicAdd2(t *testing.T) {
	t.Skip("skip network test")
	catfee, err := NewCatfeeService("4beb7017-ef60-4bdb-b880-20880af87ed3", "b24ae29d03f61fbecb5381ee41b412bc", "https://nile.catfee.io")
	if err != nil {
		fmt.Println("链接dial", err)
		t.Fatal(err)
	}
	status, err := catfee.MateOpenBasicEnable("TXLEwaBgvDqwWTcHxScqhE5Jz8Ak3n2222")

	if err != nil {

	}

	fmt.Println(status)
}
