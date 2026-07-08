package infrastructure

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type CatfeeService struct {
	apiKey    string
	apiSecret string
	url       string
}

func NewCatfeeService(apiKey, apiSecret, url string) (*CatfeeService, error) {
	return &CatfeeService{apiKey: apiKey, apiSecret: apiSecret, url: url}, nil
}

type PremiumDataResp struct {
	Code    int    `json:"code"`
	Msg     string `json:"msg"`
	SubCode string `json:"sub_code"`
	SubMsg  string `json:"sub_msg"`
	Data    struct {
		ClientOrderID string `json:"client_order_id"`
		Username      string `json:"username"`
		Months        int    `json:"months"`
		UsdtSun       int    `json:"usdt_sun"`
		CreatedAt     int    `json:"created_at"`
		Status        string `json:"status"`
	} `json:"data"`
}

//
//// 下单
//func (s CatfeeService) Order(_address string) {
//	method := "POST" // Can be "GET", "PUT", or "DELETE"
//	path := "/v1/order"
//
//	// Example: Create order
//	queryParams := map[string]string{
//		"quantity": "65000",
//		"receiver": _address,
//		"duration": "1h",
//	}
//
//	timestamp := s.GenerateTimestamp()
//	requestPath := s.BuildRequestPath(path, queryParams)
//	signature := s.GenerateSignature(timestamp, method, requestPath)
//
//	url := s.url + requestPath
//
//	resp, err := s.CreateRequest(url, method, timestamp, signature)
//	if err != nil {
//		log.Fatal("Error making request:", err)
//	}
//
//	body, err := ioutil.ReadAll(resp.Body)
//	if err != nil {
//		log.Fatal("Error reading response:", err)
//	}
//
//	fmt.Println("Response Status:", resp.Status)
//	fmt.Println("Response Body:", string(body))
//}

// 下单
func (s CatfeeService) Order(_receiveAddress string, _energyAmount string) {
	method := "POST" // Can be "GET", "PUT", or "DELETE"
	path := "/v1/order"

	// Example: Create order
	queryParams := map[string]string{
		"quantity": _energyAmount,
		"receiver": _receiveAddress,
		"duration": "1h",
	}

	timestamp := s.GenerateTimestamp()
	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	url := s.url + requestPath

	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	fmt.Println("Response Status:", resp.Status)
	fmt.Println("Response Body:", string(body))
}

// 购买电报会员
func (s CatfeeService) Premium(username string, months string) (PremiumDataResp, error) {
	method := "POST" // 可以修改为 "GET", "PUT", "DELETE"

	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/premium?username=" + username + "&months=" + months
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	// 解析JSON响应
	var dataResp PremiumDataResp
	err = json.Unmarshal(body, &dataResp)
	if err != nil {
		fmt.Printf("添加基础版地址，解析JSON失败: %v\n", err)
	}

	return dataResp, nil

}

// 增加
func (s CatfeeService) MateOpenBasicGet(_address string) (BasicAddressResp, error) {
	method := "GET" // 可以修改为 "GET", "PUT", "DELETE"

	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/mate/open/basic/" + _address
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	fmt.Println(string(body))
	// 解析JSON响应
	var baseAddressResp BasicAddressResp
	err = json.Unmarshal(body, &baseAddressResp)
	if err != nil {
		fmt.Printf("查询基础版地址，解析JSON失败: %v\n", err)
	}

	return baseAddressResp, nil

}

// 增加
func (s CatfeeService) MateOpenBasicAdd(_address, _chatID string) (string, error) {
	method := "POST" // 可以修改为 "GET", "PUT", "DELETE"

	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/mate/open/basic?address=" + _address + "&remark=" + _chatID + "&is_auto_closable=true&quota_mode=UNLIMITED"
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	// 解析JSON响应
	var baseAddressResp BasicAddressResp
	err = json.Unmarshal(body, &baseAddressResp)
	if err != nil {
		fmt.Printf("添加基础版地址，解析JSON失败: %v\n", err)
	}

	return baseAddressResp.Data.Status, nil

}

// 关闭
func (s CatfeeService) MateOpenBasicDisable(_address string) (string, error) {
	method := "PATCH" // 可以修改为 "GET", "PUT", "DELETE"
	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/mate/open/basic/" + _address + "/enable"
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{
		"value": "false",
	}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}
	fmt.Println(string(body))
	// 解析JSON响应
	var baseAddressResp BasicAddressResp
	err = json.Unmarshal(body, &baseAddressResp)
	if err != nil {
		fmt.Printf("添加基础版地址，解析JSON失败: %v\n", err)
	}

	return baseAddressResp.Data.Status, nil

}

// 启动
func (s CatfeeService) MateOpenBasicEnable(_address string) (string, error) {
	method := "PATCH" // 可以修改为 "GET", "PUT", "DELETE"
	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/mate/open/basic/" + _address + "/enable"
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{
		"value": "true",
	}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	fmt.Println(string(body))
	// 解析JSON响应
	var baseAddressResp BasicAddressResp
	err = json.Unmarshal(body, &baseAddressResp)
	if err != nil {
		fmt.Printf("添加基础版地址，解析JSON失败: %v\n", err)
	}

	return baseAddressResp.Data.Status, nil

}

// 删除
func (s CatfeeService) MateOpenBasicDelete(_address string) (int, error) {
	method := "DELETE" // 可以修改为 "GET", "PUT", "DELETE"
	//POST /v1/mate/open/basic?address=text&is_auto_closable=true&quota_mode=UNLIMITED HTTP/1.1
	//Host: api.catfee.io
	//CF-ACCESS-KEY: text
	//CF-ACCESS-SIGN: text
	//CF-ACCESS-TIMESTAMP: text
	//Accept: */*
	path := "/v1/mate/open/basic/" + _address
	//// 生成请求头
	timestamp := s.GenerateTimestamp()
	queryParams := map[string]string{}

	requestPath := s.BuildRequestPath(path, queryParams)
	signature := s.GenerateSignature(timestamp, method, requestPath)

	// 创建请求 URL
	url := s.url + requestPath

	// 发送请求
	resp, err := s.CreateRequest(url, method, timestamp, signature)
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	// 读取并输出响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	fmt.Println(string(body))
	// 解析JSON响应
	var baseAddressResp BasicAddressDeleteResp
	err = json.Unmarshal(body, &baseAddressResp)
	if err != nil {
		fmt.Printf("删除基础版地址，解析JSON失败: %v\n", err)
	}

	return baseAddressResp.Code, nil
}

// 生成当前的时间戳（ISO 8601格式）
func (s CatfeeService) GenerateTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// 构建请求路径，包括查询参数
func (s CatfeeService) BuildRequestPath(path string, queryParams map[string]string) string {
	if len(queryParams) == 0 {
		return path
	}

	queryString := "?"
	for key, value := range queryParams {
		queryString += fmt.Sprintf("%s=%s&", key, value)
	}

	// 去掉最后的"&"符号
	queryString = queryString[:len(queryString)-1]

	return path + queryString
}

// 使用 HMAC-SHA256 算法生成签名
func (s CatfeeService) GenerateSignature(timestamp, method, requestPath string) string {
	signString := timestamp + method + requestPath
	mac := hmac.New(sha256.New, []byte(s.apiSecret))
	mac.Write([]byte(signString))
	signature := mac.Sum(nil)
	return base64.StdEncoding.EncodeToString(signature)
}

// 创建 HTTP 请求
func (s CatfeeService) CreateRequest(url, method, timestamp, signature string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	// 设置请求头
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("CF-ACCESS-KEY", s.apiKey)
	req.Header.Add("CF-ACCESS-SIGN", signature)
	req.Header.Add("CF-ACCESS-TIMESTAMP", timestamp)

	return client.Do(req)
}

type BasicAddressResp struct {
	Code    int    `json:"code"`
	Msg     string `json:"msg"`
	SubCode string `json:"sub_code"`
	SubMsg  string `json:"sub_msg"`
	Data    struct {
		ID                       int    `json:"id"`
		Address                  string `json:"address"`
		PlanID                   int    `json:"plan_id"`
		PlanNameEn               string `json:"plan_name_en"`
		PlanNameZh               string `json:"plan_name_zh"`
		PlanSubscriptionSun      int    `json:"plan_subscription_sun"`
		PlanUnitSun              int    `json:"plan_unit_sun"`
		QuotaMode                string `json:"quota_mode"`
		SlotNextBillingTimestamp int    `json:"slot_next_billing_timestamp"`
		Quantity                 int    `json:"quantity"`
		UsedQuantity             int    `json:"used_quantity"`
		QuotaCount               int    `json:"quota_count"`
		UsedCount                int    `json:"used_count"`
		QuotaStartTime           int    `json:"quota_start_time"`
		TotalTransferCount       int    `json:"total_transfer_count"`
		TotalEnergyCount         int    `json:"total_energy_count"`
		TotalSlotFee             int    `json:"total_slot_fee"`
		TotalEnergyFee           int    `json:"total_energy_fee"`
		Remark                   string `json:"remark"`
		IsAutoClosable           bool   `json:"is_auto_closable"`
		IdleHours                int    `json:"idle_hours"`
		Status                   string `json:"status"`
		ExpiredAt                int    `json:"expired_at"`
		CreatedAt                int    `json:"created_at"`
	} `json:"data"`
}
type BasicAddressDeleteResp struct {
	Code int  `json:"code"`
	Data bool `json:"data"`
}
