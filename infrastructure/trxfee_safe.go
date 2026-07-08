package infrastructure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func NewTrxfeeClient(url, apiKey, apiSecret string) *TrxfeeClient {
	return &TrxfeeClient{
		URL:       strings.TrimRight(strings.TrimSpace(url), "/"),
		APIKey:    strings.TrimSpace(apiKey),
		APISecret: strings.TrimSpace(apiSecret),
	}
}

func (c *TrxfeeClient) Name() string {
	return "trxfee"
}

func (c *TrxfeeClient) IsConfigured() bool {
	if c == nil {
		return false
	}
	return strings.TrimSpace(c.URL) != ""
}

func (c *TrxfeeClient) OrderSafe(outTradeNo, receiveAddress string, energyAmount int) (string, error) {
	if c == nil {
		return "", fmt.Errorf("trxfee client is nil")
	}
	if !c.IsConfigured() {
		return "", fmt.Errorf("trxfee client is not configured")
	}
	if strings.TrimSpace(outTradeNo) == "" {
		return "", fmt.Errorf("outTradeNo is required")
	}
	if strings.TrimSpace(receiveAddress) == "" {
		return "", fmt.Errorf("receiveAddress is required")
	}
	if energyAmount <= 0 {
		return "", fmt.Errorf("energyAmount must be greater than 0")
	}

	data := struct {
		EnergyAmount   int    `json:"energy_amount"`
		Period         string `json:"period"`
		ReceiveAddress string `json:"receive_address"`
		CallbackURL    string `json:"callback_url"`
		OutTradeNo     string `json:"out_trade_no"`
		AutoActivation bool   `json:"auto_activation"`
	}{
		EnergyAmount:   energyAmount,
		Period:         "1H",
		ReceiveAddress: receiveAddress,
		CallbackURL:    "",
		OutTradeNo:     outTradeNo,
		AutoActivation: true,
	}

	bodyBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("marshal trxfee order body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.orderEndpoint(), bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("build trxfee request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("send trxfee request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read trxfee response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return string(respBody), fmt.Errorf("trxfee response status=%d body=%s", resp.StatusCode, string(respBody))
	}

	return string(respBody), nil
}

func (c *TrxfeeClient) OrderEnergy(receiveAddress string, energyAmount int) (string, error) {
	outTradeNo := fmt.Sprintf("TRXFEE%s", time.Now().UTC().Add(8*time.Hour).Format("20060102150405.000"))
	return c.OrderSafe(outTradeNo, receiveAddress, energyAmount)
}

func (c *TrxfeeClient) orderEndpoint() string {
	baseURL := strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if strings.HasSuffix(baseURL, "/api/trxfee/order") {
		return baseURL
	}
	return baseURL + "/api/trxfee/order"
}
