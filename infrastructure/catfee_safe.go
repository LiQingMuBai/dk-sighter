package infrastructure

import (
	"fmt"
	"io"
	"log"
	"strings"
)

type EnergyOrderProvider interface {
	Name() string
	IsConfigured() bool
	OrderEnergy(receiveAddress string, energyAmount int) (string, error)
}

type CatfeeSafeClient struct {
	service CatfeeService
}

func NewCatfeeSafeClient(url, apiKey, apiSecret string) *CatfeeSafeClient {
	return &CatfeeSafeClient{
		service: CatfeeService{
			url:       strings.TrimRight(strings.TrimSpace(url), "/"),
			apiKey:    strings.TrimSpace(apiKey),
			apiSecret: strings.TrimSpace(apiSecret),
		},
	}
}

func (c *CatfeeSafeClient) Name() string {
	return "catfee"
}

func (c *CatfeeSafeClient) IsConfigured() bool {
	if c == nil {
		return false
	}
	return strings.TrimSpace(c.service.url) != "" &&
		strings.TrimSpace(c.service.apiKey) != "" &&
		strings.TrimSpace(c.service.apiSecret) != ""
}

func (c *CatfeeSafeClient) OrderEnergy(receiveAddress string, energyAmount int) (respText string, err error) {
	if c == nil {
		return "", fmt.Errorf("catfee client is nil")
	}
	if !c.IsConfigured() {
		return "", fmt.Errorf("catfee client is not configured")
	}
	if strings.TrimSpace(receiveAddress) == "" {
		return "", fmt.Errorf("receiveAddress is required")
	}
	if energyAmount <= 0 {
		return "", fmt.Errorf("energyAmount must be greater than 0")
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("catfee panic recovered: %v", recovered)
		}
	}()

	method := "POST"
	path := "/v1/order"
	queryParams := map[string]string{
		"quantity": fmt.Sprintf("%d", energyAmount),
		"receiver": receiveAddress,
		"duration": "1h",
	}

	timestamp := c.service.GenerateTimestamp()
	requestPath := c.service.BuildRequestPath(path, queryParams)
	signature := c.service.GenerateSignature(timestamp, method, requestPath)
	url := c.service.url + requestPath

	resp, reqErr := c.service.CreateRequest(url, method, timestamp, signature)
	if reqErr != nil {
		return "", fmt.Errorf("catfee create request: %w", reqErr)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", fmt.Errorf("catfee read response: %w", readErr)
	}

	respText = string(body)
	if resp.StatusCode >= 300 {
		return respText, fmt.Errorf("catfee response status=%s body=%s", resp.Status, respText)
	}

	log.Printf("catfee response status=%s body=%s", resp.Status, respText)
	return respText, nil
}
