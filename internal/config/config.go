package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig           `yaml:"app"`
	MySQL         MySQLConfig         `yaml:"mysql"`
	QuickNode     QuickNodeConfig     `yaml:"quicknode"`
	Watcher       WatcherConfig       `yaml:"watcher"`
	Web           WebConfig           `yaml:"web"`
	Telegram      TelegramConfig      `yaml:"telegram"`
	Callback      CallbackConfig      `yaml:"callback"`
	Energy        EnergyConfig        `yaml:"energy"`
	Trxfee        TrxfeeConfig        `yaml:"trxfee"`
	Catfee        CatfeeConfig        `yaml:"catfee"`
	BSC           BSCConfig           `yaml:"bsc"`
	TronActivator TronActivatorConfig `yaml:"tron_activator"`
}

type AppConfig struct {
	Name          string `yaml:"name"`
	Mode          string `yaml:"mode"`
	HDWalletCount int    `yaml:"hd_wallet_count"`
	Local         bool   `yaml:"local"`
}

type MySQLConfig struct {
	DSN             string `yaml:"dsn"`
	MaxOpenConns    int    `yaml:"max_open_conns"`
	MaxIdleConns    int    `yaml:"max_idle_conns"`
	ConnMaxLifetime int    `yaml:"conn_max_lifetime_seconds"`
	SessionTimeZone string `yaml:"session_time_zone"`
}

type QuickNodeConfig struct {
	HTTPURL              string `yaml:"http_url"`
	WSSURL               string `yaml:"wss_url"`
	USDT                 string `yaml:"usdt_contract"`
	MinRequestIntervalMS int    `yaml:"min_request_interval_ms"`
	RefreshHTTPURL       string `yaml:"refresh_http_url"`
	RefreshWSSURL        string `yaml:"refresh_wss_url"`
	RefreshMinIntervalMS int    `yaml:"refresh_min_request_interval_ms"`
}

type WebConfig struct {
	Listen      string `yaml:"listen"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	SessionName string `yaml:"session_name"`
	APIKey      string `yaml:"api_key"`
}

type TelegramConfig struct {
	Enabled        bool   `yaml:"enabled"`
	BotToken       string `yaml:"bot_token"`
	ChatID         string `yaml:"chat_id"`
	APIBaseURL     string `yaml:"api_base_url"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	QueueSize      int    `yaml:"queue_size"`
	MinAmount      string `yaml:"min_amount"`
}

type CallbackConfig struct {
	Enabled        bool   `yaml:"enabled"`
	URL            string `yaml:"url"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	QueueSize      int    `yaml:"queue_size"`
	MinAmount      string `yaml:"min_amount"`
}

type TrxfeeConfig struct {
	URL       string `yaml:"url"`
	APIKey    string `yaml:"api_key"`
	APISecret string `yaml:"api_secret"`
}

type EnergyConfig struct {
	Provider string `yaml:"provider"`
}

type CatfeeConfig struct {
	URL       string `yaml:"url"`
	APIKey    string `yaml:"api_key"`
	APISecret string `yaml:"api_secret"`
}

type WatcherConfig struct {
	AddressReloadIntervalSeconds int    `yaml:"address_reload_interval_seconds"`
	BlockPollIntervalSeconds     int    `yaml:"block_poll_interval_seconds"`
	TronBlockSource              string `yaml:"tron_block_source"`
	Confirmations                int    `yaml:"confirmations"`
	StartBlock                   int64  `yaml:"start_block"`
	TXWorkers                    int    `yaml:"tx_workers"`
}

type BSCConfig struct {
	RPCHTTPURL               string `yaml:"rpc_http_url"`
	RPCWSSURL                string `yaml:"rpc_wss_url"`
	USDTContract             string `yaml:"usdt_contract"`
	GasTransferPrivateKey    string `yaml:"gas_transfer_private_key"`
	MinRequestIntervalMS     int    `yaml:"min_request_interval_ms"`
	RefreshRPCHTTPURL        string `yaml:"refresh_rpc_http_url"`
	RefreshRPCWSSURL         string `yaml:"refresh_rpc_wss_url"`
	RefreshMinIntervalMS     int    `yaml:"refresh_min_request_interval_ms"`
	StartBlock               int64  `yaml:"start_block"`
	BlockPollIntervalSeconds int    `yaml:"block_poll_interval_seconds"`
	Confirmations            int    `yaml:"confirmations"`
}

type TronActivatorConfig struct {
	Enabled     bool     `yaml:"enabled"`
	PrivateKey  string   `yaml:"private_key"`
	PrivateKeys []string `yaml:"private_keys"`
	QueueSize   int      `yaml:"queue_size"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	cfg.setDefaults()
	return cfg, nil
}

func (c *Config) setDefaults() {
	if c.App.Name == "" {
		c.App.Name = "tron-watcher"
	}
	if c.App.Mode == "" {
		c.App.Mode = "watcher"
	}
	if c.App.HDWalletCount == 0 {
		c.App.HDWalletCount = 10000
	}
	if c.MySQL.MaxOpenConns == 0 {
		c.MySQL.MaxOpenConns = 20
	}
	if c.MySQL.MaxIdleConns == 0 {
		c.MySQL.MaxIdleConns = 10
	}
	if c.MySQL.ConnMaxLifetime == 0 {
		c.MySQL.ConnMaxLifetime = 300
	}
	if c.MySQL.SessionTimeZone == "" {
		c.MySQL.SessionTimeZone = "+08:00"
	}
	if c.Watcher.AddressReloadIntervalSeconds == 0 {
		c.Watcher.AddressReloadIntervalSeconds = 15
	}
	if c.Watcher.BlockPollIntervalSeconds == 0 {
		c.Watcher.BlockPollIntervalSeconds = 3
	}
	if strings.TrimSpace(c.Watcher.TronBlockSource) == "" {
		c.Watcher.TronBlockSource = "head"
	}
	if c.Watcher.TXWorkers == 0 {
		c.Watcher.TXWorkers = 8
	}
	if c.Web.Listen == "" {
		c.Web.Listen = ":8080"
	}
	if c.Web.SessionName == "" {
		c.Web.SessionName = "tron_watcher_session"
	}
	if c.QuickNode.MinRequestIntervalMS == 0 {
		c.QuickNode.MinRequestIntervalMS = 10
	}
	if c.QuickNode.RefreshMinIntervalMS == 0 && strings.TrimSpace(c.QuickNode.RefreshHTTPURL) != "" {
		c.QuickNode.RefreshMinIntervalMS = 30
	}
	if c.BSC.MinRequestIntervalMS == 0 {
		c.BSC.MinRequestIntervalMS = 10
	}
	if c.BSC.RefreshMinIntervalMS == 0 && strings.TrimSpace(c.BSC.RefreshRPCHTTPURL) != "" {
		c.BSC.RefreshMinIntervalMS = 30
	}
	if len(c.TronActivator.PrivateKeys) == 0 && strings.TrimSpace(c.TronActivator.PrivateKey) != "" {
		c.TronActivator.PrivateKeys = []string{strings.TrimSpace(c.TronActivator.PrivateKey)}
	}
	if strings.TrimSpace(c.TronActivator.PrivateKey) == "" && len(c.TronActivator.PrivateKeys) == 1 {
		c.TronActivator.PrivateKey = strings.TrimSpace(c.TronActivator.PrivateKeys[0])
	}
	if c.Telegram.APIBaseURL == "" {
		c.Telegram.APIBaseURL = "https://api.telegram.org"
	}
	if c.Telegram.TimeoutSeconds == 0 {
		c.Telegram.TimeoutSeconds = 5
	}
	if c.Telegram.QueueSize == 0 {
		c.Telegram.QueueSize = 256
	}
	if c.Telegram.MinAmount == "" {
		c.Telegram.MinAmount = "1"
	}
	if c.Callback.TimeoutSeconds == 0 {
		c.Callback.TimeoutSeconds = 5
	}
	if c.Callback.QueueSize == 0 {
		c.Callback.QueueSize = 256
	}
	if c.Callback.MinAmount == "" {
		c.Callback.MinAmount = "1"
	}
	if c.Energy.Provider == "" {
		c.Energy.Provider = "trxfee"
	}
	if c.BSC.BlockPollIntervalSeconds == 0 {
		c.BSC.BlockPollIntervalSeconds = 3
	}
}

func (c *Config) ConnMaxLifetime() time.Duration {
	return time.Duration(c.MySQL.ConnMaxLifetime) * time.Second
}

func (c *Config) AddressReloadInterval() time.Duration {
	return time.Duration(c.Watcher.AddressReloadIntervalSeconds) * time.Second
}

func (c *Config) BlockPollInterval() time.Duration {
	return time.Duration(c.Watcher.BlockPollIntervalSeconds) * time.Second
}

func (c *Config) BSCBlockPollInterval() time.Duration {
	return time.Duration(c.BSC.BlockPollIntervalSeconds) * time.Second
}

func (c *Config) TronBlockSource() string {
	value := strings.TrimSpace(c.Watcher.TronBlockSource)
	if strings.EqualFold(value, "solid") {
		return "solid"
	}
	return "head"
}

func (c *Config) QuickNodeMinRequestInterval() time.Duration {
	value := c.QuickNode.MinRequestIntervalMS
	if value <= 0 {
		value = 10
	}
	return time.Duration(value) * time.Millisecond
}

func (c *Config) QuickNodeRefreshHTTPURL() string {
	return strings.TrimSpace(c.QuickNode.RefreshHTTPURL)
}

func (c *Config) QuickNodeRefreshWSSURL() string {
	return strings.TrimSpace(c.QuickNode.RefreshWSSURL)
}

func (c *Config) QuickNodeRefreshMinRequestInterval() time.Duration {
	value := c.QuickNode.RefreshMinIntervalMS
	if value <= 0 {
		value = c.QuickNode.MinRequestIntervalMS
	}
	if value <= 0 {
		value = 10
	}
	return time.Duration(value) * time.Millisecond
}

func (c *Config) BSCMinRequestInterval() time.Duration {
	value := c.BSC.MinRequestIntervalMS
	if value <= 0 {
		value = 10
	}
	return time.Duration(value) * time.Millisecond
}

func (c *Config) BSCRefreshHTTPURL() string {
	return strings.TrimSpace(c.BSC.RefreshRPCHTTPURL)
}

func (c *Config) BSCRefreshWSSURL() string {
	return strings.TrimSpace(c.BSC.RefreshRPCWSSURL)
}

func (c *Config) BSCRefreshMinRequestInterval() time.Duration {
	value := c.BSC.RefreshMinIntervalMS
	if value <= 0 {
		value = c.BSC.MinRequestIntervalMS
	}
	if value <= 0 {
		value = 10
	}
	return time.Duration(value) * time.Millisecond
}
