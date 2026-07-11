package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App       AppConfig       `yaml:"app"`
	MySQL     MySQLConfig     `yaml:"mysql"`
	QuickNode QuickNodeConfig `yaml:"quicknode"`
	Watcher   WatcherConfig   `yaml:"watcher"`
	Web       WebConfig       `yaml:"web"`
	Telegram  TelegramConfig  `yaml:"telegram"`
	Callback  CallbackConfig  `yaml:"callback"`
	Energy    EnergyConfig    `yaml:"energy"`
	Trxfee    TrxfeeConfig    `yaml:"trxfee"`
	Catfee    CatfeeConfig    `yaml:"catfee"`
	BSC       BSCConfig       `yaml:"bsc"`
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
	XAPIKey   string `yaml:"x_api_key"`
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
	Confirmations                int    `yaml:"confirmations"`
	StartBlock                   int64  `yaml:"start_block"`
	TXWorkers                    int    `yaml:"tx_workers"`
	TronBlockSource              string `yaml:"tron_block_source"`
	BalanceRequestDelayMS        int    `yaml:"balance_request_delay_ms"`
	ScheduledRefreshDelayMS      int    `yaml:"scheduled_refresh_delay_ms"`
	DisableBlockSync             bool   `yaml:"disable_block_sync"`
	DisableScheduledBalanceSync  bool   `yaml:"disable_scheduled_balance_sync"`
}

type BSCConfig struct {
	RPCHTTPURL                  string `yaml:"rpc_http_url"`
	RPCWSSURL                   string `yaml:"rpc_wss_url"`
	USDTContract                string `yaml:"usdt_contract"`
	GasTransferPrivateKey       string `yaml:"gas_transfer_private_key"`
	StartBlock                  int64  `yaml:"start_block"`
	BlockPollIntervalSeconds    int    `yaml:"block_poll_interval_seconds"`
	Confirmations               int    `yaml:"confirmations"`
	MinRequestIntervalMS        int    `yaml:"min_request_interval_ms"`
	ScheduledRefreshDelayMS     int    `yaml:"scheduled_refresh_delay_ms"`
	DisableBlockSync            bool   `yaml:"disable_block_sync"`
	DisableScheduledBalanceSync bool   `yaml:"disable_scheduled_balance_sync"`
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
	if c.Watcher.TXWorkers == 0 {
		c.Watcher.TXWorkers = 8
	}
	if c.Watcher.TronBlockSource == "" {
		c.Watcher.TronBlockSource = "solid"
	}
	if c.QuickNode.MinRequestIntervalMS == 0 {
		c.QuickNode.MinRequestIntervalMS = 10
	}
	if c.Watcher.BalanceRequestDelayMS == 0 {
		c.Watcher.BalanceRequestDelayMS = 10
	}
	if c.Watcher.ScheduledRefreshDelayMS == 0 {
		c.Watcher.ScheduledRefreshDelayMS = 10
	}
	if c.Web.Listen == "" {
		c.Web.Listen = ":8080"
	}
	if c.Web.SessionName == "" {
		c.Web.SessionName = "tron_watcher_session"
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
	if c.BSC.MinRequestIntervalMS == 0 {
		c.BSC.MinRequestIntervalMS = 10
	}
	if c.BSC.ScheduledRefreshDelayMS == 0 {
		c.BSC.ScheduledRefreshDelayMS = 10
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

func (c *Config) QuickNodeMinRequestInterval() time.Duration {
	return time.Duration(c.QuickNode.MinRequestIntervalMS) * time.Millisecond
}

func (c *Config) BSCMinRequestInterval() time.Duration {
	return time.Duration(c.BSC.MinRequestIntervalMS) * time.Millisecond
}

func (c *Config) HDBalanceRequestDelay() time.Duration {
	return time.Duration(c.Watcher.BalanceRequestDelayMS) * time.Millisecond
}

func (c *Config) TronScheduledRefreshDelay() time.Duration {
	return time.Duration(c.Watcher.ScheduledRefreshDelayMS) * time.Millisecond
}

func (c *Config) BSCScheduledRefreshDelay() time.Duration {
	return time.Duration(c.BSC.ScheduledRefreshDelayMS) * time.Millisecond
}
