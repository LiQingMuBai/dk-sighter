# Tron Watcher

一个基于 Go 的 Tron 地址监听服务。

功能：

- 从 MySQL `watch_addresses` 表加载待监控地址
- 实时跟踪监控地址的 `TRX` 和 `USDT(TRC20)` 转入转出
- 把流水写入 `transfer_records`
- 把 `TRX` / `USDT` 最新余额写入 `asset_balances`
- 提供内置 Web 后台页面，展示地址、TRX 余额、USDT 余额、最近更新时间（北京时间）
- 提供登录页面，后台账号密码从配置文件读取
- 登录页包含简单算术验证码
- 后台页面采用响应式 H5 布局，移动端自动切换为卡片式展示
- 后台首页支持直接增加地址，支持单个或批量添加
- 首页顶部已增加 `BSC币安链监控平台` 按钮，入口页面路径为 `/bsc`
- `/bsc` 页面读取数据库里的 `bsc_watch_addresses` 和 `bsc_asset_balances`，显示 BSC 地址、`BNB`、`USDT`、更新时间，并支持单个删除和批量删除
- BSC 相关数据表初始化脚本位于 `migrations/003_init_bsc.sql`
- 每条地址记录在 `发能两次` 旁边支持 `删除地址`，删除时会把 `watch_addresses.status` 设为 `0`
- 提供对外 API，可单个或批量新增监控地址
- 后台监控地址按分页显示，默认每页 20 条
- 首页显示最近 30 天的每日发能折线图：发能一次计 1，发能两次计 2
- 后台监控地址支持当前页勾选 checkbox 后批量执行 `发能一次` / `发能两次` / `批量删除地址`
- 用 `sync_state` 持久化扫描游标，进程重启后自动续扫
- 扫描区块源支持 `watcher.tron_block_source` 配置，可选 `head` / `solid`，默认 `head`
- 首次启动时可通过 `watcher.start_block` 指定起始区块；不配置时从当前最新区块（由 `tron_block_source` 决定）开始
- `WSS` 只做新区块触发，实际处理按配置的区块源顺序扫描

## 目录

```text
tron_watcher/
  cmd/tron-watcher/main.go
  configs/config.example.yaml
  internal/
  migrations/001_init.sql
```

## 准备

1. 创建数据库并执行 [migrations/001_init.sql](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/migrations/001_init.sql)
2. 复制 [configs/config.example.yaml](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/configs/config.example.yaml) 为你自己的配置，例如 `configs/config.yaml` 或 `config.yaml`
3. 填写 QuickNode Tron `http_url`，`wss_url` 可留空
4. 确认 USDT 合约地址
5. 配置 Web 后台监听地址 `web.listen`，以及登录账号密码 `web.username` / `web.password`
6. 按需配置对外 API 的 `web.api_key`
7. 按需配置 `watcher.start_block`
8. 往 `watch_addresses` 插入监控地址，只需要存 `T` 开头的 Tron 地址

示例：

```sql
INSERT INTO watch_addresses(address_base58, status)
VALUES
('TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', 1);
```

## 启动

```bash
cd tron_watcher
go mod tidy
go run ./cmd/tron-watcher
```

默认会按以下顺序查找配置文件：

- `configs/config.yaml`
- `config.yaml`
- `configs/config.example.yaml`

如果你想手动指定配置文件，仍然可以：

```bash
TRON_WATCHER_CONFIG=configs/config.yaml go run ./cmd/tron-watcher
```

`watcher.start_block` 说明：

- `0`：首次启动从当前最新区块（由 `tron_block_source` 决定）开始
- `> 0`：首次启动从指定区块开始
- 如果 `sync_state` 里已经有游标，会优先使用数据库游标，忽略 `start_block`

`watcher.tron_block_source` 说明：

- `head`：使用 `/wallet/getnowblock` 获取最新块号（追最新块）
- `solid`：使用 `/walletsolidity/getnowblock` 获取 solidity 块号（更稳但延迟更高）

Web 后台：

- 默认监听 `:8080`
- 需要先登录
- 登录时需要填写简单验证码
- 访问 `http://127.0.0.1:8080/`
- API 文档页：`http://127.0.0.1:8080/docs`
- OpenAPI 文件：`http://127.0.0.1:8080/openapi.json`
- 当前页面先展示以下字段：
  - 地址
  - TRX 余额
  - USDT 余额
  - 更新时间
  - 功能按钮占位：`一键归集`、`发能一次`、`发能两次`

示例配置：

```yaml
web:
  listen: ":8080"
  username: "admin"
  password: "change_me_123456"
  session_name: "tron_watcher_session"
  api_key: "change_me_api_key"

energy:
  provider: "trxfee" # 默认回退值，可选: trxfee / catfee

trxfee:
  url: "https://your-trxfee-api-host"
  api_key: "your_trxfee_api_key"
  api_secret: "your_trxfee_api_secret"

catfee:
  url: "https://your-catfee-api-host"
  api_key: "your_catfee_api_key"
  api_secret: "your_catfee_api_secret"
```

运行时 provider 切换：

- MySQL 表：`runtime_settings`
- 配置键：`energy_provider`
- 可选值：
  - 固定值：`trxfee` / `catfee`
  - 时间段规则：例如 `10-24`
- 如果数据库里没有这条配置，才会回退到 `config.yaml` 的 `energy.provider`

时间段规则说明：

- 按北京时间小时判断
- 当当前小时命中区间时，走 `trxfee`
- 其他时间走 `catfee`
- 例如 `10-24` 表示北京时间 `10:00` 到 `23:59` 走 `trxfee`，其他时间走 `catfee`

示例 SQL：

```sql
INSERT INTO runtime_settings(setting_key, setting_value)
VALUES ('energy_provider', '10-24')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);
```

对外 API：

- 地址：`POST /api/watch-addresses`
- 功能：支持单个或批量新增监控地址
- 鉴权：优先读取请求头 `X-API-Key`，也支持 `Authorization: Bearer <api_key>`
- 如果 `web.api_key` 为空，则不校验 API Key

单个地址示例：

```bash
curl -X POST http://127.0.0.1:8080/api/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{"address":"TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}'
```

批量地址示例：

```bash
curl -X POST http://127.0.0.1:8080/api/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{"addresses":["TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX","TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"]}'
```

返回示例：

```json
{
  "success": true,
  "message": "ok",
  "count": 2,
  "addresses": [
    "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
  ],
  "duplicate_addresses": [],
  "invalid_addresses": []
}
```

额外接口文档文件：

- Markdown 文档：[docs/api.md](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/docs/api.md)
- OpenAPI JSON：`/openapi.json`

## 说明

- `TRX` 原生转账来自 `TransferContract`
- `USDT` 通过 TRC20 `Transfer(address,address,uint256)` 日志识别
- 没有 `wss` 节点也可以运行，服务会只使用 HTTP 轮询新区块
- `quicknode.usdt_contract` 支持填写 `T...` 格式或 `41...` hex 格式，程序会自动转换
- `mysql.session_time_zone` 默认是 `+08:00`，后续 `CURRENT_TIMESTAMP` 写入的时间按北京时间入库
- 库里的 `created_at`、`updated_at` 这类 DATETIME 字段按北京时间写入；`block_time` 仍保留链上原始时间戳毫秒值
- 区块内交易支持并发解析，`watcher.tx_workers` 用于控制并发 worker 数；USDT 回执只对 `TriggerSmartContract` 交易查询
- 数据库里只存 `T...` 地址，程序启动后会自动转换为 hex 用于链上匹配
- `发能一次` / `发能两次` 按钮已接入真实调用：会优先根据 MySQL `runtime_settings.energy_provider` 在 `trxfee` / `catfee` 间切换，地址取 `address_base58`，能量值分别固定为 `65000` / `130000`
- `一键归集` 当前会弹出“功能未开发”的提示框，不再弹助记词输入框
- 如果 `sync_state` 里没有游标，服务会优先用 `watcher.start_block` 初始化；未配置时用当前最新区块（由 `tron_block_source` 决定）
- Web 页面展示的最近更新时间来自地址当前余额记录的最新更新时间，并按北京时间显示
- 余额不是全表重刷，而是命中任意转入转出记录后，定向刷新该地址的 `TRX` 和 `USDT` 余额
- 如果配置了 `WSS` 且连接断开，扫描器仍会继续通过 HTTP 轮询补块
- 后台页中的 `一键归集`、`发能一次`、`发能两次` 当前已支持点击占位：页面会提示功能名称，后端日志会打印功能名称和地址

## 后续建议

- 给 `transfer_records` 增加业务单号、归集状态、回调状态
- 增加历史补扫命令
- 增加 Prometheus 指标和告警
- 把地址缓存改成增量刷新
