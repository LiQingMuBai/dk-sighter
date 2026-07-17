# Tron Watcher

A Go-based monitoring service for Tron addresses.

Features:

- Loads watched addresses from the MySQL `watch_addresses` table
- Tracks `TRX` and `USDT (TRC20)` incoming and outgoing transfers for watched addresses in real time
- Writes transfer records into `transfer_records`
- Writes the latest `TRX` / `USDT` balances into `asset_balances`
- Provides a built-in web dashboard showing addresses, TRX balance, USDT balance, and the latest update time in Beijing time
- Provides a login page, with dashboard credentials loaded from the config file
- Includes a simple arithmetic captcha on the login page
- Uses a responsive H5 layout and automatically switches to a card layout on mobile devices
- Supports adding watched addresses directly from the dashboard, one by one or in batches
- Adds a `BSC Monitoring Platform` button at the top of the home page, with the entry path at `/bsc`
- The `/bsc` page reads `bsc_watch_addresses` and `bsc_asset_balances` from MySQL, displays BSC addresses, `BNB`, `USDT`, and update time, and supports single or batch deletion
- BSC schema initialization scripts are located in `migrations/003_init_bsc.sql`
- Each address row provides a `Delete Address` action next to `Send Energy Twice`, which sets `watch_addresses.status` to `0`
- Exposes public APIs for adding watched addresses individually or in batches
- Displays watched addresses with pagination, 20 items per page by default
- Shows a 30-day daily energy chart on the home page: sending energy once counts as 1, sending energy twice counts as 2
- Supports batch actions on checked addresses in the current page: `Send Energy Once`, `Send Energy Twice`, and `Batch Delete`
- Persists scan cursors in `sync_state` and resumes automatically after restart
- Supports `watcher.tron_block_source` with `head` or `solid`; the default is `head`
- Supports `watcher.start_block` on first startup; if not set, the service starts from the latest block based on `tron_block_source`
- Uses `WSS` only as a new-block trigger; actual block processing still scans in order based on the configured block source

## Layout

```text
tron_watcher/
  cmd/tron-watcher/main.go
  configs/config.example.yaml
  internal/
  migrations/001_init.sql
```

## Preparation

1. Create the database and run [migrations/001_init.sql](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/migrations/001_init.sql)
2. Copy [configs/config.example.yaml](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/configs/config.example.yaml) to your own config file, for example `configs/config.yaml` or `config.yaml`
3. Fill in the QuickNode Tron `http_url`; `wss_url` can be left empty
4. Confirm the USDT contract address
5. Configure the dashboard listen address `web.listen` and login credentials `web.username` / `web.password`
6. Configure `web.api_key` if you want to protect the external API
7. Configure `watcher.start_block` if needed
8. Insert watched addresses into `watch_addresses`; only Tron Base58 addresses starting with `T` are required

Example:

```sql
INSERT INTO watch_addresses(address_base58, status)
VALUES
('TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', 1);
```

## Start

```bash
cd tron_watcher
go mod tidy
go run ./cmd/tron-watcher
```

## Desktop Packaging

The desktop app is built with `Electron + Go`, and its entry code is under the `desktop/` directory.

Directory overview:

- `desktop/main.js`: Electron main process that launches the embedded Go service
- `desktop/build-go.js`: Builds the Go binary into `desktop/bin/<platform>/` before packaging
- `desktop/package.json`: Electron scripts and `electron-builder` config
- `configs/config.yaml`: Included directly in the installation package during desktop packaging

Preparation before the first package build:

```bash
cd desktop
npm ci
```

Run in development mode:

```bash
cd desktop
npm run build:go
npm run dev
```

Build the macOS package:

```bash
cd desktop
npm ci
npm run build:go
npm run dist:mac
```

Notes:

- Desktop packaging now requires `configs/config.yaml` to exist
- On the first desktop launch, the bundled `config.yaml` is copied to the user directory as the runtime config

Default artifacts:

- `desktop/dist/TronSight-0.1.0-arm64.dmg`
- `desktop/dist/mac-arm64/TronSight.app`

Build the Windows package:

```bash
cd desktop
npm ci
npm run build:go
npm run dist:win
```

Default artifacts:

- `desktop/dist/*.exe`
- `desktop/dist/win-*/`

Additional notes:

- Packaging scripts are committed, but `desktop/node_modules/` and `desktop/dist/` are not
- If `Developer ID Application` is not configured, the macOS package will be unsigned
- The Windows package is generated as an `nsis` installer by default through `electron-builder --win`
- The Go service automatically reads:
  - `config.example.yaml`
  - `web/templates`
  - `desktop/bin/<platform>/tron-watcher`
- To package on another machine, make sure these are installed first:
  - `Node.js`
  - `npm`
  - `Go`

For more desktop packaging details, see [desktop-packaging.md](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/docs/desktop-packaging.md).

By default, the service searches for config files in this order:

- `configs/config.yaml`
- `config.yaml`
- `configs/config.example.yaml`

You can still specify a config file manually:

```bash
TRON_WATCHER_CONFIG=configs/config.yaml go run ./cmd/tron-watcher
```

`watcher.start_block` behavior:

- `0`: on first startup, begin from the latest block based on `tron_block_source`
- `> 0`: on first startup, begin from the specified block
- If a cursor already exists in `sync_state`, the database cursor takes priority and `start_block` is ignored

`watcher.tron_block_source` behavior:

- `head`: uses `/wallet/getnowblock` to get the latest block number for the most real-time view
- `solid`: uses `/walletsolidity/getnowblock` to get the solidity block number for higher stability but more delay

Dashboard:

- Default listen address: `:8080`
- Login is required
- A simple captcha is required on login
- Dashboard URL: `http://127.0.0.1:8080/`
- API docs page: `http://127.0.0.1:8080/docs`
- OpenAPI file: `http://127.0.0.1:8080/openapi.json`
- The current page displays:
  - Address
  - TRX balance
  - USDT balance
  - Update time
  - Action placeholders: `One-Click Sweep`, `Send Energy Once`, `Send Energy Twice`

Example config:

```yaml
web:
  listen: ":8080"
  username: "admin"
  password: "change_me_123456"
  session_name: "tron_watcher_session"
  api_key: "change_me_api_key"

energy:
  provider: "trxfee" # default fallback value, options: trxfee / catfee

trxfee:
  url: "https://your-trxfee-api-host"
  api_key: "your_trxfee_api_key"
  api_secret: "your_trxfee_api_secret"
  x_api_key: "masion"

catfee:
  url: "https://your-catfee-api-host"
  api_key: "your_catfee_api_key"
  api_secret: "your_catfee_api_secret"
```

Runtime provider switching:

- MySQL table: `runtime_settings`
- Config key: `energy_provider`
- Supported values:
  - Fixed values: `trxfee` / `catfee`
  - Time-range rule: for example `10-24`
- If this setting does not exist in MySQL, the service falls back to `energy.provider` from `config.yaml`

Time-range rule:

- Evaluated by Beijing time hour
- When the current hour matches the range, the service uses `trxfee`
- At all other times, the service uses `catfee`
- For example, `10-24` means `trxfee` is used from Beijing time `10:00` to `23:59`, and `catfee` is used for the rest

Example SQL:

```sql
INSERT INTO runtime_settings(setting_key, setting_value)
VALUES ('energy_provider', '10-24')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);
```

External API:

- Endpoint: `POST /api/watch-addresses`
- Purpose: add watched addresses individually or in batches
- Authentication: checks `X-API-Key` first, and also supports `Authorization: Bearer <api_key>`
- If `web.api_key` is empty, API key verification is disabled

Single-address example:

```bash
curl -X POST http://127.0.0.1:8080/api/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{"address":"TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}'
```

Batch example:

```bash
curl -X POST http://127.0.0.1:8080/api/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{"addresses":["TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX","TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"]}'
```

Response example:

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

Balance refresh APIs:

- Unified endpoint: `POST /api/refresh-addresses`
- Legacy-compatible endpoints:
  - `POST /api/tron/refresh-address`
  - `POST /api/bsc/refresh-address`
- Purpose: refresh one or more addresses on `tron` or `bsc`
- Limit: up to `100` addresses per batch

Unified single-address example:

```bash
curl -X POST http://127.0.0.1:8080/api/refresh-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{
    "chain": "tron",
    "address": "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  }'
```

Unified batch example:

```bash
curl -X POST http://127.0.0.1:8080/api/refresh-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change_me_api_key' \
  -d '{
    "chain": "bsc",
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ]
  }'
```

Response example:

```json
{
  "success": true,
  "message": "Updated BSC address balances successfully 2 / 2",
  "chain": "bsc",
  "address": "0x1111111111111111111111111111111111111111",
  "addresses": [
    "0x1111111111111111111111111111111111111111",
    "0x2222222222222222222222222222222222222222"
  ],
  "total_count": 2,
  "success_count": 2
}
```

Additional API docs:

- Markdown doc: [api.md](file:///Users/masion/Documents/trae_projects/TronSight/tron_watcher/docs/api.md)
- OpenAPI JSON: `/openapi.json`

## Notes

- Native `TRX` transfers come from `TransferContract`
- `USDT` transfers are identified through TRC20 `Transfer(address,address,uint256)` logs
- The service can run without a `wss` node and will fall back to pure HTTP polling
- `quicknode.usdt_contract` accepts either `T...` format or `41...` hex format, and the program converts it automatically
- `mysql.session_time_zone` defaults to `+08:00`, so subsequent `CURRENT_TIMESTAMP` values are written in Beijing time
- Database `DATETIME` fields such as `created_at` and `updated_at` are stored in Beijing time; `block_time` still keeps the original on-chain millisecond timestamp
- Transactions inside a block are parsed concurrently, controlled by `watcher.tx_workers`; USDT receipt lookups are only performed for `TriggerSmartContract` transactions
- Only `T...` addresses are stored in the database; they are converted to hex automatically at startup for on-chain matching
- `Send Energy Once` / `Send Energy Twice` are wired to real providers: the service switches between `trxfee` and `catfee` according to `runtime_settings.energy_provider`, uses `address_base58` as the target address, and sends fixed energy amounts of `65000` / `130000`
- `One-Click Sweep` currently shows a "feature not implemented" prompt instead of opening the mnemonic dialog
- If `sync_state` has no cursor, the service initializes from `watcher.start_block`; otherwise it uses the current latest block based on `tron_block_source`
- When the Tron main scanner skips blocks because lag exceeds the threshold, missing ranges are written to `tron_sync_gaps`; `tron-grpc-block-sync` repairs pending gaps from `tron_sync_gaps` first and only takes over when the main cursor is missing or stale
- `tron-grpc-block-sync` runs in `head` mode by default and aligns with `tron_head_scanner`; it only switches to `solid` when `TRON_GRPC_SYNC_BLOCK_SOURCE=solid` is explicitly set
- The latest update time shown in the dashboard comes from the newest balance record for the current address and is displayed in Beijing time
- Balances are not refreshed by a full-table sweep; instead, once any incoming or outgoing transfer is matched, the service refreshes that address's `TRX` and `USDT` balances selectively
- If `WSS` is configured but disconnected, the scanner still keeps catching up through HTTP polling
- `One-Click Sweep`, `Send Energy Once`, and `Send Energy Twice` currently support click placeholders: the page shows the action name, and the backend logs the action and address

## Suggestions

- Add business order IDs, sweep status, and callback status to `transfer_records`
- Add a historical backfill command
- Add Prometheus metrics and alerts
- Change address cache refresh to incremental mode
