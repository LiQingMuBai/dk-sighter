# Tron Watcher API 文档

## 鉴权

支持两种方式：

- `X-API-Key: <api_key>`
- `Authorization: Bearer <api_key>`

如果 `web.api_key` 为空，则不校验 API Key。

## 1. 新增 Tron 监控地址

- 方法：`POST`
- 路径：`/api/tron/watch-addresses`
- Content-Type：`application/json`

### 单个地址请求

```json
{
  "address": "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
}
```

### 批量地址请求

```json
{
  "addresses": [
    "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
  ]
}
```

### 说明

- 地址必须是合法的 `T` 开头 Tron Base58 地址
- 同一请求中的重复地址会自动去重
- 数据库中已存在的地址不会重复插入，只会记录重复地址日志
- 写入成功后会立即刷新内存缓存

### 成功响应

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

### 失败响应

```json
{
  "success": false,
  "message": "no valid addresses",
  "invalid_addresses": [
    "bad-address"
  ]
}
```

### curl 示例

```bash
curl -X POST http://127.0.0.1:8080/api/tron/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: your_api_key' \
  -d '{
    "addresses": [
      "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
      "TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
    ]
  }'
```

## 2. 删除 Tron 监控地址

支持单个删除或批量删除（软删除），支持 API Key 调用或后台登录态调用。

- 方法：`POST`
- 路径：`/api/tron/delete-addresses`
- Content-Type：`application/json`

### 单个地址请求

```json
{
  "address": "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
}
```

### 批量地址请求

```json
{
  "addresses": [
    "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
  ]
}
```

### curl 示例

```bash
curl -X POST http://127.0.0.1:8080/api/tron/delete-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: your_api_key' \
  -d '{
    "addresses": [
      "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
      "TYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
    ]
  }'
```

## 3. 新增 BSC 监控地址

支持单个地址或批量地址写入。地址必须是合法的 BSC 地址，即 `0x` 开头的 40 位十六进制。

- 方法：`POST`
- 路径：`/api/bsc/watch-addresses`
- Content-Type：`application/json`

### 单个地址请求

```json
{
  "address": "0x1111111111111111111111111111111111111111"
}
```

### 批量地址请求

```json
{
  "addresses": [
    "0x1111111111111111111111111111111111111111",
    "0x2222222222222222222222222222222222222222"
  ]
}
```

### curl 示例

```bash
curl -X POST http://127.0.0.1:8080/api/bsc/watch-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: your_api_key' \
  -d '{
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ]
  }'
```

## 4. 删除 BSC 监控地址

支持单个删除或批量删除（软删除），支持 API Key 调用或后台登录态调用。

- 方法：`POST`
- 路径：`/api/bsc/delete-addresses`
- Content-Type：`application/json`

### 单个地址请求

```json
{
  "address": "0x1111111111111111111111111111111111111111"
}
```

### 批量地址请求

```json
{
  "addresses": [
    "0x1111111111111111111111111111111111111111",
    "0x2222222222222222222222222222222222222222"
  ]
}
```

### 成功响应

```json
{
  "success": true,
  "message": "删除成功",
  "count": 2,
  "addresses": [
    "0x1111111111111111111111111111111111111111",
    "0x2222222222222222222222222222222222222222"
  ],
  "deleted_count": 2
}
```

### curl 示例

```bash
curl -X POST http://127.0.0.1:8080/api/bsc/delete-addresses \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: your_api_key' \
  -d '{
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ]
  }'
```
