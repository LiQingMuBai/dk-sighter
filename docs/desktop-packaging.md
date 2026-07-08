# 桌面版打包说明

## 方案

当前桌面版采用 `Electron + Go`：

- Electron 负责桌面窗口与应用生命周期
- Go 负责启动本地 Web 服务
- Electron 启动后会拉起 Go 子进程，并通过 `/healthz` 探活

关键文件：

- `desktop/main.js`
- `desktop/build-go.js`
- `desktop/package.json`

## 前置条件

打包机器需要具备：

- `Node.js`
- `npm`
- `Go`

建议先确认版本：

```bash
node -v
npm -v
go version
```

## 安装依赖

```bash
cd desktop
npm ci
```

## 构建 Go 二进制

```bash
cd desktop
npm run build:go
```

该命令会调用 `desktop/build-go.js`，把 Go 二进制输出到：

- `desktop/bin/darwin/tron-watcher`
- `desktop/bin/win32/tron-watcher.exe`

说明：

- 在 mac 上默认先生成当前平台对应的 Go 二进制
- 如果需要跨平台正式出包，建议分别在目标平台执行打包

## 开发模式

```bash
cd desktop
npm run build:go
npm run dev
```

## 打包 mac

```bash
cd desktop
npm ci
npm run build:go
npm run dist:mac
```

默认产物：

- `desktop/dist/TronSight-0.1.0-arm64.dmg`
- `desktop/dist/mac-arm64/TronSight.app`

## 打包 Windows

```bash
cd desktop
npm ci
npm run build:go
npm run dist:win
```

默认产物：

- `desktop/dist/*.exe`
- `desktop/dist/win-*/`

当前 `package.json` 中 Windows 目标为 `nsis`。

## 资源打包内容

`electron-builder` 会额外打进这些资源：

- `configs/config.example.yaml`
- `web/templates`
- `desktop/bin`

因此桌面版运行时不依赖源码目录中的模板相对路径。

## 注意事项

- `desktop/node_modules/` 不提交到 Git
- `desktop/dist/` 为本地产物目录，不提交到 Git
- mac 未签名时，系统可能提示无法直接打开
- 若要正式分发 mac 包，建议后续补 `Developer ID` 签名和 notarization
- 若要正式分发 Windows 包，建议在 Windows 环境下完成最终打包验证

