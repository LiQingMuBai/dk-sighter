# [OPEN] desktop-go-startup

## 症状
- Electron 启动后提示：`go server exited: code=1 signal=null`

## 期望
- Go 子进程成功启动，并通过 `/healthz` 探活后加载桌面端页面。

## 假设
- 假设 1：打包后的工作目录不对，Go 进程读取配置或模板文件失败后直接退出。
- 假设 2：桌面端启动的是 `go run ./cmd/tron-watcher`，但在打包环境里没有 Go 工具链，导致子进程立即失败。
- 假设 3：`TRON_WATCHER_CONFIG` 指向的配置文件可读，但其中数据库或必填配置导致 `app.New()` 初始化失败。
- 假设 4：Go 二进制已启动，但因为 `web/templates` 相对路径失效，`web.NewServer()` 初始化失败后退出。
- 假设 5：Electron 资源中的 Go 二进制路径或执行权限有问题，spawn 成功但程序启动失败。

## 当前计划
- 复现启动过程并抓取 Go 进程退出前日志。
- 核对打包产物中的配置、模板、二进制和工作目录。
- 基于证据收敛根因后再做最小修复。

## 证据
- 已确认桌面版用户配置中的数据库名错误，修正后不再出现 `Unknown database 'tron_watcher'`。
- 已确认新的阻塞点为模板相对路径失效：`open web/templates/dashboard.html: no such file or directory`。
- 已将 `web/templates` 打进 Electron `extraResources`，并通过 `TRON_WATCHER_TEMPLATE_DIR` 传给 Go 进程。
- 验证命令可正常启动打包后的 Go 二进制，日志出现 `web dashboard listening on 127.0.0.1:18080`。
- `curl http://127.0.0.1:18080/healthz` 返回 `ok`，说明模板加载与 Web 启动链路已恢复。

## 当前状态
- 根因已定位并完成修复，等待用户实际打开桌面版确认。
