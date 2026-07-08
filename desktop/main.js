const { app, BrowserWindow, dialog } = require("electron")
const path = require("path")
const fs = require("fs")
const net = require("net")
const http = require("http")
const { spawn } = require("child_process")

let mainWindow = null
let goProcess = null
let currentPort = null

function repoRoot() {
  return path.resolve(__dirname, "..")
}

function binaryName() {
  return process.platform === "win32" ? "tron-watcher.exe" : "tron-watcher"
}

function platformDir() {
  if (process.platform === "darwin") return "darwin"
  if (process.platform === "win32") return "win32"
  return process.platform
}

function resolveGoCommand() {
  const custom = (process.env.TRON_WATCHER_GO_BIN || "").trim()
  if (custom) {
    return { command: custom, args: [], cwd: repoRoot() }
  }

  if (app.isPackaged) {
    const binPath = path.join(process.resourcesPath, "bin", platformDir(), binaryName())
    return { command: binPath, args: [], cwd: path.dirname(binPath) }
  }

  return { command: "go", args: ["run", "./cmd/tron-watcher"], cwd: repoRoot() }
}

function ensureUserConfig() {
  const userDir = app.getPath("userData")
  const cfgPath = path.join(userDir, "config.yaml")
  if (fs.existsSync(cfgPath)) return cfgPath

  const examplePath = app.isPackaged
    ? path.join(process.resourcesPath, "config.example.yaml")
    : path.join(repoRoot(), "configs", "config.example.yaml")

  fs.mkdirSync(userDir, { recursive: true })
  fs.copyFileSync(examplePath, cfgPath)
  return cfgPath
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.on("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address()
      const port = addr && typeof addr === "object" ? addr.port : null
      server.close(() => {
        if (!port) reject(new Error("failed to allocate port"))
        else resolve(port)
      })
    })
  })
}

function httpPing(url, timeoutMs) {
  return new Promise((resolve) => {
    const req = http.get(url, (res) => {
      res.resume()
      resolve(res.statusCode >= 200 && res.statusCode < 300)
    })
    req.on("error", () => resolve(false))
    req.setTimeout(timeoutMs, () => {
      req.destroy()
      resolve(false)
    })
  })
}

async function waitHealthz(port) {
  const deadline = Date.now() + 15000
  const url = `http://127.0.0.1:${port}/healthz`
  while (Date.now() < deadline) {
    const ok = await httpPing(url, 800)
    if (ok) return
    await new Promise((r) => setTimeout(r, 200))
  }
  throw new Error("healthz timeout")
}

async function startGoServer() {
  const port = await getFreePort()
  const cfgPath = ensureUserConfig()
  const cmd = resolveGoCommand()

  const env = {
    ...process.env,
    TRON_WATCHER_APP_MODE: "hd_wallet",
    TRON_WATCHER_WEB_LISTEN: `127.0.0.1:${port}`,
    TRON_WATCHER_CONFIG: cfgPath,
    TRON_WATCHER_DATA_DIR: path.join(app.getPath("userData"), "hd_wallet"),
    TRON_WATCHER_TEMPLATE_DIR: app.isPackaged
      ? path.join(process.resourcesPath, "web", "templates")
      : path.join(repoRoot(), "web", "templates")
  }

  goProcess = spawn(cmd.command, cmd.args, {
    cwd: cmd.cwd,
    env,
    stdio: "pipe",
    windowsHide: true
  })
  currentPort = port

  goProcess.stdout.on("data", (buf) => process.stdout.write(buf))
  goProcess.stderr.on("data", (buf) => process.stderr.write(buf))

  const exited = new Promise((_, reject) => {
    goProcess.once("exit", (code, signal) => {
      reject(new Error(`go server exited: code=${code} signal=${signal}`))
    })
  })

  await Promise.race([waitHealthz(port), exited])
  return port
}

function killGoServer() {
  if (!goProcess) return
  const pid = goProcess.pid
  const proc = goProcess
  goProcess = null

  if (process.platform === "win32") {
    try {
      spawn("taskkill", ["/pid", String(pid), "/T", "/F"], { windowsHide: true })
    } catch (_) {
      try {
        proc.kill()
      } catch (_) {}
    }
    return
  }

  try {
    proc.kill("SIGTERM")
  } catch (_) {}
}

async function createMainWindow() {
  const port = await startGoServer()

  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    show: true,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  })

  mainWindow.on("closed", () => {
    mainWindow = null
  })

  await mainWindow.loadURL(`http://127.0.0.1:${port}/`)
}

function setupSingleInstance() {
  const got = app.requestSingleInstanceLock()
  if (!got) {
    app.quit()
    return false
  }
  app.on("second-instance", () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore()
      mainWindow.focus()
    }
  })
  return true
}

app.on("before-quit", () => {
  killGoServer()
})

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit()
})

app.whenReady().then(async () => {
  if (!setupSingleInstance()) return
  try {
    await createMainWindow()
  } catch (err) {
    const msg = err && err.message ? err.message : String(err)
    dialog.showErrorBox("TronSight 启动失败", msg)
    killGoServer()
    app.quit()
  }
})
