const path = require("path")
const fs = require("fs")
const { spawnSync } = require("child_process")

function repoRoot() {
  return path.resolve(__dirname, "..")
}

function platformDir() {
  if (process.platform === "darwin") return "darwin"
  if (process.platform === "win32") return "win32"
  return process.platform
}

function binaryName() {
  return process.platform === "win32" ? "tron-watcher.exe" : "tron-watcher"
}

function git(args) {
  const r = spawnSync("git", args, { cwd: repoRoot(), encoding: "utf8" })
  if (r.error) return ""
  if (typeof r.status === "number" && r.status !== 0) return ""
  return String(r.stdout || "").trim()
}

function run() {
  const outDir = path.join(__dirname, "bin", platformDir())
  fs.mkdirSync(outDir, { recursive: true })

  const outPath = path.join(outDir, binaryName())
  const env = { ...process.env, CGO_ENABLED: process.env.CGO_ENABLED || "0" }

  const branch = git(["rev-parse", "--abbrev-ref", "HEAD"]) || "unknown"
  const commit = git(["rev-parse", "--short", "HEAD"]) || "unknown"
  const ldflags = `-X main.buildBranch=${branch} -X main.buildCommit=${commit}`

  const r = spawnSync("go", ["build", "-ldflags", ldflags, "-o", outPath, "./cmd/tron-watcher"], {
    cwd: repoRoot(),
    env,
    stdio: "inherit"
  })
  if (r.error) throw r.error
  if (typeof r.status === "number" && r.status !== 0) process.exit(r.status)
}

run()
