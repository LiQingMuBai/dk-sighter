#!/usr/bin/env bash
set -euo pipefail

resolve_app_path() {
  local explicit="$1"
  if [ -n "$explicit" ]; then
    if [ -d "$explicit" ]; then
      echo "$explicit"
      return 0
    fi
    return 1
  fi

  local candidates=(
    "/Applications/Cookie.app"
  )
  while IFS= read -r -d '' path; do
    candidates+=("$path")
  done < <(find /Applications -maxdepth 2 -name 'Cookie*.app' -type d -print0 2>/dev/null | sort -z)

  local chosen=""
  for p in "${candidates[@]}"; do
    if [ -d "$p" ]; then
      if [ -z "$chosen" ] || [ "$p" = "/Applications/Cookie.app" ]; then
        chosen="$p"
      fi
    fi
  done

  if [ -n "$chosen" ]; then
    echo "$chosen"
    return 0
  fi
  return 1
}

APP_PATH="$(resolve_app_path "${1:-}")" || true
if [ -z "$APP_PATH" ] || [ ! -d "$APP_PATH" ]; then
  echo "Error: no Cookie*.app bundle found."
  if [ -n "${1:-}" ]; then
    echo "  provided path did not exist: $1"
  fi
  echo
  echo "Usage: $0 [path/to/Cookie.app]"
  echo "  default lookup: /Applications/Cookie.app and any /Applications/**/Cookie*.app"
  exit 1
fi

echo "Target app: $APP_PATH"
echo

echo "[1/2] Removing quarantine attribute ..."
xattr -dr com.apple.quarantine "$APP_PATH"

echo "[2/2] Ad-hoc deep signing ..."
codesign --force --deep --sign - "$APP_PATH"

echo
echo "Done. You can now open $(basename "$APP_PATH") normally (right-click -> Open the first time)."
