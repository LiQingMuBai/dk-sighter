#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

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
    "/Applications/Meee.app"
  )
  while IFS= read -r -d '' path; do
    candidates+=("$path")
  done < <(find /Applications -maxdepth 2 -name 'Meee*.app' -type d -print0 2>/dev/null | sort -z)

  local chosen=""
  for p in "${candidates[@]}"; do
    if [ -d "$p" ]; then
      if [ -z "$chosen" ] || [ "$p" = "/Applications/Meee.app" ]; then
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

APP="$(resolve_app_path "${1:-}")" || true

clear
echo "========================================"
echo " Meee Gatekeeper Fix Tool"
echo "========================================"
echo

if [ -z "$APP" ] || [ ! -d "$APP" ]; then
  echo "X No Meee*.app bundle found in /Applications"
  echo
  if [ -n "${1:-}" ]; then
    echo "  provided path did not exist: $1"
    echo
  fi
  echo "Please drag-and-drop your Meee app (e.g. Meee 2.app)"
  echo "onto this .command file, or install it into /Applications first."
  echo
  read -rp "Press Enter to exit... "
  exit 1
fi

echo "Target app : $APP"
echo

echo "[1/2] Removing quarantine attribute..."
if xattr -dr com.apple.quarantine "$APP"; then
  echo "  OK - quarantine removed"
else
  echo "  WARN - xattr failed (maybe file was not quarantined)"
fi

echo
echo "[2/2] Performing ad-hoc deep code signing..."
if codesign --force --deep --sign - "$APP"; then
  echo "  OK - ad-hoc signed"
else
  echo
  echo "X codesign failed. Please ensure Xcode Command Line Tools are installed:"
  echo "  xcode-select --install"
  echo
  read -rp "Press Enter to exit... "
  exit 1
fi

echo
echo "========================================"
echo " Done! You can now open $(basename "$APP")."
echo " (Right-click -> Open the first launch.)"
echo "========================================"
echo
read -rp "Press Enter to close this window... "
