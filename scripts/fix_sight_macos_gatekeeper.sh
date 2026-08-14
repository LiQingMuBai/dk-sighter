#!/usr/bin/env bash
set -euo pipefail

APP="${1:-/Applications/Sight.app}"

if [ ! -d "$APP" ]; then
  echo "Error: app bundle not found at $APP"
  echo "Usage: $0 [path/to/Sight.app]"
  echo "  default path: /Applications/Sight.app"
  exit 1
fi

echo "[1/2] Removing quarantine attribute from $APP ..."
xattr -dr com.apple.quarantine "$APP"

echo "[2/2] Ad-hoc deep signing $APP ..."
codesign --force --deep --sign - "$APP"

echo
echo "Done. You can now open Sight normally (right-click -> Open the first time)."
