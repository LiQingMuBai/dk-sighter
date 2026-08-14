#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
APP="${1:-/Applications/Sight.app}"

clear
echo "========================================"
echo " Sight Gatekeeper Fix Tool"
echo "========================================"
echo

if [ ! -d "$APP" ]; then
  echo "X App bundle not found: $APP"
  echo
  echo "Please make sure Sight.app is installed into /Applications"
  echo "or drag-and-drop Sight.app onto this .command file."
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
echo " Done! You can now open Sight.app."
echo " (Right-click -> Open the first launch.)"
echo "========================================"
echo
read -rp "Press Enter to close this window... "
