#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GOOS="${GOOS:-darwin}"
GOARCH="${GOARCH:-arm64}"
APP_NAME="${APP_NAME:-Cookie}"
OUT_DIR="${OUT_DIR:-$ROOT/dist}"
STAGE_DIR="$OUT_DIR/${APP_NAME}_${GOOS}_${GOARCH}"
APP_BUNDLE="$STAGE_DIR/$APP_NAME.app"
BIN_DIR="$APP_BUNDLE/Contents/MacOS"
RES_DIR="$APP_BUNDLE/Contents/Resources"
CONF_DIR="$APP_BUNDLE/Contents/Resources/configs"
MIG_DIR="$APP_BUNDLE/Contents/Resources/migrations"
WEB_DIR="$APP_BUNDLE/Contents/Resources/web"
LOG_DIR="$APP_BUNDLE/Contents/Resources/logs"
DATA_DIR="$APP_BUNDLE/Contents/Resources/data"

VERSION="${VERSION:-dev}"
BUNDLE_ID="${BUNDLE_ID:-com.tronsight.cookie}"
BINARY_NAME="tron-watcher-desktop"

rm -rf "$STAGE_DIR"
mkdir -p "$BIN_DIR" "$RES_DIR" "$CONF_DIR" "$WEB_DIR" "$LOG_DIR" "$DATA_DIR"
if [ -d "$ROOT/migrations" ]; then
  mkdir -p "$MIG_DIR"
  cp -R "$ROOT/migrations/." "$MIG_DIR/"
fi

# 1) build Go binary
echo "[1/4] build $BINARY_NAME ($GOOS/$GOARCH)..."
# force binding to loopback; config can still override via env/listen field
export CGO_ENABLED=0
export GOOS GOARCH
# ensure desktop cmd exists and is buildable
go build -trimpath -ldflags "-s -w -X main.version=$VERSION" \
  -o "$BIN_DIR/$BINARY_NAME" \
  ./cmd/tron-watcher-desktop
chmod +x "$BIN_DIR/$BINARY_NAME"

# 2) ship resources (templates, config, migrations, logs dir)
echo "[2/4] bundle resources..."
if [ -d "$ROOT/web/templates" ]; then
  # ensure WEB_DIR points into Resources web dir
  mkdir -p "$WEB_DIR/templates"
  cp -R "$ROOT/web/templates/." "$WEB_DIR/templates/"
fi
if [ -f "$ROOT/configs/config.example.yaml" ]; then
  cp "$ROOT/configs/config.example.yaml" "$CONF_DIR/config.example.yaml"
fi

# 3) build macOS app bundle skeleton + Info.plist
echo "[3/4] write Info.plist + PkgInfo..."
mkdir -p "$APP_BUNDLE/Contents"
PLIST="$APP_BUNDLE/Contents/Info.plist"
cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>zh_CN</string>
  <key>CFBundleDisplayName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleExecutable</key>
  <string>${BINARY_NAME}</string>
  <key>CFBundleIdentifier</key>
  <string>${BUNDLE_ID}</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>${APP_NAME}</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>${VERSION}</string>
  <key>CFBundleVersion</key>
  <string>${VERSION}</string>
  <key>NSHighResolutionCapable</key>
  <true/>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
  <key>LSApplicationCategoryType</key>
  <string>public.app-category.finance</string>
</dict>
</plist>
PLIST_EOF
echo -n "APPL????" > "$APP_BUNDLE/Contents/PkgInfo"

# 4) placeholder icon + zip the bundle dir
echo "[4/4] placeholder icon and zip..."
ICON_DIR="$RES_DIR/AppIcon.iconset"
mkdir -p "$ICON_DIR"
export ICON_DIR
python3 - <<'PY'
import os, base64, zlib, struct
out=os.environ['ICON_DIR']
# write a single 10x10 solid blue PNG; macOS will auto-scale others if missing
def chunk(t, data):
    return struct.pack('>I', len(data)) + t + data + struct.pack('>I', zlib.crc32(t+data) & 0xffffffff)
w=h=10
raw=b''
for y in range(h):
    raw += b'\x00' + (b'\x00\x47\xab')*w  # blue #0047AB
png = b'\x89PNG\r\n\x1a\n'
png += chunk(b'IHDR', struct.pack('>IIBBBBB', w, h, 8, 2, 0, 0, 0))
png += chunk(b'IDAT', zlib.compress(raw, 9))
png += chunk(b'IEND', b'')
open(os.path.join(out, 'icon_16x16.png'),'wb').write(png)
open(os.path.join(out, 'icon_16x16@2x.png'),'wb').write(png)
open(os.path.join(out, 'icon_32x32.png'),'wb').write(png)
open(os.path.join(out, 'icon_32x32@2x.png'),'wb').write(png)
open(os.path.join(out, 'icon_128x128.png'),'wb').write(png)
open(os.path.join(out, 'icon_128x128@2x.png'),'wb').write(png)
open(os.path.join(out, 'icon_256x256.png'),'wb').write(png)
open(os.path.join(out, 'icon_256x256@2x.png'),'wb').write(png)
open(os.path.join(out, 'icon_512x512.png'),'wb').write(png)
open(os.path.join(out, 'icon_512x512@2x.png'),'wb').write(png)
PY
rm -f "$RES_DIR/AppIcon.icns"
if command -v iconutil >/dev/null 2>&1; then
  iconutil -c icns "$ICON_DIR" -o "$RES_DIR/AppIcon.icns" >/dev/null 2>&1 || true
fi
rm -rf "$ICON_DIR"

ZIP_PATH="$OUT_DIR/${APP_NAME}_${GOOS}_${GOARCH}.zip"
rm -f "$ZIP_PATH"
(
  cd "$STAGE_DIR"
  ditto -c -k --sequesterRsrc --keepParent "$APP_NAME.app" "$ZIP_PATH" >/dev/null
)

echo
echo "DONE"
echo "  app bundle: $APP_BUNDLE"
echo "  zip       : $ZIP_PATH"
echo
echo "To double-click run on macOS, unpack the .zip then right-click Open the .app."
