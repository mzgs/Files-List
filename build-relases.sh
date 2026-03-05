#!/usr/bin/env bash
set -euo pipefail

TARGET_MAC="aarch64-apple-darwin"
TARGET_WINDOWS="x86_64-pc-windows-gnu"
APP_NAME="files-list"
RELEASE_DIR="release"

usage() {
  cat <<'EOF'
Usage: ./build-relases.sh [--all|--mac|--windows]

Build release binaries:
  --all      Build macOS arm64 and Windows x64 (default)
  --mac      Build only macOS arm64
  --windows  Build only Windows x64
  -h, --help Show this help

Artifacts are copied to ./release
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command: $1" >&2
    exit 1
  fi
}

build_mac() {
  echo "==> Building macOS arm64 release (${TARGET_MAC})"
  rustup target add "${TARGET_MAC}" >/dev/null
  cargo build --release --target "${TARGET_MAC}"

  local src="target/${TARGET_MAC}/release/${APP_NAME}"
  local dst="${RELEASE_DIR}/${APP_NAME}-mac-arm64"
  cp "${src}" "${dst}"
  chmod +x "${dst}"
  echo "    output: ${dst}"
}

build_windows() {
  local allow_skip="${1:-false}"
  echo "==> Building Windows x64 release (${TARGET_WINDOWS})"

  if command -v zig >/dev/null 2>&1 && command -v cargo-zigbuild >/dev/null 2>&1; then
    rustup target add "${TARGET_WINDOWS}" >/dev/null
    echo "    toolchain: zig + cargo-zigbuild"
    cargo zigbuild --release --target "${TARGET_WINDOWS}"
  elif command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
    rustup target add "${TARGET_WINDOWS}" >/dev/null
    echo "    toolchain: mingw-w64 (x86_64-w64-mingw32-gcc)"
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc \
      cargo build --release --target "${TARGET_WINDOWS}"
  else
    if [[ "${allow_skip}" == "true" ]]; then
      echo "warning: no Windows cross-linker found; skipping Windows build."
      echo "Install one of these options to enable Windows builds:"
      echo "  1) Zig path: brew install zig && cargo install cargo-zigbuild"
      echo "  2) MinGW path: brew install mingw-w64"
      return 0
    else
      echo "error: no Windows cross-linker found." >&2
      echo "Install one of these options:" >&2
      echo "  1) Zig path: brew install zig && cargo install cargo-zigbuild" >&2
      echo "  2) MinGW path: brew install mingw-w64" >&2
      exit 1
    fi
  fi

  local src="target/${TARGET_WINDOWS}/release/${APP_NAME}.exe"
  local dst="${RELEASE_DIR}/${APP_NAME}-windows-x64.exe"
  cp "${src}" "${dst}"
  echo "    output: ${dst}"
}

MODE="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      MODE="all"
      shift
      ;;
    --mac)
      MODE="mac"
      shift
      ;;
    --windows)
      MODE="windows"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"
echo "==> Release folder: ${RELEASE_DIR}"

require_cmd cargo
require_cmd rustup

case "${MODE}" in
  all)
    build_mac
    build_windows true
    ;;
  mac)
    build_mac
    ;;
  windows)
    build_windows false
    ;;
esac

echo "==> Done."
