#!/bin/sh
# grandmasend installer and updater for macOS and Linux.
#
#   curl -fsSL https://github.com/edward3423/grandmasend/releases/latest/download/install.sh | sh
#
# Installs the latest release to ~/.local/bin without sudo. Running it again
# updates in place; the binary itself contains no self-update code.

set -eu

BASE_URL="${GRANDMASEND_BASE_URL:-https://github.com/edward3423/grandmasend/releases/latest/download}"
BIN_DIR="${GRANDMASEND_BIN_DIR:-$HOME/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
    Darwin)
        case "$arch" in
            arm64) target="aarch64-apple-darwin" ;;
            x86_64) target="x86_64-apple-darwin" ;;
            *) echo "Unsupported macOS architecture: $arch" >&2; exit 1 ;;
        esac
        ;;
    Linux)
        case "$arch" in
            x86_64) target="x86_64-unknown-linux-musl" ;;
            aarch64) target="aarch64-unknown-linux-musl" ;;
            *) echo "Unsupported Linux architecture: $arch" >&2; exit 1 ;;
        esac
        ;;
    *)
        echo "Unsupported operating system: $os" >&2
        exit 1
        ;;
esac

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

echo "Fetching grandmasend..." >&2
curl -fsSL "$BASE_URL/grandmasend-$target.tar.gz" | tar -xz -C "$tmp"

mkdir -p "$BIN_DIR"
# mv over the top is atomic on the same filesystem and safe while an old
# binary is running.
chmod +x "$tmp/grandmasend"
mv -f "$tmp/grandmasend" "$BIN_DIR/grandmasend"

echo "Installed $("$BIN_DIR/grandmasend" --version) to $BIN_DIR/grandmasend" >&2

# Only touch shell profiles for the real install location, never for a
# GRANDMASEND_BIN_DIR override (tests, custom setups).
if [ "$BIN_DIR" != "$HOME/.local/bin" ]; then
    exit 0
fi

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *)
        profile=""
        case "${SHELL:-}" in
            */zsh) profile="$HOME/.zshrc" ;;
            */bash) profile="$HOME/.bashrc" ;;
            *) profile="$HOME/.profile" ;;
        esac
        line="export PATH=\"\$HOME/.local/bin:\$PATH\""
        if [ ! -f "$profile" ] || ! grep -F "$line" "$profile" > /dev/null 2>&1; then
            printf '\n%s\n' "$line" >> "$profile"
            echo "Added $BIN_DIR to PATH in $profile - open a new terminal to use it." >&2
        fi
        ;;
esac
