#!/bin/sh
# grandmasend bootstrap: transient receiver for macOS and Linux.
#
# The one constant command a receiver ever pastes:
#   curl -fsSL https://github.com/edward3423/grandmasend/releases/latest/download/bootstrap.sh | sh
#
# Fetches the latest release binary to a temp dir, runs it once in transient
# receive mode (prompts for the four-word code), then deletes it. Installs
# nothing, needs no sudo.

set -eu

BASE_URL="${GRANDMASEND_BASE_URL:-https://github.com/edward3423/grandmasend/releases/latest/download}"

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

# Test hook: CI drives the paste-path without a terminal via env vars.
if [ -n "${GRANDMASEND_CODE:-}" ]; then
    # Intentional word splitting: the code is four words.
    set -- receive --transient $GRANDMASEND_CODE
    [ -n "${GRANDMASEND_DEST:-}" ] && set -- "$@" --dest "$GRANDMASEND_DEST"
    [ -n "${GRANDMASEND_SENDER_ADDR:-}" ] && set -- "$@" --sender-addr "$GRANDMASEND_SENDER_ADDR"
    "$tmp/grandmasend" "$@"
elif [ -t 0 ]; then
    "$tmp/grandmasend" receive --transient
else
    # When piped into sh, stdin is the script itself; the code prompt needs
    # the terminal.
    "$tmp/grandmasend" receive --transient < /dev/tty
fi
