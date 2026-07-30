#!/usr/bin/env bash
#  Project:   dfe-transform-vector
#  File:      scripts/fetch-vector.sh
#  Purpose:   Download and cache Vector binary for integration tests
#  Language:  Bash
#
#  License:   BUSL-1.1
#  Copyright: (c) 2026 HYPERI PTY LIMITED

# Downloads the Vector binary for the current platform, caches it in .tmp/vector/,
# and prints the absolute path to stdout (last line). Idempotent — re-running
# reuses the cache unless VECTOR_VERSION changes.
#
# Usage:
#   ./scripts/fetch-vector.sh                    # latest release
#   VECTOR_VERSION=0.48.0 ./scripts/fetch-vector.sh  # pinned version
#
# The binary is cached in .tmp/vector/bin/vector (gitignored).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/.tmp/vector"
BIN_DIR="${CACHE_DIR}/bin"
VECTOR_BIN="${BIN_DIR}/vector"

# Detect architecture
ARCH="$(uname -m)"
case "${ARCH}" in
    x86_64)  VECTOR_ARCH="x86_64" ;;
    aarch64) VECTOR_ARCH="aarch64" ;;
    arm64)   VECTOR_ARCH="aarch64" ;;
    *)
        echo "Unsupported architecture: ${ARCH}" >&2
        exit 1
        ;;
esac

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "${OS}" in
    linux)  VECTOR_OS="unknown-linux-gnu" ;;
    darwin) VECTOR_OS="apple-darwin" ;;
    *)
        echo "Unsupported OS: ${OS}" >&2
        exit 1
        ;;
esac

# Desired version: env override, else the version this image ships.
#
# It used to default to whatever GitHub called `latest`, which meant tests ran
# against a different Vector to the one we deploy -- and drifted apart silently
# as upstream released. That is the wrong direction for a dependency whose CLI
# flags and config schema move between minors.
#
# The single source is the const in src/deployment.rs, which also drives the
# Dockerfile ARG and the runtime version check. Read rather than duplicated, so
# there is one place to bump and Renovate has one annotation to act on.
if [[ -z "${VECTOR_VERSION:-}" ]]; then
    VERSION_SRC="${REPO_ROOT:-$(dirname "$0")/..}/src/deployment.rs"
    VECTOR_VERSION=$(sed -n 's/^pub const VECTOR_VERSION: &str = "\(.*\)";$/\1/p' "${VERSION_SRC}")

    if [[ -z "${VECTOR_VERSION}" ]]; then
        echo "Could not read VECTOR_VERSION from ${VERSION_SRC}" >&2
        echo "Set VECTOR_VERSION explicitly, or check the const's shape there." >&2
        exit 1
    fi
fi

# Check cached version
cached_version() {
    if [[ -x "${VECTOR_BIN}" ]]; then
        "${VECTOR_BIN}" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
    fi
}

CACHED="$(cached_version || true)"
if [[ "${CACHED}" == "${VECTOR_VERSION}" ]]; then
    echo "${VECTOR_BIN}"
    exit 0
fi

echo "Downloading Vector ${VECTOR_VERSION} for ${VECTOR_ARCH}-${VECTOR_OS}..." >&2

mkdir -p "${BIN_DIR}"

# Download and extract
TARBALL="vector-${VECTOR_VERSION}-${VECTOR_ARCH}-${VECTOR_OS}.tar.gz"
URL="https://packages.timber.io/vector/${VECTOR_VERSION}/${TARBALL}"

curl -fsSL "${URL}" \
    | tar xz -C "${BIN_DIR}" --strip-components=2 \
        "./vector-${VECTOR_ARCH}-${VECTOR_OS}/bin/vector"

chmod +x "${VECTOR_BIN}"

# Verify
INSTALLED="$(cached_version || true)"
if [[ -z "${INSTALLED}" ]]; then
    echo "Failed to install Vector ${VECTOR_VERSION}" >&2
    exit 1
fi

echo "Cached Vector ${INSTALLED} at ${VECTOR_BIN}" >&2

# Print path (last line — this is what callers read)
echo "${VECTOR_BIN}"
