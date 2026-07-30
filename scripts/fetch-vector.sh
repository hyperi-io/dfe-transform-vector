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
#   ./scripts/fetch-vector.sh                    # the version this image ships
#   VECTOR_VERSION=0.48.0 ./scripts/fetch-vector.sh  # override
#
# The binary is cached in .tmp/vector/<version>-<arch>-<os>/vector (gitignored).
#
# SAFE TO RUN CONCURRENTLY. Tests call this from a per-process OnceLock and
# nextest runs one process per test, so N tests invoke it at once. Two things
# make that safe:
#
#   - The cache directory is keyed by version, so no run ever writes over a
#     binary another run is about to exec. Extracting straight into a shared
#     bin/ produced `ETXTBSY` ("Text file busy") when one test exec'd the binary
#     while another was still writing it.
#   - Download and extraction happen in a private temp directory, promoted with
#     one rename, so a reader sees either no directory or a complete one. A
#     mkdir lock stops N runs downloading the same tarball; losing that race
#     means waiting for the winner, not failing.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/.tmp/vector"

# Target triple, as upstream spells it in the release asset names.
#
# The arch spelling is NOT portable across their OS builds: 64-bit ARM is
# `aarch64-unknown-linux-gnu` but `arm64-apple-darwin`. Deriving one arch name
# and appending an OS -- which this did -- 404s on macOS, so the whole triple is
# chosen here.
ARCH="$(uname -m)"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "${OS}-${ARCH}" in
    linux-x86_64)                TRIPLE="x86_64-unknown-linux-gnu" ;;
    linux-aarch64|linux-arm64)   TRIPLE="aarch64-unknown-linux-gnu" ;;
    darwin-arm64|darwin-aarch64) TRIPLE="arm64-apple-darwin" ;;
    darwin-x86_64)               TRIPLE="x86_64-apple-darwin" ;;
    *)
        echo "ERROR: no Vector build known for ${OS}/${ARCH}" >&2
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

# Version-keyed, so a fetch for one version cannot disturb another.
VERSION_DIR="${CACHE_DIR}/${VECTOR_VERSION}-${TRIPLE}"
VECTOR_BIN="${VERSION_DIR}/vector"
LOCK_DIR="${CACHE_DIR}/.lock-${VECTOR_VERSION}-${TRIPLE}"

if [[ -x "${VECTOR_BIN}" ]]; then
    echo "${VECTOR_BIN}"
    exit 0
fi

mkdir -p "${CACHE_DIR}"

# mkdir is atomic on POSIX: exactly one concurrent run creates the lock and
# downloads; the rest wait for the binary to appear.
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    echo "Another run is fetching Vector ${VECTOR_VERSION}; waiting..." >&2
    for _ in $(seq 1 300); do
        if [[ -x "${VECTOR_BIN}" ]]; then
            echo "${VECTOR_BIN}"
            exit 0
        fi
        sleep 1
    done
    echo "ERROR: timed out waiting for another run to cache Vector ${VECTOR_VERSION}" >&2
    echo "If no fetch is running, remove the stale lock: ${LOCK_DIR}" >&2
    exit 1
fi

# Release the lock however we leave, so a failed download does not wedge every
# later run behind a lock nobody holds.
cleanup() {
    rm -rf "${LOCK_DIR}" "${WORK_DIR:-}"
}
trap cleanup EXIT

WORK_DIR="$(mktemp -d "${CACHE_DIR}/.fetch-XXXXXX")"

echo "Downloading Vector ${VECTOR_VERSION} for ${TRIPLE}..." >&2

TARBALL="vector-${VECTOR_VERSION}-${TRIPLE}.tar.gz"
URL="https://packages.timber.io/vector/${VECTOR_VERSION}/${TARBALL}"

# Extract the whole archive rather than naming one member. Asking tar for
# `./vector-<triple>/bin/vector` matched nothing in the macOS tarball, whose
# members carry no `./` prefix -- so nothing was extracted and the failure
# surfaced later as a chmod on a missing file. The Linux tarball does carry the
# prefix, which is why this only broke off-CI.
curl -fsSL "${URL}" | tar xz -C "${WORK_DIR}"

EXTRACTED_BIN="${WORK_DIR}/vector-${TRIPLE}/bin"
if [[ ! -f "${EXTRACTED_BIN}/vector" ]]; then
    echo "ERROR: no vector binary in ${TARBALL} at the expected path" >&2
    exit 1
fi
chmod +x "${EXTRACTED_BIN}/vector"

INSTALLED="$("${EXTRACTED_BIN}/vector" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
if [[ -z "${INSTALLED}" ]]; then
    echo "Failed to install Vector ${VECTOR_VERSION}" >&2
    exit 1
fi

# One rename publishes the directory. The target existing means a concurrent run
# beat us, which is a success, not a conflict.
if [[ -x "${VECTOR_BIN}" ]]; then
    echo "Vector ${VECTOR_VERSION} cached by a concurrent run" >&2
else
    mv "${EXTRACTED_BIN}" "${VERSION_DIR}"
fi

if [[ ! -x "${VECTOR_BIN}" ]]; then
    echo "ERROR: Vector binary not found at ${VECTOR_BIN} after extraction" >&2
    exit 1
fi

echo "Cached Vector ${INSTALLED} at ${VECTOR_BIN}" >&2

# Print path (last line — this is what callers read)
echo "${VECTOR_BIN}"
