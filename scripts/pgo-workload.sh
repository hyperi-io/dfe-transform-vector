#!/usr/bin/env bash
#
# Project:   dfe-transform-vector
# File:      scripts/pgo-workload.sh
# Purpose:   PGO/BOLT workload -- drive the direct-transport bridge, the subprocess watch and the metrics merge
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-transform-vector-binary>
#
# hyperi-ci runs this twice on a release build -- once against the
# PGO-instrumented binary, once against the BOLT-instrumented one -- and
# passes the binary as $1.
#
# What it profiles, and what it cannot:
#   The supervisor's own hot path is the DIRECT transport, where every record
#   crosses this process twice (src/bridge.rs). On the bus, Vector talks to
#   Kafka itself and the per-record CPU sits in the upstream Vector binary the
#   image downloads rather than compiles (scripts/fetch-vector.sh), so no PGO
#   or BOLT build of ours reaches it. This workload therefore drives the direct
#   path, the Vector subprocess watch, and the Vector metrics merge -- which is
#   all of the supervisor that carries load.
#
# What it starts (all loopback, no broker, no container, no download):
#   - the supervisor binary under test, on a direct-transport config
#   - a stand-in for the Vector binary: a script that answers `validate` and
#     then idles, so the subprocess watch has a real child to supervise
#   - target/<profile>/pgo-driver, which closes the loop around the supervisor
#     (upstream pusher, Vector's two protocol ends, the next stage) and serves
#     the Vector-shaped exposition the supervisor scrapes
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS   Seconds of load (default 300, floor 60)
#   PGO_WORKLOAD_PORT_BASE       First of the six loopback ports to claim
#   PGO_WORKLOAD_KEEP            1 to leave the work dir and processes behind
#   PGO_DRIVER_PATH              Use this pgo-driver rather than building one
#   PGO_DRIVER_PROFILE           release (default) or debug, for the build
#   PGO_DRIVER_RPS               Records per second (default 5000)
#
# The only outbound connection the run makes is scalo's startup version check,
# which is fire-and-forget and cannot hold the workload up.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
DRIVER_PROFILE="${PGO_DRIVER_PROFILE:-release}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Set as each piece comes up; read by cleanup() and the driver's environment.
WORK_DIR=""
SUPERVISOR_PID=""
DRIVER_PID=""
PORT_BASE=""

log() { echo "pgo-workload: $*" >&2; }
die() { log "$*"; exit 1; }

#######################################
# Tear down whatever is still running, unless asked to leave it.
#######################################
cleanup() {
    local rc=$?
    if [[ "${KEEP}" == "1" ]]; then
        log "PGO_WORKLOAD_KEEP=1 -- leaving ${WORK_DIR} and pids ${SUPERVISOR_PID} ${DRIVER_PID}"
        exit "${rc}"
    fi
    if [[ -n "${DRIVER_PID}" ]]; then
        kill -TERM "${DRIVER_PID}" 2>/dev/null || true
    fi
    stop_supervisor
    if [[ -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
        rm -rf "${WORK_DIR}"
    fi
    exit "${rc}"
}

#######################################
# True when nothing is listening on a loopback port.
#######################################
port_is_free() {
    ! (exec 3<>"/dev/tcp/127.0.0.1/${1}") 2>/dev/null
}

# The six ports sit below the ephemeral range every Linux kernel allocates
# outbound source ports from -- a range that starts at 10240 on some hosts, and
# a supervisor whose own OTLP connection took the port cannot then bind it.
readonly PORT_FLOOR=6200
readonly PORT_SLOTS=300

#######################################
# First of six consecutive free loopback ports.
#
# Varies with the pid and the attempt, so the PGO pass, the BOLT pass and a
# concurrent job do not claim the same six.
#######################################
pick_port_base() {
    local attempt_seed="${1}"
    local base="${PGO_WORKLOAD_PORT_BASE:-$((PORT_FLOOR + ((($$ + attempt_seed * 77) % PORT_SLOTS) * 10)))}"
    local tries=0
    local offset
    local taken
    while [[ "${tries}" -lt 50 ]]; do
        taken=0
        for offset in 0 1 2 3 4 5; do
            if ! port_is_free "$((base + offset))"; then
                taken=1
                break
            fi
        done
        if [[ "${taken}" -eq 0 ]]; then
            echo "${base}"
            return 0
        fi
        base=$((base + 10))
        tries=$((tries + 1))
    done
    die "no six consecutive free loopback ports from ${PGO_WORKLOAD_PORT_BASE:-${PORT_FLOOR}}"
}

#######################################
# Path to the pgo-driver binary, building it if it is not there yet.
#######################################
resolve_driver() {
    local path="${PGO_DRIVER_PATH:-}"
    if [[ -z "${path}" ]]; then
        path="${PROJECT_ROOT}/target/${DRIVER_PROFILE}/pgo-driver"
    fi
    if [[ -x "${path}" ]]; then
        echo "${path}"
        return 0
    fi

    log "building pgo-driver (${DRIVER_PROFILE})"
    local build_args=(build --manifest-path "${PROJECT_ROOT}/Cargo.toml" --features pgo-driver --bin pgo-driver)
    if [[ "${DRIVER_PROFILE}" == "release" ]]; then
        build_args+=(--release)
    fi
    cargo "${build_args[@]}" >&2 || die "could not build pgo-driver"

    [[ -x "${path}" ]] || die "pgo-driver is still missing at ${path}"
    echo "${path}"
}

#######################################
# Write the Vector stand-in: answers `validate`, then idles under supervision.
#
# The real binary is a separate download and nothing we compile, so running it
# here would buy a network dependency and profile Vector's CPU, not ours.
#######################################
write_vector_stand_in() {
    local path="${1}"
    cat > "${path}" <<'STAND_IN'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
    --version) echo "vector 0.0.0 (dfe-transform-vector pgo-workload stand-in)"; exit 0 ;;
    validate)  exit 0 ;;
esac
trap 'exit 0' TERM INT
while true; do
    sleep 1
done
STAND_IN
    chmod +x "${path}"
}

#######################################
# Write the supervisor config and the transform the assembler wires in.
#######################################
write_config() {
    local base="${1}"

    mkdir -p "${WORK_DIR}/transforms" "${WORK_DIR}/data" "${WORK_DIR}/vector-config"

    cat > "${WORK_DIR}/transforms/00_no_op.yaml" <<'TRANSFORM'
transforms:
  dfe_transform:
    type: remap
    inputs:
      - dfe_source
    source: |
      .pgo = true
TRANSFORM

    cat > "${WORK_DIR}/config.yaml" <<CONFIG
pipeline:
  name: "pgo-workload"

source:
  transport: "direct"
  listen: "127.0.0.1:$((base + 0))"

sink:
  transport: "direct"
  endpoint: "http://127.0.0.1:$((base + 3))"
  topic: "pgo_load"

bridge:
  to_vector: "127.0.0.1:$((base + 1))"
  from_vector: "127.0.0.1:$((base + 2))"
  batch_size: 500

transforms:
  dir: "${WORK_DIR}/transforms"

vector:
  binary: "${WORK_DIR}/vector-stand-in.sh"
  data_dir: "${WORK_DIR}/data"
  config_dir: "${WORK_DIR}/vector-config"
  api_address: ""
  version: ""
  version_check: "disabled"

metrics:
  address: "127.0.0.1:$((base + 4))"
  vector_metrics_address: "127.0.0.1:$((base + 5))"

logging:
  level: "warn"
  format: "json"

reload:
  enabled: false
CONFIG
}

#######################################
# Wait for /readyz, which only answers 200 once the subprocess is supervised.
# Returns non-zero when the supervisor dies or never reports ready.
#######################################
wait_for_ready() {
    local addr="${1}"
    local waited=0
    while [[ "${waited}" -lt 60 ]]; do
        if ! kill -0 "${SUPERVISOR_PID}" 2>/dev/null; then
            log "the supervisor died during startup"
            return 1
        fi
        if curl -sf -o /dev/null --max-time 1 "http://${addr}/readyz"; then
            log "supervisor ready after ${waited}s"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    log "the supervisor did not become ready in 60s"
    return 1
}

#######################################
# Stop the supervisor and forget its pid.
#######################################
stop_supervisor() {
    [[ -n "${SUPERVISOR_PID}" ]] || return 0
    kill -TERM "${SUPERVISOR_PID}" 2>/dev/null || true
    local waited=0
    while kill -0 "${SUPERVISOR_PID}" 2>/dev/null && [[ "${waited}" -lt 10 ]]; do
        sleep 1
        waited=$((waited + 1))
    done
    kill -KILL "${SUPERVISOR_PID}" 2>/dev/null || true
    SUPERVISOR_PID=""
}

#######################################
# Start the supervisor on a fresh set of ports and wait for it to report ready.
#
# Sets PORT_BASE to the six it settled on. A port free at probe time can be
# taken before the bind, so a failed start retries on a different six rather
# than failing the build.
#######################################
start_supervisor() {
    local binary="${1}"
    local attempt=1
    while [[ "${attempt}" -le 3 ]]; do
        PORT_BASE="$(pick_port_base "${attempt}")"
        write_config "${PORT_BASE}"
        log "starting ${binary} on ports ${PORT_BASE}..$((PORT_BASE + 5)) (attempt ${attempt})"
        "${binary}" --config "${WORK_DIR}/config.yaml" \
            --metrics-addr "127.0.0.1:$((PORT_BASE + 4))" \
            run >"${WORK_DIR}/supervisor.log" 2>&1 &
        SUPERVISOR_PID=$!

        if wait_for_ready "127.0.0.1:$((PORT_BASE + 4))"; then
            return 0
        fi
        tail -20 "${WORK_DIR}/supervisor.log" >&2 || true
        stop_supervisor
        attempt=$((attempt + 1))
    done
    die "the supervisor would not come up in 3 attempts"
}

main() {
    [[ $# -ge 1 ]] || die "usage: $0 <path-to-dfe-transform-vector-binary>"

    local binary="${1}"
    [[ -x "${binary}" ]] || die "${binary} is not executable"
    command -v curl >/dev/null 2>&1 || die "curl is required for the readiness probe"

    # A short workload profiles startup rather than the hot path, which makes
    # PGO negative rather than merely useless.
    [[ "${DURATION}" -ge 60 ]] || die "PGO_WORKLOAD_DURATION_SECS must be >= 60 (got ${DURATION})"

    local driver
    driver="$(resolve_driver)"

    trap cleanup EXIT INT TERM
    WORK_DIR="$(mktemp -d -t pgo-workload-XXXXXX)"
    log "work dir ${WORK_DIR}"

    write_vector_stand_in "${WORK_DIR}/vector-stand-in.sh"

    # cargo-pgo bakes the profile directory into the instrumented binary; this
    # only matters when the script is run by hand against a plain build.
    export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-${PROJECT_ROOT}/target/pgo-profiles/pgo-%p_%m.profraw}"
    mkdir -p "$(dirname "${LLVM_PROFILE_FILE}")"

    start_supervisor "${binary}"

    log "driving the direct transport for ${DURATION}s via ${driver}"
    PGO_DRIVER_DURATION_SECS="${DURATION}" \
    PGO_DRIVER_PUSH_ENDPOINT="http://127.0.0.1:$((PORT_BASE + 0))" \
    PGO_DRIVER_TO_VECTOR="127.0.0.1:$((PORT_BASE + 1))" \
    PGO_DRIVER_FROM_VECTOR="127.0.0.1:$((PORT_BASE + 2))" \
    PGO_DRIVER_SINK_LISTEN="127.0.0.1:$((PORT_BASE + 3))" \
    PGO_DRIVER_VECTOR_METRICS="127.0.0.1:$((PORT_BASE + 5))" \
        "${driver}" &
    DRIVER_PID=$!

    local driver_rc=0
    wait "${DRIVER_PID}" || driver_rc=$?
    DRIVER_PID=""
    if [[ "${driver_rc}" -ne 0 ]]; then
        tail -100 "${WORK_DIR}/supervisor.log" >&2 || true
        die "the driver exited ${driver_rc} -- the profile would not be worth keeping"
    fi

    # Let the supervisor drain what is still in flight and flush its profile.
    sleep 5
    log "done"
}

main "$@"
