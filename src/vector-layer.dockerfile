# Vector binary -- downloaded inside the build for portability. Multi-arch via
# $TARGETARCH. Bump the version deliberately: Vector's CLI flags and config
# schema can shift between minor releases.
#
# Vector is MPL-2.0, so redistributing this binary carries two obligations --
# ship the licence text, and tell recipients where the source is. Both are
# discharged from the release archive's own LICENSE / NOTICE / licenses tree
# rather than a separate download, so the attribution always matches the exact
# binary shipped.
#
# This fragment is spliced into the generated Dockerfile by
# `src/deployment.rs::emit_dockerfile()`, which substitutes @VECTOR_VERSION@,
# @VECTOR_SHA256_AMD64@ and @VECTOR_SHA256_ARM64@.
# It lives in its own file rather than inside a Rust string so that hadolint
# and shellcheck can actually read it -- shell embedded in `format!` is
# validated by nothing until the image build runs in CI.
ARG VECTOR_VERSION=@VECTOR_VERSION@
# Pinned from Vector's own published SHA256SUMS for this version, cross-checked
# against the GitHub release asset of the same name -- not fetched at build
# time, which would only prove the download matches itself.
ARG VECTOR_SHA256_AMD64=@VECTOR_SHA256_AMD64@
ARG VECTOR_SHA256_ARM64=@VECTOR_SHA256_ARM64@
ARG TARGETARCH
# hadolint ignore=DL4006
RUN set -eu \
    && case "${TARGETARCH:-amd64}" in \
        amd64) ARCH=x86_64; VECTOR_SHA256="${VECTOR_SHA256_AMD64}" ;; \
        arm64) ARCH=aarch64; VECTOR_SHA256="${VECTOR_SHA256_ARM64}" ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && VDIR="/tmp/vector-${ARCH}-unknown-linux-gnu" \
    && curl -fsSL "https://packages.timber.io/vector/${VECTOR_VERSION}/vector-${VECTOR_VERSION}-${ARCH}-unknown-linux-gnu.tar.gz" \
        -o /tmp/vector.tar.gz \
    && echo "${VECTOR_SHA256}  /tmp/vector.tar.gz" | sha256sum -c - \
    && tar xz -C /tmp -f /tmp/vector.tar.gz \
    && mv "${VDIR}/bin/vector" /usr/local/bin/vector \
    && chmod +x /usr/local/bin/vector \
    && mkdir -p /usr/share/doc/vector \
    && cp -a "${VDIR}/LICENSE" "${VDIR}/NOTICE" "${VDIR}/LICENSE-3rdparty.csv" \
        "${VDIR}/licenses" /usr/share/doc/vector/ \
    && printf '%s\n' \
        'Vector is redistributed here unmodified, under the Mozilla Public' \
        'License 2.0. Version: @VECTOR_VERSION@' \
        '' \
        'Source code for this version (MPL-2.0 section 3.2(a)):' \
        '  https://github.com/vectordotdev/vector/releases/tag/v@VECTOR_VERSION@' \
        '' \
        'Upstream LICENSE, NOTICE and third-party attributions are in this' \
        'directory, taken from the release archive itself.' \
        > /usr/share/doc/vector/README.hyperi \
    && rm -rf /tmp/vector.tar.gz "${VDIR}" \
    && /usr/local/bin/vector --version

# Vector data and config directories
RUN mkdir -p /var/lib/vector /var/run/vector/config /etc/dfe-transform-vector/transforms \
    && chown -R appuser:appuser /var/lib/vector /var/run/vector /etc/dfe-transform-vector

LABEL io.hyperi.vector.version="@VECTOR_VERSION@"
