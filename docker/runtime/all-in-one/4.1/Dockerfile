# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# ============================================================================
# Apache Doris all-in-one image for integration testing -- branch-4.1 only.
#
# A single container running one FE and one BE, sized for use as a test
# fixture in downstream CI. Build it with ./build.sh; the commands below are
# what that script ends up running.
#
#   base flavor (default):
#     docker buildx build --build-arg DORIS_VERSION=4.1.3 \
#       -f docker/runtime/all-in-one/4.1/Dockerfile \
#       -t apache/doris:all-in-one-4.1.3 .
#
#   full flavor (adds hudi / trino / maxcompute):
#     ... --build-arg FLAVOR=full -t apache/doris:all-in-one-4.1.3-full .
#
# The build context is the repository root (so ARTIFACT_SOURCE=local can pick
# up ./output); Dockerfile.dockerignore narrows it down to a few KB.
# ============================================================================

# ---- global args: must be declared before the first FROM ----
ARG DORIS_VERSION=4.1.3
ARG ARTIFACT_SOURCE=image
ARG FE_IMAGE=apache/doris:fe-${DORIS_VERSION}
ARG BE_IMAGE=apache/doris:be-${DORIS_VERSION}
ARG BASE_IMAGE=ubuntu:22.04

# ============================ artifact sources ==============================
# Three interchangeable ways to get fe/ and be/ into /artifacts. Everything
# downstream only knows about /artifacts, never about where it came from.

FROM ${FE_IMAGE} AS fe-src
FROM ${BE_IMAGE} AS be-src

# strip(1) lives here, so this is ubuntu rather than busybox.
FROM ${BASE_IMAGE} AS artifacts-base
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        -o Acquire::Retries=3 binutils && \
    rm -rf /var/lib/apt/lists/*

# A. the official component images (default)
FROM artifacts-base AS artifacts-image
COPY --from=fe-src /opt/apache-doris/fe /artifacts/fe
COPY --from=be-src /opt/apache-doris/be /artifacts/be

# B. a locally built ./output, for developers testing their own build
FROM artifacts-base AS artifacts-local
ARG LOCAL_OUTPUT=output
COPY ${LOCAL_OUTPUT}/fe /artifacts/fe
COPY ${LOCAL_OUTPUT}/be /artifacts/be

# C. an extracted release tarball
FROM artifacts-base AS artifacts-tarball
ARG TARBALL_DIR
COPY ${TARBALL_DIR}/fe /artifacts/fe
COPY ${TARBALL_DIR}/be /artifacts/be

# ========================= strip + flavor pruning ===========================
# Both have to happen HERE, before the final COPY. Doing them in the runtime
# stage would only add a whiteout layer -- the bytes would still ship.

FROM artifacts-${ARTIFACT_SOURCE} AS artifacts
ARG CTX_PREFIX=docker/runtime/all-in-one/4.1
ARG FLAVOR=base
ARG STRIP_BE=debug

COPY ${CTX_PREFIX}/resource/prune.txt /tmp/prune.txt

RUN set -eux; \
    case "${FLAVOR}" in base|full) ;; *) echo "bad FLAVOR=${FLAVOR}" >&2; exit 1 ;; esac; \
    # 4.1.3 ships doris_be with debug info: 2213M -> 430M, and --strip-debug
    # keeps .symtab so crash backtraces still resolve function names.
    case "${STRIP_BE}" in \
      debug) strip --strip-debug /artifacts/be/lib/doris_be ;; \
      full)  strip -s            /artifacts/be/lib/doris_be ;; \
      none)  : ;; \
      *) echo "bad STRIP_BE=${STRIP_BE}" >&2; exit 1 ;; \
    esac; \
    # "always" rows apply to both flavors, "base" rows only to the base flavor.
    awk -v f="${FLAVOR}" '$1=="always" || $1==f {print $2}' /tmp/prune.txt \
      | while IFS= read -r p; do echo "prune: ${p}"; rm -rf "/artifacts/${p}"; done; \
    rm -f /tmp/prune.txt; \
    rm -rf /artifacts/fe/log /artifacts/be/log; \
    mkdir -p /artifacts/fe/log /artifacts/be/log \
             /artifacts/fe/doris-meta /artifacts/be/storage; \
    echo "=== artifact size (${FLAVOR}) ==="; du -sh /artifacts/fe /artifacts/be

# ================================ runtime ===================================

FROM ${BASE_IMAGE} AS runtime
ARG TARGETARCH
ARG DORIS_VERSION
ARG FLAVOR=base
ARG CTX_PREFIX=docker/runtime/all-in-one/4.1
ARG JDK_PKG=openjdk-17-jre-headless
# FE ships -Xmx8192m, far more than a CI runner can spare; BE ships -Xmx2048m
# for its JNI-side JVM. Both are rewritten in place below.
ARG FE_HEAP=2048m
ARG BE_HEAP=1024m

ENV DORIS_HOME=/opt/apache-doris \
    CI_HOME=/opt/doris-ci \
    LANG=C.UTF-8 \
    TZ=UTC

RUN set -eux; \
    apt-get update -y; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        -o Acquire::Retries=3 \
        ${JDK_PKG} \
        mysql-client-core-8.0 \
        curl \
        tini \
        procps \
        tzdata \
        ca-certificates; \
    ln -sfn "/usr/lib/jvm/java-17-openjdk-${TARGETARCH:-amd64}" /usr/lib/jvm/java; \
    ln -sf /usr/share/zoneinfo/UTC /etc/localtime; \
    rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/*

# SKIP_CHECK_ULIMIT: start_be.sh checks vm.max_map_count / swap / ulimit -n and
# exits 1 when they are too low. vm.max_map_count is not a namespaced sysctl, so
# a container cannot fix it -- the check has to be skipped here.
ENV JAVA_HOME=/usr/lib/jvm/java \
    PATH=/usr/lib/jvm/java/bin:/opt/apache-doris/fe/bin:/opt/apache-doris/be/bin:$PATH \
    SKIP_CHECK_ULIMIT=true

COPY --from=artifacts /artifacts/fe ${DORIS_HOME}/fe
COPY --from=artifacts /artifacts/be ${DORIS_HOME}/be
COPY ${CTX_PREFIX}/resource/ ${CI_HOME}/

RUN set -eux; \
    # Integration-test defaults are appended, never edited in: the upstream
    # values stay visible above them and the last assignment wins for both the
    # shell-sourced ALL_CAPS vars and the property-style lowercase keys.
    cat "${CI_HOME}/conf/fe_ci.conf" >> "${DORIS_HOME}/fe/conf/fe.conf"; \
    cat "${CI_HOME}/conf/be_ci.conf" >> "${DORIS_HOME}/be/conf/be.conf"; \
    # The heap lives inside a long JAVA_OPTS_FOR_JDK_17 line that also carries
    # every --add-opens FE needs on JDK 17. Rewriting just the -Xmx/-Xms tokens
    # keeps the rest of that line exactly as upstream shipped it.
    sed -i -E "s/-Xmx[0-9]+[kKmMgG]/-Xmx${FE_HEAP}/g; s/-Xms[0-9]+[kKmMgG]/-Xms${FE_HEAP}/g" \
        "${DORIS_HOME}/fe/conf/fe.conf"; \
    sed -i -E "s/-Xmx[0-9]+[kKmMgG]/-Xmx${BE_HEAP}/g; s/-Xms[0-9]+[kKmMgG]/-Xms${BE_HEAP}/g" \
        "${DORIS_HOME}/be/conf/be.conf"; \
    chmod +x "${CI_HOME}"/*.sh

LABEL org.opencontainers.image.title="Apache Doris all-in-one" \
      org.opencontainers.image.description="Single-container Doris (FE+BE) for integration testing" \
      org.opencontainers.image.version="${DORIS_VERSION}" \
      org.opencontainers.image.source="https://github.com/apache/doris" \
      org.apache.doris.allinone.flavor="${FLAVOR}" \
      org.apache.doris.allinone.branch="4.1"

# 8030 FE http | 9030 FE mysql | 8040 BE http (stream load) | 9050 BE heartbeat
EXPOSE 8030 9030 8040 9050

WORKDIR ${DORIS_HOME}

HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=90 \
    CMD /opt/doris-ci/health_check.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/doris-ci/entrypoint.sh"]
