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

# Local single-container FE+BE image for development (see README.md in this directory).
#
# --- A) Default: copy local tree (source build output/ or extracted release tarball) ---
#   docker build -f docker/runtime/local-dev/Dockerfile -t doris-local:dev .
#
# --- B) Prebuilt: download official binary inside build (no local compile) ---
#   docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-prebuilt \
#     --build-arg DORIS_VERSION=3.0.5 -t doris-local:3.0.5 .
#   For linux/amd64 without AVX2 packages, add: --build-arg BINARY_VARIANT=noavx2
#   For cross-platform images use buildx, e.g. --platform linux/arm64
#
# --- C) Host download script then build target local ---
#   docker/runtime/local-dev/download-binary.sh -v 3.0.5
#   docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-local \
#     --build-arg OUTPUT_PATH=./docker/runtime/local-dev/.prebuilt/apache-doris-3.0.5-bin-arm64 -t doris-local:3.0.5 .
#
# Run (example):
#   docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \
#     -v doris-meta:/opt/apache-doris/fe/doris-meta \
#     -v doris-be-storage:/opt/apache-doris/be/storage \
#     doris-local:dev
#
# Security: default root / admin empty password — only for isolated local use.

# Doris 2.1 / 3.x: JDK 17. Doris 2.0: pass --build-arg JDK_IMAGE=openjdk:8u342-jdk
ARG JDK_IMAGE=openjdk:17.0.1-jdk-slim
ARG OUTPUT_PATH=./output

FROM ${JDK_IMAGE} AS base-local-dev

RUN sed -i -e s@/deb.debian.org/@/mirrors.aliyun.com/@g -e s@/security.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list \
    && apt-get clean \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        default-mysql-client \
        curl \
        procps \
        tzdata \
        util-linux \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SKIP_CHECK_ULIMIT=true \
    PATH="/opt/apache-doris/fe/bin:/opt/apache-doris/be/bin:${PATH}"

# Download official release during docker build (matches TARGETARCH: arm64 / amd64).
FROM base-local-dev AS doris-local-dev-prebuilt
ARG DORIS_VERSION
ARG TARGETARCH
ARG BINARY_VARIANT=
ARG DOWNLOAD_BASE=https://apache-doris-releases.oss-accelerate.aliyuncs.com
RUN set -eux; \
    if [ -z "${DORIS_VERSION}" ]; then \
        echo "ERROR: pass --build-arg DORIS_VERSION=x.y.z when using target doris-local-dev-prebuilt" >&2; \
        exit 1; \
    fi; \
    case "${TARGETARCH}" in \
        arm64) D=arm64 ;; \
        amd64) \
            if [ "${BINARY_VARIANT}" = "noavx2" ]; then D=x64-noavx2; else D=x64; fi ;; \
        *) \
            echo "ERROR: Unsupported TARGETARCH=${TARGETARCH}; use docker buildx --platform linux/arm64 or linux/amd64" >&2; \
            exit 1 ;; \
    esac; \
    PKG="apache-doris-${DORIS_VERSION}-bin-${D}"; \
    curl -fSL "${DOWNLOAD_BASE}/${PKG}.tar.gz" | tar xz -C /tmp; \
    mkdir -p /opt/apache-doris; \
    mv "/tmp/${PKG}/fe" "/tmp/${PKG}/be" /opt/apache-doris/
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh
COPY docker/runtime/local-dev/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
WORKDIR /opt/apache-doris
EXPOSE 8030 8040 8070 9010 9020 9030 9050 9060 8050 8060
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Copy fe/ + be/ from build context (e.g. ./output or download-binary.sh output).
FROM base-local-dev AS doris-local-dev-local
ARG OUTPUT_PATH
COPY ${OUTPUT_PATH} /opt/apache-doris/
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh
COPY docker/runtime/local-dev/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
WORKDIR /opt/apache-doris
EXPOSE 8030 8040 8070 9010 9020 9030 9050 9060 8050 8060
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Default image when no --target is given (same as before: use OUTPUT_PATH=./output or override).
FROM doris-local-dev-local
