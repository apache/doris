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

# how to use Dockerfile.
# this is dockerfile for build doris fe image
# when build youself image.
# 1. pull binary from official website and decompress into resource directory that the level equals with this Dockerfile.
# 2. untar xxxx.tar.gz in resource/{arch} directory, makesure your doris package real version and target arch.
# 3. run commad docker build -t xxx.doris.fe:xx -f Dockerfile --build-arg DORIS_VERSION=2.1.2

# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:2.0.1)`-bin-`architecture(amd64/arm64)` mode.

# Standalone builds can use the published legacy base. The release build
# script supplies its locally built base through BASE_IMAGE.
ARG BASE_IMAGE=doris-legacy-base
FROM apache/doris:base-6.0@sha256:ff69bd48c3f36d06403fbc8647be007d9c218f949637cfb34e5939f6946f415b AS doris-legacy-base

ARG JAVA_VERSION
ARG DORIS_VERSION="x.x.x"

RUN set -eu; \
    if [ -z "${JAVA_VERSION:-}" ]; then \
        if printf '%s\n' "${DORIS_VERSION}" | grep -Eq '^(master|(branch-)?([3-9]|[1-9][0-9]))'; then \
            JAVA_VERSION=17; \
        else \
            JAVA_VERSION=8; \
        fi; \
    fi; \
    case "${JAVA_VERSION}" in \
        8) unused_jdk=17 ;; \
        17) unused_jdk=8 ;; \
        *) echo "Unsupported JAVA_VERSION: ${JAVA_VERSION}" >&2; exit 1 ;; \
    esac; \
    ln -s "jdk-${JAVA_VERSION}" /usr/lib/jvm/java; \
    rm -rf "/usr/lib/jvm/jdk-${unused_jdk}"

FROM ${BASE_IMAGE} AS artifacts

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"

COPY resource/${TARGETARCH:-amd64}/apache-doris-${DORIS_VERSION}-bin-*/fe /artifacts/fe
# Exclude these before the final COPY: deleting them afterwards would leave
# the bundled private keys and Arthas dependencies in a shipped image layer.
RUN rm -rf /artifacts/fe/arthas /artifacts/fe/mysql_ssl_default_certificate

FROM ${BASE_IMAGE} AS runtime

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java
ENV PATH=$PATH:$JAVA_HOME/bin:/opt/apache-doris/fe/bin

COPY --from=artifacts /artifacts/fe /opt/apache-doris/fe

COPY resource/init_fe.sh /usr/local/bin/

COPY resource/fe_*.sh /opt/apache-doris/

# when you use beijing zone, please enable the set.
# RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /opt/apache-doris

ENTRYPOINT ["bash","init_fe.sh"]
