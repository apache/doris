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
# this is dockerfile for build doris be image
# when build youself image.
# 1. pull binary from official website and decompress into resource directory that the level equals with this Dockerfile.
# 2. untar xxxx.tar.gz in resource/{arch} directory, makesure your doris package real version and target arch.
# 3. run commad docker build -t xxx.doris.be:xx -f Dockerfile --build-arg DORIS_VERSION=2.1.2

# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:2.0.1)`-bin-`architecture(amd64/arm64)` mode.

# choose a base image
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

FROM ${BASE_IMAGE}

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"


# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java
ENV PATH=$PATH:$JAVA_HOME/bin:/opt/apache-doris/be/bin

COPY resource/${TARGETARCH:-amd64}/apache-doris-${DORIS_VERSION}-bin-*/be /opt/apache-doris/be

COPY resource/be_*.sh /opt/apache-doris/

COPY resource/entry_point.sh /usr/local/bin/

COPY resource/init_be.sh /usr/local/bin/

#RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /opt/apache-doris

ENTRYPOINT ["bash","entry_point.sh"]
