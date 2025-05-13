#!/bin/bash
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

#### START ARG ####

# docker build cmd example:
# docker build -f docker/runtime/doris-compose/Dockerfile -t <your-image-name>:<version> .

# choose a base image
ARG JDK_IMAGE=openjdk:17-jdk-slim
#ARG JDK_IMAGE=openjdk:8u342-jdk

# user can download a doris release package, extract it, then build its image used arg `OUTPUT_PATH`
# for example:
#
# ```
# cd ~/tmp
# wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.5-bin-x64.tar.gz
# tar xvf apache-doris-3.0.5-bin-x64.tar.gz  # after extract, there will be a directory ./apache-doris-3.0.5-bin-x64/{fe, be, ms}
#
# docker build \
#     --build-arg OUTPUT_PATH=./apache-doris-3.0.5-bin-x64
#     -f ~/workspace/doris/docker/runtime/doris-compose/Dockerfile \
#     -t my-doris:v3.0.5 \
#     .
# ```
ARG OUTPUT_PATH=./output

#### END ARG ####

# user can speedup with these env:
# export DOCKER_BUILDKIT=1
# export BUILDKIT_PROGRESS=plain

# 第一阶段：安装依赖
FROM ${JDK_IMAGE} as base
# 安装依赖，这部分不会因为 OUTPUT_PATH 变化而重建
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    mkdir -p /opt/apache-doris/{coverage,fdb} /jacoco && \
    sed -i s@/deb.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        default-mysql-client \
        python \
        lsof \
        tzdata \
        curl \
        unzip \
        patchelf \
        jq \
        procps \
        util-linux \
        gosu && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 第二阶段：下载 JaCoCo
FROM base as jacoco
RUN --mount=type=cache,target=/root/.cache \
    curl -fsSL https://repo1.maven.org/maven2/org/jacoco/jacoco/0.8.8/jacoco-0.8.8.zip | unzip -qd /jacoco -

# 最终阶段：组装文件
FROM base as final
COPY --from=jacoco /jacoco /jacoco
COPY --chmod=777 ${OUTPUT_PATH} /opt/apache-doris/

# in docker, run 'chmod 755 doris_be' first time cost 1min, remove it.
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh
RUN if [ -d /opt/apache-doris/ms/bin ]; then                            \
        sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/ms/bin/start.sh ; \
    fi
