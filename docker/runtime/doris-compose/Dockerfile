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
# doris 2.1, 3.0+, master use JDK 17
ARG JDK_IMAGE=openjdk:17-jdk-slim

# doris 2.0 use JDK 8
# build 2.0 image, example:
# docker build --build-arg JDK_IMAGE=openjdk:8u342-jdk \
#     -f docker/runtime/doris-compose/Dockerfile \
#     -t <your-image-name>:<version> .
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
#     --build-arg OUTPUT_PATH=./apache-doris-3.0.5-bin-x64 \
#     -f ~/workspace/doris/docker/runtime/doris-compose/Dockerfile \
#     -t my-doris:v3.0.5 \
#     .
# ```
ARG OUTPUT_PATH=./output

#### END ARG ####

FROM ${JDK_IMAGE}

# set environment variables
ENV JACOCO_VERSION=0.8.8

RUN sed -i -e s@/deb.debian.org/@/mirrors.aliyun.com/@g -e s@/security.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list \
    && apt-get clean \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
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
        gosu \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && apt-get clean

RUN mkdir -p /opt/apache-doris/fdb \
    && mkdir -p /opt/apache-doris/coverage

RUN curl -f https://repo1.maven.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip -o jacoco.zip \
    && mkdir /jacoco \
    && unzip jacoco.zip -d /jacoco

# COPY need refine ARG after FROM
ARG OUTPUT_PATH
COPY --chmod=777 ${OUTPUT_PATH} /opt/apache-doris/
# in docker, run 'chmod 755 doris_be' first time cost 1min, remove it.
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh
RUN if [ -d /opt/apache-doris/ms/bin ]; then                            \
        sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/ms/bin/start.sh ; \
    fi