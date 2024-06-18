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

#### END ARG ####

FROM ${JDK_IMAGE}

RUN <<EOF
    if [ -d "/usr/local/openjdk-17" ]; then
        ln -s /usr/local/openjdk-17  /usr/local/openjdk
    else \
        ln -s /usr/local/openjdk-8  /usr/local/openjdk
    fi
EOF

# set environment variables
ENV JAVA_HOME="/usr/local/openjdk"
ENV JACOCO_VERSION 0.8.8

RUN mkdir -p /opt/apache-doris/coverage

RUN  sed -i s@/deb.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list
RUN  apt-get clean

RUN apt-get update && \
    apt-get install -y default-mysql-client python lsof tzdata curl unzip patchelf jq procps && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean

RUN curl -f https://repo1.maven.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip -o jacoco.zip && \
    mkdir /jacoco && \
    unzip jacoco.zip -d /jacoco

# cloud
COPY cloud/CMakeLists.txt cloud/output* output/ms* /opt/apache-doris/cloud/
RUN <<EOF
    mkdir /opt/apache-doris/fdb
    if [ -d /opt/apache-doris/cloud/bin ]; then
        sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/cloud/bin/start.sh
    fi
EOF

# fe and be
COPY output /opt/apache-doris/
# in docker, run 'chmod 755 doris_be' first time cost 1min, remove it.
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh

