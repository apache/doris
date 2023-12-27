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

# how to use Dockerfile.
# this is dockerfile for build doris fe image on amd64.
# when build youself image.
# 1. pull binary from official website and decompress into resource directory that the level equals with Dockerfile_be_ubuntu.
# 2. untar xxxx.tar.gz in resource directory, update the dockerfile field `apache-doris-xxx`, replace with real version.
# 3. run commad docker build -t xxx.doris.be:xx -f Dockerfile_be_ubuntu.

# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:2.0.1)`-bin-`architecture(amd64/arm64)` mode.

FROM ubuntu:22.04

ARG TARGETARCH

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-${TARGETARCH:-amd64} \
        PATH="/opt/apache-doris/fe/bin:${PATH}"

# apache-doris/fe from doris release tar.gz, please update the version in follows x.x.x.
COPY resource/apache-doris-x.x.x-bin-${TARGETARCH:-amd64}/fe /opt/apache-doris/fe

RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    patchelf gdb binutils binutils-common mysql-client \
    curl wget less vim htop iproute2 numactl jq iotop sysstat tzdata \
    tcpdump iputils-ping dnsutils strace lsof blktrace \
    bpfcc-tools linux-headers-realtime linux-tools-realtime silversearcher-ag \
    net-tools openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

ADD resource/init_fe.sh /opt/apache-doris/fe/bin/

COPY resource/fe_*.sh /opt/apache-doris/

WORKDIR /opt/apache-doris

ENTRYPOINT ["/opt/apache-doris/fe/bin/init_fe.sh"]
