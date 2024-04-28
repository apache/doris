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
# this is dockerfile for build doris broker image on amd64.
# when build youself image.
# 1. pull binary from official website and decompress into resource directory that the level equals with Dockerfile_broker_ubuntu.
# 2. untar xxxx.tar.gz in resource directory, update the dockerfile field `apache-doris-xxx`, replace with real version.
# 3. run commad docker build -t xxx.doris.broker:xx -f Dockerfile_broker_ubuntu.

# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:2.0.1)`-bin-`architecture(amd64/arm64)` mode.
#

FROM ubuntu:22.04

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"

RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    patchelf gdb binutils binutils-common mysql-client \
    curl wget less vim htop iproute2 numactl jq iotop sysstat \
    tcpdump iputils-ping dnsutils strace lsof blktrace tzdata \
    bpfcc-tools linux-headers-realtime linux-tools-realtime silversearcher-ag \
    net-tools openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-${TARGETARCH:-amd64}

# apache-doris/broker from doris release xxxx.tar.gz
ADD resource/apache-doris-${DORIS_VERSION}-bin-${TARGETARCH:-amd64}/extensions/apache_hdfs_broker /opt/apache-doris/apache_hdfs_broker

COPY resource/broker_*.sh /opt/apache-doris/

RUN chmod +x /opt/apache-doris/broker_*.sh

ADD ./resource/init_broker.sh /usr/local/bin/

RUN chmod 755 /usr/local/bin/init_broker.sh

WORKDIR /opt/apache-doris

ENTRYPOINT ["bash","init_broker.sh"]
