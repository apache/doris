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
# 2. untar xxxx.tar.gz in resource directory, update the dockerfile field `apache-doris-xxx`, replace with real version and target arch.
# 3. run commad docker build -t xxx.doris.be:xx -f Dockerfile --build-arg DORIS_VERSION=2.1.2


# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:2.0.1)`-bin-`architecture(amd64/arm64)` mode.

# choose a base image
FROM ubuntu:22.04

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"

RUN sed -i -e 's/^APT/# APT/' -e 's/^DPkg/# DPkg/' /etc/apt/apt.conf.d/docker-clean && \
    apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    patchelf gdb binutils binutils-common mysql-client \
    curl wget less vim htop iproute2 numactl jq iotop sysstat \
    tcpdump iputils-ping dnsutils strace lsof blktrace tzdata \
    bpfcc-tools linux-headers-realtime linux-tools-realtime silversearcher-ag \
    net-tools



RUN if echo $DORIS_VERSION | grep -E '^([3-9]|([1-9]([0-9]))).*' >>/dev/null ; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-17-jdk && \
        ln -s /usr/lib/jvm/java-17-openjdk-${TARGETARCH:-amd64} /usr/lib/jvm/java; \
    else \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-8-jdk && \
        ln -s /usr/lib/jvm/java-8-openjdk-${TARGETARCH:-amd64} /usr/lib/jvm/java; \
    fi; \
    rm -rf /var/lib/apt/lists/*


# set environment variables

ENV JAVA_HOME=/usr/lib/jvm/java
ENV PATH=$PATH:/opt/apache-doris/be/bin

# apache-doris/be from doris release xxxx.tar.gz.please update the version in follows x.x.x.
ADD resource/apache-doris-${DORIS_VERSION}-bin-${TARGETARCH:-amd64}/be /opt/apache-doris/be

COPY resource/be_*.sh /opt/apache-doris/

COPY --from=selectdb/doris-debug-ubuntu:latest /doris-debug /opt/apache-doris/

COPY resource/entry_point.sh /usr/local/bin/

COPY resource/init_be.sh /usr/local/bin/

#RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /opt/apache-doris

ENTRYPOINT ["bash","entry_point.sh"]
