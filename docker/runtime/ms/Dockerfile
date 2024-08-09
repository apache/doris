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
# this is dockerfile for build doris ms image
# when build youself image.
# 1. pull binary from official website and decompress into resource directory that the level equals with this Dockerfile.
# 2. untar xxxx.tar.gz in resource/{arch} directory, makesure your doris package real version and target arch.
# 3. run commad docker build -t xxx.doris.ms:xx -f Dockerfile --build-arg DORIS_VERSION=3.0.0

# we have support buildx for amd64 and arm64 architecture image build.
# get the binary from doris github and utar into resource, update the directory as apache-`version(example:3.0.0)`-bin-`architecture(amd64/arm64)` mode.

# choose a base image
FROM selectdb/base:latest

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"


RUN if echo $DORIS_VERSION | grep -E '^([3-9]|([1-9]([0-9])))|^branch\-([3-9]|([1-9]([0-9])))|master.*' >>/dev/null ; then \
        ln -s /usr/lib/jvm/jdk-17 /usr/lib/jvm/java && \
        rm -rf /usr/lib/jvm/jdk-8; \
    else \
        ln -s /usr/lib/jvm/jdk-8 /usr/lib/jvm/java && \
        rm -rf /usr/lib/jvm/jdk-17; \
    fi;

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java
ENV PATH=$PATH:$JAVA_HOME/bin:/opt/apache-doris/ms/bin

COPY resource/${TARGETARCH:-amd64}/apache-doris-${DORIS_VERSION}-bin-*/ms /opt/apache-doris/ms

COPY resource/ms_*.sh /opt/apache-doris/

#RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /opt/apache-doris

#ENTRYPOINT ["bash","entry_point.sh"]
ENTRYPOINT ["bash","ms_disaggregated_entrypoint.sh"]
