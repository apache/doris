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

FROM centos:7 AS builder

# install epel repo for ccache
RUN yum install epel-release -y && yum clean all && yum makecache

# install dependencies
RUN yum install -y byacc patch automake libtool perf vim make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux \
    ccache wget git python2 bison java-11-openjdk-devel java-1.8.0-openjdk-devel

# clean cache
RUN yum clean all

# install maven 3.6.3
RUN mkdir -p /usr/share/maven /usr/share/maven/ref && \
    wget -q -O /tmp/apache-maven.tar.gz https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz && \
    tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 && \
    rm -f /tmp/apache-maven.tar.gz && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# install nodejs
RUN wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz \
    -q -O /tmp/node-v12.13.0-linux-x64.tar.gz \
    && cd /tmp/ && tar -xf node-v12.13.0-linux-x64.tar.gz \
    && cp -r node-v12.13.0-linux-x64/* /usr/local/ \
    && rm /tmp/node-v12.13.0-linux-x64.tar.gz && rm -rf node-v12.13.0-linux-x64

# install ldb-toolchain
RUN wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/ldb_toolchain_gen-v0.9.1.sh \
    -q -O /tmp/ldb_toolchain_gen.sh \
    && sh /tmp/ldb_toolchain_gen.sh /var/local/ldb-toolchain/ \
    && rm /tmp/ldb_toolchain_gen.sh

# there is a repo which is included all of thirdparty
ENV REPOSITORY_URL="https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/" \
    DEFAULT_DIR="/var/local" \
    JAVA_HOME="/usr/lib/jvm/java-1.8.0" \
    PATH="/var/local/ldb-toolchain/bin/:$PATH"
    # USE_AVX2=0 # set it when building no-avx2 image

# disable auto enable ccache
RUN rm -f /etc/profile.d/ccache.*

# clone lastest source code, download and build third party
COPY doris ${DEFAULT_DIR}/doris
RUN cd ${DEFAULT_DIR}/doris && /bin/bash thirdparty/build-thirdparty.sh \
    && rm -rf ${DEFAULT_DIR}/doris/thirdparty/src \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty.tar.gz \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty \
    && mkdir -p ${DEFAULT_DIR}/thirdparty \
    && mv ${DEFAULT_DIR}/doris/thirdparty/installed ${DEFAULT_DIR}/thirdparty/ \
    && rm -rf ${DEFAULT_DIR}/doris

RUN alternatives --set java java-1.8.0-openjdk.x86_64 && alternatives --set javac java-1.8.0-openjdk.x86_64


# squash all layers to reduce the image size
FROM scratch
COPY --from=builder / /
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0" \
    MAVEN_HOME="/usr/share/maven" \
    REPOSITORY_URL="https://doris-thirdparty-repo.bj.bcebos.com/thirdparty" \
    DEFAULT_DIR="/var/local" \
    PATH="/var/local/ldb-toolchain/bin/:/var/local/thirdparty/installed/bin/:$PATH" \
    DORIS_THIRDPARTY="/var/local/thirdparty"
WORKDIR /root

CMD ["/bin/bash"]
