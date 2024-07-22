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

# Switch repos to point to to vault.centos.org because CentOS 7 is EOL
RUN sed -i \
  -e 's/^mirrorlist/#mirrorlist/' \
  -e 's/^#baseurl/baseurl/' \
  -e 's/mirror\.centos\.org/vault.centos.org/' \
  /etc/yum.repos.d/*.repo

# install epel repo for ccache
RUN yum install epel-release -y && yum install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm -y && yum clean all && yum makecache

# install dependencies
RUN yum install -y byacc patch automake libtool perf vim make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux \
    wget git python2 bison java-1.8.0-openjdk-devel

# clean cache
RUN yum clean all

# install maven 3.6.3
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
    && wget -q -O /tmp/apache-maven.tar.gz https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/tools/apache-maven-3.6.3-bin.tar.gz \
    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
    && rm -f /tmp/apache-maven.tar.gz \
    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# install nodejs
RUN wget https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/tools/node-v12.13.0-linux-x64.tar.gz \
    -q -O /tmp/node-v12.13.0-linux-x64.tar.gz \
    && cd /tmp/ && tar -xf node-v12.13.0-linux-x64.tar.gz \
    && cp -r node-v12.13.0-linux-x64/* /usr/local/ \
    && rm /tmp/node-v12.13.0-linux-x64.tar.gz && rm -rf node-v12.13.0-linux-x64

# install jdk17
RUN wget https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/tools/openjdk-17.0.2_linux-x64_bin.tar.gz \
    -q -O /tmp/openjdk-17.0.2_linux-x64_bin.tar.gz \
    && cd /tmp && tar -xzf openjdk-17.0.2_linux-x64_bin.tar.gz \
    && cp -r jdk-17.0.2/ /usr/lib/jvm/ \
    && rm /tmp/openjdk-17.0.2_linux-x64_bin.tar.gz && rm -rf /tmp/jdk-17.0.2/

# install ldb-toolchain
# 0.17
RUN wget https://doris-community-bj-1308700295.cos.ap-beijing.myqcloud.com/tools/ldb_toolchain_gen.sh \
    -q -O /tmp/ldb_toolchain_gen.sh \
    && sh /tmp/ldb_toolchain_gen.sh /var/local/ldb-toolchain/ \
    && rm /tmp/ldb_toolchain_gen.sh

# there is a repo which is included all of thirdparty
ENV REPOSITORY_URL="https://doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com/thirdparty" \
    DEFAULT_DIR="/var/local" \
    JAVA_HOME="/usr/lib/jvm/java-1.8.0" \
    PATH="/var/local/ldb-toolchain/bin/:$PATH"
    # USE_AVX2=0

# install ccache
RUN wget https://doris-community-bj-1308700295.cos.ap-beijing.myqcloud.com/tools/ccache-4.8.tar.gz \
    -q -O /tmp/ccache-4.8.tar.gz \
    && cd /tmp/ && tar xzf ccache-4.8.tar.gz \
    && cd ccache-4.8 \
    && cmake -B _build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Release . \
    && cmake --build _build --config Release -j 4 \
    && cp _build/ccache /var/local/ldb-toolchain/bin/

# disable auto enable ccache
# and resolve curl error setting certificate verfiy location error
RUN rm -f /etc/profile.d/ccache.* \
    && cp /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/certs/ca-certificates.crt

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
