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
# EPEL 7 is EOL; use its archive instead of the retired metalink.
RUN yum install epel-release -y \
    && sed -i -e 's|^metalink=|#metalink=|' \
       -e 's|^#baseurl=.*/pub/epel/7/|baseurl=https://archives.fedoraproject.org/pub/archive/epel/7/|' \
       /etc/yum.repos.d/epel.repo \
    && yum install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm -y \
    && yum clean all && yum makecache

# install dependencies
RUN yum install -y byacc patch automake libtool perf vim make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux \
    wget git python2 python3 bison java-1.8.0-openjdk-devel

# clean cache
RUN yum clean all

# install jdk17
RUN wget https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/tools/openjdk-17.0.2_linux-x64_bin.tar.gz \
    -q -O /tmp/openjdk-17.0.2_linux-x64_bin.tar.gz \
    && cd /tmp && tar -xzf openjdk-17.0.2_linux-x64_bin.tar.gz \
    && cp -r jdk-17.0.2/ /usr/lib/jvm/ \
    && rm /tmp/openjdk-17.0.2_linux-x64_bin.tar.gz && rm -rf /tmp/jdk-17.0.2/

# install maven 3.9.9
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
    && wget -q -O /tmp/apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz \
    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
    && rm -f /tmp/apache-maven.tar.gz \
    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# install nodejs
ARG NODEJS_VERSION=22.12.0
ARG NODEJS_SHA256=a739c219ec1ebdb33a26cb93cce32189924612c8f32aefbac25d34fe18d47a38
# Official Node.js 22 Linux binaries require glibc 2.28. Use the Node.js
# unofficial-builds compatibility binary to retain this image's glibc 2.17 baseline.
RUN wget https://unofficial-builds.nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64-glibc-217.tar.xz \
    -q -O /tmp/node-v${NODEJS_VERSION}-linux-x64-glibc-217.tar.xz \
    && echo "${NODEJS_SHA256}  /tmp/node-v${NODEJS_VERSION}-linux-x64-glibc-217.tar.xz" | sha256sum --check --strict \
    && cd /tmp/ && tar -xf node-v${NODEJS_VERSION}-linux-x64-glibc-217.tar.xz \
    && cp -r node-v${NODEJS_VERSION}-linux-x64-glibc-217/* /usr/local/ \
    && rm /tmp/node-v${NODEJS_VERSION}-linux-x64-glibc-217.tar.xz \
    && rm -rf node-v${NODEJS_VERSION}-linux-x64-glibc-217 \
    && node --version | grep -Fx "v${NODEJS_VERSION}" \
    && npm --version \
    && python3 -c 'import sys; assert sys.version_info >= (3, 6)'

# install ldb-toolchain
## 0.18
RUN wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.18/ldb_toolchain_gen.sh \
    -q -O /tmp/ldb_toolchain_gen_018.sh \
    && sh /tmp/ldb_toolchain_gen_018.sh /var/local/ldb-toolchain-018/ \
    && rm /tmp/ldb_toolchain_gen_018.sh

## 0.25
RUN wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.25/ldb_toolchain_gen.sh \
    -q -O /tmp/ldb_toolchain_gen_025.sh \
    && sh /tmp/ldb_toolchain_gen_025.sh /var/local/ldb-toolchain \
    && rm /tmp/ldb_toolchain_gen_025.sh

## Use clang-format in 0.18
RUN echo '#!/bin/bash' > /usr/local/bin/clang-format && \
    echo 'exec /var/local/ldb-toolchain-018/bin/clang-format "$@"' >> /usr/local/bin/clang-format && \
    chmod +x /usr/local/bin/clang-format

# there is a repo which is included all of thirdparty
ENV REPOSITORY_URL="https://doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com/thirdparty" \
    DEFAULT_DIR="/var/local" \
    JAVA_HOME="/usr/lib/jvm/java-1.8.0/" \
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

# Install the Rust toolchain required by Lance-C.
ENV CARGO_HOME=/opt/cargo RUSTUP_HOME=/opt/rustup
ENV PATH="/opt/cargo/bin:${PATH}"
RUN wget -q https://sh.rustup.rs -O /tmp/rustup-init.sh \
    && sh /tmp/rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain 1.91.0 \
    && rm -f /tmp/rustup-init.sh \
    && cargo --version && rustc --version

# clone lastest source code, download and build third party
COPY doris ${DEFAULT_DIR}/doris
# Cargo build scripts run on CentOS 7. The LDB linker introduces a
# GLIBC_2.18 dependency; use system GCC for Rust linking and retain LDB for C/C++.
RUN cd ${DEFAULT_DIR}/doris \
    && CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/gcc \
       /bin/bash thirdparty/build-thirdparty.sh \
    && rm -rf "${CARGO_HOME}/registry" "${CARGO_HOME}/git" \
    && rm -rf ${DEFAULT_DIR}/doris/thirdparty/src \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty.tar.gz \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty \
    && mkdir -p ${DEFAULT_DIR}/thirdparty \
    && mv ${DEFAULT_DIR}/doris/thirdparty/installed ${DEFAULT_DIR}/thirdparty/ \
    && rm -rf ${DEFAULT_DIR}/doris

# squash all layers to reduce the image size
FROM scratch
COPY --from=builder / /
ENV JAVA_HOME="/usr/lib/jvm/jdk-17.0.2/" \
    MAVEN_HOME="/usr/share/maven" \
    REPOSITORY_URL="https://doris-thirdparty-repo.bj.bcebos.com/thirdparty" \
    DEFAULT_DIR="/var/local" \
    CARGO_HOME="/opt/cargo" \
    RUSTUP_HOME="/opt/rustup" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="/usr/bin/gcc" \
    PATH="/opt/cargo/bin:/usr/local/bin:/var/local/ldb-toolchain/bin/:/var/local/thirdparty/installed/bin/:/usr/lib/jvm/jdk-17.0.2/bin/:$PATH" \
    DORIS_THIRDPARTY="/var/local/thirdparty"
WORKDIR /root

CMD ["/bin/bash"]
