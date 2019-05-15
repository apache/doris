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

FROM centos:centos7.5.1804

MAINTAINER tangxiaoqing214445

ENV DEFAULT_DIR /var/local

ARG GCC_VERSION=7.3.0
ARG GCC_URL=https://mirrors.ustc.edu.cn/gnu/gcc/gcc-${GCC_VERSION}

# install dependencies and build gcc
RUN yum install -y bzip2 wget git gcc-c++ libstdc++-static byacc flex automake libtool binutils-devel bison ncurses-devel make mlocate unzip patch which vim-common redhat-lsb-core zip libcurl-devel \
  && updatedb \
  && yum -y clean all \
  && rm -rf /var/cache/yum \
  && mkdir -p  /var/local/gcc \
  && curl -fsSL -o /tmp/gcc.tar.gz  ${GCC_URL}/gcc-${GCC_VERSION}.tar.gz \
  && tar -xzf /tmp/gcc.tar.gz -C /var/local/gcc --strip-components=1 \
  && cd /var/local/gcc \
  && sed -i 's/ftp:\/\/gcc.gnu.org\/pub\/gcc\/infrastructure\//http:\/\/mirror.linux-ia64.org\/gnu\/gcc\/infrastructure\//g' contrib/download_prerequisites \
  && ./contrib/download_prerequisites \
  && ./configure --disable-multilib --enable-languages=c,c++ --prefix=/usr \
  && make -j$[$(nproc)/4+1] && make install \
  && rm -rf /var/local/gcc \
  && rm -f /tmp/gcc.tar.gz

# build cmake
ARG CMAKE_VERSION=3.12.3
ARG CMAKE_DOWNLOAD_URL=https://cmake.org/files/v3.12/cmake-${CMAKE_VERSION}.tar.gz
RUN mkdir -p /tmp/cmake && curl -fsSL -o /tmp/cmake.tar.gz ${CMAKE_DOWNLOAD_URL} \
    && tar -zxf /tmp/cmake.tar.gz -C /tmp/cmake --strip-components=1 \
    && cd /tmp/cmake \
    && ./bootstrap --system-curl \
    && gmake -j$[$(nproc)/4+1] \
    && gmake install \
    && rm -rf /tmp/cmake.tar.gz \
    && rm -rf /tmp/cmake

# install jdk
COPY ./jdk.rpm ./
RUN touch  ${DEFAULT_DIR}/install_jdk.sh \
    && echo '#!/bin/bash' >> ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'rpm -Uvh jdk.rpm > /dev/null 2>&1' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'mv /usr/java/jdk* /usr/java/jdk' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'export JAVA_HOME=/usr/java/jdk' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'echo "export JAVA_HOME=/usr/java/jdk" >> /etc/environment' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'export JRE_HOME=/usr/java/jdk/jre' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'echo "export JRE_HOME=/usr/java/jdk/jre" >> /etc/environment' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'ls /usr/java/jdk > /dev/null 2>&1' >>  ${DEFAULT_DIR}/install_jdk.sh \
    && echo 'echo "export JAVA_HOME=/usr/java/jdk" >> /root/.bashrc' >> ${DEFAULT_DIR}/install_jdk.sh \
    && chmod 777 ${DEFAULT_DIR}/install_jdk.sh \
    && /bin/bash ${DEFAULT_DIR}/install_jdk.sh \
    && rm -rf *.rpm \
    && rm ${DEFAULT_DIR}/install_jdk.sh

ENV JAVA_HOME /usr/java/jdk

# install maven 3.6.0
ARG MAVEN_VERSION=3.6.0
ARG SHA=fae9c12b570c3ba18116a4e26ea524b29f7279c17cbaadc3326ca72927368924d9131d11b9e851b8dc9162228b6fdea955446be41207a5cfc61283dd8a561d2f
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

# build environment
WORKDIR ${DEFAULT_DIR}

# there is a repo which is included all of thirdparty
ENV REPOSITORY_URL=https://doris-incubating-repo.bj.bcebos.com/thirdparty

# clone lastest source code, download and build thirdparty
COPY incubator-doris ${DEFAULT_DIR}/incubator-doris
RUN cd ${DEFAULT_DIR}/incubator-doris && /bin/bash thirdparty/build-thirdparty.sh \
    && rm -rf ${DEFAULT_DIR}/incubator-doris/thirdparty/src \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty.tar.gz \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty \
    && mkdir -p ${DEFAULT_DIR}/thirdparty \
    && mv ${DEFAULT_DIR}/incubator-doris/thirdparty/installed ${DEFAULT_DIR}/thirdparty/ \
    && rm -rf ${DEFAULT_DIR}/incubator-doris

ENV DORIS_THIRDPARTY /var/local/thirdparty
WORKDIR /root
