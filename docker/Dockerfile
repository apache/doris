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

# change .bashrc
RUN echo -e "if [ ! -d "/var/local/incubator-doris/thirdparty/installed" ]; then\n\tmkdir /var/local/incubator-doris/thirdparty/installed\n\tcp -rf /var/local/thirdparty/installed/*  /var/local/incubator-doris/thirdparty/installed/\n\texport JAVA_HOME=/var/local/incubator-doris/thirdparty/installed/jdk1.8.0_131\n\tln -s /var/local/incubator-doris/thirdparty/installed/bin/thrift /usr/bin/thrift\n\tln -s /var/local/incubator-doris/thirdparty/installed/jdk1.8.0_131/bin/java /usr/bin/java \nfi" >> /root/.bashrc

ARG GCC_VERSION=7.3.0
ARG GCC_URL=https://mirrors.ustc.edu.cn/gnu/gcc/gcc-${GCC_VERSION}

# install dependencies and build gcc 
RUN yum install -y bzip2 wget gcc-c++ libstdc++-static byacc flex automake libtool binutils-devel bison ncurses-devel make mlocate unzip patch which vim-common redhat-lsb-core zip \
  && updatedb \
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
    && ./bootstrap --prefix=/usr/local \
    && make -j$[$(nproc)/4+1] \
    && make install \
    && rm -rf /tmp/cmake.tar.gz \
    && rm -rf /tmp/cmake

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

# download third party, you should download latest third party if you build the latest code  
ARG DORIS_THRIDPARTY_URL=http://doris-opensource.bj.bcebos.com/doris-thirdparty-20181102.tar.gz?authorization=bce-auth-v1/069fc2786e464e63a5f1183824ddb522/2018-11-02T09:27:57Z/-1/host/b30621ca2be77596cec9477f6cfb3608b681206d73084338d1b2f1204a3e3848

# clone source code and build third party
RUN yum install -y git \
    && git clone https://github.com/apache/incubator-doris.git \
    && mkdir -p ${DEFAULT_DIR}/incubator-doris/thirdparty/src ${DEFAULT_DIR}/doris-thirdparty \
    && curl -fsSL -o ${DEFAULT_DIR}/doris-thirdparty.tar.gz ${DORIS_THRIDPARTY_URL} \
    && tar -zxf ${DEFAULT_DIR}/doris-thirdparty.tar.gz -C ${DEFAULT_DIR}/doris-thirdparty --strip-components=1 \
    && mv ${DEFAULT_DIR}/doris-thirdparty/*  ${DEFAULT_DIR}/incubator-doris/thirdparty/src/ \
    && cd ${DEFAULT_DIR}/incubator-doris && /bin/bash thirdparty/build-thirdparty.sh \
    && rm -rf ${DEFAULT_DIR}/incubator-doris/thirdparty/src \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty.tar.gz \
    && rm -rf ${DEFAULT_DIR}/doris-thirdparty \
    && mkdir -p ${DEFAULT_DIR}/thirdparty \
    && mv ${DEFAULT_DIR}/incubator-doris/thirdparty/installed ${DEFAULT_DIR}/thirdparty/

WORKDIR ${DEFAULT_DIR}/incubator-doris

