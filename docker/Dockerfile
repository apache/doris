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

FROM centos:7

# install gcc-10 repo
RUN echo $'\
[ol7_software_collections]\n\
name=Software Collection packages for Oracle Linux 7 ($basearch)\n\
baseurl=http://yum.oracle.com/repo/OracleLinux/OL7/SoftwareCollections/$basearch/\n\
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-oracle\n\
gpgcheck=1\n\
enabled=1' >> /etc/yum.repos.d/CentOS-SCLo-scl.ol.repo

# install dependencies
RUN yum makecache && yum -y update && yum -y groupinstall 'Development Tools' && \
    yum install -y byacc automake java-11-openjdk-devel libtool bison binutils-devel zip unzip \
    ncurses-devel curl git wget python2 glibc-static libstdc++-static which psl libpsl-devel

# install gcc10
RUN wget http://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle && \
    rpm --import  /etc/pki/rpm-gpg/RPM-GPG-KEY-* && \
    yum install -y devtoolset-10-gcc* 

ENV JAVA_HOME="/usr/lib/jvm/java-11"

# build cmake
ARG CMAKE_VERSION=3.19.4
ARG CMAKE_BASE_URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}
RUN wget ${CMAKE_BASE_URL}/cmake-${CMAKE_VERSION}-Linux-x86_64.sh -q -O /tmp/cmake-install.sh && \
    chmod u+x /tmp/cmake-install.sh && \
    /tmp/cmake-install.sh --skip-license --prefix=/usr --exclude-subdir && \
    rm /tmp/cmake-install.sh
      
# build  ninja
ARG NINJA_VER=1.10.2
ARG NINJA_BASE_URL=https://github.com/ninja-build/ninja/releases/download/v${NINJA_VER}
RUN wget -q ${NINJA_BASE_URL}/ninja-linux.zip -O /tmp/ninja-linux.zip && \
    unzip /tmp/ninja-linux.zip -d /usr/bin/ && \
    rm /tmp/ninja-linux.zip

# install maven 3.6.3
ARG MAVEN_VERSION=3.6.3
ARG MAVEN_BASE_URL=https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries
RUN mkdir -p /usr/share/maven /usr/share/maven/ref && \
    curl -fsSL -o /tmp/apache-maven.tar.gz ${MAVEN_BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 && \
    rm -f /tmp/apache-maven.tar.gz && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

# build flex
ARG FLEX_VERSION=2.6.4
RUN wget https://github.com/westes/flex/releases/download/v$FLEX_VERSION/flex-$FLEX_VERSION.tar.gz \
    -q -O /tmp/flex-$FLEX_VERSION.tar.gz \
    && cd /tmp/ \
    && tar -xf flex-$FLEX_VERSION.tar.gz \
    && cd flex-$FLEX_VERSION \
    && ./configure --enable-shared=NO \
    && make \
    && make install \
    && rm /tmp/flex-$FLEX_VERSION.tar.gz \
    && rm -rf /tmp/flex-$FLEX_VERSION

ENV BASH_ENV=/opt/rh/devtoolset-10/enable  \
    ENV=/opt/rh/devtoolset-10/enable  \
    PROMPT_COMMAND=". /opt/rh/devtoolset-10/enable"

# there is a repo which is included all of thirdparty
ENV REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
ENV DEFAULT_DIR /var/local

# clone lastest source code, download and build third party
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
