# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ORC compile for CentOS 7
#

FROM fedora:37
LABEL maintainer="Apache ORC project <dev@orc.apache.org>"
ARG jdk=17

RUN yum check-update || true
RUN yum install -y \
  cmake3 \
  curl-devel \
  cyrus-sasl-devel \
  expat-devel \
  gcc \
  gcc-c++ \
  gettext-devel \
  git \
  libtool \
  make \
  maven \
  openssl-devel \
  tar \
  wget \
  which \
  zlib-devel && \
  if [ "${jdk}" = "11" ] ; then \
    yum install -y \
    java-11-openjdk \
    java-11-openjdk-devel \
    && \
    update-alternatives --set java  $(readlink -f /usr/lib/jvm/java-11-openjdk/bin/java) && \
    update-alternatives --set javac $(readlink -f /usr/lib/jvm/java-11-openjdk/bin/javac) \
  ; elif [ "${jdk}" = "8" ] ; then \
    yum install -y \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
  ; fi

ENV TZ=America/Los_Angeles
WORKDIR /root
VOLUME /root/.m2/repository

CMD if [ ! -d orc ]; then \
      echo "No volume provided, building from apache main."; \
      echo "Pass '-v`pwd`:/root/orc' to docker run to build local source."; \
      git clone https://github.com/apache/orc.git -b main; \
    fi && \
    mkdir build && \
    cd build && \
    cmake ../orc && \
    make package test-out
