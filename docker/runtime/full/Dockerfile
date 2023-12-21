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

FROM openjdk:8u342-jdk

ENV JAVA_HOME="/usr/local/openjdk-8/"
RUN echo "vm.max_map_count=2000000" >> /etc/sysctl.conf

RUN sed -i 's@http://deb.debian.org@http://mirrors.aliyun.com@g' /etc/apt/sources.list &&\
     apt-get clean &&\
     apt-get update &&\
     apt-get install -y default-mysql-client

ARG TARGETARCH
ARG DORIS_VERSION=2.0.3

RUN arch=$([ "${TARGETARCH:-amd64}" = "arm64" ] && echo "arm64" || echo "x64") &&\
     echo "TARGETARCH=${TARGETARCH}\narch=${arch}" > /opt/bin.info &&\
     cd /opt &&\
     wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz &&\
     tar -xf ./apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz &&\
     mv /opt/apache-doris-${DORIS_VERSION}-bin-${arch} /opt/apache-doris &&\
     rm -f /opt/apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz

ENV PATH="/opt/apache-doris/be/bin:$PATH"

ADD ./resource/init.sh /opt/apache-doris/
RUN chmod 755 /opt/apache-doris/init.sh

ENTRYPOINT ["/opt/apache-doris/init.sh"]

