#!/bin/bash
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

# choose a base image
FROM openjdk:8u342-jdk

# set environment variables
ENV JAVA_HOME="/usr/local/openjdk-8/" \
	PATH="/opt/apache-doris/fe/bin:${PATH}"

# download the software to the mirror and replace it as needed
COPY ./resource/apache-doris-x.x.x-bin-x64.tar.gz /opt/doris.tar.gz
COPY ./resource/init_fe.sh /opt/

# deploy software
RUN apt-get update && \
	apt-get install -y default-mysql-client && \
	apt-get clean && \
	mkdir /opt/apache-doris && \
	cd /opt && \
	mkdir doris && \
        tar zxf doris.tar.gz -C doris --strip-components=1 && \
        mv doris/fe /opt/apache-doris && \
        rm doris.tar.gz && \
        rm -rf doris && \
        mv init_fe.sh /opt/apache-doris/fe/bin && \
        chmod 755 /opt/apache-doris/fe/bin/init_fe.sh

ENTRYPOINT ["/opt/apache-doris/fe/bin/init_fe.sh"]
