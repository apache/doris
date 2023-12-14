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

ARG OUT_DIRECTORY=output

# set environment variables
ENV JAVA_HOME="/usr/local/openjdk-8/"
ENV jacoco_version 0.8.8

RUN  sed -i s@/deb.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list
RUN  apt-get clean


RUN apt-get update && \
    apt-get install -y default-mysql-client python lsof tzdata curl unzip && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean

RUN curl -f https://repo1.maven.org/maven2/org/jacoco/jacoco/$jacoco_version/jacoco-$jacoco_version.zip -o jacoco.zip && \
    mkdir /jacoco && \
    unzip jacoco.zip -d /jacoco

ADD $OUT_DIRECTORY /opt/apache-doris/
RUN  mkdir -p /opt/apache-doris/coverage
# in docker, run 'chmod 755 doris_be' first time cost 1min, remove it.
RUN sed -i 's/\<chmod\>/echo/g' /opt/apache-doris/be/bin/start_be.sh
