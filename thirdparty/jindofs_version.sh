#!/usr/bin/env bash
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

# Shared by the third-party downloader and standalone FE/BE packaging.
JINDOFS_VERSION=6.10.9
JINDOFS_MAVEN_REPOSITORY=https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/mvn-repo/com/aliyun/jindodata

JINDOFS_NAME="jindo-core-${JINDOFS_VERSION}.jar"
JINDOFS_DOWNLOAD="${JINDOFS_MAVEN_REPOSITORY}/jindo-core/${JINDOFS_VERSION}/${JINDOFS_NAME}"
JINDOFS_SOURCE=
JINDOFS_MD5SUM=d91160ffe01904618c010910bd0b0ba8

JINDOFS_SDK_NAME="jindo-sdk-${JINDOFS_VERSION}.jar"
JINDOFS_SDK_DOWNLOAD="${JINDOFS_MAVEN_REPOSITORY}/jindo-sdk/${JINDOFS_VERSION}/${JINDOFS_SDK_NAME}"
JINDOFS_SDK_SOURCE=
JINDOFS_SDK_MD5SUM=f5eb7b17ba53c0e33ab0a5fd2aaf581b

JINDOFS_UBUNTU22_NAME="jindo-core-linux-ubuntu22-x86_64-${JINDOFS_VERSION}.jar"
JINDOFS_UBUNTU22_DOWNLOAD="${JINDOFS_MAVEN_REPOSITORY}/jindo-core-linux-ubuntu22-x86_64/${JINDOFS_VERSION}/${JINDOFS_UBUNTU22_NAME}"
JINDOFS_UBUNTU22_SOURCE=
JINDOFS_UBUNTU22_MD5SUM=0441e3821a17695d46c9d3a9e70db988

JINDOFS_AARCH64_NAME="jindo-core-linux-el7-aarch64-${JINDOFS_VERSION}.jar"
JINDOFS_AARCH64_DOWNLOAD="${JINDOFS_MAVEN_REPOSITORY}/jindo-core-linux-el7-aarch64/${JINDOFS_VERSION}/${JINDOFS_AARCH64_NAME}"
JINDOFS_AARCH64_SOURCE=
JINDOFS_AARCH64_MD5SUM=ce59a7195512230f78d663aadea1a76c
