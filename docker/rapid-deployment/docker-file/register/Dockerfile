# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Base Images
FROM bash:latest

# add script file
ADD *.sh /root/

# set working directory
WORKDIR /root/

# Set the mirror source and basic environment
RUN /bin/sh -c echo 'http://mirrors.sjtug.sjtu.edu.cn/alpine/v3.15/main' > /etc/apk/repositories && \
    echo 'http://mirrors.sjtug.sjtu.edu.cn/alpine/v3.15/community' >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache mysql-client docker && \
    rm -rf /usr/bin/docker-proxy /usr/bin/docker-init /usr/bin/dockerd && \
    chmod 755 *.sh
