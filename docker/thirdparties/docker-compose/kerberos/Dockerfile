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

FROM apache/hive:3.1.3@sha256:d102ba29ad07e93c303894896203a80b903c0001d80221f1cb9fea92dcac06e4

USER root

RUN { echo 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260831T210000Z/ bullseye main'; \
        echo 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20260831T210000Z/ bullseye-security main'; } \
        > /tmp/doris-kerberos.sources.list \
    && apt-get -o Dir::Etc::sourcelist=/tmp/doris-kerberos.sources.list \
        -o Dir::Etc::sourceparts=- \
        -o Acquire::Retries=3 \
        -o APT::Update::Error-Mode=any update \
    && DEBIAN_FRONTEND=noninteractive apt-get \
        -o Dir::Etc::sourcelist=/tmp/doris-kerberos.sources.list \
        -o Dir::Etc::sourceparts=- \
        install --no-install-recommends -y \
        krb5-admin-server \
        krb5-kdc \
        krb5-user \
    && rm -rf /var/lib/apt/lists/* /tmp/doris-kerberos.sources.list

COPY entrypoint-hive-master.sh /opt/doris/entrypoint.sh
COPY health-checks/health.sh /opt/doris/health.sh

RUN chmod 755 /opt/doris/entrypoint.sh /opt/doris/health.sh

ENTRYPOINT ["/opt/doris/entrypoint.sh"]
