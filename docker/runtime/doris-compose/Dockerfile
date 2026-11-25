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

# Build context: the repository root. For an external release directory use
# --build-context doris-output=<directory> --build-arg ARTIFACT_SOURCE=dir.
ARG JDK_IMAGE=eclipse-temurin:17.0.21_9-jdk-resolute@sha256:c19b0e2ecdec69220c6c69e7fc7f4ab5b233ccfcce65f0b87c4411e71ca82fdc
ARG OUTPUT_PATH=./output
ARG ARTIFACT_SOURCE=context
ARG JACOCO_VERSION=0.8.16

FROM ${JDK_IMAGE} AS coverage-tools
ARG JACOCO_VERSION
ARG JACOCO_SHA512=0f5f900302fb97c097e38cc1845af3c073776f0dd32dc8ead93608cc371124c6fb4e5de4d93a83537094362b7f82bc5563dd0d22e49451f62f979166054b5ff3
COPY docker/runtime/base-image/install-runtime.sh /tmp/install-runtime.sh
RUN bash /tmp/install-runtime.sh curl unzip && \
    curl --fail --show-error --silent --location --proto '=https' --proto-redir '=https' \
        --tlsv1.2 "https://repo.maven.apache.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip" \
        -o /tmp/jacoco.zip && \
    echo "${JACOCO_SHA512}  /tmp/jacoco.zip" | sha512sum -c - && \
    mkdir /jacoco && \
    unzip -q /tmp/jacoco.zip 'lib/*' 'doc/license.html' -d /jacoco && \
    rm /tmp/jacoco.zip /tmp/install-runtime.sh

FROM ${JDK_IMAGE} AS artifacts-context
ARG OUTPUT_PATH
COPY --chmod=777 ${OUTPUT_PATH}/ /payload/

FROM ${JDK_IMAGE} AS artifacts-dir
COPY --chmod=777 --from=doris-output / /payload/

FROM artifacts-${ARTIFACT_SOURCE} AS artifacts
# Only component directories enter the final image; unrelated output files,
# example private keys and Arthas stay in this disposable stage.
RUN set -eux; \
    test -d /payload/fe; \
    test -d /payload/be; \
    find /payload -mindepth 1 -maxdepth 1 ! -name fe ! -name be ! -name ms -exec rm -rf {} +; \
    mkdir -p /payload/fdb /payload/coverage; \
    rm -rf /payload/fe/arthas /payload/fe/mysql_ssl_default_certificate; \
    # The launcher chmod is redundant with the mode above and expensive on a
    # large BE binary. Preserve the existing non-root compose behavior.
    sed -i 's/\<chmod\>/echo/g' /payload/be/bin/start_be.sh; \
    if [ -d /payload/ms/bin ]; then \
        sed -i 's/\<chmod\>/echo/g' /payload/ms/bin/start.sh; \
    fi

FROM ${JDK_IMAGE} AS runtime-base
ARG JACOCO_VERSION
ENV JACOCO_VERSION=${JACOCO_VERSION}
COPY docker/runtime/base-image/install-runtime.sh /tmp/install-runtime.sh
RUN bash /tmp/install-runtime.sh mysql-client-core python-is-python3 lsof \
        tzdata curl xz-utils jq procps util-linux gosu && \
    rm /tmp/install-runtime.sh && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

COPY --from=coverage-tools /jacoco/lib /jacoco/lib

FROM runtime-base AS runtime
COPY --from=artifacts /payload/ /opt/apache-doris/
COPY --from=coverage-tools /jacoco/doc/license.html /jacoco/LICENSE.html
