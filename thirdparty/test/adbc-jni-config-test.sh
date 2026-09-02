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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd)"
EXTERNAL_FE_CONF="${ROOT}/regression-test/pipeline/external/conf/fe.conf"
# shellcheck disable=SC2016 # Match the literal option before fe.conf expands DORIS_HOME.
JNI_LIBRARY_OPTION='-Darrow.adbc.driver.jni.library.path=${DORIS_HOME}/lib'

# External Regression replaces the packaged FE configuration instead of merging it.
# Keep the JNI override in that replacement so ADBC cannot fall back to the jar binary.
java_opts_assignment="$(grep '^JAVA_OPTS_FOR_JDK_17=' "${EXTERNAL_FE_CONF}" || true)"
if [[ " ${java_opts_assignment} " != *" ${JNI_LIBRARY_OPTION} "* ]]; then
    echo "FAIL: ${EXTERNAL_FE_CONF} drops the packaged ADBC JNI library path." >&2
    echo "Add ${JNI_LIBRARY_OPTION} to its FE JVM options." >&2
    exit 1
fi

echo "PASS"
