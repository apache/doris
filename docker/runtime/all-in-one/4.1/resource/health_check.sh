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
#
# Backs the image HEALTHCHECK. Downstream CI waits on the resulting docker
# health status instead of sleeping.

set -uo pipefail

DORIS_HOME="${DORIS_HOME:-/opt/apache-doris}"
HOST=127.0.0.1
FE_HTTP_PORT="${FE_HTTP_PORT:-8030}"
BE_HTTP_PORT="${BE_HTTP_PORT:-8040}"

# Bootstrap not finished yet: FE may answer while the backend is still being
# registered, and a test that connects then sees a cluster with no capacity.
[[ -f "${DORIS_HOME}/.ready" ]] || exit 1

# FE readiness and backend liveness in one request: HealthAction returns 503
# until FE is ready, and online_backend_num once it is.
curl -fsS --max-time 4 "http://${HOST}:${FE_HTTP_PORT}/api/health" 2>/dev/null \
    | grep -qE '"online_backend_num"[[:space:]]*:[[:space:]]*[1-9]' || exit 1

# The BE http port also serves stream load, so check it directly.
curl -fsS --max-time 4 "http://${HOST}:${BE_HTTP_PORT}/api/health" >/dev/null 2>&1 || exit 1

exit 0
