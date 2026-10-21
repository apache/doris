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
# Backs the image HEALTHCHECK. The entrypoint drops a ready flag once its
# role has come up; from then on the role's own endpoint has to keep
# answering, so a dead process turns the container unhealthy.

set -uo pipefail
CI_HOME="${CI_HOME:-/opt/doris-ci}"
# shellcheck source=lib.sh
source "${CI_HOME}/lib.sh"

[[ -f "${READY_FLAG}" ]] || exit 1
case "${DORIS_ROLE}" in
    all)
        fe_health 127.0.0.1 | grep -qE '"online_backend_num"[[:space:]]*:[[:space:]]*[1-9]' || exit 1
        curl -fsS --max-time 4 "http://127.0.0.1:${BE_HTTP_PORT}/api/health" >/dev/null 2>&1 || exit 1
        ;;
    fe)          fe_health 127.0.0.1 >/dev/null || exit 1 ;;
    be)          curl -fsS --max-time 4 "http://127.0.0.1:${BE_HTTP_PORT}/api/health" >/dev/null 2>&1 || exit 1 ;;
    ms|recycler) curl -fsS --max-time 4 "http://127.0.0.1:${MS_PORT}/health" >/dev/null 2>&1 || exit 1 ;;
    client)      ;;
    *)           exit 1 ;;
esac
exit 0
