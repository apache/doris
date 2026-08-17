# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/bin/bash
set -ex

ADMIN=http://localhost:6080
AUTH=admin:Ranger1234
# Bind mounted from docker-compose/ranger/cache, fetched on the host.
SERVICE_DEF=/opt/doris-ranger-artifacts/ranger-servicedef-doris.json
# Only used by the admin UI's resource lookup, never by policy evaluation, so a
# Doris that is not up yet (the usual case here) costs nothing.
DORIS_JDBC_URL="${DORIS_JDBC_URL:-jdbc:mysql://host.docker.internal:9030}"
DORIS_JDBC_USER="${DORIS_JDBC_USER:-root}"

# Polled far more often than the container healthcheck's own 30s interval on purpose: `--wait` on the
# compose stack returns as soon as that healthcheck passes, so anything slower here can hand a "ready"
# stack back to the caller with neither the Doris service definition nor its instance registered yet.
until curl -f "${ADMIN}"; do
    echo "Waiting for service to be healthy..."
    sleep 2
done

# Both steps are idempotent: this script reruns on every container restart, and
# a 400 "duplicate" out of `set -e` would take the whole stack down. The existence
# probe in front of each POST is what makes that safe, so the POSTs themselves keep
# `-f`: without it a real failure is swallowed and the stack comes up healthy with
# neither the Doris service definition nor its instance registered.
if curl -sf -u "${AUTH}" "${ADMIN}/service/plugins/definitions/name/doris" >/dev/null; then
    echo "Doris service definition already registered"
else
    curl -fsS -u "${AUTH}" -X POST \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        "${ADMIN}/service/plugins/definitions" \
        -d@"${SERVICE_DEF}"
fi

# The regression suites create policies under a service *instance* named
# `doris` -- that is the name RangerDorisAccessControllerFactory asks for.
if curl -sf -u "${AUTH}" "${ADMIN}/service/plugins/services/name/doris" >/dev/null; then
    echo "Doris service instance already exists"
else
    curl -fsS -u "${AUTH}" -X POST \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        "${ADMIN}/service/plugins/services" \
        -d "{
              \"name\": \"doris\",
              \"type\": \"doris\",
              \"description\": \"Doris regression test service\",
              \"isEnabled\": true,
              \"configs\": {
                \"username\": \"${DORIS_JDBC_USER}\",
                \"password\": \"\",
                \"jdbc.driver_class\": \"com.mysql.cj.jdbc.Driver\",
                \"jdbc.url\": \"${DORIS_JDBC_URL}\"
              }
            }"
fi
