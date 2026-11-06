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

set -euo pipefail
rm -f /delta-state/SUCCESS
cp /delta-jars/*.jar /opt/spark/jars/
config=/mnt/scripts/deltalake/spark-defaults.conf
fixtures=/mnt/scripts/create_preinstalled_scripts/deltalake
case "${DELTALAKE_REGENERATE_FIXTURES:-0}" in
    0|1) ;;
    *) echo "DELTALAKE_REGENERATE_FIXTURES must be 0 or 1" >&2; exit 1 ;;
esac
if [[ ! -f /delta-state/FIXTURES_READY || "${DELTALAKE_REGENERATE_FIXTURES:-0}" == 1 ]]; then
    rm -f /delta-state/FIXTURES_READY
    # Use one session for SQL fixtures; each Python helper owns and closes its Spark session.
    for fixture in "${fixtures}"/*.sql; do
        printf 'source %s;\n' "${fixture}"
    done > /tmp/deltalake-fixtures.sql
    /opt/spark/bin/spark-sql --properties-file "${config}" -f /tmp/deltalake-fixtures.sql
    /opt/spark/bin/spark-submit --properties-file "${config}" "${fixtures}/04-prepare-metadata.py"
    /opt/spark/bin/spark-submit --properties-file "${config}" "${fixtures}/05-parquet-pruning.py"
    touch /delta-state/FIXTURES_READY
else
    echo "Reusing retained Delta fixtures; skipping data generation"
fi
/opt/spark/sbin/start-thriftserver.sh --properties-file "${config}" \
    --hiveconf hive.server2.thrift.port=10000
for attempt in $(seq 1 120); do
    if /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000/default -n hadoop \
            -e 'SELECT 1' >/tmp/delta-thrift-ready.log 2>&1; then
        touch /delta-state/SUCCESS
        exec tail -f /dev/null
    fi
    sleep 1
done
cat /tmp/delta-thrift-ready.log >&2
exit 1
