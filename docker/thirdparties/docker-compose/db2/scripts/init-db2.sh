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
rm -f /tmp/doris_db2_initialized

# The IBM image executes /var/custom scripts after creating DBNAME. It does
# not execute SQL files mounted at /docker-entrypoint-initdb.d by itself.
su - "${DB2INSTANCE}" -c 'bash -se' <<'EOF'
. "${HOME}/sqllib/db2profile"
db2 connect to doris
schema_count=$(db2 -x "SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME='DORIS_TEST'")
if ((schema_count == 0)); then
    db2 -stvf /docker-entrypoint-initdb.d/02-create-schema.sql
    db2 -stvf /docker-entrypoint-initdb.d/03-create-table.sql
    db2 -stvf /docker-entrypoint-initdb.d/04-insert.sql
fi
rows=$(db2 -x 'SELECT COUNT(*) FROM doris_test.sample_table')
[[ "${rows}" -eq 3 ]]
db2 connect reset
EOF

touch /tmp/doris_db2_initialized
