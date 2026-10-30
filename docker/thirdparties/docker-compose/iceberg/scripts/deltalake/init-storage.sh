#!/bin/sh
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

set -eu
case "${DELTALAKE_REGENERATE_FIXTURES:-0}" in
    0|1) ;;
    *) echo "DELTALAKE_REGENERATE_FIXTURES must be 0 or 1" >&2; exit 1 ;;
esac
mc alias set delta http://minio:9000 admin password
mc mb --ignore-existing delta/delta
if [ -f /delta-state/GOLDEN_READY ] && [ "${DELTALAKE_REGENERATE_FIXTURES:-0}" = 0 ]; then
    echo "Reusing retained Delta golden fixtures; skipping upload"
    exit 0
fi
rm -f /delta-state/GOLDEN_READY
mc mirror --overwrite /golden/ delta/delta/warehouse/delta_golden/
touch /delta-state/GOLDEN_READY
