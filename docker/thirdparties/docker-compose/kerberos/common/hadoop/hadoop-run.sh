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

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

HADOOP_INIT_D=${HADOOP_INIT_D:-/etc/hadoop-init.d/}

echo "Applying hadoop init.d scripts from ${HADOOP_INIT_D}"
if test -d "${HADOOP_INIT_D}"; then
    for init_script in "${HADOOP_INIT_D}"*; do
        chmod a+x "${init_script}"
        "${init_script}"
    done
fi

trap exit INT

echo "Running services with supervisord"

supervisord -c /etc/supervisord.conf
