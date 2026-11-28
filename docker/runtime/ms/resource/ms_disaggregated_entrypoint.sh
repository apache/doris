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

set -eo pipefail

CONFIGMAP_PATH=${CONFIGMAP_PATH:-/etc/doris}
DORIS_HOME=${DORIS_HOME:-/opt/apache-doris}

update_conf_from_configmap()
{
    local source target file
    if [[ -d "${CONFIGMAP_PATH}" ]]; then
        for source in "${CONFIGMAP_PATH}"/*; do
            [[ -f "${source}" ]] || continue
            file=$(basename "${source}")
            target="${DORIS_HOME}/ms/conf/${file}"
            if [[ -e "${target}" || -L "${target}" ]]; then
                mv -f "${target}" "${target}.bak"
            fi
            if [[ "${file}" == doris_cloud.conf ]]; then
                # Projected ConfigMaps are read-only; startup needs a writable copy.
                cp "${source}" "${target}"
                chmod u+w "${target}"
            else
                ln -sfT "${source}" "${target}"
            fi
        done
    fi

    if [[ -n "${FDB_ENDPOINT:-}" ]]; then
        target="${DORIS_HOME}/ms/conf/doris_cloud.conf"
        # Do not accumulate endpoint assignments when the entrypoint is rerun.
        sed -i '/^[[:space:]]*fdb_cluster[[:space:]]*=/d' "${target}"
        printf '\nfdb_cluster=%s\n' "${FDB_ENDPOINT}" >> "${target}"
    fi
}

update_conf_from_configmap
"${DORIS_HOME}/doris-debug" --component ms
exec "${DORIS_HOME}/ms/bin/start.sh" --console "--conf=${DORIS_HOME}/ms/conf/doris_cloud.conf" "$@"
