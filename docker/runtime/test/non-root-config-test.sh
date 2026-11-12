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

RUNTIME_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TEST_DIR=$(mktemp -d)
trap 'chmod -R u+w "${TEST_DIR}"; rm -rf "${TEST_DIR}"' EXIT

log_stderr() { :; }
add_fqdn_config() { echo 'enable_fqdn_mode = true' >> "${DORIS_HOME}/conf/fe.conf"; }

add_cluster_info_to_conf() { echo 'deploy_mode = cloud' >> "${DORIS_HOME}/conf/fe.conf"; }

for variant in fe be fe_disaggregated be_disaggregated; do
    (
        component=${variant%%_*}
        DORIS_HOME="${TEST_DIR}/${variant}"
        CONFIGMAP_MOUNT_PATH="${TEST_DIR}/${variant}-configmap"
        mkdir -p "${DORIS_HOME}/conf" "${CONFIGMAP_MOUNT_PATH}/..data"
        printf '# projected config\n' > "${CONFIGMAP_MOUNT_PATH}/..data/${component}.conf"
        chmod 444 "${CONFIGMAP_MOUNT_PATH}/..data/${component}.conf"
        ln -s "..data/${component}.conf" "${CONFIGMAP_MOUNT_PATH}/${component}.conf"
        printf '# original config\n' > "${DORIS_HOME}/conf/${component}.conf"
        # Load only the configuration function, without executing the entrypoint.
        if [[ "${variant}" == fe_disaggregated ]]; then
            # shellcheck disable=SC1090
            source <(sed -n '/^function link_config_files()/,/^}/p' \
                "${RUNTIME_DIR}/fe/resource/fe_disaggregated_entrypoint.sh")
            link_config_files
        else
            # shellcheck disable=SC1090
            source <(sed -n '/^update_conf_from_configmap()/,/^}/p' \
                "${RUNTIME_DIR}/${component}/resource/${variant}_entrypoint.sh")
            update_conf_from_configmap
        fi
        test ! -L "${DORIS_HOME}/conf/${component}.conf"
        grep -q '^# projected config$' "${DORIS_HOME}/conf/${component}.conf"
        grep -q '^# original config$' "${DORIS_HOME}/conf/${component}.conf.bak"
        echo 'test_setting = true' >> "${DORIS_HOME}/conf/${component}.conf"
        test "$(wc -l < "${CONFIGMAP_MOUNT_PATH}/${component}.conf")" -eq 1
    )
done

echo 'Non-root projected configuration tests passed'

for variant in fe be be_disaggregated; do
    (
        component=${variant%%_*}
        DORIS_HOME="${TEST_DIR}/${variant}"
        KRB5_MOUNT_PATH="${TEST_DIR}/${variant}-kerberos"
        KRB5_CONFIG="${TEST_DIR}/${variant}-etc/krb5.conf"
        export KEYTAB_MOUNT_PATH=
        export KEYTAB_FINAL_USED_PATH=
        mkdir -p "${KRB5_MOUNT_PATH}" "$(dirname "${KRB5_CONFIG}")"
        printf '[libdefaults]\n default_realm = TEST.LOCAL\n' > "${KRB5_MOUNT_PATH}/krb5.conf"
        ln -s "${DORIS_HOME}/conf/krb5.conf" "${KRB5_CONFIG}"
        chmod 555 "$(dirname "${KRB5_CONFIG}")"
        # shellcheck disable=SC1090
        source <(sed -n '/^mount_kerberos_config()/,/^}/p' \
            "${RUNTIME_DIR}/${component}/resource/${variant}_entrypoint.sh")
        mount_kerberos_config
        mount_kerberos_config
        cmp "${KRB5_MOUNT_PATH}/krb5.conf" "${KRB5_CONFIG}"
        test -L "${DORIS_HOME}/conf/krb5.conf"
    )
done

echo 'Non-root Kerberos configuration tests passed'
