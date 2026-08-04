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

: "${HOST:?}"
: "${REALM:?}"
: "${KDC_PORT:?}"
: "${FS_PORT:?}"
: "${HMS_PORT:?}"
: "${HIVE_CLIENT_KEYTAB:?}"
: "${PRESTO_CLIENT_KEYTAB:?}"

export HADOOP_CONF_DIR=/opt/doris/conf
export HIVE_CONF_DIR=/opt/doris/conf
export KRB5_KDC_PROFILE=/opt/doris/conf/kdc.conf
export PATH="/usr/local/sbin:/usr/sbin:${PATH}"
export HADOOP_CLIENT_OPTS="-Xms192m -Xmx320m"

readonly HDFS_PRINCIPAL="hdfs/${HOST}@${REALM}"
readonly HTTP_PRINCIPAL="HTTP/${HOST}@${REALM}"
readonly HIVE_PRINCIPAL="hive/${HOST}@${REALM}"
readonly HIVE_CLIENT_PRINCIPAL="hive/presto-master.docker.cluster@${REALM}"
readonly PRESTO_CLIENT_PRINCIPAL="presto-server/presto-master.docker.cluster@${REALM}"
readonly PRINCIPAL_PASSWORD="doris-kerberos-test"

declare -a SERVICE_PIDS=()

stop_services() {
    kill "${SERVICE_PIDS[@]}" 2>/dev/null || true
    wait "${SERVICE_PIDS[@]}" 2>/dev/null || true
}

trap stop_services EXIT INT TERM

start_service() {
    "$@" &
    SERVICE_PIDS+=("$!")
}

wait_for_port() {
    local host=$1
    local port=$2
    local service=$3

    for _ in {1..120}; do
        if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
            exec 3>&-
            exec 3<&-
            return
        fi
        sleep 1
    done

    echo "Timed out waiting for ${service} on ${host}:${port}" >&2
    return 1
}

# Keys must stay identical across container rebuilds. Deployments that run
# Doris on separate hosts from this container provision /keytabs out of band,
# so a key that is re-randomized on every start makes every such client fail
# the AS-REP decryption with "GeneralSecurityException: Checksum failed".
# A fixed password plus the single fixed enctype in kdc.conf yields a stable
# key, and -norandkey exports that key instead of rolling a new one.
create_keytab() {
    local principal=$1
    local keytab=$2

    kadmin.local -r "${REALM}" -q "addprinc -pw ${PRINCIPAL_PASSWORD} ${principal}"
    kadmin.local -r "${REALM}" -q "ktadd -k ${keytab} -norandkey ${principal}"
}

report_stage() {
    echo "DORIS_KERBEROS_STAGE=$1"
}

report_stage "prepare-data"
mkdir -p /data/hdfs/data /data/hdfs/name /data/kdc /data/keytabs /data/metastore /keytabs
rm -rf /tmp/hadoop-server-conf
cp -R "${HADOOP_CONF_DIR}" /tmp/hadoop-server-conf
sed -i "s#hdfs://${HOST}:${FS_PORT}#hdfs://127.0.0.1:${FS_PORT}#" \
    /tmp/hadoop-server-conf/core-site.xml

report_stage "initialize-kdc"
kdb5_util create -s -r "${REALM}" -P doris-kerberos-test
create_keytab "${HDFS_PRINCIPAL}" /data/keytabs/hdfs.keytab
create_keytab "${HTTP_PRINCIPAL}" /data/keytabs/spnego.keytab
create_keytab "${HIVE_PRINCIPAL}" /data/keytabs/hive.keytab
create_keytab "${HIVE_CLIENT_PRINCIPAL}" "/keytabs/${HIVE_CLIENT_KEYTAB}"
create_keytab "${PRESTO_CLIENT_PRINCIPAL}" "/keytabs/${PRESTO_CLIENT_KEYTAB}"
chmod 644 /keytabs/*.keytab

report_stage "start-kdc"
start_service krb5kdc -n -r "${REALM}"
wait_for_port 127.0.0.1 "${KDC_PORT}" "Kerberos KDC"

report_stage "start-namenode"
export HDFS_NAMENODE_OPTS="-Xms128m -Xmx256m"
export HDFS_DATANODE_OPTS="-Xms96m -Xmx192m"
hdfs namenode -format -force -nonInteractive
start_service hdfs namenode
wait_for_port "${HOST}" "${FS_PORT}" "HDFS NameNode"

report_stage "start-datanode"
start_service env HADOOP_CONF_DIR=/tmp/hadoop-server-conf hdfs datanode
wait_for_port "${HOST}" "${DFS_DN_PORT}" "HDFS DataNode"

report_stage "wait-for-datanode-registration"
export KRB5CCNAME=FILE:/tmp/hdfs-admin.ccache
kinit -kt /data/keytabs/hdfs.keytab "${HDFS_PRINCIPAL}"
for _ in {1..120}; do
    if hdfs dfsadmin -Dfs.defaultFS="hdfs://127.0.0.1:${FS_PORT}" -report 2>/dev/null \
            | grep -q '^Live datanodes (1):'; then
        break
    fi
    sleep 1
done
hdfs dfsadmin -Dfs.defaultFS="hdfs://127.0.0.1:${FS_PORT}" -report \
    | grep -q '^Live datanodes (1):'
kdestroy

report_stage "initialize-hive-metastore"
schematool -dbType derby -initSchema
# The Paimon tables declare no columns, so the metastore itself resolves their
# schema through the storage handler, and registering ali_db at an oss:// location
# makes it resolve that scheme too. Both classes must therefore be on the
# metastore service's own classpath, not merely on the DDL client's.
if [[ -d /opt/doris/auxlib ]]; then
    export HIVE_AUX_JARS_PATH=/opt/doris/auxlib
fi
start_service hive --service metastore -p "${HMS_PORT}"
wait_for_port "${HOST}" "${HMS_PORT}" "Hive Metastore"

# Register the Paimon fixture consumed by test_paimon_hms_catalog. This runs
# before the readiness marker below on purpose: run-thirdparties-docker.sh
# releases the pipeline on DORIS_KERBEROS_READY, so anything published later
# would race the suites. A failure here aborts the container (set -e) instead of
# handing out an environment that is silently missing hdfs_db / ali_db.
#
# Both kerberos containers run this entrypoint with the same rendered env
# switch, but the fixture and its mounts (sql/, paimon_data/, auxlib/) belong to
# kerberos1 only - its metastore (9583) is the one the suite talks to. Gate on
# the container role, not on the mounts, so a broken mount on kerberos1 still
# fails loudly while kerberos2 skips deterministically.
if [[ "${enablePaimonHms:-false}" == "true" && "${HOST:-}" == "hadoop-master" ]]; then
    report_stage "load-paimon-hms"
    export KRB5CCNAME=FILE:/tmp/hive-admin.ccache
    kinit -kt /data/keytabs/hive.keytab "${HIVE_PRINCIPAL}"
    hdfs dfs -mkdir -p /user/hive/warehouse
    hdfs dfs -put -f /opt/doris/paimon_data/* /user/hive/warehouse/
    paimon_hql="$(mktemp /tmp/create_paimon_hive_table.XXXXXX.hql)"
    sed "s|__OSS_BUCKET__|${OSSBucket}|g" /opt/doris/sql/create_paimon_hive_table.hql >"${paimon_hql}"
    hive -f "${paimon_hql}"
    rm -f "${paimon_hql}"
    kdestroy
fi

touch /tmp/SUCCESS
echo "Minimal Kerberos HDFS and Hive Metastore environment is ready"
echo "DORIS_KERBEROS_READY"

wait -n "${SERVICE_PIDS[@]}"
