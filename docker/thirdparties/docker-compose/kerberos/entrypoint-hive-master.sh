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

create_keytab() {
    local principal=$1
    local keytab=$2

    kadmin.local -r "${REALM}" -q "addprinc -randkey ${principal}"
    kadmin.local -r "${REALM}" -q "ktadd -k ${keytab} ${principal}"
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
start_service hive --service metastore -p "${HMS_PORT}"
wait_for_port "${HOST}" "${HMS_PORT}" "Hive Metastore"

touch /tmp/SUCCESS
echo "Minimal Kerberos HDFS and Hive Metastore environment is ready"
echo "DORIS_KERBEROS_READY"

wait -n "${SERVICE_PIDS[@]}"
