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

#TODO: convert to "_"
MS_ENDPOINT=${MS_ENDPOINT}
MS_TOKEN=${MS_TOKEN:="greedisgood9999"}
DORIS_HOME=${DORIS_HOME:="/opt/apache-doris"}
CONFIGMAP_PATH=${CONFIGMAP_PATH:="/etc/doris"}
INSTANCE_ID=${INSTANCE_ID}
INSTANCE_NAME=${INSTANCE_NAME}
HEARTBEAT_PORT=9050
CLUSTER_NMAE=${CLUSTER_NAME}
#option:IP,FQDN
HOST_TYPE=${HOST_TYPE:="FQDN"}
STATEFULSET_NAME=${STATEFULSET_NAME}
POD_NAMESPACE=$POD_NAMESPACE
DEFAULT_CLUSTER_ID=${POD_NAMESPACE}"_"${STATEFULSET_NAME}
CLUSTER_ID=${CLUSTER_ID:="$DEFAULT_CLUSTER_ID"}
POD_NAME=${POD_NAME}
CLOUD_UNIQUE_ID_PRE=${CLOUD_UNIQUE_ID_PRE:="1:$INSTANCE_ID"}
CLOUD_UNIQUE_ID="$CLOUD_UNIQUE_ID_PRE:$POD_NAME"
# replace "-" with "_" in CLUSTER_ID and CLOUD_UNIQUE_ID
CLUSTER_ID=$(sed 's/-/_/g' <<<$CLUSTER_ID)

CONFIG_FILE="$DORIS_HOME/be/conf/be.conf"
MY_SELF=

DEFAULT_CLUSTER_NAME=$(awk -F $INSTANCE_NAME"-" '{print $NF}' <<<$STATEFULSET_NAME)
CLUSTER_NAME=${CLUSTER_NAME:="$DEFAULT_CLUSTER_NAME"}

#TODO: check config or not, add default
echo 'file_cache_path = [{"path":"/opt/apache-doris/be/storage","total_size":107374182400,"query_limit":107374182400}]' >> $DORIS_HOME/be/conf/be.conf

function log_stderr()
{
    echo "[`date`] $@" >& 1
}

function add_cluster_info_to_conf()
{
    echo "meta_service_endpoint=$MS_ENDPOINT" >> $DORIS_HOME/be/conf/be.conf
    echo "cloud_unique_id=$CLOUD_UNIQUE_ID" >> $DORIS_HOME/be/conf/be.conf
    echo "meta_service_use_load_balancer = false" >> $DORIS_HOME/be/conf/be.conf
    echo "enable_file_cache = true" >> $DORIS_HOME/be/conf/be.conf
}

function link_config_files()
{
    if [[ -d $CONFIGMAP_PATH ]]; then
        for file in `ls $CONFIGMAP_PATH`;
        do
            if [[ -f $DORIS_HOME/be/conf/$file ]]; then
                mv $DORIS_HOME/be/conf/$file $DORIS_HOME/be/conf/$file.bak
            fi
        done
    fi

    for file in `ls $CONFIGMAP_PATH`;
    do
        if [[ "$file" == "be.conf" ]]; then
            cp $CONFIGMAP_PATH/$file $DORIS_HOME/be/conf/$file
            add_cluster_info_to_conf
            continue
        fi

        ln -sfT $CONFIGMAP_PATH/$file $DORIS_HOME/be/conf/$file 
    done
}

function parse_config_file_with_key()
{
    local key=$1
    local value=`grep "^\s*$key\s*=" $CONFIG_FILE | sed "s|^\s*$key\s*=\s*\(.*\)\s*$|\1|g"`
}

function parse_my_self_address()
{
    local my_ip=`hostname -i | awk '{print $1}'`
    local my_fqdn=`hostname -f`
    if [[ $HOST_TYPE == "IP" ]]; then
        MY_SELF=$my_ip
    else
        MY_SELF=$my_fqdn
    fi
}

function variables_initial()
{
    parse_my_self_address
    local heartbeat_port=$(parse_config_file_with_key "heartbeat_service_port")
    if [[ "x$heartbeat_port" != "x" ]]; then
        HEARTBEAT_PORT=$heartbeat_port
    fi
}

function check_or_register_in_ms()
{
    interval=5
    start=$(date +%s)
    timeout=60
    while true;
    do
        local find_address="http://$MS_ENDPOINT/MetaService/http/get_cluster?token=$MS_TOKEN"
        local output=$(curl -s $find_address \
              -d '{"cloud_unique_id":"'$CLOUD_UNIQUE_ID'","cluster_id":"'$CLUSTER_ID'"}')
        if grep -q -w "$MY_SELF" <<< $output &>/dev/null; then
            log_stderr "[INFO] $MY_SELF have register in instance id $INSTANCE_ID cluser id $CLUSTER_ID!"
            return
        fi

        local code=$(jq -r ".code" <<< $output)
        if [[ "$code" == "NOT_FOUND" ]]; then
           # if grep -q -w "$CLUSTER_ID" <<< $output &>/dev/null; then
           #     log_stderr "[INFO] cluster id $CLUSTER_ID have exists, only register self.!"
           #     add_my_self
           # else
           log_stderr "[INFO] register cluster id $CLUSTER_ID with myself $MY_SELF into instance id $INSTANCE_ID."
           add_my_self_with_cluster
           # fi
        else
            log_stderr "[INFO] register $MY_SELF into cluster id $CLUSTER_ID!"
            add_my_self
        fi

        local now=$(date +%s)
        let "expire=start+timeout"
        if [[ $expire -le $now ]]; then
            log_stderr "[ERROR] Timeout for register myself to ms, abort!"
            exit 1
        fi
        sleep $interval
    done
}

function add_my_self()
{
    local register_address="http://$MS_ENDPOINT/MetaService/http/add_node?token=$MS_TOKEN"
    local output=$(curl -s $register_address \
              -d '{"instance_id":"'$INSTANCE_ID'",
              "cluster":{"type":"COMPUTE","cluster_id":"'$CLUSTER_ID'",
              "nodes":[{"cloud_unique_id":"'$CLOUD_UNIQUE_ID'","ip":"'$MY_SELF'","host":"'$MY_SELF'","heartbeat_port":'$HEARTBEAT_PORT'}]}}')
    local code=$(jq -r ".code" <<< $output)
    if [[ "$code" == "OK" ]]; then
        log_stderr "[INFO] my_self $MY_SELF register to ms $MS_ENDPOINT instance_id $INSTANCE_ID be cluster $CLUSTER_ID success."
    else
        log_stderr "[ERROR] my_self $MY_SELF register ms $MS_ENDPOINT instance_id $INSTANCE_ID be cluster $CLUSTER_ID failed,err=$output!"
    fi
}

function add_my_self_with_cluster()
{
    local register_address="http://$MS_ENDPOINT/MetaService/http/add_cluster?token=$MS_TOKEN"
    local output=$(curl -s $register_address \
              -d '{"instance_id":"'$INSTANCE_ID'",
              "cluster":{"type":"COMPUTE","cluster_name":"'$CLUSTER_NAME'","cluster_id":"'$CLUSTER_ID'",
              "nodes":[{"cloud_unique_id":"'$CLOUD_UNIQUE_ID'","ip":"'$MY_SELF'","host":"'$MY_SELF'","heartbeat_port":'$HEARTBEAT_PORT'}]}}')
    local code=$(jq -r ".code" <<< $output)
    if [[ "$code" == "OK" ]]; then
        log_stderr "[INFO] cluster $CLUSTER_ID contains $MY_SELF register to ms $MS_ENDPOINT instance_id $INSTANCE_ID success."
    else
        log_stderr "[ERROR] cluster $CLUSTER_ID contains $MY_SELF register to ms $MS_ENDPOINT instance_id $INSTANCE_ID failed,err=$output!"
    fi
}

add_cluster_info_to_conf
link_config_files
variables_initial
check_or_register_in_ms

$DORIS_HOME/be/bin/start_be.sh --console
