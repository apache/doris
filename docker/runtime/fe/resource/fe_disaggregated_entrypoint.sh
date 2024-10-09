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


# ms address, fe pod's address should register in it.
MS_ENDPOINT=${MS_ENDPOINT}
MS_TOKEN=${MS_TOKEN:="greedisgood9999"}
ELECT_NUMBER=${ELECT_NUMBER:=1}
FE_EDIT_PORT=${FE_EDIT_PORT:=9010}
# cloud_id is default.
CLUSTER_ID=${CLUSTER_ID:="RESERVED_CLUSTER_ID_FOR_SQL_SERVER"}
# cloud_name is default.
CLUSTER_NAME=${CLUSTER_NAME:="RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"}
#the instance id, pod's address should register in instance->cluster.
INSTANCE_ID=${INSTANCE_ID}
MY_SELF=
HOSTNAME=`hostname`
STATEFULSET_NAME=${STATEFULSET_NAME}
POD_NAME=${POD_NAME}
CLOUD_UNIQUE_ID_PRE=${CLOUD_UNIQUE_ID_PRE:="1:$INSTANCE_ID"}
CLOUD_UNIQUE_ID="$CLOUD_UNIQUE_ID_PRE:$POD_NAME"

CONFIGMAP_PATH=${CONFIGMAP_MOUNT_PATH:="/etc/doris"}
DORIS_HOME=${DORIS_HOME:="/opt/apache-doris"}
CONFIG_FILE="$DORIS_HOME/fe/conf/fe.conf"

SEQUENCE_NUMBER=$(hostname | awk -F '-' '{print $NF}')
NODE_TYPE="FE_MASTER"

if [ "$SEQUENCE_NUMBER" -ge "$ELECT_NUMBER" ]; then
    NODE_TYPE="FE_OBSERVER"
fi

# 1. add default config in config file or link config files.
# 2. assign global variables.
# 3. register myself.


function log_stderr()
{
    echo "[`date`] $@" >& 1
}

function add_cluster_info_to_conf()
{
    echo "meta_service_endpoint=$MS_ENDPOINT" >> $DORIS_HOME/fe/conf/fe.conf
    echo "cloud_unique_id=$CLOUD_UNIQUE_ID" >> $DORIS_HOME/fe/conf/fe.conf
}

function link_config_files()
{
    if [[ -d $CONFIGMAP_PATH ]]; then
        #backup files want to replace
        for file in `ls $CONFIGMAP_PATH`;
        do
            if [[ -f $DORIS_HOME/fe/conf/$file ]]; then
                mv $DORIS_HOME/fe/conf/$file $DORIS_HOME/fe/conf/$file.bak
            fi
        done

        for file in `ls $CONFIGMAP_PATH`;
        do
            if [[ "$file" == "fe.conf" ]]; then
                cp $CONFIGMAP_PATH/$file $DORIS_HOME/fe/conf/$file
                add_cluster_info_to_conf
                continue
            fi

            ln -sfT $CONFIGMAP_PATH/$file $DORIS_HOME/fe/conf/$file
        done
    fi
}

parse_config_file_with_key()
{
    local key=$1
    local value=`grep "^\s*$key\s*=" $CONFIG_FILE | sed "s|^\s*$key\s*=\s*\(.*\)\s*$|\1|g"`
    echo $value
}

# confirm the register address, if config `enable_fqdn_mode=true` use fqdn start or use ip.
function parse_my_self_address()
{
    local value=`parse_config_file_with_key "enable_fqdn_mode"`

    local my_ip=`hostname -i | awk '{print $1}'`
    local my_fqdn=`hostname -f`
    if [[ $value == "true" ]]; then
        MY_SELF=$my_fqdn
    else
        MY_SELF=$my_ip
    fi
}

function variables_inital()
{
    parse_my_self_address
    local edit_port=$(parse_config_file_with_key "edit_log_port")
    if [[ "x$edit_port" != "x" ]]; then
        FE_EDIT_PORT=${edit_port:=$FE_EDIT_PORT}
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
                  -d '{"cloud_unique_id": "'$CLOUD_UNIQUE_ID'",
                  "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"}')
        if grep -q -w $MY_SELF <<< $output &>/dev/null ; then
            log_stderr "[INFO] $MY_SELF have registerd in metaservice!"
            return
        fi

        local code=$(jq -r ".code" <<< $output)
        if [[ "$code" == "NOT_FOUND" ]]; then
        #    if grep -q -w "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER" <<< $output &>/dev/null; then
        #        log_stderr "[INFO] RESERVED_CLUSTER_NAME_FOR_SQL_SERVER fe cluster have exist, register node $MY_SELF."
        #        add_my_self
        #    else
             log_stderr "[INFO] RESERVED_CLUSTER_NAME_FOR_SQL_SERVER fe cluster not exist, register fe clsuter."
             add_my_self_with_cluster
        #    fi
        else
            log_stderr "[INFO] register myself $MY_SELF into fe cluster cloud_unique_id $CLOUD_UNIQUE_ID."
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
    local curl_cmd="curl -s $register_address -d '{\"instance_id\":\"$INSTANCE_ID\",\"cluster\":{\"type\":\"SQL\",\"cluster_name\":\"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER\",\"cluster_id\":\"RESERVED_CLUSTER_ID_FOR_SQL_SERVER\",\"nodes\":[{\"cloud_unique_id\":\"$CLOUD_UNIQUE_ID\",\"ip\":\"$MY_SELF\",\"host\":\"$MY_SELF\",\"edit_log_port\":9010,\"node_type\":\"$NODE_TYPE\"}]}}'"
    # echo "add_my_self: $curl_cmd"
    local output=$(eval "$curl_cmd")
    # echo "add_my_self response:$output"
    local code=$(jq -r ".code" <<< $output)
    if [[ "$code" == "OK" ]]; then
        log_stderr "[INFO] my_self $MY_SELF register to ms $MS_ENDPOINT instance_id $INSTANCE_ID fe cluster RESERVED_CLUSTER_NAME_FOR_SQL_SERVER success!"
    else
        log_stderr "[ERROR] my_self register ms $MS_ENDPOINT instance_id $INSTANCE_ID fe cluster failed, response $output!"
    fi
}

function add_my_self_with_cluster()
{
    local register_address="http://$MS_ENDPOINT/MetaService/http/add_cluster?token=$MS_TOKEN"
    local curl_data="{\"instance_id\":\"$INSTANCE_ID\",\"cluster\":{\"type\":\"SQL\",\"cluster_name\":\"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER\",\"cluster_id\":\"RESERVED_CLUSTER_ID_FOR_SQL_SERVER\",\"nodes\":[{\"cloud_unique_id\":\"$CLOUD_UNIQUE_ID\",\"ip\":\"$MY_SELF\",\"host\":\"$MY_SELF\",\"node_type\":\"$NODE_TYPE\",\"edit_log_port\":$FE_EDIT_PORT}]}}"
    local curl_cmd="curl -s $register_address -d '$curl_data'"
    # echo "add_my_self_with_cluster: $curl_cmd"
    local output=$(eval "$curl_cmd")
    # echo "add_my_self_with_cluster response: $output"
    code=$(jq -r ".code" <<< $output)
    if [[ "$code" == "OK" ]]; then
        log_stderr "[INFO] fe cluster contains $MY_SELF node_type $NODE_TYPE register to ms $MS_ENDPOINT instance_id $INSTANCE_ID success."
    else
        log_stderr "[ERROR] fe cluster contains $MY_SELF node_type $NODE_TYPE register to ms $MS_ENDPOINT instance_id $INSTANCE_ID faied, $output!"
    fi
}

function check_and_modify_fqdn_config()
{
    local enable_fqdn=`parse_config_file_with_key "enable_fqdn_mode"`
    log_stderr "enable_fqdn is : $enable_fqdn"
    if [[ "x$enable_fqdn" != "xtrue" ]] ; then
        log_stderr "add enable_fqdn_mode = true to $CONFIG_FILE"
        echo "enable_fqdn_mode = true" >> $CONFIG_FILE
    fi
}

add_cluster_info_to_conf
check_and_modify_fqdn_config
link_config_files
variables_inital
check_or_register_in_ms

check_or_register_in_ms
/opt/apache-doris/fe/bin/start_fe.sh --console

