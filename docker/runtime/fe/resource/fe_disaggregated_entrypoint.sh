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

DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
# if config secret for basic auth about operate node of doris, the path must be `/etc/doris/basic_auth`. This is set by operator and the key of password must be `password`.
AUTH_PATH="/etc/basic_auth"
# annotations_for_recovery_start
ANNOTATION_PATH="/etc/podinfo/annotations"
RECOVERY_KEY=""
# fe location
DORIS_HOME=${DORIS_ROOT}/fe
# participant election number of fe.
ELECT_NUMBER=${ELECT_NUMBER:=1}
# query port for mysql connection.
QUERY_PORT=${FE_QUERY_PORT:-9030}
EDIT_LOG_PORT=9010
CLUSTER_ID=${CLUSTER_ID:=10000001}

# location of fe config store.
FE_CONFFILE=$DORIS_HOME/conf/fe.conf
# represents the type for fe communication: domain or IP.
START_TYPE=
# the master node in fe cluster.
FE_MASTER=
# pod ordinal of statefulset deployed pod.
POD_INDEX=$(hostname | awk -F '-' '{print $NF}')

# probe interval: 2 seconds
PROBE_INTERVAL=2
# timeout for probe master: 60 seconds
PROBE_MASTER_POD0_TIMEOUT=30 # at most 30 attempts, no less than the times needed for an election
# no-0 ordinal pod timeout for probe master: 90 times
PROBE_MASTER_PODX_TIMEOUT=180 # at most 90 attempts
# administrator for administrate the cluster.
DB_ADMIN_USER=${USER:-"root"}

DB_ADMIN_PASSWD=$PASSWD
# myself as IP or FQDN
MYSELF=

function log_stderr()
{
  echo "[`date`] $@" >& 2
}

#parse the `$FE_CONFFILE` file, passing the key need resolve as parameter.
parse_confval_from_fe_conf()
{
    # a naive script to grep given confkey from fe conf file
    # assume conf format: ^\s*<key>\s*=\s*<value>\s*$
    local confkey=$1
    local confvalue=`grep "\<$confkey\>" $FE_CONFFILE | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    echo "$confvalue"
}

collect_env_info()
{
    # set POD_IP, POD_FQDN, EDIT_LOG_PORT, QUERY_PORT
    if [[ "x$POD_IP" == "x" ]] ; then
        POD_IP=`hostname -i | awk '{print $1}'`
    fi

    if [[ "x$POD_FQDN" == "x" ]] ; then
        POD_FQDN=`hostname -f`
    fi

    # fqdn is forced to open without using ip method(enable_fqdn_mode = true).
    # Therefore START_TYPE is true
    START_TYPE=`parse_confval_from_fe_conf "enable_fqdn_mode"`

    if [[ "x$START_TYPE" == "xtrue" ]]; then
        MYSELF=$POD_FQDN
    else
        MYSELF=$POD_IP
    fi

    # edit_log_port from conf file
    local edit_log_port=`parse_confval_from_fe_conf "edit_log_port"`
    if [[ "x$edit_log_port" != "x" ]] ; then
        EDIT_LOG_PORT=$edit_log_port
    fi

    # query_port from conf file
    local query_port=`parse_confval_from_fe_conf "query_port"`
    if [[ "x$query_port" != "x" ]] ; then
        QUERY_PORT=$query_port
    fi
}

# get all registered fe in cluster.
function show_frontends()
{
    local addr=$1
    # fist start use root and no password check. avoid use pre setted username and password.
    frontends=`timeout 15 mysql --connect-timeout 2 -h $addr -P $QUERY_PORT -uroot --batch -e 'show frontends;' 2>&1`
    log_stderr "[info] use root no password show frontends result '$frontends'"
    if echo $frontends | grep -w "1045" | grep -q -w "28000" &>/dev/null ; then
        log_stderr "[info] use username and password that configured show frontends."
        frontends=`timeout 15 mysql --connect-timeout 2 -h $addr -P $QUERY_PORT -u$DB_ADMIN_USER -p$DB_ADMIN_PASSWD --batch -e 'show frontends;' 2>&1`
    fi
   echo "$frontends"

}

# add myself in cluster for FOLLOWER.
function add_self_follower()
{
    add_result=`mysql --connect-timeout 2 -h $FE_MASTER -P $QUERY_PORT -uroot --skip-column-names --batch -e "ALTER SYSTEM ADD FOLLOWER \"$MYSELF:$EDIT_LOG_PORT\";" 2>&1`
    log_stderr "[info] use root no password to add follower result '$add_result'"
    if echo $add_result | grep -w "1045" | grep -q -w "28000" &>/dev/null ; then
        log_stderr "[info] use username and password that configured to add self as follower."
        mysql --connect-timeout 2 -h $FE_MASTER -P $QUERY_PORT -u$DB_ADMIN_USER -p$DB_ADMIN_PASSWD --skip-column-names --batch -e "ALTER SYSTEM ADD FOLLOWER \"$MYSELF:$EDIT_LOG_PORT\";"
    fi

}

# add myself in cluster for OBSERVER.
function add_self_observer()
{
    add_result=`mysql --connect-timeout 2 -h $FE_MASTER -P $QUERY_PORT -uroot --skip-column-names --batch -e "ALTER SYSTEM ADD OBSERVER \"$MYSELF:$EDIT_LOG_PORT\";" 2>&1`
    log_stderr "[info] use root no password to add self as observer result '$add_result'."
    if echo $add_result | grep -w "1045" | grep -q -w "28000" &>/dev/null ; then
        log_stderr "[info] use username and password that configed to add self as observer."
        mysql --connect-timeout 2 -h $FE_MASTER -P $QUERY_PORT -u$DB_ADMIN_USER -p$DB_ADMIN_PASSWD --skip-column-names --batch -e "ALTER SYSTEM ADD OBSERVER \"$MYSELF:$EDIT_LOG_PORT\";"
    fi

}

# `dori-meta/image` not exist start as first time.
function start_fe_no_meta()
{
    local opts="--console"
    local start=`date +%s`
    local has_member=false
    local member_list=
    if [[ "x$FE_MASTER" != "x" ]] ; then
        local start=`date +%s`
        while true
        do
            # for statefulset manage fe pods, when `ELECT_NUMBER` greater than `POD_INDEX`
            if [[ ELECT_NUMBER -gt $POD_INDEX ]]; then
                log_stderr "Add myself($MYSELF:$EDIT_LOG_PORT) to master as follower ..."
                add_self_follower
            else
                log_stderr "Add myself($MYSELF:$EDIT_LOG_PORT) to master as observer ..."
                add_self_observer
            fi
               # if successfully exit circulate register logic and start fe.
            if show_frontends $addr | grep -q -w "$MYSELF" &>/dev/null ; then
                break;
            fi

            local now=`date +%s`
            let "expire=start+30" # 30s timeout
            # if timeout for register self exit 1.
            if [[ $expire -le $now ]] ; then
                log_stderr "Timed out, abort!"
                exit 1
            fi

            log_stderr "Sleep a while and retry adding ..."
            sleep $PROBE_INTERVAL
        done
    fi
    log_stderr "first start with no meta run start_fe.sh with additional options: '$opts'"
    $DORIS_HOME/bin/start_fe.sh $opts
}

# the ordinal is 0, probe timeout as 60s, when have not meta and not `MASTER` in fe cluster, 0 start as master.
probe_master_for_pod()
{
    # possible to have no result at all, because myself is the first FE instance in the cluster
    local svc=$1
    local start=`date +%s`
    local has_member=false
    local memlist=
    while true
    do
        memlist=`show_frontends $svc`
        # find master by column `IsMaster`
        local pos=`echo "$memlist" | grep '\<IsMaster\>' | awk -F '\t' '{for(i=1;i<NF;i++) {if ($i == "IsMaster") print i}}'`
        local master=`echo "$memlist" | grep '\<FOLLOWER\>' | awk -v p="$pos" -F '\t' '{if ($p=="true") print $2}'`

        log_stderr "'IsMaster' sequence in columns is $pos, master=$master."
        if [[ "x$master" == "x" ]]; then
            log_stderr "[info] resolve the eighth column for finding master !"
            master=`echo "$memlist" | grep '\<FOLLOWER\>' | awk -F '\t' '{if ($8=="true") print $2}'`
        fi

        if [[ "x$master" == "x" ]]; then
           log_stderr "[info] resoluve the ninth column for finding master!"
           master=`echo "$memlist" | grep '\<FOLLOWER\>' | awk -F '\t' '{if ($9=="true") print $2}'`
        fi

        if [[ "x$master" != "x" ]] ; then
            # has master, done
            log_stderr "Find master: $master!"
            FE_MASTER=$master
            return 0
        fi

        # show frontens has members
        if [[ "x$memlist" != "x" && "x$pos" != "x" ]] ; then
            # has member list ever before
            has_member=true
        fi

        # no master yet, check if needs timeout and quit
        log_stderr "[info] master is not elected, has_member: $has_member, this may be first start or master changing, wait $PROBE_INTERVAL s to next probe..."
        local timeout=$PROBE_MASTER_POD0_TIMEOUT
        if  "$has_member" == true || [ "$POD_INDEX" -ne "0" ] ; then
            # set timeout to the same as PODX since there are other members
            timeout=$PROBE_MASTER_PODX_TIMEOUT
        fi

        local now=`date +%s`
        let "expire=start+timeout"
        if [[ $expire -le $now ]] ; then
            log_stderr "[info]  exit probe master for probing timeout, if it is the first pod will start as master. .."
            # empty FE_MASTER
            FE_MASTER=""
            return 0
        fi
        sleep $PROBE_INTERVAL
    done
}

# when not meta exist, fe start should probe
probe_master()
{
    # fe endpoint
    local svc=$1
    # resolve svc as array.
    local addArr=${svc//,/ }
    for addr in ${addArr[@]}
    do
        # if have master break for register or check.
        if [[ "x$FE_MASTER" != "x" ]]; then
            break
        fi

        # find master under current service and set to FE_MASTER
        probe_master_for_pod $addr
    done

    # if first pod assume first start should as master. others first start have not master exit.
    if [[ "x$FE_MASTER" == "x" ]]; then
        if [[ "$POD_INDEX" -eq 0 ]]; then
            return 0
        else
            log_stderr "the pod can't connect to pod 0, the network may be not work. please verify domain connectivity with two pods in different node and verify the pod 0 ready or not."
            exit 1
        fi
    fi
}

function check_and_modify_fqdn_config()
{
    local enable_fqdn=`parse_confval_from_fe_conf "enable_fqdn_mode"`
    log_stderr "enable_fqdn is : $enable_fqdn"
    if [[ "x$enable_fqdn" == "xfalse" ]] ; then
        return 0
    fi
    if [[ "x$enable_fqdn" != "xtrue" ]] ; then
        log_stderr "add enable_fqdn_mode = true to ${DORIS_HOME}/conf/fe.conf"
        echo "enable_fqdn_mode = true" >>${DORIS_HOME}/conf/fe.conf
    fi
}

function add_cluster_info_to_conf()
{
    echo "meta_service_endpoint=$MS_ENDPOINT" >> ${DORIS_HOME}/conf/fe.conf
    echo "cluster_id=$CLUSTER_ID" >> ${DORIS_HOME}/conf/fe.conf
    echo "deploy_mode=cloud" >> ${DORIS_HOME}/conf/fe.conf
}

function link_config_files()
{
    if [[ -d $CONFIGMAP_MOUNT_PATH ]]; then
        #backup files want to replace
        for file in `ls $CONFIGMAP_MOUNT_PATH`;
        do
            if [[ -f $DORIS_HOME/conf/$file ]]; then
                mv $DORIS_HOME/conf/$file $DORIS_HOME/conf/$file.bak
            fi
        done

        for file in `ls $CONFIGMAP_MOUNT_PATH`;
        do
            if [[ "$file" == "fe.conf" ]]; then
                cp $CONFIGMAP_MOUNT_PATH/$file $DORIS_HOME/conf/$file
                add_cluster_info_to_conf
                continue
            fi

            ln -sfT $CONFIGMAP_MOUNT_PATH/$file $DORIS_HOME/conf/$file
        done
    fi
}

# resolve password for root
resolve_password_from_secret()
{
    if [[ -f "$AUTH_PATH/password" ]]; then
        DB_ADMIN_PASSWD=`cat $AUTH_PATH/password`
    fi

    if [[ -f "$AUTH_PATH/username" ]]; then
        DB_ADMIN_USER=`cat $AUTH_PATH/username`
    fi
}

start_fe_with_meta()
{
    # the server will start in the current terminal session, and the log output and console interaction will be printed to that terminal
    local opts="--console"
    local recovery=`grep "\<selectdb.com.doris/recovery\>" $ANNOTATION_PATH | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    if [[ "x$recovery" != "x" ]]; then
        opts=${opts}" --metadata_failure_recovery"
    fi

    log_stderr "start with meta run start_fe.sh with additional options: '$opts'"
    $DORIS_HOME/bin/start_fe.sh  $opts
}

#fist start create account and grant 'NODE_PRIV'
create_account()
{
    if [[ "x$FE_MASTER" == "x" ]]; then
		return 0
	fi

    # if not set password, the account not config.
    if [[ "x$DB_ADMIN_PASSWD" == "x" ]]; then
        return 0
    fi

    users=`timeout 15 mysql --connect-timeout 2 -h $FE_MASTER -P$QUERY_PORT -uroot --skip-column-names --batch -e 'SHOW ALL GRANTS;' 2>&1`
    if echo $users | grep -w "1045" | grep -q -w "28000" &>/dev/null; then
        log_stderr "the 'root' account have set paasword! not need auto create management account."
        return 0
    fi

    if echo $users | grep -q -w "$DB_ADMIN_USER" &>/dev/null; then
       log_stderr "the $DB_ADMIN_USER have exit in doris."
       return 0
    fi

    `mysql --connect-timeout 2 -h $FE_MASTER -P$QUERY_PORT -uroot --skip-column-names --batch -e "CREATE USER '$DB_ADMIN_USER' IDENTIFIED BY '$DB_ADMIN_PASSWD';GRANT NODE_PRIV ON *.*.* TO $DB_ADMIN_USER;" 2>&1`
    log_stderr "created new account and grant NODE_PRIV!"
}

fe_addrs=$1
if [[ "x$fe_addrs" == "x" ]]; then
    echo "need fe address as parameter!"
    exit
fi

add_cluster_info_to_conf
check_and_modify_fqdn_config
link_config_files
# resolve password for root to manage nodes in doris.
resolve_password_from_secret
if [[ -f "$DORIS_HOME/doris-meta/image/ROLE" ]]; then
    log_stderr "start fe with exist meta."
    ./doris-debug --component fe
    start_fe_with_meta
else
    log_stderr "first start fe with meta not exist."
    collect_env_info
    probe_master $fe_addrs
    #create account about node management
    create_account
    start_fe_no_meta
fi
