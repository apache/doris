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
DORIS_HOME=${DORIS_ROOT}/be

BE_CONFIG=$DORIS_HOME/conf/be.conf

AUTH_PATH="/etc/basic_auth"

DB_ADMIN_USER=${USER:-"root"}

DB_ADMIN_PASSWD=$PASSWD

ENV_FE_ADDR=$ENV_FE_ADDR
FE_QUERY_PORT=${FE_QUERY_PORT:-9030}


HEARTBEAT_PORT=9050
MY_SELF=
MY_IP=`hostname -i`
MY_HOSTNAME=`hostname -f`

log_stderr()
{
    echo "[`date`] $@" >&2
}

resolve_password_from_secret()
{
    if [[ -f "$AUTH_PATH/password" ]]; then
        DB_ADMIN_PASSWD=`cat $AUTH_PATH/password`
    fi
    if [[ -f "$AUTH_PATH/username" ]]; then
        DB_ADMIN_USER=`cat $AUTH_PATH/username`
    fi
}

parse_confval_from_conf()
{
    # a naive script to grep given confkey from fe conf file
    # assume conf format: ^\s*<key>\s*=\s*<value>\s*$
    local confkey=$1
    local confvalue=`grep "\<$confkey\>" $BE_CONFIG | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    echo "$confvalue"
}


collect_env_info()
{
    # heartbeat_port from conf file
    local heartbeat_port=`parse_confval_from_conf "heartbeat_service_port"`
    if [[ "x$heartbeat_port" != "x" ]] ; then
        HEARTBEAT_PORT=$heartbeat_port
    fi

    if [[ "x$HOST_TYPE" == "xIP" ]] ; then
        MY_SELF=$MY_IP
    else
        MY_SELF=$MY_HOSTNAME
    fi
}

function show_frontends()
{
    local addr=$1
    frontends=`timeout 15 mysql --connect-timeout 2 -h $addr -P $FE_QUERY_PORT -uroot --batch -e 'show frontends;' 2>&1`
    log_stderr "[info] use root no password show frontends result $frontends ."
    if echo $frontends | grep -w "1045" | grep -q -w "28000" &>/dev/null; then
        log_stderr "[info] use username and passwore that configured to show frontends."
        frontends=`timeout 15 mysql --connect-timeout 2 -h $addr -P $FE_QUERY_PORT -u$DB_ADMIN_USER -p$DB_ADMIN_PASSWD --batch -e 'show frontends;'`
    fi

    echo "$frontends"
}

disable_query_and_load(){
    local svc=$1
    disable_result=`timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -uroot --skip-column-names --batch -e "ALTER SYSTEM MODIFY BACKEND \"$MY_SELF:$HEARTBEAT_PORT\" SET (\"disable_query\" = \"true\",\"disable_load\"=\"true\");" 2>&1`
    if echo $disable_result | grep -w "1045" | grep -q -w "28000" &>/dev/null ; then
        log_stderr "disable_query_and_load use pwd to run 'ALTER SYSTEM MODIFY BACKEND' sql ."
        disable_result=`timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u$DB_ADMIN_USER -p$DB_ADMIN_PASSWD --skip-column-names --batch -e "ALTER SYSTEM MODIFY BACKEND \"$MY_SELF:$HEARTBEAT_PORT\" SET (\"disable_query\" = \"true\",\"disable_load\"=\"true\");" 2>&1`
    fi
    if [[ "x$disable_result" == "x" ]] ; then
        log_stderr "disable_query_and_load success ."
    else
        log_stderr "disable_query_and_load failed: $disable_result ."
    fi
}

resolve_password_from_secret
collect_env_info
disable_query_and_load $ENV_FE_ADDR


echo "===================================end===================================="

$DORIS_HOME/bin/stop_be.sh --grace
