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
DORIS_HOME=${DORIS_ROOT}/fe
FE_CONFFILE=$DORIS_HOME/conf/fe.conf

log_stderr()
{
    echo "[`date`] $@" >&2
}

parse_confval_from_fe_conf()
{
    # a naive script to grep given confkey from fe conf file
    local confkey=$1

    esc_key=$(printf '%s\n' "$confkey" | sed 's/[[\.*^$()+?{|]/\\&/g')
    local confvalue=$(
        grep -v '^[[:space:]]*#' "$FE_CONFFILE" |
        grep -E "^[[:space:]]*${esc_key}[[:space:]]*=" |
        tail -n1 |
        sed -E 's/^[[:space:]]*[^=]+[[:space:]]*=[[:space:]]*//' |
        sed -E 's/[[:space:]]*#.*$//' |
        sed -E 's/^[[:space:]]+|[[:space:]]+$//g'
    )
    log_stderr "[info] read 'fe.conf' config [ $confkey: $confvalue]"
    echo "$confvalue"
}

log_dir=`parse_confval_from_fe_conf "LOG_DIR"`

if [[ "x$log_dir" == "x" ]]; then
    log_dir="/opt/apache-doris/fe/log"
fi

log_replace_var_dir=`eval echo "$log_dir"`
kill_time=$(date  "+%Y-%m-%d %H:%M:%S")
eval echo "[fe_prestop.sh] ${kill_time} kubelet kill call the fe_prestop.sh to stop fe service." >> "$log_replace_var_dir/fe.log"
#eval echo "[fe_prestop.sh] ${kill_time} kubelet kill call the fe_prestop.sh to stop fe service." 2>&1
eval echo "[fe_prestop.sh] ${kill_time} kubelet kill call the fe_prestop.sh to stop fe service ." >> "/proc/1/fd/1"
$DORIS_HOME/bin/stop_fe.sh
