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

#

DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
DORIS_HOME=${DORIS_ROOT}/fe
FE_CONFFILE=$DORIS_HOME/conf/fe.conf

parse_confval_from_fe_conf()
{
    local confkey=$1
    local confvalue=`grep "^\s*$confkey" $FE_CONFFILE | grep -v "^\s\#" | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    echo $confvalue
}

log_dir=`parse_confval_from_fe_conf "LOG_DIR"`

if [[ "x$log_dir" == "x" ]]; then
    log_dir="/opt/apache-doris/fe/log"
fi

log_replace_var_dir=`eval echo "$log_dir"`
kill_time=$(date  "+%Y-%m-%d %H:%M:%S")
eval echo "[fe_disaggregated_prestop.sh] ${kill_time} kubelet kill call the fe_disaggregated_prestop.sh to stop fe service." >> "$log_replace_var_dir/fe.log"
#eval echo "[fe_disaggregated_prestop.sh] ${kill_time} kubelet kill call the fe_disaggregated_prestop.sh to stop fe service." 2>&1
eval echo "[fe_disaggregated_prestop.sh] ${kill_time} kubelet kill call the fe_disaggregated_prestop.sh to stop fe service ." >> "/proc/1/fd/1"
$DORIS_HOME/bin/stop_fe.sh --grace
