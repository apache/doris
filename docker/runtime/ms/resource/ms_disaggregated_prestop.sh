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


#get from env
DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
DORIS_HOME=${DORIS_ROOT}/ms
MS_CONFFILE=${DORIS_HOME}/conf/doris_cloud.conf.conf

log_file="/opt/apache-doris/ms/log/meta_service.INFO"
kill_time=$(date  "+%Y-%m-%d %H:%M:%S")
eval echo "[ms_disaggregated_prestop.sh] ${kill_time} kubelet kill call the ms_disaggregated_prestop.sh to stop ms service." >> "$log_file"
#eval echo "[fe_prestop.sh] ${kill_time} kubelet kill call the fe_prestop.sh to stop fe service." 2>&1
eval echo "[ms_disaggregated_prestop.sh] ${kill_time} kubelet kill call the ms_disaggregated_prestop.sh to stop ms service ." >> "/proc/1/fd/1"

$DORIS_HOME/bin/stop.sh --$1

