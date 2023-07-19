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

##############################################################
# See emr_tools.sh
##############################################################

# specified sevices: ali,hw,tx
export SERVICE=ali
# doris host
export HOST=127.0.0.1
# doris user
export USER=root
# doris mysql cli port
export PORT=9030

# prepare endpoint,region,ak/sk
if [[ ${SERVICE} == 'ali' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=oss-cn-beijing-internal.aliyuncs.com
    export REGION=oss-cn-beijing
elif [[ ${SERVICE} == 'hw' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=obs.cn-north-4.myhuaweicloud.com
    export REGION=cn-north-4
elif [[ ${SERVICE} == 'tx' ]]; then
    export CASE=ping
    export AK=ak
    export SK=sk
    export ENDPOINT=cos.ap-beijing.mycloud.com
    export REGION=ap-beijing
fi
