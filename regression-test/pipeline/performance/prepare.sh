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

# Build Step: Command Line
: <<EOF
#!/bin/bash

set -x
pwd
rm -rf ../.old/*
set +x

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/prepare.sh" && exit 1
fi
EOF

## run.sh content ##

if ${DEBUG:-false}; then
    pull_request_num="28431"
    commit_id_from_trigger="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
    commit_id="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${commit_id_from_trigger}" ||
    -z ${commit_id:-} ||
    -z ${pull_request_num:-} ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or commit_id_from_trigger
    or commit_id or pull_request_num not set" && exit 1
fi
commit_id_from_checkout=${commit_id}

echo "#### 1. check if need run"
if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "从触发流水线 -> 流水线开始跑，这个时间段中如果有新commit，
这时候流水线 checkout 出来的 commit 就不是触发时的传过来的 commit了，
这种情况不需要跑，预期pr owner会重新触发。"
    echo -e "ERROR: PR(${pull_request_num}),
    the lastest commit id
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi

# echo "#### 2. check if tpch depending files exist"
# if ! [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/fe.conf &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/be.conf &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/deploy.sh &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/run.sh &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh &&
#     -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh &&
#     -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/load-tpch-data.sh &&
#     -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/create-tpch-tables.sh &&
#     -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh ]]; then
#     echo "ERROR: depending files missing" && exit 1
# fi
