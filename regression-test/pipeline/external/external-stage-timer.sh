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

if [[ -z "${teamcity_build_checkoutDir:-}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir not set"
    return 1 2>/dev/null || exit 1
fi

# shellcheck source=/dev/null
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/stage-timer.sh

# Current TeamCity external regression still uses an inline command-line script.
# Add the calls below into that script at the same anchor points:
#
# 1. After env check / variable initialization:
#    source "${teamcity_build_checkoutDir}"/regression-test/pipeline/external/external-stage-timer.sh
#    external_regression_stage_timer_init
#
# 2. Right before:
#    cd $work_path && bash deploy_cluster.sh $cluster_name
#    external_regression_stage_timer_enter_start_doris
#
# 3. Right before:
#    echo "START EXTERNAL DOCKER"
#    external_regression_stage_timer_enter_start_dependencies
#
# 4. Right before:
#    echo "RUN EXTERNAL CASE"
#    external_regression_stage_timer_enter_run_cases
#
# 5. Right before:
#    echo "COLLECT DOCKER LOGS"
#    external_regression_stage_timer_enter_cleanup
#
# stage-timer.sh installs an EXIT trap, so the summary is printed automatically
# when the whole TeamCity shell finishes, even on failure.

external_regression_stage_timer_init() {
    stage_timer_init "external regression"
    stage_timer_enter "前置准备"
}

external_regression_stage_timer_enter_start_doris() {
    stage_timer_enter "启动 Doris"
}

external_regression_stage_timer_enter_start_dependencies() {
    stage_timer_enter "启动依赖"
}

external_regression_stage_timer_enter_run_cases() {
    stage_timer_enter "执行 Case"
}

external_regression_stage_timer_enter_cleanup() {
    stage_timer_enter "收尾归档"
}
