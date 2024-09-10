// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "agent/be_exec_version_manager.h"

namespace doris {

const std::map<int, const std::set<std::string>> AGGREGATION_CHANGE_MAP = {
        {AGGREGATION_2_1_VERSION,
         {"window_funnel", "stddev_samp", "variance_samp", "percentile_approx_weighted",
          "percentile_approx", "covar_samp", "percentile", "percentile_array"}}};

Status BeExecVersionManager::check_be_exec_version(int be_exec_version) {
    if (be_exec_version > max_be_exec_version || be_exec_version < min_be_exec_version) {
        return Status::InternalError(
                "Received be_exec_version is not supported, be_exec_version={}, "
                "min_be_exec_version={}, max_be_exec_version={}, maybe due to FE version not "
                "match with BE.",
                be_exec_version, min_be_exec_version, max_be_exec_version);
    }
    return Status::OK();
}

void BeExecVersionManager::check_agg_state_compatibility(int current_be_exec_version,
                                                         int data_be_exec_version,
                                                         std::string function_name) {
    if (current_be_exec_version > AGGREGATION_2_1_VERSION &&
        data_be_exec_version <= AGGREGATION_2_1_VERSION &&
        AGGREGATION_CHANGE_MAP.find(AGGREGATION_2_1_VERSION)->second.contains(function_name)) {
        throw Exception(Status::InternalError(
                "agg state data with {} is not supported, "
                "current_be_exec_version={}, data_be_exec_version={}, need to rebuild the data "
                "or set the be_exec_version={} in fe.conf",
                function_name, current_be_exec_version, data_be_exec_version,
                AGGREGATION_2_1_VERSION));
    }
}

/**
 * When we have some breaking change for execute engine, we should update be_exec_version.
 * NOTICE: The change could only be dont in X.Y.0 version. and if you introduced new version number N,
 *  remember remove version N-1's all REUSEABLE changes in master branch only. REUSEABLE means scalar or agg functions' replacement.
 *  If not, the old replacement will happens in the new version which is wrong.
 *
 * 0: not contain be_exec_version.
 * 1: start from doris 1.2.0
 *    a. remove ColumnString terminating zero.
 *    b. runtime filter use new hash method.
 * 2: start from doris 2.0.0
 *    a. function month/day/hour/minute/second's return type is changed to smaller type.
 *    b. in order to solve agg of sum/count is not compatibility during the upgrade process
 *    c. change the string hash method in runtime filter
 *    d. elt function return type change to nullable(string)
 *    e. add repeat_max_num in repeat function
 * 3: start from doris 2.0.0 (by some mistakes)
 *    a. aggregation function do not serialize bitmap to string.
 *    b. support window funnel mode.
 * 4/5: start from doris 2.1.0
 *    a. ignore this line, window funnel mode should be enabled from 2.0.
 *    b. array contains/position/countequal function return nullable in less situations.
 *    c. cleared old version of Version 2.
 *    d. unix_timestamp function support timestamp with float for datetimev2, and change nullable mode.
 *    e. change shuffle serialize/deserialize way 
 *    f. shrink some function's nullable mode.
 *    g. do local merge of remote runtime filter
 *    h. "now": ALWAYS_NOT_NULLABLE -> DEPEND_ON_ARGUMENTS
 *
 * 7: start from doris 3.0.0
 *    a. change the impl of percentile (need fix)
 *    b. clear old version of version 3->4
 *    c. change FunctionIsIPAddressInRange from AlwaysNotNullable to DependOnArguments
 *    d. change some agg function nullable property: PR #37215
 *    e. change variant serde to fix PR #38413
 */
const int BeExecVersionManager::max_be_exec_version = 7;
const int BeExecVersionManager::min_be_exec_version = 0;

} // namespace doris
