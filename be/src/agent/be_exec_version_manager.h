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

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include "common/exception.h"
#include "common/status.h"

namespace doris {

constexpr inline int BITMAP_SERDE = 3;
constexpr inline int USE_NEW_SERDE = 4;         // release on DORIS version 2.1
constexpr inline int OLD_WAL_SERDE = 3;         // use to solve compatibility issues, see pr #32299
constexpr inline int AGG_FUNCTION_NULLABLE = 5; // change some agg nullable property: PR #37215
constexpr inline int VARIANT_SERDE = 6;         // change variant serde to fix PR #38413
constexpr inline int AGGREGATION_2_1_VERSION =
        5; // some aggregation changed the data format after this version

class BeExecVersionManager {
public:
    BeExecVersionManager() = delete;

    static Status check_be_exec_version(int be_exec_version);

    static void check_agg_state_compatibility(int current_be_exec_version, int data_be_exec_version,
                                              std::string function_name);

    static int get_newest_version() { return max_be_exec_version; }

private:
    static const int max_be_exec_version;
    static const int min_be_exec_version;
};

} // namespace doris
