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

#include "gen_cpp/PlanNodes_types.h"

namespace doris {

inline constexpr int32_t ICEBERG_SCAN_SEMANTICS_VERSION_1 = 1;

inline bool supports_iceberg_scan_semantics_v1(const TFileScanRangeParams* params) {
    // Old FE plans can carry IDs and encoded defaults too, so only this explicit version marker
    // may opt a new BE into result-changing semantics during a rolling upgrade.
    return params != nullptr && params->__isset.iceberg_scan_semantics_version &&
           params->iceberg_scan_semantics_version >= ICEBERG_SCAN_SEMANTICS_VERSION_1;
}

} // namespace doris
