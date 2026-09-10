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

#include <cstdint>
#include <optional>
#include <string>

#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::parquet {

inline constexpr int32_t PARQUET_TIMESTAMP_SEMANTICS_VERSION_1 = 1;

inline std::optional<std::string> get_int96_timezone_override(const TFileScanRangeParams* params) {
    // Only the marker can distinguish a new FE's explicit wall-clock semantics from an old FE
    // plan whose missing timezone field must keep legacy session-timezone decoding.
    if (params == nullptr || !params->__isset.parquet_timestamp_semantics_version ||
        params->parquet_timestamp_semantics_version < PARQUET_TIMESTAMP_SEMANTICS_VERSION_1) {
        return std::nullopt;
    }
    return params->__isset.hive_parquet_time_zone ? params->hive_parquet_time_zone : std::string {};
}

} // namespace doris::format::parquet
