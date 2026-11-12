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

namespace doris {

inline constexpr uint64_t VERSION_METRIC_COMPONENT_BASE = 100;

inline constexpr bool is_valid_version_metric_component(int64_t component) {
    return component >= 0 && component < static_cast<int64_t>(VERSION_METRIC_COMPONENT_BASE);
}

// Treat each component as one base-100 digit so numeric ordering matches version ordering.
// For example, 26.0.1.0 is encoded as 26000100 and 26.0.1.1 as 26000101.
// Callers must validate every component with is_valid_version_metric_component first.
inline constexpr uint64_t encode_version_metric_value(int64_t major, int64_t minor, int64_t patch,
                                                      int64_t hotfix) {
    uint64_t encoded = static_cast<uint64_t>(major);
    encoded = encoded * VERSION_METRIC_COMPONENT_BASE + static_cast<uint64_t>(minor);
    encoded = encoded * VERSION_METRIC_COMPONENT_BASE + static_cast<uint64_t>(patch);
    return encoded * VERSION_METRIC_COMPONENT_BASE + static_cast<uint64_t>(hotfix);
}

} // namespace doris
