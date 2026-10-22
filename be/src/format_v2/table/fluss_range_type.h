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

#include "runtime/runtime_profile.h"

namespace doris::format::fluss {

// The per-range dispatch key and its values, exactly as FE writes them. LOG, PK_FULL and PK_TAIL
// are ranges of fluss's own log; LAKE and LAKE_SUPPRESS are the sibling's lake splits, the latter
// with a log tail bound to it. Two readers read them -- the hybrid reader dispatches every range
// on this key, the union lake reader takes the two lake kinds and synthesizes a LOG range for the
// tail -- so the spellings live here rather than in each reader's anonymous namespace: a unity
// build folds sibling translation units into one, and two anonymous namespaces defining the same
// name in it are a redefinition, not two private copies.
inline constexpr const char* PROP_RANGE_TYPE = "fluss.range_type";
inline constexpr const char* RANGE_TYPE_LOG = "LOG";
inline constexpr const char* RANGE_TYPE_PK_FULL = "PK_FULL";
inline constexpr const char* RANGE_TYPE_PK_TAIL = "PK_TAIL";
inline constexpr const char* RANGE_TYPE_LAKE = "LAKE";
inline constexpr const char* RANGE_TYPE_LAKE_SUPPRESS = "LAKE_SUPPRESS";

// Both readers report into counters a profile may not have registered: a null counter is skipped,
// not dereferenced.
inline void update_counter(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
    }
}

} // namespace doris::format::fluss
