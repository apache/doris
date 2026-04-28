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
#include <unordered_map>

#include "core/data_type/data_type.h"

namespace doris {
namespace segment_v2 {
struct ZoneMap;
}

struct ZoneMapEvalContext {
    std::unordered_map<int, const segment_v2::ZoneMap*> slot_index_to_zone_map;
    std::unordered_map<int, DataTypePtr> slot_index_to_data_type;
    mutable int64_t type_mismatch_count = 0;
};

} // namespace doris
