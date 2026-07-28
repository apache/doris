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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace doris::snii::query::internal {

inline bool build_position_offsets(size_t count, std::vector<uint32_t>* out) {
    if (count >= std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    out->clear();
    out->reserve(count);
    uint32_t offset = 0;
    while (out->size() < count) {
        out->push_back(offset);
        ++offset;
    }
    return true;
}

inline bool add_position_offset(uint32_t start, uint32_t offset, uint32_t* out) {
    if (start > std::numeric_limits<uint32_t>::max() - offset) return false;
    *out = start + offset;
    return true;
}

} // namespace doris::snii::query::internal
