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

namespace doris::segment_v2::inverted_index {

// The capacity an analysis buffer may keep from one value to the next. Values have no length
// limit, so a buffer that grew for a long value is released once the value is done; otherwise a
// stream reused across values would hold, say, 800 MB after one 50 MB value until the segment
// ends. 64 KiB is far above what ordinary values need, so steady values allocate nothing.
constexpr size_t ANALYZER_SCRATCH_HIGH_WATER_BYTES = 64 * 1024;

// Releases `container` when its capacity is above the high water mark.
template <typename Container>
void release_oversized_scratch(Container& container) {
    if (container.capacity() * sizeof(typename Container::value_type) >
        ANALYZER_SCRATCH_HIGH_WATER_BYTES) {
        Container().swap(container);
    }
}

} // namespace doris::segment_v2::inverted_index
