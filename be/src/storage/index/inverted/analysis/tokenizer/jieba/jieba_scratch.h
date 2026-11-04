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
#include <vector>

#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index::jieba {

// What a cut works on besides the dictionary, all of it in proportion to the value: the caller
// keeps one from value to value, so that a steady stream of values allocates nothing, and calls
// release_oversized() between values. The dictionary is shared and read only; this is not.
struct JiebaScratch {
    // The code point of every rune of the value and its byte offset, and after the last rune the
    // size of the value: the runes [l, r] are the bytes [offsets[l], offsets[r + 1]).
    std::vector<uint32_t> runes;
    std::vector<uint32_t> offsets;
    // The maximum-probability cut of a range: from each rune, the best weight of the rest of the
    // range and the runes of the word that starts it.
    std::vector<double> dp;
    std::vector<uint32_t> best_len;
    // Viterbi over a run of runes: for each rune and status, the status of the rune before; then
    // the status of each rune.
    std::vector<int8_t> path;
    std::vector<uint8_t> status;

    void release_oversized() {
        release_oversized_scratch(runes);
        release_oversized_scratch(offsets);
        release_oversized_scratch(dp);
        release_oversized_scratch(best_len);
        release_oversized_scratch(path);
        release_oversized_scratch(status);
    }

    size_t reserved_bytes() const {
        return runes.capacity() * sizeof(uint32_t) + offsets.capacity() * sizeof(uint32_t) +
               dp.capacity() * sizeof(double) + best_len.capacity() * sizeof(uint32_t) +
               path.capacity() * sizeof(int8_t) + status.capacity() * sizeof(uint8_t);
    }
};

} // namespace doris::segment_v2::inverted_index::jieba
