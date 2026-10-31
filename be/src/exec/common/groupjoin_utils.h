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
#include <tuple>

#include "exec/common/join_op_utils.h"

namespace doris {

// Before matching, bucket_nums contains candidate chain heads. For matched probe rows,
// replace them with the first equal build row, which identifies the aggregate group.
// A resumed row already has its representative; build_row continues the remaining chain.
template <typename Method>
size_t match_batch(Method& method, uint32_t probe_rows, uint32_t& probe_row, uint32_t& build_row,
                   uint32_t* probe_indices, uint32_t* build_indices) {
    uint32_t previous_probe = build_row != 0 ? probe_row : probe_rows;
    bool probe_visited = false;
    const auto [next_probe, next_build, matches] =
            method.hash_table->template find_batch<TJoinOp::INNER_JOIN>(
                    method.keys, method.bucket_nums.data(), static_cast<int>(probe_row), build_row,
                    static_cast<int>(probe_rows), probe_indices, probe_visited, build_indices,
                    nullptr, false, false, false);
    for (uint32_t i = 0; i < matches; ++i) {
        const auto probe = probe_indices[i];
        if (probe != previous_probe) {
            method.bucket_nums[probe] = build_indices[i];
            previous_probe = probe;
        }
    }
    probe_row = next_probe;
    build_row = next_build;
    return matches;
}

} // namespace doris
