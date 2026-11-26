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

#include "exec/common/hash_table/join_hash_table.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <functional>
#include <tuple>

namespace doris {

TEST(JoinHashTableTest, NullAwareOtherConjunctSentinelResetsNullFlag) {
    JoinHashTable<uint32_t, std::hash<uint32_t>, false> table;
    table.prepare_build<TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>(2, 8, false, 0);

    const std::array<uint32_t, 2> build_keys = {0, 7};
    const std::array<uint32_t, 2> build_buckets = {
            0, table.hash(build_keys[1]) & (table.get_bucket_size() - 1)};
    table.build(build_keys.data(), build_buckets.data(), build_keys.size(), false);

    const std::array<uint32_t, 1> probe_keys = {42};
    const uint32_t probe_bucket = table.hash(probe_keys[0]) & (table.get_bucket_size() - 1);
    const std::array<uint32_t, 1> build_idx_map = {table.get_first()[probe_bucket]};
    std::array<uint32_t, 9> probe_idxs = {};
    std::array<uint32_t, 9> build_idxs = {};
    std::array<uint8_t, 9> null_flags = {};
    null_flags[0] = 1; // A previous NULL-key probe left a NULL mark at this output position.

    const auto [probe_idx, build_idx, matched_cnt, picking_null_keys] =
            table.find_null_aware_with_other_conjuncts(
                    probe_keys.data(), build_idx_map.data(), 0, 0, probe_keys.size(),
                    probe_idxs.data(), build_idxs.data(), null_flags.data(), false, nullptr);

    EXPECT_EQ(1, probe_idx);
    EXPECT_EQ(0, build_idx);
    ASSERT_EQ(1, matched_cnt);
    EXPECT_FALSE(picking_null_keys);
    EXPECT_EQ(0, probe_idxs[0]);
    EXPECT_EQ(0, build_idxs[0]);
    EXPECT_EQ(0, null_flags[0]);
}

} // namespace doris
