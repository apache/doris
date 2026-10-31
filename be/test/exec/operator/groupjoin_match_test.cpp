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

#include <gtest/gtest.h>

#include "core/column/column_string.h"
#include "exec/common/groupjoin_utils.h"
#include "exec/common/hash_table/join_hash_table_utils.h"
#include "exec/common/join_utils.h"
#include "exec/operator/groupjoin_operator_utils.h"

namespace doris {
namespace {

template <typename Key>
constexpr auto numeric_type() {
    if constexpr (sizeof(Key) == 8) {
        return TYPE_BIGINT;
    } else if constexpr (sizeof(Key) == 4) {
        return TYPE_INT;
    } else if constexpr (sizeof(Key) == 2) {
        return TYPE_SMALLINT;
    } else {
        return TYPE_TINYINT;
    }
}

template <typename Method>
void check_numeric_matches(Method& method, bool long_chain) {
    using Key = typename Method::Key;
    constexpr auto type = numeric_type<Key>();
    auto build = ColumnVector<type>::create();
    if (long_chain) {
        build->get_data().resize_fill(8194, 42);
    } else {
        build->get_data() = {0, 42, 7, 42, 0, 42};
    }
    build->get_data()[0] = 0;
    const auto build_rows = static_cast<uint32_t>(build->size());
    ColumnRawPtrs build_columns {build.get()};
    std::vector<uint8_t> build_nulls(build->size(), 0);
    build_nulls[2] = 1;
    build_join_hash_table<TJoinOp::INNER_JOIN>(method, build_columns, build_rows, 3,
                                               build_nulls.data(), true, false);
    auto probe = ColumnVector<type>::create();
    probe->get_data() = {42, 7, 42, 0, 99};
    const auto probe_rows = static_cast<uint32_t>(probe->size());
    ColumnRawPtrs probe_columns {probe.get()};
    const uint8_t probe_nulls[] = {0, 0, 1, 0, 0};
    prepare_join_probe(method, probe_columns, probe_rows, probe_nulls);
    uint32_t probe_row = 0;
    uint32_t build_row = 0;
    uint32_t probe_indices[3];
    uint32_t build_indices[3];
    std::vector<std::pair<uint32_t, uint32_t>> actual;
    while (probe_row < probe_rows) {
        const auto count = groupjoin::match_batch(method, probe_rows, probe_row, build_row,
                                                  probe_indices, build_indices);
        ASSERT_LE(count, 3);
        for (size_t i = 0; i < count; ++i) {
            actual.emplace_back(probe_indices[i], build_indices[i]);
            EXPECT_EQ(method.bucket_nums[probe_indices[i]],
                      probe_indices[i] == 0 ? build->size() - 1 : 4);
        }
    }
    std::vector<std::pair<uint32_t, uint32_t>> expected;
    for (uint32_t p = 0; p < probe_rows; ++p) {
        if (probe_nulls[p]) {
            continue;
        }
        for (uint32_t b = build_rows - 1; b > 0; --b) {
            if (!build_nulls[b] && build->get_data()[b] == probe->get_data()[p]) {
                expected.emplace_back(p, b);
            }
        }
    }
    EXPECT_EQ(actual, expected);
}

} // namespace

TEST(GroupJoinMatchTest, NumericRowsNullsAndLongChainContinuation) {
    PrimaryTypeHashTableContext<UInt64> wide;
    check_numeric_matches(wide, false);
    check_numeric_matches(wide, true);
    PrimaryTypeHashTableContext<UInt32> integer;
    check_numeric_matches(integer, false);
    DirectPrimaryTypeHashTableContext<UInt8> tiny(42, 0);
    check_numeric_matches(tiny, false);
    DirectPrimaryTypeHashTableContext<UInt16> small(42, 0);
    check_numeric_matches(small, true);
}

TEST(GroupJoinMatchTest, RepresentativeIsFirstEqualRowRatherThanBucketHead) {
    PrimaryTypeHashTableContext<UInt64> method;
    Int64 collision = 2;
    while ((method.hash_table->hash(collision) & 7) != (method.hash_table->hash(1) & 7)) {
        ++collision;
    }
    auto build = ColumnInt64::create();
    build->get_data() = {0, 1, collision, 1, collision};
    build_join_hash_table<TJoinOp::INNER_JOIN>(method, {build.get()}, 5, 1, nullptr, false, false);
    auto probe = ColumnInt64::create();
    probe->get_data() = {1, collision, 1};
    prepare_join_probe(method, {probe.get()}, 3, nullptr);
    uint32_t p = 0, b = 0, pi = 0, bi = 0;
    for (const auto& expected : std::vector<std::pair<uint32_t, uint32_t>> {
                 {0, 3}, {0, 1}, {1, 4}, {1, 2}, {2, 3}, {2, 1}}) {
        ASSERT_EQ(groupjoin::match_batch(method, 3, p, b, &pi, &bi), 1);
        EXPECT_EQ(std::make_pair(pi, bi), expected);
        EXPECT_EQ(method.bucket_nums[pi], pi == 1 ? 4 : 3);
    }
    EXPECT_EQ(groupjoin::match_batch(method, 3, p, b, &pi, &bi), 0);
}

TEST(GroupJoinMatchTest, EmptyBuildAndSerializedBinaryStrings) {
    SerializedHashTableContext method;
    auto build = ColumnString::create();
    build->insert_data("", 0);
    build_join_hash_table<TJoinOp::INNER_JOIN>(method, {build.get()}, 1, 1, nullptr, false, false);
    auto probe = ColumnString::create();
    probe->insert_data("a\0b", 3);
    prepare_join_probe(method, {probe.get()}, 1, nullptr);
    uint32_t p = 0, b = 0, pi = 0, bi = 0;
    EXPECT_EQ(groupjoin::match_batch(method, 1, p, b, &pi, &bi), 0);
    build->insert_data("a\0b", 3);
    build->insert_data("a", 1);
    build->insert_data("a\0b", 3);
    build_join_hash_table<TJoinOp::INNER_JOIN>(method, {build.get()}, 4, 1, nullptr, false, false);
    prepare_join_probe(method, {probe.get()}, 1, nullptr);
    p = b = 0;
    ASSERT_EQ(groupjoin::match_batch(method, 1, p, b, &pi, &bi), 1);
    EXPECT_EQ(bi, 3);
    EXPECT_EQ(method.bucket_nums[0], 3);
    ASSERT_EQ(groupjoin::match_batch(method, 1, p, b, &pi, &bi), 1);
    EXPECT_EQ(bi, 1);
    EXPECT_EQ(method.bucket_nums[0], 3);
}

TEST(GroupJoinMatchTest, ProbeContextSharesOnlyTableAndPreservesDirectBounds) {
    JoinDataVariants build;
    build.method_variant.emplace<DirectPrimaryTypeHashTableContext<UInt64>>(9, 7);
    auto& build_method = std::get<DirectPrimaryTypeHashTableContext<UInt64>>(build.method_variant);
    auto keys = ColumnInt64::create();
    keys->get_data() = {0, 7, 9, 7};
    build_join_hash_table<TJoinOp::INNER_JOIN>(build_method, {keys.get()}, 4, 2, nullptr, false,
                                               false);
    auto probe = make_join_probe_context(build);
    auto& probe_method = std::get<DirectPrimaryTypeHashTableContext<UInt64>>(probe->method_variant);
    EXPECT_EQ(probe_method.hash_table, build_method.hash_table);
    EXPECT_EQ(probe_method._min_key, 7);
    EXPECT_EQ(probe_method._max_key, 9);
    prepare_join_probe(probe_method, {keys.get()}, 4, nullptr);
    EXPECT_NE(probe_method.bucket_nums.data(), build_method.bucket_nums.data());
    EXPECT_EQ(static_cast<const void*>(build_method.hash_table->get_build_keys()),
              static_cast<const void*>(keys->get_data().data()));
}

} // namespace doris
