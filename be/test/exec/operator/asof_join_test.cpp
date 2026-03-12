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

#include <initializer_list>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/value/timestamptz_value.h"
#include "exec/common/join_utils.h"

namespace doris {
static ColumnPtr make_int32_column(const std::vector<int32_t>& values) {
    auto col = ColumnInt32::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    return col;
}

static ColumnPtr make_datev2_column(const std::vector<uint32_t>& values) {
    auto col = ColumnDateV2::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    return col;
}

template <typename IntType>
static AsofIndexGroup<IntType> make_group(
        std::initializer_list<std::pair<IntType, uint32_t>> rows) {
    AsofIndexGroup<IntType> group;
    for (const auto& [value, row_idx] : rows) {
        group.add_row(value, row_idx);
    }
    group.sort_and_finalize();
    return group;
}

template <typename IntType>
static void expect_sorted(const std::vector<IntType>& values) {
    for (size_t i = 1; i < values.size(); ++i) {
        EXPECT_LE(values[i - 1], values[i]);
    }
}

static bool contains_row(uint32_t row_idx, std::initializer_list<uint32_t> candidates) {
    for (uint32_t candidate : candidates) {
        if (candidate == row_idx) {
            return true;
        }
    }
    return false;
}

class AsofIndexGroupFinalizeTest : public ::testing::Test {};

TEST_F(AsofIndexGroupFinalizeTest, EmptyGroup) {
    AsofIndexGroup<uint32_t> group;
    group.sort_and_finalize();
    EXPECT_TRUE(group.entries.empty());
    EXPECT_TRUE(group.asof_values.empty());
    EXPECT_TRUE(group.row_indexes.empty());
}

TEST_F(AsofIndexGroupFinalizeTest, SortAndFinalizeBuildsSoA) {
    auto group = make_group<uint32_t>({{40, 1}, {10, 2}, {30, 3}, {20, 4}, {50, 5}});
    EXPECT_TRUE(group.entries.empty());
    EXPECT_EQ(group.asof_values, (std::vector<uint32_t> {10, 20, 30, 40, 50}));
    EXPECT_EQ(group.row_indexes, (std::vector<uint32_t> {2, 4, 3, 1, 5}));
    EXPECT_EQ(group.values_data(), group.asof_values.data());
}

TEST_F(AsofIndexGroupFinalizeTest, SortAndFinalizeRetainsValueRowAlignment) {
    std::vector<std::pair<uint32_t, uint32_t>> rows = {{30, 7}, {10, 2}, {30, 5}, {20, 9}, {10, 4}};
    AsofIndexGroup<uint32_t> group;
    for (const auto& [value, row_idx] : rows) {
        group.add_row(value, row_idx);
    }
    group.sort_and_finalize();

    EXPECT_TRUE(group.entries.empty());
    expect_sorted(group.asof_values);
    ASSERT_EQ(group.asof_values.size(), rows.size());
    ASSERT_EQ(group.row_indexes.size(), rows.size());
    for (size_t i = 0; i < group.row_indexes.size(); ++i) {
        bool found = false;
        for (const auto& [value, row_idx] : rows) {
            if (row_idx == group.row_indexes[i]) {
                EXPECT_EQ(value, group.asof_values[i]);
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

TEST_F(AsofIndexGroupFinalizeTest, SingleElementSkipsPdqsort) {
    auto group = make_group<uint32_t>({{42, 7}});
    EXPECT_TRUE(group.entries.empty());
    ASSERT_EQ(group.asof_values.size(), 1U);
    EXPECT_EQ(group.asof_values[0], 42U);
    EXPECT_EQ(group.row_indexes[0], 7U);
}

TEST_F(AsofIndexGroupFinalizeTest, DoubleFinalizeIsIdempotent) {
    auto group = make_group<uint32_t>({{20, 2}, {10, 1}});
    EXPECT_EQ(group.asof_values, (std::vector<uint32_t> {10, 20}));
    group.sort_and_finalize();
    EXPECT_EQ(group.asof_values, (std::vector<uint32_t> {10, 20}));
    EXPECT_EQ(group.row_indexes, (std::vector<uint32_t> {1, 2}));
}

TEST_F(AsofIndexGroupFinalizeTest, UInt64GroupsAreSupported) {
    auto group =
            make_group<uint64_t>({{30000000003ULL, 3}, {10000000001ULL, 1}, {20000000002ULL, 2}});
    EXPECT_EQ(group.asof_values,
              (std::vector<uint64_t> {10000000001ULL, 20000000002ULL, 30000000003ULL}));
    EXPECT_EQ(group.row_indexes, (std::vector<uint32_t> {1, 2, 3}));
}

class AsofIndexGroupBoundTest : public ::testing::Test {};

TEST_F(AsofIndexGroupBoundTest, EmptyGroupBoundReturnsZero) {
    AsofIndexGroup<uint32_t> group;
    group.sort_and_finalize();
    EXPECT_EQ(group.lower_bound(42), 0U);
    EXPECT_EQ(group.upper_bound(42), 0U);
}

TEST_F(AsofIndexGroupBoundTest, UInt64LowerAndUpperBound) {
    auto group = make_group<uint64_t>(
            {{10000000001ULL, 1}, {20000000002ULL, 2}, {20000000002ULL, 3}, {40000000004ULL, 4}});
    EXPECT_EQ(group.lower_bound(5000000000ULL), 0U);
    EXPECT_EQ(group.upper_bound(5000000000ULL), 0U);
    EXPECT_EQ(group.lower_bound(20000000002ULL), 1U);
    EXPECT_EQ(group.upper_bound(20000000002ULL), 3U);
    EXPECT_EQ(group.lower_bound(30000000000ULL), 3U);
    EXPECT_EQ(group.upper_bound(30000000000ULL), 3U);
    EXPECT_EQ(group.lower_bound(50000000000ULL), 4U);
    EXPECT_EQ(group.upper_bound(50000000000ULL), 4U);
}

TEST_F(AsofIndexGroupBoundTest, LowerAndUpperBoundRespectDuplicates) {
    auto group = make_group<uint32_t>({{10, 1}, {20, 2}, {20, 3}, {40, 4}});
    EXPECT_EQ(group.lower_bound(5), 0U);
    EXPECT_EQ(group.upper_bound(5), 0U);
    EXPECT_EQ(group.lower_bound(20), 1U);
    EXPECT_EQ(group.upper_bound(20), 3U);
    EXPECT_EQ(group.lower_bound(30), 3U);
    EXPECT_EQ(group.upper_bound(30), 3U);
    EXPECT_EQ(group.lower_bound(50), 4U);
    EXPECT_EQ(group.upper_bound(50), 4U);
}

class AsofIndexGroupMatchTest : public ::testing::Test {
protected:
    AsofIndexGroup<uint32_t> group;

    void SetUp() override {
        group = make_group<uint32_t>({{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}});
    }
};

TEST_F(AsofIndexGroupMatchTest, FourInequalitiesAcrossRepresentativeValues) {
    struct MatchCase {
        uint32_t probe;
        uint32_t ge;
        uint32_t gt;
        uint32_t le;
        uint32_t lt;
    };
    std::vector<MatchCase> cases = {{5, 0, 0, 1, 1},  {10, 1, 0, 1, 2}, {25, 2, 2, 3, 3},
                                    {30, 3, 2, 3, 4}, {50, 5, 4, 5, 0}, {100, 5, 5, 0, 0}};

    for (const auto& test_case : cases) {
        EXPECT_EQ((group.template find_best_match<true, false>(test_case.probe)), test_case.ge);
        EXPECT_EQ((group.template find_best_match<true, true>(test_case.probe)), test_case.gt);
        EXPECT_EQ((group.template find_best_match<false, false>(test_case.probe)), test_case.le);
        EXPECT_EQ((group.template find_best_match<false, true>(test_case.probe)), test_case.lt);
    }
}

TEST_F(AsofIndexGroupMatchTest, SingleElementGroupAcrossAllModes) {
    auto single = make_group<uint32_t>({{30, 3}});
    EXPECT_EQ((single.template find_best_match<true, false>(30)), 3U);
    EXPECT_EQ((single.template find_best_match<true, false>(40)), 3U);
    EXPECT_EQ((single.template find_best_match<true, false>(20)), 0U);
    EXPECT_EQ((single.template find_best_match<true, true>(30)), 0U);
    EXPECT_EQ((single.template find_best_match<true, true>(40)), 3U);
    EXPECT_EQ((single.template find_best_match<false, false>(30)), 3U);
    EXPECT_EQ((single.template find_best_match<false, false>(20)), 3U);
    EXPECT_EQ((single.template find_best_match<false, false>(40)), 0U);
    EXPECT_EQ((single.template find_best_match<false, true>(30)), 0U);
    EXPECT_EQ((single.template find_best_match<false, true>(20)), 3U);
    EXPECT_EQ((single.template find_best_match<false, true>(40)), 0U);
}

TEST_F(AsofIndexGroupMatchTest, AllEqualValuesAcrossAllModes) {
    auto equal_group = make_group<uint32_t>({{30, 1}, {30, 2}, {30, 3}});

    uint32_t row = equal_group.template find_best_match<true, false>(30);
    EXPECT_TRUE(contains_row(row, {1, 2, 3}));
    EXPECT_EQ((equal_group.template find_best_match<true, true>(30)), 0U);

    row = equal_group.template find_best_match<true, false>(40);
    EXPECT_TRUE(contains_row(row, {1, 2, 3}));

    row = equal_group.template find_best_match<false, false>(30);
    EXPECT_TRUE(contains_row(row, {1, 2, 3}));

    EXPECT_EQ((equal_group.template find_best_match<false, true>(30)), 0U);

    row = equal_group.template find_best_match<false, false>(20);
    EXPECT_TRUE(contains_row(row, {1, 2, 3}));
}

TEST_F(AsofIndexGroupMatchTest, LargeGroupBinarySearch) {
    AsofIndexGroup<uint32_t> large_group;
    for (uint32_t i = 1; i <= 1000; ++i) {
        large_group.add_row(i * 2, i);
    }
    large_group.sort_and_finalize();

    EXPECT_EQ((large_group.template find_best_match<true, false>(1)), 0U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(500)), 250U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(999)), 499U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(1000)), 500U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(1001)), 500U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(2000)), 1000U);
    EXPECT_EQ((large_group.template find_best_match<true, false>(2001)), 1000U);

    EXPECT_EQ((large_group.template find_best_match<false, false>(1)), 1U);
    EXPECT_EQ((large_group.template find_best_match<false, false>(999)), 500U);
    EXPECT_EQ((large_group.template find_best_match<false, false>(2001)), 0U);

    EXPECT_EQ((large_group.template find_best_match<true, true>(2)), 0U);
    EXPECT_EQ((large_group.template find_best_match<true, true>(500)), 249U);
    EXPECT_EQ((large_group.template find_best_match<true, true>(1000)), 499U);
    EXPECT_EQ((large_group.template find_best_match<true, true>(2000)), 999U);
    EXPECT_EQ((large_group.template find_best_match<true, true>(2001)), 1000U);

    EXPECT_EQ((large_group.template find_best_match<false, true>(1)), 1U);
    EXPECT_EQ((large_group.template find_best_match<false, true>(2)), 2U);
    EXPECT_EQ((large_group.template find_best_match<false, true>(999)), 500U);
    EXPECT_EQ((large_group.template find_best_match<false, true>(2000)), 0U);
    EXPECT_EQ((large_group.template find_best_match<false, true>(2001)), 0U);
}

TEST_F(AsofIndexGroupMatchTest, EmptyGroupFindBestMatchReturnsZero) {
    AsofIndexGroup<uint32_t> empty_group;
    empty_group.sort_and_finalize();
    EXPECT_EQ((empty_group.template find_best_match<true, false>(42)), 0U);
    EXPECT_EQ((empty_group.template find_best_match<true, true>(42)), 0U);
    EXPECT_EQ((empty_group.template find_best_match<false, false>(42)), 0U);
    EXPECT_EQ((empty_group.template find_best_match<false, true>(42)), 0U);
}

TEST_F(AsofIndexGroupMatchTest, UInt64FindBestMatch) {
    auto group64 = make_group<uint64_t>(
            {{10000000001ULL, 1}, {20000000002ULL, 2}, {30000000003ULL, 3}, {40000000004ULL, 4}});
    EXPECT_EQ((group64.template find_best_match<true, false>(25000000000ULL)), 2U);
    EXPECT_EQ((group64.template find_best_match<true, true>(30000000003ULL)), 2U);
    EXPECT_EQ((group64.template find_best_match<false, false>(25000000000ULL)), 3U);
    EXPECT_EQ((group64.template find_best_match<false, true>(40000000004ULL)), 0U);
}

class AsofColumnDispatchTest : public ::testing::Test {};

TEST_F(AsofColumnDispatchTest, DateV2Dispatch) {
    auto col = make_datev2_column({1, 2, 3});
    bool dispatched_to_datev2 = false;
    asof_column_dispatch(col.get(), [&](const auto* typed_col) {
        if constexpr (std::is_same_v<std::decay_t<decltype(*typed_col)>, ColumnDateV2>) {
            dispatched_to_datev2 = true;
        }
    });
    EXPECT_TRUE(dispatched_to_datev2);
}

TEST_F(AsofColumnDispatchTest, DateTimeV2Dispatch) {
    auto col = ColumnDateTimeV2::create();
    uint64_t v1 = 1, v2 = 2;
    col->insert_data(reinterpret_cast<const char*>(&v1), 0);
    col->insert_data(reinterpret_cast<const char*>(&v2), 0);
    bool dispatched_to_dtv2 = false;
    asof_column_dispatch(col.get(), [&](const auto* typed_col) {
        if constexpr (std::is_same_v<std::decay_t<decltype(*typed_col)>, ColumnDateTimeV2>) {
            dispatched_to_dtv2 = true;
        }
    });
    EXPECT_TRUE(dispatched_to_dtv2);
}

TEST_F(AsofColumnDispatchTest, TimestampTZDispatch) {
    auto col = ColumnTimeStampTz::create();
    uint64_t v1 = 1, v2 = 2;
    col->insert_data(reinterpret_cast<const char*>(&v1), 0);
    col->insert_data(reinterpret_cast<const char*>(&v2), 0);
    bool dispatched_to_tstz = false;
    asof_column_dispatch(col.get(), [&](const auto* typed_col) {
        if constexpr (std::is_same_v<std::decay_t<decltype(*typed_col)>, ColumnTimeStampTz>) {
            dispatched_to_tstz = true;
        }
    });
    EXPECT_TRUE(dispatched_to_tstz);
}

TEST_F(AsofColumnDispatchTest, FallbackDispatch) {
    auto col = make_int32_column({1, 2, 3});
    bool dispatched_to_icol = false;
    asof_column_dispatch(col.get(), [&](const auto* typed_col) {
        if constexpr (std::is_same_v<std::decay_t<decltype(*typed_col)>, IColumn>) {
            dispatched_to_icol = true;
        }
    });
    EXPECT_TRUE(dispatched_to_icol);
}

class AsofJoinHelperTest : public ::testing::Test {};

TEST_F(AsofJoinHelperTest, IsAsofJoinRuntime) {
    EXPECT_TRUE(is_asof_join(TJoinOp::ASOF_LEFT_INNER_JOIN));
    EXPECT_TRUE(is_asof_join(TJoinOp::ASOF_LEFT_OUTER_JOIN));
    EXPECT_FALSE(is_asof_join(TJoinOp::INNER_JOIN));
    EXPECT_FALSE(is_asof_join(TJoinOp::LEFT_OUTER_JOIN));
}

TEST_F(AsofJoinHelperTest, IsAsofJoinCompileTime) {
    static_assert(is_asof_join_op_v<TJoinOp::ASOF_LEFT_INNER_JOIN>);
    static_assert(is_asof_join_op_v<TJoinOp::ASOF_LEFT_OUTER_JOIN>);
    static_assert(!is_asof_join_op_v<TJoinOp::INNER_JOIN>);
    static_assert(!is_asof_join_op_v<TJoinOp::LEFT_OUTER_JOIN>);

    static_assert(!is_asof_outer_join_op_v<TJoinOp::ASOF_LEFT_INNER_JOIN>);
    static_assert(is_asof_outer_join_op_v<TJoinOp::ASOF_LEFT_OUTER_JOIN>);
    static_assert(!is_asof_outer_join_op_v<TJoinOp::INNER_JOIN>);
}

class AsofIndexVariantTest : public ::testing::Test {};

TEST_F(AsofIndexVariantTest, MonostateIsDefault) {
    AsofIndexVariant variant;
    EXPECT_TRUE(std::holds_alternative<std::monostate>(variant));
}

TEST_F(AsofIndexVariantTest, EmplaceUint32Groups) {
    AsofIndexVariant variant;
    auto& groups = variant.emplace<std::vector<AsofIndexGroup<uint32_t>>>();
    groups.emplace_back();
    groups[0].add_row(10, 1);
    groups[0].sort_and_finalize();
    EXPECT_TRUE(std::holds_alternative<std::vector<AsofIndexGroup<uint32_t>>>(variant));
    EXPECT_EQ(std::get<std::vector<AsofIndexGroup<uint32_t>>>(variant)[0].asof_values[0], 10U);
}

TEST_F(AsofIndexVariantTest, EmplaceUint64Groups) {
    AsofIndexVariant variant;
    auto& groups = variant.emplace<std::vector<AsofIndexGroup<uint64_t>>>();
    groups.emplace_back();
    groups[0].add_row(99999999999ULL, 1);
    groups[0].sort_and_finalize();
    EXPECT_TRUE(std::holds_alternative<std::vector<AsofIndexGroup<uint64_t>>>(variant));
    EXPECT_EQ(std::get<std::vector<AsofIndexGroup<uint64_t>>>(variant)[0].asof_values[0],
              99999999999ULL);
}

} // namespace doris
