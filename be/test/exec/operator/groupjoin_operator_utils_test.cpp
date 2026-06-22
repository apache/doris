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

#include "exec/operator/groupjoin_operator_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <variant>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/template_helpers.hpp"
#include "exec/pipeline/dependency.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {

#define ASSERT_OK(stmt)                     \
    do {                                    \
        const auto& status = (stmt);        \
        ASSERT_TRUE(status.ok()) << status; \
    } while (false)

Status init_int32_groupjoin_state(GroupJoinSharedState* shared_state) {
    return init_hash_method<GroupJoinDataVariants>(shared_state->data_variants.get(),
                                                   {std::make_shared<DataTypeInt32>()}, true);
}

size_t count_hash_table_entries(GroupJoinSharedState* shared_state) {
    return std::visit(
            Overload {[&](std::monostate&) -> size_t { return 0; },
                      [&](auto& hash_method) -> size_t {
                          size_t count = 0;
                          hash_method.hash_table->for_each_mapped([&](auto&) { ++count; });
                          return count;
                      }},
            shared_state->data_variants->method_variant);
}

TEST(GroupJoinOperatorUtilsTest, ExtractKeyColumnsBuildsCombinedNullMap) {
    auto key0 = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 4}, {0, 1, 0, 0});
    auto key1 = ColumnHelper::create_nullable_column<DataTypeInt32>({10, 20, 30, 40}, {0, 0, 1, 0});
    std::vector<ColumnPtr> key_columns_holder {key0, key1};

    ColumnRawPtrs key_not_nullable_columns;
    ColumnUInt8::MutablePtr null_map_column;
    ASSERT_OK(groupjoin::extract_key_columns(4, key_columns_holder, key_not_nullable_columns,
                                             null_map_column));

    ASSERT_TRUE(null_map_column.get() != nullptr);
    ASSERT_EQ(key_not_nullable_columns.size(), 2);
    EXPECT_EQ(key_not_nullable_columns[0],
              &assert_cast<const ColumnNullable&>(*key0).get_nested_column());
    EXPECT_EQ(key_not_nullable_columns[1],
              &assert_cast<const ColumnNullable&>(*key1).get_nested_column());

    const auto& null_map = null_map_column->get_data();
    ASSERT_EQ(null_map.size(), 4);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 1);
    EXPECT_EQ(null_map[3], 0);
}

TEST(GroupJoinOperatorUtilsTest, AddBuildCountsWithoutNullMapUsesBatchPath) {
    GroupJoinSharedState shared_state;
    ASSERT_OK(init_int32_groupjoin_state(&shared_state));

    auto build_key = ColumnHelper::create_column<DataTypeInt32>({1, 2, 1, 3});
    ColumnRawPtrs build_key_columns {build_key.get()};
    std::vector<AggregateDataPtr> build_places(build_key->size());
    std::vector<int> aggregate_indices;
    ASSERT_OK(groupjoin::add_build_counts_by_key(&shared_state, *shared_state.arena,
                                                 build_key_columns,
                                                 static_cast<uint32_t>(build_key->size()), nullptr,
                                                 aggregate_indices, build_places.data()));

    EXPECT_EQ(count_hash_table_entries(&shared_state), 3);

    auto probe_key = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});
    ColumnRawPtrs probe_key_columns {probe_key.get()};
    std::vector<AggregateDataPtr> probe_places(probe_key->size());
    int64_t matched_rows = 0;
    uint32_t matched_probe_rows = 0;
    ASSERT_OK(groupjoin::update_probe_counts(&shared_state, *shared_state.arena, probe_key_columns,
                                             static_cast<uint32_t>(probe_key->size()), nullptr,
                                             aggregate_indices, probe_places.data(), matched_rows,
                                             matched_probe_rows));

    EXPECT_EQ(matched_probe_rows, 3);
    EXPECT_EQ(matched_rows, 4);
}

TEST(GroupJoinOperatorUtilsTest, AddBuildCountsSkipsNullKeyRowsBeforeEmplace) {
    GroupJoinSharedState shared_state;
    ASSERT_OK(init_int32_groupjoin_state(&shared_state));

    auto build_key = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 0, 1, 2, 0, 2},
                                                                         {0, 1, 0, 0, 1, 0});
    std::vector<ColumnPtr> key_columns_holder {build_key};
    ColumnRawPtrs build_key_not_nullable_columns;
    ColumnUInt8::MutablePtr null_map_column;
    ASSERT_OK(groupjoin::extract_key_columns(build_key->size(), key_columns_holder,
                                             build_key_not_nullable_columns, null_map_column));
    ASSERT_TRUE(null_map_column.get() != nullptr);

    std::vector<AggregateDataPtr> build_places(build_key->size());
    std::vector<int> aggregate_indices;
    ASSERT_OK(groupjoin::add_build_counts_by_key(
            &shared_state, *shared_state.arena, build_key_not_nullable_columns,
            static_cast<uint32_t>(build_key->size()), null_map_column->get_data().data(),
            aggregate_indices, build_places.data()));

    EXPECT_EQ(count_hash_table_entries(&shared_state), 2);
    // This side has no aggregate function in this case, so all places stay nullptr.
    // The important contract here is that NULL-key rows are skipped before emplace.
    EXPECT_EQ(build_places[1], nullptr);
    EXPECT_EQ(build_places[4], nullptr);

    auto probe_key = ColumnHelper::create_column<DataTypeInt32>({0, 1, 2});
    ColumnRawPtrs probe_key_columns {probe_key.get()};
    std::vector<AggregateDataPtr> probe_places(probe_key->size());
    int64_t matched_rows = 0;
    uint32_t matched_probe_rows = 0;
    ASSERT_OK(groupjoin::update_probe_counts(&shared_state, *shared_state.arena, probe_key_columns,
                                             static_cast<uint32_t>(probe_key->size()), nullptr,
                                             aggregate_indices, probe_places.data(), matched_rows,
                                             matched_probe_rows));

    EXPECT_EQ(matched_probe_rows, 2);
    EXPECT_EQ(matched_rows, 4);
}

} // namespace
} // namespace doris
