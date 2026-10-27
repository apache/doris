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

#include "core/data_type_serde/arrow_parent_nulls.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace doris {
namespace {

std::shared_ptr<arrow::Array> int32_array(const std::vector<std::optional<int32_t>>& values) {
    arrow::Int32Builder builder;
    for (const auto& value : values) {
        EXPECT_TRUE((value.has_value() ? builder.Append(*value) : builder.AppendNull()).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::vector<bool> nulls_of(const arrow::Array& array) {
    std::vector<bool> nulls;
    for (int64_t row = 0; row < array.length(); ++row) {
        nulls.push_back(array.IsNull(row));
    }
    return nulls;
}

std::pair<int64_t, int64_t> pairs_of_two(int64_t row) {
    return {row * 2, row * 2 + 2};
}

} // namespace

TEST(ArrowParentNullsTest, ReturnsChildUnchangedWithoutNullParents) {
    const auto child = int32_array({1, 2, 3, 4});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 2, {nullptr}, {child->data()}, 0));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 2, child, pairs_of_two, &masked)
                    .ok());
    EXPECT_EQ(child.get(), masked.get());
}

// Rows [1, 2] and [3, 4] of FIXED_SIZE_LIST<INT32, 2>; the NULL row 1 owns child slots 2 and 3.
TEST(ArrowParentNullsTest, ClearsOnlyTheSlotsOfNullParents) {
    const auto child = int32_array({1, 2, 3, 4, std::nullopt, 6});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 3,
            {arrow::Buffer::FromVector(std::vector<uint8_t> {0x05})}, {child->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 3, child, pairs_of_two, &masked)
                    .ok());
    ASSERT_NE(child.get(), masked.get());
    EXPECT_EQ((std::vector<bool> {false, false, true, true, true, false}), nulls_of(*masked));
    EXPECT_EQ(3, masked->null_count());
    const auto& values = static_cast<const arrow::Int32Array&>(*masked);
    EXPECT_EQ(1, values.Value(0));
    EXPECT_EQ(6, values.Value(5));
    // The source array keeps its own validity.
    EXPECT_EQ((std::vector<bool> {false, false, false, false, true, false}), nulls_of(*child));
}

// A struct sliced at row 1 hands out fields with offset 1: the bits are cleared after it.
TEST(ArrowParentNullsTest, HonorsTheChildArrayOffset) {
    const auto field = int32_array({7, 8, 9});
    auto struct_result =
            arrow::StructArray::Make({field}, {arrow::field("v", arrow::int32())},
                                     arrow::Buffer::FromVector(std::vector<uint8_t> {0x05}), 1);
    ASSERT_TRUE(struct_result.ok()) << struct_result.status().ToString();
    const auto sliced = (*struct_result)->Slice(1, 2);
    const auto& sliced_struct = static_cast<const arrow::StructArray&>(*sliced);
    const auto child = sliced_struct.field(0);
    ASSERT_EQ(1, child->offset());
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(mask_arrow_children_hidden_by_null_parents(
                        sliced_struct, 0, 2, child,
                        [](int64_t row) { return std::pair<int64_t, int64_t>(row, row + 1); },
                        &masked)
                        .ok());
    EXPECT_EQ((std::vector<bool> {true, false}), nulls_of(*masked));
    EXPECT_EQ(9, static_cast<const arrow::Int32Array&>(*masked).Value(1));
}

} // namespace doris
