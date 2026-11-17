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
#include <memory>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/function_utils.h"

namespace doris {
namespace {

ColumnPtr nullable_column(ColumnPtr data, std::initializer_list<UInt8> nulls) {
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(nulls.begin(), nulls.end());
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

ColumnPtr array_column(ColumnPtr data, std::initializer_list<UInt64> offsets) {
    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->get_data().assign(offsets.begin(), offsets.end());
    return ColumnArray::create(std::move(data), std::move(offsets_column));
}

ColumnPtr int_arrays(std::initializer_list<Int32> values, std::initializer_list<UInt8> nulls,
                     std::initializer_list<UInt64> offsets) {
    auto data = ColumnInt32::create();
    data->get_data().assign(values.begin(), values.end());
    return array_column(nullable_column(std::move(data), nulls), offsets);
}

DataTypePtr array_type(size_t depth) {
    DataTypePtr type = std::make_shared<DataTypeInt32>();
    for (size_t i = 0; i < depth; ++i) {
        type = std::make_shared<DataTypeArray>(make_nullable(type));
    }
    return type;
}

void expect_flatten(ColumnPtr source, DataTypePtr source_type, ColumnPtr expected) {
    Block block;
    const auto rows = source->size();
    auto result_type = array_type(1);
    if (source_type->is_nullable()) {
        result_type = make_nullable(result_type);
    }
    block.insert({std::move(source), source_type, "source"});
    auto function = SimpleFunctionFactory::instance().get_function(
            "array_flatten", block.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);

    FunctionUtils fn_utils(result_type, {source_type}, false);
    auto* context = fn_utils.get_fn_ctx();
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    block.insert({nullptr, result_type, "result"});
    auto status = function->execute(context, block, {0}, 1, rows);
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(status.ok()) << status;

    auto result = block.get_by_position(1).column->convert_to_full_column_if_const();
    ASSERT_EQ(result->size(), expected->size());
    for (size_t i = 0; i < expected->size(); ++i) {
        EXPECT_EQ(result->compare_at(i, i, *expected, 1), 0) << "row " << i;
    }
    const auto& result_array = assert_cast<const ColumnArray&>(*remove_nullable(result));
    EXPECT_EQ(result_array.get_offsets().back(), result_array.get_data().size());
}

} // namespace

TEST(FunctionArrayFlattenTest, SkipsHiddenSubarrayElementsAcrossRows) {
    // [NULL, [9]], [[201], [9]]; the first NULL still owns [101, 102].
    auto children = int_arrays({101, 102, 9, 201, 9}, {0, 0, 0, 0, 0}, {2, 3, 4, 5});
    auto source = array_column(nullable_column(std::move(children), {1, 0, 0, 0}), {2, 4});
    expect_flatten(std::move(source), array_type(2), int_arrays({9, 201, 9}, {0, 0, 0}, {1, 3}));
}

TEST(FunctionArrayFlattenTest, SkipsNullParentsAndNullDescendants) {
    // [NULL, [NULL, [9]]]; NULL parents and descendants both hide nonempty data.
    auto children = int_arrays({101, 102, 9}, {0, 0, 0}, {1, 2, 3});
    auto parents = array_column(nullable_column(std::move(children), {0, 1, 0}), {1, 3});
    auto source = array_column(nullable_column(std::move(parents), {1, 0}), {2});
    expect_flatten(std::move(source), array_type(3), int_arrays({9}, {0}, {1}));
}

TEST(FunctionArrayFlattenTest, HandlesEmptyRowsAndAllNullChildren) {
    // [], [NULL, NULL], [[], [NULL, 9]], [] with hidden payload in both NULL arrays.
    auto children = int_arrays({101, 102, 0, 9}, {0, 0, 1, 0}, {1, 2, 2, 4});
    auto source = array_column(nullable_column(std::move(children), {1, 1, 0, 0}), {0, 2, 4, 4});
    expect_flatten(std::move(source), array_type(2), int_arrays({0, 9}, {1, 0}, {0, 0, 2, 2}));
}

TEST(FunctionArrayFlattenTest, PreservesOuterNullsThroughFunctionFramework) {
    auto children = int_arrays({101, 9, 201, 9}, {0, 0, 0, 0}, {1, 2, 3, 4});
    auto arrays = array_column(nullable_column(std::move(children), {1, 0, 0, 0}), {2, 4});
    auto source = nullable_column(std::move(arrays), {1, 0});
    auto expected = nullable_column(int_arrays({201, 9}, {0, 0}, {0, 2}), {1, 0});
    expect_flatten(std::move(source), make_nullable(array_type(2)), std::move(expected));
}

TEST(FunctionArrayFlattenTest, HandlesConstantNestedArrayWithHiddenElements) {
    auto children = int_arrays({101, 102, 9}, {0, 0, 0}, {2, 3});
    auto array = array_column(nullable_column(std::move(children), {1, 0}), {2});
    auto source = ColumnConst::create(std::move(array), 3);
    expect_flatten(std::move(source), array_type(2), int_arrays({9, 9, 9}, {0, 0, 0}, {1, 2, 3}));
}

TEST(FunctionArrayFlattenTest, HandlesOnlyNullInputThroughFunctionFramework) {
    auto children = int_arrays({101, 9}, {0, 0}, {1, 2});
    auto arrays = array_column(nullable_column(std::move(children), {0, 0}), {1, 2});
    auto source = nullable_column(std::move(arrays), {1, 1});
    auto expected = nullable_column(int_arrays({}, {}, {0, 0}), {1, 1});
    expect_flatten(std::move(source), make_nullable(array_type(2)), std::move(expected));
}

TEST(FunctionArrayFlattenTest, PreservesArraysWithoutNullChildren) {
    auto children = int_arrays({1, 0, 9}, {0, 1, 0}, {0, 2, 3});
    auto source = array_column(nullable_column(std::move(children), {0, 0, 0}), {0, 2, 3});
    expect_flatten(std::move(source), array_type(2), int_arrays({1, 0, 9}, {0, 1, 0}, {0, 2, 3}));
    expect_flatten(int_arrays({1, 0, 9}, {0, 1, 0}, {0, 2, 3}), array_type(1),
                   int_arrays({1, 0, 9}, {0, 1, 0}, {0, 2, 3}));
}

} // namespace doris
