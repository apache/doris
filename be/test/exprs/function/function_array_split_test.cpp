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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/function_utils.h"

namespace doris {

namespace {

using IntRows = std::vector<std::vector<Int32>>;

ColumnPtr create_int_array_column(const IntRows& rows) {
    auto data = ColumnInt32::create();
    auto nested_null_map = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    size_t offset = 0;
    for (const auto& row : rows) {
        for (const auto value : row) {
            data->get_data().push_back(value);
            nested_null_map->get_data().push_back(0);
        }
        offset += row.size();
        offsets->get_data().push_back(offset);
    }

    return ColumnArray::create(ColumnNullable::create(std::move(data), std::move(nested_null_map)),
                               std::move(offsets));
}

ColumnPtr create_compacted_nullable_predicate(const std::vector<UInt8>& valid_row) {
    auto data = ColumnUInt8::create();
    data->get_data().assign(valid_row.begin(), valid_row.end());
    auto nested_null_map = ColumnUInt8::create(valid_row.size(), 0);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({0, valid_row.size()});
    auto array =
            ColumnArray::create(ColumnNullable::create(std::move(data), std::move(nested_null_map)),
                                std::move(offsets));
    auto array_null_map = ColumnUInt8::create();
    array_null_map->get_data().assign({1, 0});
    return ColumnNullable::create(std::move(array), std::move(array_null_map));
}

DataTypePtr int_array_type() {
    return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
}

DataTypePtr nullable_bool_array_type() {
    auto array = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeUInt8>()));
    return make_nullable(array);
}

Status execute_array_split(const std::string& name, ColumnPtr source, ColumnPtr predicate,
                           Block* block) {
    auto source_type = int_array_type();
    auto predicate_type = nullable_bool_array_type();
    auto result_type = make_nullable(std::make_shared<DataTypeArray>(make_nullable(source_type)));
    block->insert({std::move(source), source_type, "source"});
    block->insert({std::move(predicate), predicate_type, "predicate"});

    auto function = SimpleFunctionFactory::instance().get_function(
            name, block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);

    FunctionUtils fn_utils(result_type, {source_type, predicate_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
    block->insert({nullptr, result_type, "result"});
    auto status = function->execute(fn_ctx, *block, {0, 1}, 2, 2);
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    return status;
}

void expect_nullable_split_result(const std::string& name, const std::vector<UInt8>& predicate) {
    Block block;
    auto status = execute_array_split(name, create_int_array_column({{100, 101}, {1, 2}}),
                                      create_compacted_nullable_predicate(predicate), &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({1, 0}));

    const auto& outer_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    const auto& inner_array = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(outer_array.get_data()).get_nested_column());
    const size_t valid_row_begin = outer_array.get_offsets()[0];
    EXPECT_EQ(outer_array.get_offsets()[1] - valid_row_begin, 2);
    EXPECT_EQ(inner_array.size_at(valid_row_begin), 1);
    EXPECT_EQ(inner_array.size_at(valid_row_begin + 1), 1);
}

} // namespace

TEST(function_array_split_test, skips_null_array_row_with_compacted_predicate) {
    expect_nullable_split_result("array_split", {0, 1});
}

TEST(function_array_split_test, reverse_skips_null_array_row_with_compacted_predicate) {
    expect_nullable_split_result("array_reverse_split", {1, 0});
}

TEST(function_array_split_test, rejects_mismatched_non_null_row_after_null_array_row) {
    Block block;
    auto status = execute_array_split("array_split", create_int_array_column({{100, 101}, {1, 2}}),
                                      create_compacted_nullable_predicate({1}), &block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("uneven arguments on row 1"), std::string::npos) << status;
}

} // namespace doris
