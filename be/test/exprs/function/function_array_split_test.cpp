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
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
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

ColumnPtr create_const_predicate(const std::vector<UInt8>& row, size_t rows) {
    auto data = ColumnUInt8::create();
    data->get_data().assign(row.begin(), row.end());
    auto nested_null_map = ColumnUInt8::create(row.size(), 0);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(row.size());
    auto array =
            ColumnArray::create(ColumnNullable::create(std::move(data), std::move(nested_null_map)),
                                std::move(offsets));
    auto nullable = ColumnNullable::create(std::move(array), ColumnUInt8::create(1, 0));
    return ColumnConst::create(std::move(nullable), rows);
}

ColumnPtr create_all_null_predicate(size_t rows) {
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().resize_fill(rows, 0);
    auto array = ColumnArray::create(
            ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create()),
            std::move(offsets));
    return ColumnNullable::create(std::move(array), ColumnUInt8::create(rows, 1));
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
    const size_t input_rows_count = source->size();
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
    auto status = function->execute(fn_ctx, *block, {0, 1}, 2, input_rows_count);
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    return status;
}

void expect_all_null_predicate_skips_const_source_expansion(const std::string& name) {
    constexpr size_t row_count = 512;
    constexpr size_t array_size = 4096;
    constexpr int64_t max_execution_bytes = 1024 * 1024;

    auto source = ColumnConst::create(create_int_array_column({std::vector<Int32>(array_size, 1)}),
                                      row_count);
    auto predicate = create_all_null_predicate(row_count);

    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "ArraySplitAllNullFastPath");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t baseline = tracker->consumption();

    Block block;
    auto status = execute_array_split(name, std::move(source), std::move(predicate), &block);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t execution_peak = tracker->peak_consumption() - baseline;

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(block.get_by_position(2).column->only_null());
    EXPECT_EQ(block.get_by_position(2).column->size(), row_count);
    EXPECT_LT(execution_peak, max_execution_bytes);
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

TEST(function_array_split_test, reads_const_predicate_for_each_source_row) {
    Block block;
    auto status = execute_array_split("array_split", create_int_array_column({{1, 2}, {3, 4}}),
                                      create_const_predicate({0, 1}, 2), &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& outer_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    const auto& inner_array = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(outer_array.get_data()).get_nested_column());
    ASSERT_EQ(outer_array.get_offsets(), ColumnArray::Offsets64({2, 4}));
    ASSERT_EQ(inner_array.get_offsets(), ColumnArray::Offsets64({1, 2, 3, 4}));
    EXPECT_EQ(inner_array.size_at(0), 1);
    EXPECT_EQ(inner_array.size_at(1), 1);
    EXPECT_EQ(inner_array.size_at(2), 1);
    EXPECT_EQ(inner_array.size_at(3), 1);
    const auto& values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(inner_array.get_data()).get_nested_column());
    EXPECT_EQ(values.get_data(), ColumnInt32::Container({1, 2, 3, 4}));
}

TEST(function_array_split_test, all_null_predicate_skips_const_source_expansion) {
    expect_all_null_predicate_skips_const_source_expansion("array_split");
}

TEST(function_array_split_test, reverse_all_null_predicate_skips_const_source_expansion) {
    expect_all_null_predicate_skips_const_source_expansion("array_reverse_split");
}

} // namespace doris
