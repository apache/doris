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
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/function_utils.h"

namespace doris {

namespace {

using IntRows = std::vector<std::vector<Int32>>;

ColumnPtr create_array_column(const IntRows& rows) {
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

DataTypePtr array_int_type() {
    return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
}

DataTypePtr array_zip_result_type(size_t field_count, bool nullable) {
    DataTypes fields(field_count, make_nullable(std::make_shared<DataTypeInt32>()));
    auto tuple_type = make_nullable(std::make_shared<DataTypeStruct>(fields));
    auto array_type = std::make_shared<DataTypeArray>(std::move(tuple_type));
    return nullable ? make_nullable(array_type) : array_type;
}

Status execute_array_zip(ColumnPtr lhs, ColumnPtr rhs, const DataTypePtr& lhs_type,
                         const DataTypePtr& rhs_type, size_t rows, Block* block) {
    auto result_type = array_zip_result_type(2, lhs_type->is_nullable() || rhs_type->is_nullable());
    block->insert({std::move(lhs), lhs_type, "lhs"});
    block->insert({std::move(rhs), rhs_type, "rhs"});

    auto function = SimpleFunctionFactory::instance().get_function(
            "array_zip", block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);

    FunctionUtils fn_utils(result_type, {lhs_type, rhs_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
    block->insert({nullptr, result_type, "result"});
    auto status = function->execute(fn_ctx, *block, {0, 1}, 2, rows);
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    return status;
}

} // namespace

TEST(function_array_zip_test, supports_untyped_null_literal) {
    auto null_literal_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_NULL, true);
    auto array_type = array_int_type();
    Block block;

    auto status = execute_array_zip(null_literal_type->create_column_const(2, Field()),
                                    create_array_column({{10}, {20}}), null_literal_type,
                                    array_type, 2, &block);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(block.get_by_position(2).column->only_null());
    EXPECT_EQ(block.get_by_position(2).column->size(), 2);
}

TEST(function_array_zip_test, ignores_hidden_payload_of_outer_null_rows) {
    auto lhs_array = create_array_column({{100, 101}, {1}, {200, 201}});
    auto lhs_null_map = ColumnUInt8::create();
    lhs_null_map->get_data().assign({1, 0, 1});
    auto lhs = ColumnNullable::create(std::move(lhs_array), std::move(lhs_null_map));
    auto rhs = create_array_column({{10}, {20}, {30}});

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), make_nullable(array_type),
                                    array_type, 3, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({1, 0, 1}));

    const auto& result_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({0, 1, 1}));

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    const auto& rhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[1]).get_nested_column());
    ASSERT_EQ(lhs_values.size(), 1);
    ASSERT_EQ(rhs_values.size(), 1);
    EXPECT_EQ(lhs_values.get_data()[0], 1);
    EXPECT_EQ(rhs_values.get_data()[0], 20);
}

TEST(function_array_zip_test, equal_hidden_offsets_reuse_input_columns) {
    auto lhs_array = create_array_column({{100}, {1}});
    const auto& lhs_array_ref = assert_cast<const ColumnArray&>(*lhs_array);
    const auto* lhs_data = lhs_array_ref.get_data_ptr().get();
    const auto* lhs_offsets = lhs_array_ref.get_offsets_ptr().get();
    auto lhs_null_map = ColumnUInt8::create();
    lhs_null_map->get_data().assign({1, 0});
    auto lhs = ColumnNullable::create(std::move(lhs_array), std::move(lhs_null_map));
    auto rhs = create_array_column({{10}, {20}});
    const auto& rhs_array = assert_cast<const ColumnArray&>(*rhs);
    const auto* rhs_data = rhs_array.get_data_ptr().get();

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), make_nullable(array_type),
                                    array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({1, 0}));

    const auto& result_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    EXPECT_EQ(result_array.get_offsets_ptr().get(), lhs_offsets);
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({1, 2}));

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    EXPECT_EQ(tuple.get_columns()[0].get(), lhs_data);
    EXPECT_EQ(tuple.get_columns()[1].get(), rhs_data);
}

TEST(function_array_zip_test, rejects_different_lengths_on_non_null_rows) {
    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(create_array_column({{1, 2}}), create_array_column({{10}}),
                                    array_type, array_type, 1, &block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("same offsets"), std::string::npos) << status;
}

TEST(function_array_zip_test, validates_late_mismatch_before_compacting) {
    constexpr size_t row_count = 64;
    constexpr size_t row_size = 2048;
    constexpr int64_t max_execution_bytes = 256 * 1024;

    IntRows lhs_rows(row_count, std::vector<Int32>(row_size, 1));
    IntRows rhs_rows(row_count, std::vector<Int32>(row_size, 2));
    lhs_rows[0] = {999};
    rhs_rows[0].clear();
    rhs_rows.back().pop_back();

    auto lhs_array = create_array_column(lhs_rows);
    auto lhs_null_map = ColumnUInt8::create(row_count, 0);
    lhs_null_map->get_data()[0] = 1;
    auto lhs = ColumnNullable::create(std::move(lhs_array), std::move(lhs_null_map));
    auto rhs = create_array_column(rhs_rows);

    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "ArrayZipValidateBeforeCompact");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t baseline = tracker->consumption();

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), make_nullable(array_type),
                                    array_type, row_count, &block);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t execution_peak = tracker->peak_consumption() - baseline;

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("same offsets"), std::string::npos) << status;
    EXPECT_LT(execution_peak, max_execution_bytes);
}

TEST(function_array_zip_test, aligned_offsets_reuse_input_columns) {
    auto lhs = create_array_column({{1, 2}, {3}});
    auto rhs = create_array_column({{10, 20}, {30}});
    const auto& lhs_array = assert_cast<const ColumnArray&>(*lhs);
    const auto& rhs_array = assert_cast<const ColumnArray&>(*rhs);
    const auto* lhs_data = lhs_array.get_data_ptr().get();
    const auto* rhs_data = rhs_array.get_data_ptr().get();
    const auto* lhs_offsets = lhs_array.get_offsets_ptr().get();

    auto array_type = array_int_type();
    Block block;
    auto status =
            execute_array_zip(std::move(lhs), std::move(rhs), array_type, array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_array = assert_cast<const ColumnArray&>(*block.get_by_position(2).column);
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({2, 3}));
    EXPECT_EQ(result_array.get_offsets_ptr().get(), lhs_offsets);
    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    EXPECT_EQ(tuple.get_columns()[0].get(), lhs_data);
    EXPECT_EQ(tuple.get_columns()[1].get(), rhs_data);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    const auto& rhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[1]).get_nested_column());
    EXPECT_EQ(lhs_values.get_data(), ColumnInt32::Container({1, 2, 3}));
    EXPECT_EQ(rhs_values.get_data(), ColumnInt32::Container({10, 20, 30}));
}

TEST(function_array_zip_test, empty_outer_null_rows_reuse_input_columns) {
    auto lhs_array = create_array_column({{}, {1, 2}});
    const auto& lhs_array_ref = assert_cast<const ColumnArray&>(*lhs_array);
    const auto* lhs_data = lhs_array_ref.get_data_ptr().get();
    const auto* lhs_offsets = lhs_array_ref.get_offsets_ptr().get();
    auto lhs_null_map = ColumnUInt8::create();
    lhs_null_map->get_data().assign({1, 0});
    auto lhs = ColumnNullable::create(std::move(lhs_array), std::move(lhs_null_map));

    auto rhs = create_array_column({{}, {10, 20}});
    const auto& rhs_array = assert_cast<const ColumnArray&>(*rhs);
    const auto* rhs_data = rhs_array.get_data_ptr().get();

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), make_nullable(array_type),
                                    array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({1, 0}));
    const auto& result_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    EXPECT_EQ(result_array.get_offsets_ptr().get(), lhs_offsets);

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    EXPECT_EQ(tuple.get_columns()[0].get(), lhs_data);
    EXPECT_EQ(tuple.get_columns()[1].get(), rhs_data);
}

TEST(function_array_zip_test, const_array_does_not_copy_aligned_normal_input) {
    auto lhs = ColumnConst::create(create_array_column({{1, 2}}), 2);
    auto rhs = create_array_column({{10, 20}, {30, 40}});
    const auto& rhs_array = assert_cast<const ColumnArray&>(*rhs);
    const auto* rhs_data = rhs_array.get_data_ptr().get();
    const auto* rhs_offsets = rhs_array.get_offsets_ptr().get();

    auto array_type = array_int_type();
    Block block;
    auto status =
            execute_array_zip(std::move(lhs), std::move(rhs), array_type, array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_array = assert_cast<const ColumnArray&>(*block.get_by_position(2).column);
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({2, 4}));
    EXPECT_EQ(result_array.get_offsets_ptr().get(), rhs_offsets);

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    EXPECT_EQ(tuple.get_columns()[1].get(), rhs_data);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    EXPECT_EQ(lhs_values.get_data(), ColumnInt32::Container({1, 2, 1, 2}));
}

TEST(function_array_zip_test, supports_all_const_arrays) {
    auto lhs = ColumnConst::create(create_array_column({{1, 2}}), 2);
    auto rhs = ColumnConst::create(create_array_column({{10, 20}}), 2);

    auto array_type = array_int_type();
    Block block;
    auto status =
            execute_array_zip(std::move(lhs), std::move(rhs), array_type, array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto result_column = block.get_by_position(2).column->convert_to_full_column_if_const();
    const auto& result_array = assert_cast<const ColumnArray&>(*result_column);
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({2, 4}));

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    const auto& rhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[1]).get_nested_column());
    EXPECT_EQ(lhs_values.get_data(), ColumnInt32::Container({1, 2, 1, 2}));
    EXPECT_EQ(rhs_values.get_data(), ColumnInt32::Container({10, 20, 10, 20}));
}

TEST(function_array_zip_test, const_array_ignores_mismatched_hidden_payload) {
    auto lhs = ColumnConst::create(create_array_column({{1, 2}}), 3);
    auto rhs_array = create_array_column({{10}, {20, 30}, {40, 50}});
    auto rhs_null_map = ColumnUInt8::create();
    rhs_null_map->get_data().assign({1, 0, 0});
    auto rhs = ColumnNullable::create(std::move(rhs_array), std::move(rhs_null_map));

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), array_type,
                                    make_nullable(array_type), 3, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({1, 0, 0}));

    const auto& result_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    EXPECT_EQ(result_array.get_offsets(), ColumnArray::Offsets64({0, 2, 4}));

    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    const auto& rhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[1]).get_nested_column());
    EXPECT_EQ(lhs_values.get_data(), ColumnInt32::Container({1, 2, 1, 2}));
    EXPECT_EQ(rhs_values.get_data(), ColumnInt32::Container({20, 30, 40, 50}));
}

TEST(function_array_zip_test, nullable_const_array_reuses_aligned_normal_input) {
    auto lhs_null_map = ColumnUInt8::create();
    lhs_null_map->get_data().push_back(0);
    auto lhs_value = ColumnNullable::create(create_array_column({{1, 2}}), std::move(lhs_null_map));
    auto lhs = ColumnConst::create(std::move(lhs_value), 2);
    auto rhs = create_array_column({{10, 20}, {30, 40}});
    const auto& rhs_array = assert_cast<const ColumnArray&>(*rhs);
    const auto* rhs_data = rhs_array.get_data_ptr().get();
    const auto* rhs_offsets = rhs_array.get_offsets_ptr().get();

    auto array_type = array_int_type();
    Block block;
    auto status = execute_array_zip(std::move(lhs), std::move(rhs), make_nullable(array_type),
                                    array_type, 2, &block);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_null_map_data(), ColumnUInt8::Container({0, 0}));

    const auto& result_array = assert_cast<const ColumnArray&>(result.get_nested_column());
    EXPECT_EQ(result_array.get_offsets_ptr().get(), rhs_offsets);
    const auto& nullable_tuple =
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column();
    const auto& tuple = assert_cast<const ColumnStruct&>(nullable_tuple);
    EXPECT_EQ(tuple.get_columns()[1].get(), rhs_data);
    const auto& lhs_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*tuple.get_columns()[0]).get_nested_column());
    EXPECT_EQ(lhs_values.get_data(), ColumnInt32::Container({1, 2, 1, 2}));
}

} // namespace doris
