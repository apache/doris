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

#include <cstddef>
#include <memory>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/function_utils.h"

namespace doris {
namespace {

Status execute_complex_equals(ColumnPtr left, ColumnPtr right, const DataTypePtr& type, size_t rows,
                              Block* block) {
    auto result_type = std::make_shared<DataTypeBool>();
    block->insert({std::move(left), type, "left"});
    block->insert({std::move(right), type, "right"});
    auto function = SimpleFunctionFactory::instance().get_function(
            "complex_equals", block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);
    FunctionUtils utils(result_type, {type, type}, false);
    auto* context = utils.get_fn_ctx();
    RETURN_IF_ERROR(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    block->insert({nullptr, result_type, "result"});
    auto status = function->execute(context, *block, {0, 1}, 2, rows);
    RETURN_IF_ERROR(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    RETURN_IF_ERROR(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    return status;
}

void expect_results(const Block& block, size_t rows, UInt8 value) {
    auto result = block.get_by_position(2).column->convert_to_full_column_if_const();
    const auto& values = assert_cast<const ColumnUInt8&>(*result).get_data();
    ASSERT_EQ(values.size(), rows);
    for (auto actual : values) {
        EXPECT_EQ(actual, value);
    }
}

} // namespace

TEST(FunctionComplexEqualsTest, KeepsConstantPayloadCompact) {
    constexpr size_t rows = 256;
    constexpr size_t elements = 16384;
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto type =
            std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeArray>(int_type),
                                                        std::make_shared<DataTypeFloat64>()},
                                             Strings {"a", "d"});
    auto array = ColumnArray::create(ColumnNullable::create(ColumnInt32::create(elements, 1),
                                                            ColumnUInt8::create(elements, 0)),
                                     ColumnArray::ColumnOffsets::create(1, elements));
    ColumnPtr constant = ColumnConst::create(
            ColumnStruct::create(Columns {std::move(array), ColumnFloat64::create(1, 0)}), rows);
    auto empty = type->create_column();
    empty->insert_many_defaults(rows);
    ColumnPtr vector = std::move(empty);
    // Both operand orders must keep the one-row payload. The old expansion allocated more than
    // 20 MiB even though the empty source arrays cannot match; output needs only 256 bytes.
    for (const bool reverse : {false, true}) {
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "ComplexEqualsConstant");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        Block block;
        ASSERT_TRUE(execute_complex_equals(reverse ? constant : vector, reverse ? vector : constant,
                                           type, rows, &block)
                            .ok());
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_LT(tracker->peak_consumption() - baseline, 2 * 1024 * 1024);
        expect_results(block, rows, 0);
    }
    for (const auto& value : {constant, vector}) {
        Block block;
        ASSERT_TRUE(execute_complex_equals(value, value, type, rows, &block).ok());
        expect_results(block, rows, 1);
    }
}

TEST(FunctionComplexEqualsTest, NormalizesNestedMapsWithoutChangingInputs) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(int_type, int_type);
    auto type =
            std::make_shared<DataTypeStruct>(DataTypes {map_type, map_type}, Strings {"m1", "m2"});
    auto left = type->create_column();
    auto right = type->create_column();
    Field field;
    ASSERT_TRUE(type->get_serde()->from_fe_string("{{1:1,1:2},{2:3}}", field).ok());
    left->insert(field);
    ASSERT_TRUE(type->get_serde()->from_fe_string("{{1:2},{2:3}}", field).ok());
    right->insert(field);
    const auto before = type->to_string(*left, 0);
    ColumnPtr left_column = std::move(left);
    ColumnPtr right_column = std::move(right);
    // This models native Arrow duplicates rather than map(), which has already deduplicated them.
    for (const bool constant : {false, true}) {
        ColumnPtr left_input = constant ? ColumnConst::create(left_column, 1) : left_column;
        Block block;
        ASSERT_TRUE(execute_complex_equals(left_input, right_column, type, 1, &block).ok());
        expect_results(block, 1, 1);
        EXPECT_EQ(type->to_string(*left_column, 0), before);
    }
}

TEST(FunctionComplexEqualsTest, PreservesTopLevelAndNestedNulls) {
    auto type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"v"}));
    auto left = type->create_column();
    auto right = type->create_column();
    left->insert_default();
    right->insert_default();
    Field field;
    ASSERT_TRUE(remove_nullable(type)->get_serde()->from_fe_string("{null}", field).ok());
    left->insert(field);
    right->insert(field);
    ColumnPtr left_column = std::move(left);
    ColumnPtr right_column = std::move(right);
    Block equal;
    ASSERT_TRUE(execute_complex_equals(left_column, right_column, type, 2, &equal).ok());
    expect_results(equal, 2, 1);
    auto null_data = type->create_column();
    null_data->insert_default();
    ColumnPtr null = ColumnConst::create(std::move(null_data), 2);
    Block mixed;
    ASSERT_TRUE(execute_complex_equals(left_column, null, type, 2, &mixed).ok());
    const auto& result =
            assert_cast<const ColumnUInt8&>(*mixed.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 0);
}
} // namespace doris
