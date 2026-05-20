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

#include "format/reader/expr/literal.h"
#include "format/reader/expr/slot_ref.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"

namespace doris {

TEST(TableLiteralTest, CreatesConstColumnWithGivenTypeAndField) {
    auto type = std::make_shared<DataTypeInt32>();
    auto literal = TableLiteral::create_shared(type, Field::create_field<TYPE_INT>(123));

    ASSERT_EQ(literal->data_type(), type);
    ASSERT_TRUE(literal->is_literal());

    const auto& column = literal->get_column_ptr();
    ASSERT_EQ(column->size(), 1);
    ASSERT_TRUE(is_column_const(*column));
    EXPECT_EQ(column->get_int(0), 123);
}

TEST(TableLiteralTest, ExecutesAsConstColumn) {
    auto type = std::make_shared<DataTypeInt32>();
    auto literal = TableLiteral::create_shared(type, Field::create_field<TYPE_INT>(456));

    ColumnPtr result_column;
    ASSERT_TRUE(literal->execute_column(nullptr, nullptr, nullptr, 3, result_column).ok());

    ASSERT_EQ(result_column->size(), 3);
    ASSERT_TRUE(is_column_const(*result_column));
    EXPECT_EQ(result_column->get_int(0), 456);
    EXPECT_EQ(result_column->get_int(2), 456);
}

TEST(TableLiteralTest, ExecuteAppendsConstColumnToBlock) {
    auto type = std::make_shared<DataTypeInt32>();
    auto literal = TableLiteral::create_shared(type, Field::create_field<TYPE_INT>(789));
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3}));

    int result_column_id = -1;
    ASSERT_TRUE(literal->execute(nullptr, &block, &result_column_id).ok());

    ASSERT_EQ(result_column_id, 1);
    ASSERT_EQ(block.columns(), 2);
    const auto& result_column = block.get_by_position(result_column_id).column;
    ASSERT_EQ(result_column->size(), 3);
    ASSERT_TRUE(is_column_const(*result_column));
    EXPECT_EQ(result_column->get_int(0), 789);
    EXPECT_EQ(result_column->get_int(2), 789);
    EXPECT_EQ(block.get_by_position(result_column_id).type, type);
}

TEST(TableSlotRefTest, KeepsSlotColumnIdsAndType) {
    auto type = std::make_shared<DataTypeInt32>();
    auto slot_ref = TableSlotRef::create_shared(10, 20, 30, type);

    EXPECT_EQ(slot_ref->slot_id(), 10);
    EXPECT_EQ(slot_ref->column_id(), 20);
    EXPECT_EQ(slot_ref->data_type(), type);
    EXPECT_FALSE(slot_ref->is_constant());

    std::set<int> column_ids;
    slot_ref->collect_slot_column_ids(column_ids);
    ASSERT_EQ(column_ids.size(), 1);
    EXPECT_EQ(*column_ids.begin(), 20);
}

TEST(TableSlotRefTest, PrepareDoesNotRequireRowDescriptor) {
    auto type = std::make_shared<DataTypeInt32>();
    auto slot_ref = TableSlotRef::create_shared(10, 20, 30, type);

    EXPECT_TRUE(slot_ref->prepare(nullptr, RowDescriptor(), nullptr).ok());
}

TEST(TableSlotRefTest, ExecuteReturnsReferencedColumnId) {
    auto type = std::make_shared<DataTypeInt32>();
    auto slot_ref = TableSlotRef::create_shared(10, 1, 30, type);
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3}));
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({4, 5, 6}));

    int result_column_id = -1;
    ASSERT_TRUE(slot_ref->execute(nullptr, &block, &result_column_id).ok());

    EXPECT_EQ(result_column_id, 1);
    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(result_column_id).column->get_int(0), 4);
    EXPECT_EQ(block.get_by_position(result_column_id).column->get_int(2), 6);
}

} // namespace doris
