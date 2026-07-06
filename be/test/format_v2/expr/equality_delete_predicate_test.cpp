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

#include "format_v2/expr/equality_delete_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "format_v2/expr/cast.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::format {

class EqualityDeletePredicateTest : public testing::Test {
protected:
    static ColumnWithTypeAndName make_nullable_int_column(
            const std::string& name, const std::vector<std::optional<int>>& values) {
        auto data = ColumnInt32::create();
        auto null_map = ColumnUInt8::create();
        for (const auto& value : values) {
            data->insert_value(value.value_or(0));
            null_map->insert_value(!value.has_value());
        }
        auto type = make_nullable(std::make_shared<DataTypeInt32>());
        return {ColumnNullable::create(std::move(data), std::move(null_map)), type, name};
    }

    static ColumnWithTypeAndName make_nullable_string_column(
            const std::string& name, const std::vector<std::optional<std::string>>& values) {
        auto data = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        for (const auto& value : values) {
            const std::string data_value = value.value_or("");
            data->insert_data(data_value.data(), data_value.size());
            null_map->insert_value(!value.has_value());
        }
        auto type = make_nullable(std::make_shared<DataTypeString>());
        return {ColumnNullable::create(std::move(data), std::move(null_map)), type, name};
    }

    static std::vector<UInt8> result_column_data(const Block& block, int result_column_id) {
        const auto& result_column =
                assert_cast<const ColumnBool&>(*block.get_by_position(result_column_id).column);
        return {result_column.get_data().begin(), result_column.get_data().end()};
    }

    static Status execute_equality_delete_predicate(Block delete_block, std::vector<int> field_ids,
                                                    Block* data_block, int* result_column_id) {
        auto predicate =
                std::make_shared<EqualityDeletePredicate>(std::move(delete_block), field_ids);
        predicate->_open_finished = true;
        for (size_t idx = 0; idx < field_ids.size(); ++idx) {
            predicate->add_child(
                    std::make_shared<MockSlotRef>(idx, data_block->get_by_position(idx).type));
        }

        VExprContext context(predicate);
        return predicate->execute(&context, data_block, result_column_id);
    }

    static Status execute_prepared_equality_delete_predicate(const VExprContextSPtr& context,
                                                             MockRuntimeState* state,
                                                             Block* data_block,
                                                             int* result_column_id) {
        RETURN_IF_ERROR(context->prepare(state, RowDescriptor()));
        RETURN_IF_ERROR(context->open(state));
        return context->execute(data_block, result_column_id);
    }
};

TEST_F(EqualityDeletePredicateTest, MatchSingleColumn) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {1, 4}));
    Block data_block;
    data_block.insert(make_nullable_int_column("id", {1, 2, 3, 4}));

    int result_column_id = -1;
    auto status = execute_equality_delete_predicate(std::move(delete_block), {1}, &data_block,
                                                    &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(data_block, result_column_id), std::vector<UInt8>({1, 0, 0, 1}));
}

TEST_F(EqualityDeletePredicateTest, MatchMultipleColumns) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {1, 2}));
    delete_block.insert(make_nullable_string_column("name", {"a", "b"}));
    Block data_block;
    data_block.insert(make_nullable_int_column("id", {1, 1, 2, 2}));
    data_block.insert(make_nullable_string_column("name", {"a", "b", "a", "b"}));

    int result_column_id = -1;
    auto status = execute_equality_delete_predicate(std::move(delete_block), {1, 2}, &data_block,
                                                    &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(data_block, result_column_id), std::vector<UInt8>({1, 0, 0, 1}));
}

TEST_F(EqualityDeletePredicateTest, MatchNullValues) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {std::nullopt}));
    Block data_block;
    data_block.insert(make_nullable_int_column("id", {1, std::nullopt, 3}));

    int result_column_id = -1;
    auto status = execute_equality_delete_predicate(std::move(delete_block), {1}, &data_block,
                                                    &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(data_block, result_column_id), std::vector<UInt8>({0, 1, 0}));
}

TEST_F(EqualityDeletePredicateTest, MatchAfterCastToDeleteKeyType) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {1, 4}));
    Block data_block;
    data_block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 4}));

    auto predicate = std::make_shared<EqualityDeletePredicate>(std::move(delete_block),
                                                               std::vector<int> {1});
    auto cast_expr = Cast::create_shared(make_nullable(std::make_shared<DataTypeInt32>()));
    cast_expr->add_child(std::make_shared<MockSlotRef>(0, data_block.get_by_position(0).type));
    predicate->add_child(std::move(cast_expr));
    auto context = VExprContext::create_shared(predicate);
    MockRuntimeState state;

    int result_column_id = -1;
    auto status = execute_prepared_equality_delete_predicate(context, &state, &data_block,
                                                             &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(data_block, result_column_id), std::vector<UInt8>({1, 0, 1}));
    context->close();
}

TEST_F(EqualityDeletePredicateTest, ChildCountMismatchReturnsError) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {1}));
    auto predicate = std::make_shared<EqualityDeletePredicate>(std::move(delete_block),
                                                               std::vector<int> {1});
    predicate->_open_finished = true;
    Block data_block;
    data_block.insert(make_nullable_int_column("id", {1}));
    VExprContext context(predicate);

    int result_column_id = -1;
    auto status = predicate->execute(&context, &data_block, &result_column_id);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("should have 1 child exprs"), std::string::npos);
}

} // namespace doris::format
