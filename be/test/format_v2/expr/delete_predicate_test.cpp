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

#include "format_v2/expr/delete_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::format {

class DeletePredicateTest : public testing::Test {
protected:
    static Block make_block(const std::vector<int64_t>& row_ids) {
        auto column = ColumnInt64::create();
        for (auto row_id : row_ids) {
            column->insert_value(row_id);
        }

        Block block;
        block.insert({std::move(column), std::make_shared<DataTypeInt64>(), "row_id"});
        return block;
    }

    static std::vector<UInt8> result_column_data(const Block& block, int result_column_id) {
        const auto& result_column =
                assert_cast<const ColumnBool&>(*block.get_by_position(result_column_id).column);
        return {result_column.get_data().begin(), result_column.get_data().end()};
    }

    static Status execute_delete_predicate(const std::vector<int64_t>& deleted_rows, Block* block,
                                           int* result_column_id) {
        auto delete_predicate = std::make_shared<DeletePredicate>(deleted_rows);
        delete_predicate->_open_finished = true;
        delete_predicate->add_child(
                std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>()));

        VExprContext context(delete_predicate);
        return delete_predicate->execute(&context, block, result_column_id);
    }

    static Status execute_delete_predicate(const roaring::Roaring64Map& deletion_vector,
                                           Block* block, int* result_column_id) {
        auto delete_predicate = std::make_shared<DeletePredicate>(deletion_vector);
        delete_predicate->_open_finished = true;
        delete_predicate->add_child(
                std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>()));

        VExprContext context(delete_predicate);
        return delete_predicate->execute(&context, block, result_column_id);
    }
};

TEST_F(DeletePredicateTest, MatchDeletedRowsInInputRange) {
    const std::vector<int64_t> deleted_rows {-3, 1, 4, 8, 12, 20};
    auto block = make_block({0, 1, 2, 3, 4, 5, 8, 12});

    int result_column_id = -1;
    auto status = execute_delete_predicate(deleted_rows, &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(result_column_id, 1);
    EXPECT_EQ(result_column_data(block, result_column_id),
              std::vector<UInt8>({0, 1, 0, 0, 1, 0, 1, 1}));
}

TEST_F(DeletePredicateTest, MatchCompressedDeletionVectorWithoutExpansion) {
    // The cached DV remains a Roaring bitmap all the way into the file-level predicate. Include
    // values on both sides of the current batch to exercise the iterator range seek.
    const roaring::Roaring64Map deletion_vector {1, 4, 8, 12, 20};
    auto block = make_block({0, 1, 2, 3, 4, 5, 8, 12});

    int result_column_id = -1;
    const auto status = execute_delete_predicate(deletion_vector, &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(block, result_column_id),
              std::vector<UInt8>({0, 1, 0, 0, 1, 0, 1, 1}));
}

TEST_F(DeletePredicateTest, EmptyDeletedRowsReturnAllFalse) {
    const std::vector<int64_t> deleted_rows;
    auto block = make_block({1, 2, 3});

    int result_column_id = -1;
    auto status = execute_delete_predicate(deleted_rows, &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(result_column_data(block, result_column_id), std::vector<UInt8>({0, 0, 0}));
}

TEST_F(DeletePredicateTest, DeletedRowsOutsideInputRangeReturnAllFalse) {
    const std::vector<int64_t> deleted_rows {-10, -1, 10, 11};
    auto block = make_block({1, 2, 3});

    int result_column_id = -1;
    auto status = execute_delete_predicate(deleted_rows, &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(result_column_data(block, result_column_id), std::vector<UInt8>({0, 0, 0}));
}

TEST_F(DeletePredicateTest, EmptyRowIdColumnAppendsEmptyResultColumn) {
    const std::vector<int64_t> deleted_rows {1, 2, 3};
    auto block = make_block({});

    int result_column_id = -1;
    auto status = execute_delete_predicate(deleted_rows, &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(result_column_id, 1);
    EXPECT_EQ(result_column_data(block, result_column_id), std::vector<UInt8>({}));
}

TEST_F(DeletePredicateTest, MissingRowIdColumnReturnsError) {
    const std::vector<int64_t> deleted_rows {1, 2, 3};
    Block block;

    int result_column_id = -1;
    auto status = execute_delete_predicate(deleted_rows, &block, &result_column_id);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid column id"), std::string::npos);
    EXPECT_EQ(block.columns(), 0);
    EXPECT_EQ(result_column_id, -1);
}

TEST_F(DeletePredicateTest, MissingRowIdChildReturnsError) {
    const std::vector<int64_t> deleted_rows {1};
    auto block = make_block({1});
    auto delete_predicate = std::make_shared<DeletePredicate>(deleted_rows);
    delete_predicate->_open_finished = true;
    VExprContext context(delete_predicate);

    int result_column_id = -1;
    auto status = delete_predicate->execute(&context, &block, &result_column_id);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exactly 1 child expr"), std::string::npos);
}

TEST_F(DeletePredicateTest, ExecuteColumnImplReturnsError) {
    const std::vector<int64_t> deleted_rows {1};
    DeletePredicate delete_predicate(deleted_rows);
    VExprContext context(std::make_shared<DeletePredicate>(deleted_rows));
    ColumnPtr result_column;

    auto status =
            delete_predicate.execute_column_impl(&context, nullptr, nullptr, 0, result_column);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("DeletePredicate::execute_column_impl"), std::string::npos);
}

TEST_F(DeletePredicateTest, LifecycleAndDebugString) {
    const std::vector<int64_t> deleted_rows {1};
    DeletePredicate delete_predicate(deleted_rows);
    VExprContext context(std::make_shared<DeletePredicate>(deleted_rows));
    RowDescriptor row_desc;

    auto status = delete_predicate.prepare(nullptr, row_desc, &context);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(delete_predicate.expr_name(), "DeletePredicate");
    EXPECT_EQ(delete_predicate.debug_string(), "DeletePredicate");

    status = delete_predicate.open(nullptr, &context, FunctionContext::THREAD_LOCAL);
    ASSERT_TRUE(status.ok()) << status;
    delete_predicate.close(&context, FunctionContext::THREAD_LOCAL);
}

} // namespace doris::format
