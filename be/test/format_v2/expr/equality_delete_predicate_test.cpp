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

TEST_F(EqualityDeletePredicateTest, UsesPopulatedPredicateColumnForLazyBatchSize) {
    Block delete_block;
    delete_block.insert(make_nullable_int_column("id", {1, 4}));
    Block data_block;
    // A lazy reader can leave the first projected column unread while decoding a later predicate
    // column. Block::rows() is therefore zero even though the equality key has four rows.
    data_block.insert(make_nullable_string_column("unread", {}));
    data_block.insert(make_nullable_int_column("id", {1, 2, 3, 4}));

    auto predicate = std::make_shared<EqualityDeletePredicate>(std::move(delete_block),
                                                               std::vector<int> {1});
    predicate->_open_finished = true;
    predicate->add_child(std::make_shared<MockSlotRef>(1, data_block.get_by_position(1).type));
    VExprContext context(predicate);

    int result_column_id = -1;
    auto status = predicate->execute(&context, &data_block, &result_column_id);
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

// A delete set is a set, so the repeats a caller happens to hand over cost memory and hash-map entries
// without changing a single answer.
TEST_F(EqualityDeletePredicateTest, DistinctRowsKeepsOneOfEachRepeatedKey) {
    Block keys;
    keys.insert(make_nullable_int_column("id", {1, 2, 1, 1, 3, 2}));

    const Block distinct = EqualityDeletePredicate::distinct_rows(keys);

    ASSERT_EQ(distinct.rows(), 3);
    // First occurrence wins, so the surviving order is the order they were first named in.
    Block data_block;
    data_block.insert(make_nullable_int_column("id", {1, 2, 3, 4}));
    int result_column_id = -1;
    auto status = execute_equality_delete_predicate(distinct, {1}, &data_block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(result_column_data(data_block, result_column_id), std::vector<UInt8>({1, 1, 1, 0}));
}

// The invariant the whole thing rests on: whatever a caller deduplicates with this, the predicate it
// then builds answers exactly what the predicate built from the raw block answers. A dedup that called
// two keys the same where the matching would not would silently stop deleting one of them.
TEST_F(EqualityDeletePredicateTest, DistinctRowsAnswersWhatTheRawBlockAnswers) {
    // Composite, nullable, with repeats that agree on one column only -- and a NULL named twice, which
    // matching treats as one value and so must dedup as one value.
    const auto ids = std::vector<std::optional<int>> {1, 1, 2, 1, std::nullopt, std::nullopt, 2};
    const auto names =
            std::vector<std::optional<std::string>> {"a", "b", "a", "a", "z", "z", std::nullopt};
    const auto make_keys = [&]() {
        Block keys;
        keys.insert(make_nullable_int_column("id", ids));
        keys.insert(make_nullable_string_column("name", names));
        return keys;
    };
    const auto make_data = [&]() {
        Block data_block;
        data_block.insert(
                make_nullable_int_column("id", {1, 1, 2, std::nullopt, std::nullopt, 2, 3}));
        data_block.insert(
                make_nullable_string_column("name", {"a", "c", "a", "z", "y", std::nullopt, "a"}));
        return data_block;
    };

    const Block distinct = EqualityDeletePredicate::distinct_rows(make_keys());
    EXPECT_EQ(distinct.rows(), 5) << "expected only the exact repeats to be dropped";

    Block raw_data = make_data();
    int raw_result = -1;
    ASSERT_TRUE(
            execute_equality_delete_predicate(make_keys(), {1, 2}, &raw_data, &raw_result).ok());

    Block distinct_data = make_data();
    int distinct_result = -1;
    ASSERT_TRUE(
            execute_equality_delete_predicate(distinct, {1, 2}, &distinct_data, &distinct_result)
                    .ok());

    EXPECT_EQ(result_column_data(distinct_data, distinct_result),
              result_column_data(raw_data, raw_result));
    // Pinned, so that "they agree" cannot become "they agree on nothing at all".
    EXPECT_EQ(result_column_data(raw_data, raw_result), std::vector<UInt8>({1, 0, 1, 1, 0, 1, 0}));
}

// Nothing to drop must not cost a copy of every column, and the degenerate sizes must not need a
// special case at every call site.
TEST_F(EqualityDeletePredicateTest, DistinctRowsPassesThroughWhenThereAreNoRepeats) {
    Block empty;
    empty.insert(make_nullable_int_column("id", {}));
    EXPECT_EQ(EqualityDeletePredicate::distinct_rows(empty).rows(), 0);

    Block one;
    one.insert(make_nullable_int_column("id", {7}));
    EXPECT_EQ(EqualityDeletePredicate::distinct_rows(one).rows(), 1);

    Block all_distinct;
    all_distinct.insert(make_nullable_int_column("id", {3, 1, 2}));
    const Block result = EqualityDeletePredicate::distinct_rows(all_distinct);
    ASSERT_EQ(result.rows(), 3);
    EXPECT_EQ(result.get_by_position(0).column.get(), all_distinct.get_by_position(0).column.get())
            << "a block with no repeats should be returned as-is, not rebuilt";
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
