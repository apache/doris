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

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/scan/scanner.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {
namespace {

class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                RuntimeProfile* profile)
            : Scanner(state, local_state, limit, profile) {}

    Status filter_for_test(Block* block) { return _filter_output_block(block); }

    Status project_for_test(Block* origin_block, Block* output_block) {
        return _do_projections(origin_block, output_block);
    }

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* /*block*/, bool* eof) override {
        *eof = true;
        return Status::OK();
    }
};

class MockFilterExpr final : public VExpr {
public:
    MockFilterExpr(ColumnPtr filter_column, DataTypePtr data_type)
            : VExpr(std::move(data_type), false), _filter_column(std::move(filter_column)) {}

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* selector, size_t count,
                               ColumnPtr& result_column) const override {
        result_column = filter_column_with_selector(_filter_column, selector, count);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _name; }

private:
    ColumnPtr _filter_column;
    std::string _name = "MockFilterExpr";
};

VExprContextSPtr make_context(VExprSPtr expr) {
    auto ctx = VExprContext::create_shared(std::move(expr));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr make_filter_context(ColumnPtr filter_column, DataTypePtr data_type) {
    return make_context(
            std::make_shared<MockFilterExpr>(std::move(filter_column), std::move(data_type)));
}

VExprContextSPtr make_filter_context(const std::vector<uint8_t>& filter) {
    auto column = ColumnUInt8::create();
    for (auto v : filter) {
        column->insert_value(v);
    }
    return make_filter_context(std::move(column), std::make_shared<DataTypeUInt8>());
}

VExprContextSPtr make_nullable_filter_context(const std::vector<uint8_t>& filter,
                                              const std::vector<uint8_t>& null_map) {
    auto nested = ColumnUInt8::create();
    for (auto v : filter) {
        nested->insert_value(v);
    }
    auto null_column = ColumnUInt8::create();
    for (auto v : null_map) {
        null_column->insert_value(v);
    }
    return make_filter_context(
            ColumnNullable::create(std::move(nested), std::move(null_column)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()));
}

VExprContextSPtr make_const_filter_context(bool value, size_t rows) {
    return make_filter_context(ColumnConst::create(ColumnHelper::create_column<DataTypeUInt8>(
                                                           {static_cast<uint8_t>(value)}),
                                                   rows),
                               std::make_shared<DataTypeUInt8>());
}

ColumnWithTypeAndName make_int32_column(std::string name, const std::vector<int32_t>& values) {
    return {ColumnHelper::create_column<DataTypeInt32>(values), std::make_shared<DataTypeInt32>(),
            std::move(name)};
}

ColumnWithTypeAndName make_int64_column(std::string name, const std::vector<int64_t>& values) {
    return {ColumnHelper::create_column<DataTypeInt64>(values), std::make_shared<DataTypeInt64>(),
            std::move(name)};
}

ColumnWithTypeAndName make_string_column(std::string name, const std::vector<std::string>& values) {
    return {ColumnHelper::create_column<DataTypeString>(values), std::make_shared<DataTypeString>(),
            std::move(name)};
}

ColumnWithTypeAndName make_nullable_int32_column(std::string name,
                                                 const std::vector<int32_t>& values,
                                                 const std::vector<uint8_t>& null_map) {
    return {ColumnHelper::create_nullable_column<DataTypeInt32>(values, null_map),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), std::move(name)};
}

std::vector<int32_t> int32_values(const Block& block, size_t column_id) {
    const auto& column = assert_cast<const ColumnInt32&>(*block.get_by_position(column_id).column);
    std::vector<int32_t> values;
    values.reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i) {
        values.push_back(column.get_element(i));
    }
    return values;
}

std::vector<std::string> string_values(const Block& block, size_t column_id) {
    const auto& column = assert_cast<const ColumnString&>(*block.get_by_position(column_id).column);
    std::vector<std::string> values;
    values.reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i) {
        values.emplace_back(column.get_data_at(i).to_string());
    }
    return values;
}

std::vector<std::optional<int32_t>> nullable_int32_values(const Block& block, size_t column_id) {
    const auto& nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(column_id).column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    std::vector<std::optional<int32_t>> values;
    values.reserve(nullable.size());
    for (size_t i = 0; i < nullable.size(); ++i) {
        if (nullable.is_null_at(i)) {
            values.emplace_back(std::nullopt);
        } else {
            values.emplace_back(nested.get_element(i));
        }
    }
    return values;
}

class ScannerDirectProjectionTest : public ::testing::Test {
protected:
    void init_scanner(DataTypes output_types, VExprContextSPtrs projections,
                      VExprContextSPtrs conjuncts, int64_t limit = -1) {
        op = std::make_shared<MockScanOperatorX>();
        op->_row_descriptor = MockRowDescriptor(output_types, &pool);
        op->_output_row_descriptor = std::make_unique<MockRowDescriptor>(output_types, &pool);
        op->_output_tuple_desc = op->_output_row_descriptor->tuple_descriptors()[0];

        local_state = std::make_shared<MockScanLocalState>(&state, op.get());
        local_state->_projections = std::move(projections);

        scanner = std::make_unique<TestScanner>(&state, local_state.get(), limit, &profile);
        ASSERT_TRUE(scanner->init(&state, conjuncts).ok());
    }

    Block filter_and_project(Block origin_block) {
        EXPECT_TRUE(scanner->filter_for_test(&origin_block).ok());
        Block output_block;
        EXPECT_TRUE(scanner->project_for_test(&origin_block, &output_block).ok());
        return output_block;
    }

    ObjectPool pool;
    MockRuntimeState state;
    RuntimeProfile profile {"scanner_projection_test"};
    std::shared_ptr<MockScanOperatorX> op;
    std::shared_ptr<MockScanLocalState> local_state;
    std::unique_ptr<TestScanner> scanner;
};

TEST_F(ScannerDirectProjectionTest, initKeepsDirectProjectionWhenConjunctExists) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({1, 1, 1})});

    ASSERT_EQ(scanner->_direct_slot_ref_projection_column_ids.size(), 1);
    EXPECT_EQ(scanner->_direct_slot_ref_projection_column_ids[0], 1);
}

TEST_F(ScannerDirectProjectionTest, allPassConjunctForwardsExternalFixedLengthColumn) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({1, 1, 1, 1})});

    auto owner =
            std::make_shared<std::vector<int32_t>>(std::initializer_list<int32_t> {10, 20, 30, 40});
    auto external_column = ColumnInt32::create();
    external_column->insert_many_fix_len_data_with_owner(
            reinterpret_cast<const char*>(owner->data()), owner->size(), owner);
    const auto* external_data = reinterpret_cast<const char*>(owner->data());

    Block output = filter_and_project(Block(
            {make_int32_column("k", {1, 2, 3, 4}), {std::move(external_column), int_type, "v"}}));

    ASSERT_EQ(output.rows(), 4);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {10, 20, 30, 40}));
    EXPECT_EQ(output.get_by_position(0).column->get_raw_data().data, external_data);
}

TEST_F(ScannerDirectProjectionTest,
       partialConjunctMaterializesExternalColumnButStillProjectsCorrectly) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({0, 1, 0, 1, 1})});

    auto owner = std::make_shared<std::vector<int32_t>>(
            std::initializer_list<int32_t> {10, 20, 30, 40, 50});
    auto external_column = ColumnInt32::create();
    external_column->insert_many_fix_len_data_with_owner(
            reinterpret_cast<const char*>(owner->data()), owner->size(), owner);
    const auto* external_data = reinterpret_cast<const char*>(owner->data());

    Block output = filter_and_project(Block({make_int32_column("k", {1, 2, 3, 4, 5}),
                                             {std::move(external_column), int_type, "v"}}));

    ASSERT_EQ(output.rows(), 3);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {20, 40, 50}));
    EXPECT_NE(output.get_by_position(0).column->get_raw_data().data, external_data);
}

TEST_F(ScannerDirectProjectionTest, fixedLengthPartialFilterTransfersFilteredColumn) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({1, 0, 1, 0, 1})});

    Block origin({make_int32_column("k", {1, 2, 3, 4, 5}),
                  make_int32_column("v", {11, 22, 33, 44, 55})});
    ASSERT_TRUE(scanner->filter_for_test(&origin).ok());
    auto filtered_value_column = origin.get_by_position(1).column;

    Block output;
    ASSERT_TRUE(scanner->project_for_test(&origin, &output).ok());

    ASSERT_EQ(output.rows(), 3);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {11, 33, 55}));
    EXPECT_EQ(output.get_by_position(0).column, filtered_value_column);
}

TEST_F(ScannerDirectProjectionTest, nullableDataColumnKeepsNullMapAfterConjunctFiltering) {
    auto nullable_int_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    init_scanner({nullable_int_type}, {MockSlotRef::create_mock_context(1, nullable_int_type)},
                 {make_filter_context({1, 1, 0, 1})});

    Block output = filter_and_project(
            Block({make_int32_column("k", {1, 2, 3, 4}),
                   make_nullable_int32_column("v", {10, 20, 30, 40}, {0, 1, 0, 0})}));

    ASSERT_EQ(output.rows(), 3);
    EXPECT_EQ(nullable_int32_values(output, 0),
              (std::vector<std::optional<int32_t>> {10, std::nullopt, 40}));
}

TEST_F(ScannerDirectProjectionTest, nullableConjunctTreatsNullAsFilteredOut) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_nullable_filter_context({1, 1, 1, 0, 1}, {0, 1, 0, 0, 0})});

    Block output = filter_and_project(Block({make_int32_column("k", {1, 2, 3, 4, 5}),
                                             make_int32_column("v", {10, 20, 30, 40, 50})}));

    ASSERT_EQ(output.rows(), 3);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {10, 30, 50}));
}

TEST_F(ScannerDirectProjectionTest, reorderAndDuplicateSlotRefsAfterConjunctFiltering) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    init_scanner({string_type, int_type, string_type},
                 {MockSlotRef::create_mock_context(2, string_type),
                  MockSlotRef::create_mock_context(0, int_type),
                  MockSlotRef::create_mock_context(2, string_type)},
                 {make_filter_context({0, 1, 1, 0})});

    Block output = filter_and_project(Block({make_int32_column("k", {1, 2, 3, 4}),
                                             make_int64_column("unused", {100, 200, 300, 400}),
                                             make_string_column("s", {"a", "b", "c", "d"})}));

    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(string_values(output, 0), (std::vector<std::string> {"b", "c"}));
    EXPECT_EQ(int32_values(output, 1), (std::vector<int32_t> {2, 3}));
    EXPECT_EQ(string_values(output, 2), (std::vector<std::string> {"b", "c"}));
    EXPECT_EQ(output.get_by_position(0).column, output.get_by_position(2).column);
}

TEST_F(ScannerDirectProjectionTest, multipleConjunctsKeepDirectProjectionCorrect) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({1, 1, 0, 1, 1}), make_filter_context({0, 1, 1, 1, 0})});

    Block output = filter_and_project(Block({make_int32_column("k", {1, 2, 3, 4, 5}),
                                             make_int32_column("v", {10, 20, 30, 40, 50})}));

    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {20, 40}));
}

TEST_F(ScannerDirectProjectionTest, allFilteredConjunctReturnsEmptyBlockWithoutProjectionCrash) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_const_filter_context(false, 4)});

    Block output = filter_and_project(Block(
            {make_int32_column("k", {1, 2, 3, 4}), make_int32_column("v", {10, 20, 30, 40})}));

    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(output.columns(), 0);
}

TEST_F(ScannerDirectProjectionTest,
       nonSlotRefProjectionWithConjunctStillUsesGenericProjectionPath) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type},
                 {make_context(std::make_shared<MockFilterExpr>(
                         ColumnHelper::create_column<DataTypeInt32>({100, 300}), int_type))},
                 {make_filter_context({1, 0, 1})});

    EXPECT_TRUE(scanner->_direct_slot_ref_projection_column_ids.empty());

    Block output = filter_and_project(
            Block({make_int32_column("k", {1, 2, 3}), make_int32_column("v", {10, 20, 30})}));

    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {100, 300}));
}

TEST_F(ScannerDirectProjectionTest, positiveLimitStillUsesGenericProjectionPath) {
    auto int_type = std::make_shared<DataTypeInt32>();
    init_scanner({int_type}, {MockSlotRef::create_mock_context(1, int_type)},
                 {make_filter_context({1, 0, 1})}, 2);

    EXPECT_TRUE(scanner->_direct_slot_ref_projection_column_ids.empty());

    Block output = filter_and_project(
            Block({make_int32_column("k", {1, 2, 3}), make_int32_column("v", {10, 20, 30})}));

    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(int32_values(output, 0), (std::vector<int32_t> {10, 30}));
}

} // namespace
} // namespace doris
