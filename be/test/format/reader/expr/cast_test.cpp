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

#include "format/reader/expr/cast.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vexpr_context.h"
#include "format/reader/column_mapper.h"
#include "format/reader/expr/literal.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class CastTest : public testing::Test {
protected:
    void SetUp() override { state.set_enable_strict_cast(true); }

    static VExprContextSPtr create_context(const DataTypePtr& return_type,
                                           const DataTypePtr& child_type, int child_column_id = 0) {
        auto cast = Cast::create_shared(return_type);
        cast->add_child(TableSlotRef::create_shared(child_column_id, child_column_id, -1,
                                                    child_type, "source_column"));
        return VExprContext::create_shared(cast);
    }

    Status prepare_open_execute(VExprContext* context, Block* block, int* result_column_id) {
        RETURN_IF_ERROR(context->prepare(&state, RowDescriptor()));
        RETURN_IF_ERROR(context->open(&state));
        return context->execute(block, result_column_id);
    }

    MockRuntimeState state;
};

class Int64ChildGreaterThanExpr final : public VExpr {
public:
    explicit Int64ChildGreaterThanExpr(int64_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr child_column;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, child_column));
        const auto& input = assert_cast<const ColumnInt64&>(*child_column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            result_data[row] = input.get_element(row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int64_t _value;
    const std::string _expr_name = "Int64ChildGreaterThanExpr";
};

TEST_F(CastTest, CastIntSlotToBigInt) {
    auto source_type = std::make_shared<DataTypeInt32>();
    auto return_type = std::make_shared<DataTypeInt64>();
    auto context = create_context(return_type, source_type);
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, -2, 3}));

    int result_column_id = -1;
    auto status = prepare_open_execute(context.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_EQ(result_column_id, 1);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(result_column_id).type, return_type);
    const auto& result_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(result_column_id).column);
    EXPECT_EQ(result_column.get_data()[0], 1);
    EXPECT_EQ(result_column.get_data()[1], -2);
    EXPECT_EQ(result_column.get_data()[2], 3);

    context->close();
}

TEST_F(CastTest, CastStringSlotToNullableInt) {
    state.set_enable_strict_cast(false);
    auto source_type = std::make_shared<DataTypeString>();
    auto return_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto context = create_context(return_type, source_type);
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeString>({"10", "bad", "-3"}));

    int result_column_id = -1;
    auto status = prepare_open_execute(context.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    const auto& nullable_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(result_column_id).column);
    const auto& result_column =
            assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    const auto& null_map = nullable_column.get_null_map_data();
    EXPECT_EQ(result_column.get_data()[0], 10);
    EXPECT_EQ(result_column.get_data()[2], -3);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 0);

    context->close();
}

TEST_F(CastTest, CastLiteralToString) {
    auto source_type = std::make_shared<DataTypeInt32>();
    auto return_type = std::make_shared<DataTypeString>();
    auto cast = Cast::create_shared(return_type);
    cast->add_child(TableLiteral::create_shared(source_type, Field::create_field<TYPE_INT>(123)));
    auto context = VExprContext::create_shared(cast);
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2, 3}));

    int result_column_id = -1;
    auto status = prepare_open_execute(context.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result = block.get_by_position(result_column_id);
    EXPECT_EQ(result.type->to_string(*result.column, 0), "123");
    EXPECT_EQ(result.type->to_string(*result.column, 1), "123");
    EXPECT_EQ(result.type->to_string(*result.column, 2), "123");

    context->close();
}

TEST_F(CastTest, EmptyBlockAppendsEmptyResultColumn) {
    auto source_type = std::make_shared<DataTypeInt32>();
    auto return_type = std::make_shared<DataTypeInt64>();
    auto context = create_context(return_type, source_type);
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({}));

    int result_column_id = -1;
    auto status = prepare_open_execute(context.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_EQ(result_column_id, 1);
    EXPECT_EQ(block.get_by_position(result_column_id).column->size(), 0);

    context->close();
}

TEST_F(CastTest, PrepareRejectsMissingChild) {
    auto cast = Cast::create_shared(std::make_shared<DataTypeInt64>());
    VExprContext context(cast);

    auto status = context.prepare(&state, RowDescriptor());
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exactly 1 child expr"), std::string::npos);
}

TEST_F(CastTest, PrepareRejectsMultipleChildren) {
    auto child_type = std::make_shared<DataTypeInt32>();
    auto cast = Cast::create_shared(std::make_shared<DataTypeInt64>());
    cast->add_child(TableSlotRef::create_shared(0, 0, -1, child_type, "c0"));
    cast->add_child(TableSlotRef::create_shared(1, 1, -1, child_type, "c1"));
    VExprContext context(cast);

    auto status = context.prepare(&state, RowDescriptor());
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exactly 1 child expr"), std::string::npos);
}

TEST_F(CastTest, ColumnMapperBuildsCastProjectionForTypeMismatch) {
    reader::TableColumnMapper mapper;
    reader::TableColumn table_column;
    table_column.id = 7;
    table_column.name = "value";
    table_column.type = std::make_shared<DataTypeInt64>();
    std::vector<reader::TableColumn> projected_columns {table_column};

    reader::SchemaField file_field;
    file_field.id = 0;
    file_field.name = "value";
    file_field.type = std::make_shared<DataTypeInt32>();
    std::vector<reader::SchemaField> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto& mapping = mapper.mappings()[0];
    EXPECT_FALSE(mapping.is_trivial);
    ASSERT_NE(mapping.projection, nullptr);

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    int result_column_id = -1;
    status = prepare_open_execute(mapping.projection.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(result_column_id).column);
    EXPECT_EQ(result_column.get_data()[0], 11);
    EXPECT_EQ(result_column.get_data()[1], 22);

    mapping.projection->close();
}

TEST_F(CastTest, ColumnMapperBuildsCastFilterForTypeMismatch) {
    reader::TableColumnMapper mapper;
    reader::TableColumn table_column;
    table_column.id = 7;
    table_column.name = "value";
    table_column.type = std::make_shared<DataTypeInt64>();
    std::vector<reader::TableColumn> projected_columns {table_column};

    reader::SchemaField file_field;
    file_field.id = 0;
    file_field.name = "value";
    file_field.type = std::make_shared<DataTypeInt32>();
    std::vector<reader::SchemaField> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64ChildGreaterThanExpr>(15);
    predicate->add_child(TableSlotRef::create_shared(7, 7, -1, table_column.type, "value"));
    reader::TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.slot_ids = {7};

    reader::FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, {}, projected_columns, &file_request)
                        .ok());
    ASSERT_EQ(file_request.expression_filters.size(), 1);
    ASSERT_EQ(file_request.predicate_columns, std::vector<reader::ColumnId>({0}));

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    auto* conjunct = file_request.expression_filters[0].conjunct.get();
    status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false,
                                      &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);

    file_request.expression_filters[0].conjunct->close();
}

} // namespace doris
