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

#include "exprs/function/functions_logical.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr_context.h"
#include "testutil/column_helper.h"

namespace doris {

namespace {

ColumnWithTypeAndName nullable_boolean_column(std::vector<UInt8> data, std::vector<UInt8> null_map,
                                              std::string name) {
    return {ColumnHelper::create_nullable_column<DataTypeUInt8>(data, null_map),
            make_nullable(std::make_shared<DataTypeUInt8>()), std::move(name)};
}

ColumnPtr execute_or(ColumnWithTypeAndName left, ColumnWithTypeAndName right, size_t rows) {
    auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    Block block({std::move(left), std::move(right), {nullptr, result_type, "result"}});
    auto status = FunctionOr::create()->execute_impl(nullptr, block, {0, 1}, 2, rows);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return block.get_by_position(2).column;
}

class ColumnExpr final : public VExpr {
public:
    ColumnExpr(ColumnPtr column, DataTypePtr type)
            : VExpr(std::move(type), false), _column(std::move(column)) {}

    const std::string& expr_name() const override {
        static const std::string name = "ColumnExpr";
        return name;
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr& result_column) const override {
        result_column = _column;
        return Status::OK();
    }

private:
    ColumnPtr _column;
};

ColumnPtr execute_compound_or(ColumnWithTypeAndName left, ColumnWithTypeAndName right,
                              size_t rows) {
    TExprNode node;
    node.__set_type(create_type_desc(TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(TExprOpcode::COMPOUND_OR);
    node.__set_num_children(2);
    node.__set_is_nullable(true);

    auto compound = VCompoundPred::create_shared(node);
    compound->add_child(std::make_shared<ColumnExpr>(std::move(left.column), left.type));
    compound->add_child(std::make_shared<ColumnExpr>(std::move(right.column), right.type));
    VExprContext context(compound);
    ColumnPtr result;
    auto status = compound->execute_column(&context, nullptr, nullptr, rows, result);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return result;
}

void expect_boolean(const ColumnNullable& result, size_t row, UInt8 value) {
    EXPECT_FALSE(result.is_null_at(row));
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(result.get_nested_column()).get_data()[row], value);
}

} // namespace

TEST(FunctionsLogicalTest, NullableOrIgnoresNullPayload) {
    auto result =
            execute_or(nullable_boolean_column({65, 0, 65, 1, 65}, {1, 0, 1, 0, 1}, "left"),
                       nullable_boolean_column({1, 0, 0, 65, 127}, {0, 0, 0, 1, 1}, "right"), 5);

    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    expect_boolean(nullable_result, 0, 1);
    expect_boolean(nullable_result, 1, 0);
    EXPECT_TRUE(nullable_result.is_null_at(2));
    expect_boolean(nullable_result, 3, 1);
    EXPECT_TRUE(nullable_result.is_null_at(4));
}

TEST(FunctionsLogicalTest, NullableOrWithTrueConstantProducesCanonicalTrue) {
    constexpr size_t rows = 4;
    auto left = nullable_boolean_column({3, 65, 0, 255}, {1, 1, 1, 1}, "left");
    ColumnWithTypeAndName right {
            ColumnConst::create(ColumnHelper::create_column<DataTypeUInt8>({1}), rows),
            std::make_shared<DataTypeUInt8>(), "right"};

    auto result = execute_or(std::move(left), std::move(right), rows);
    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    for (size_t row = 0; row < rows; ++row) {
        expect_boolean(nullable_result, row, 1);
    }
}

TEST(FunctionsLogicalTest, CompoundNullableOrIgnoresNullPayload) {
    auto result = execute_compound_or(
            nullable_boolean_column({65, 0, 65, 1, 65}, {1, 0, 1, 0, 1}, "left"),
            nullable_boolean_column({1, 0, 0, 65, 127}, {0, 0, 0, 1, 1}, "right"), 5);

    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    expect_boolean(nullable_result, 0, 1);
    expect_boolean(nullable_result, 1, 0);
    EXPECT_TRUE(nullable_result.is_null_at(2));
    expect_boolean(nullable_result, 3, 1);
    EXPECT_TRUE(nullable_result.is_null_at(4));
}

} // namespace doris
