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

#include "exprs/vcondition_expr.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"

namespace doris {

// Build a minimal TExprNode as the input of VectorizedCoalesceExpr.
// Only fields required by the VExpr base ctor (so that create_data_type works) are set.
static TExprNode make_coalesce_node(TPrimitiveType::type ptype, bool is_nullable) {
    TExprNode node;
    node.node_type = TExprNodeType::FUNCTION_CALL;
    node.num_children = 0;
    node.__set_is_nullable(is_nullable);

    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(ptype);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    node.__set_type(type_desc);

    TFunction fn;
    TFunctionName fn_name;
    fn_name.function_name = "coalesce";
    fn.name = fn_name;
    node.__set_fn(fn);

    return node;
}

// Mock child expression: returns the pre-injected ColumnPtr / DataTypePtr to the parent expr.
// Behavior is modeled after MockVExprForTryCast in try_cast_expr_test.cpp.
class MockChildVExpr : public VExpr {
public:
    MockChildVExpr(ColumnPtr column, DataTypePtr type)
            : _column(std::move(column)), _type(std::move(type)) {}

    MOCK_CONST_METHOD0(clone, VExprSPtr());

    const std::string& expr_name() const override { return _name; }

    Status execute(VExprContext* context, Block* block, int* result_column_id) const override {
        block->insert({_column, _type, "mock_child"});
        *result_column_id = static_cast<int>(block->columns()) - 1;
        return Status::OK();
    }

    Status execute_column(VExprContext* /*context*/, const Block* /*block*/,
                          Selector* /*selector*/, size_t /*count*/,
                          ColumnPtr& result_column) const override {
        result_column = _column;
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    ColumnPtr _column;
    DataTypePtr _type;
    std::string _name = "mock_child";
};

// Helper: build a nullable Float64 column from a list of (value, is_null) pairs.
static ColumnPtr make_nullable_float64_column(const std::vector<std::pair<double, bool>>& values) {
    auto nested = ColumnFloat64::create();
    auto null_map = ColumnUInt8::create();
    for (auto& [v, is_null] : values) {
        nested->insert_value(v);
        null_map->insert_value(is_null ? 1 : 0);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

// Helper: build a non-nullable Float64 column from a list of doubles.
static ColumnPtr make_float64_column(const std::vector<double>& values) {
    auto col = ColumnFloat64::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    return col;
}

// Helper: extract the Float64 value at `row` from the result column,
// handling both nullable and non-nullable cases.
static double get_float64_value(const ColumnPtr& column, size_t row, bool* is_null = nullptr) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        if (is_null) {
            *is_null = nullable->is_null_at(row);
        }
        const auto& nested =
                assert_cast<const ColumnFloat64&>(nullable->get_nested_column()).get_data();
        return nested[row];
    }
    if (is_null) {
        *is_null = false;
    }
    return assert_cast<const ColumnFloat64*>(column.get())->get_data()[row];
}

class VConditionExprCoalesceTest : public ::testing::Test {};

// Targets the fix: after some rows of col0 are filled, NaN/Inf values in the
// same rows of later columns must not pollute the already-filled result via
// "value * 0" arithmetic.
// Input:
//   col0 (nullable, double): [1.0, NULL]
//   col1 (non-nullable, double): [NaN, 100.0]
// Expected coalesce result: [1.0, 100.0]
TEST_F(VConditionExprCoalesceTest, Float64_NaN_NotPolluteResult) {
    auto coalesce_node = make_coalesce_node(TPrimitiveType::DOUBLE, /*is_nullable=*/true);
    auto coalesce_expr = VectorizedCoalesceExpr::create_shared(coalesce_node);

    // _data_type is already set to Nullable(Float64) by the base ctor; reassert it explicitly
    // through the public data_type() accessor (the underlying field is protected).
    coalesce_expr->data_type() =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());

    const double kNaN = std::numeric_limits<double>::quiet_NaN();
    auto col0 = make_nullable_float64_column({{1.0, false}, {0.0, true}});
    auto col1 = make_float64_column({kNaN, 100.0});

    auto child0 = std::make_shared<MockChildVExpr>(
            col0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
    auto child1 = std::make_shared<MockChildVExpr>(col1, std::make_shared<DataTypeFloat64>());
    coalesce_expr->add_child(child0);
    coalesce_expr->add_child(child1);

    VExprContext context(coalesce_expr);
    ColumnPtr result;
    auto st = coalesce_expr->execute_column(&context, /*block=*/nullptr,
                                            /*selector=*/nullptr, /*count=*/2, result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(result.get() != nullptr);
    ASSERT_EQ(result->size(), 2);

    bool is_null = false;
    auto v0 = get_float64_value(result, 0, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(std::isnan(v0)) << "row0 must not be polluted by NaN, got " << v0;
    EXPECT_DOUBLE_EQ(v0, 1.0);

    auto v1 = get_float64_value(result, 1, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_DOUBLE_EQ(v1, 100.0);
}

// Same shape as above, but the polluting value in the later column is +Inf:
//   col0 (nullable, double): [2.0, NULL]
//   col1 (non-nullable, double): [+Inf, 200.0]
// Expected coalesce result: [2.0, 200.0]
TEST_F(VConditionExprCoalesceTest, Float64_Inf_NotPolluteResult) {
    auto coalesce_node = make_coalesce_node(TPrimitiveType::DOUBLE, /*is_nullable=*/true);
    auto coalesce_expr = VectorizedCoalesceExpr::create_shared(coalesce_node);
    coalesce_expr->data_type() =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());

    const double kInf = std::numeric_limits<double>::infinity();
    auto col0 = make_nullable_float64_column({{2.0, false}, {0.0, true}});
    auto col1 = make_float64_column({kInf, 200.0});

    coalesce_expr->add_child(std::make_shared<MockChildVExpr>(
            col0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())));
    coalesce_expr->add_child(
            std::make_shared<MockChildVExpr>(col1, std::make_shared<DataTypeFloat64>()));

    VExprContext context(coalesce_expr);
    ColumnPtr result;
    auto st = coalesce_expr->execute_column(&context, /*block=*/nullptr,
                                            /*selector=*/nullptr, /*count=*/2, result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result->size(), 2);

    bool is_null = false;
    auto v0 = get_float64_value(result, 0, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(std::isnan(v0)) << "row0 must not be polluted by Inf*0, got " << v0;
    EXPECT_TRUE(std::isfinite(v0));
    EXPECT_DOUBLE_EQ(v0, 2.0);

    auto v1 = get_float64_value(result, 1, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_DOUBLE_EQ(v1, 200.0);
}

// Verify that when NaN itself is the valid non-null value picked by coalesce,
// the result must preserve the NaN (the fix should not over-correct).
//   col0 (nullable, double): [NaN, NULL]
//   col1 (non-nullable, double): [10.0, 20.0]
// Expected: row0 takes col0's NaN (non-null, value is NaN); row1 takes col1's 20.0.
TEST_F(VConditionExprCoalesceTest, Float64_NaN_PreservedWhenSelected) {
    auto coalesce_node = make_coalesce_node(TPrimitiveType::DOUBLE, /*is_nullable=*/true);
    auto coalesce_expr = VectorizedCoalesceExpr::create_shared(coalesce_node);
    coalesce_expr->data_type() =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());

    const double kNaN = std::numeric_limits<double>::quiet_NaN();
    auto col0 = make_nullable_float64_column({{kNaN, false}, {0.0, true}});
    auto col1 = make_float64_column({10.0, 20.0});

    coalesce_expr->add_child(std::make_shared<MockChildVExpr>(
            col0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())));
    coalesce_expr->add_child(
            std::make_shared<MockChildVExpr>(col1, std::make_shared<DataTypeFloat64>()));

    VExprContext context(coalesce_expr);
    ColumnPtr result;
    auto st = coalesce_expr->execute_column(&context, /*block=*/nullptr,
                                            /*selector=*/nullptr, /*count=*/2, result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result->size(), 2);

    bool is_null = false;
    auto v0 = get_float64_value(result, 0, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_TRUE(std::isnan(v0)) << "row0 should preserve NaN from col0";

    auto v1 = get_float64_value(result, 1, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_DOUBLE_EQ(v1, 20.0);
}

// Float32 variant: same shape as the Float64 NaN case.
TEST_F(VConditionExprCoalesceTest, Float32_NaN_NotPolluteResult) {
    auto coalesce_node = make_coalesce_node(TPrimitiveType::FLOAT, /*is_nullable=*/true);
    auto coalesce_expr = VectorizedCoalesceExpr::create_shared(coalesce_node);
    coalesce_expr->data_type() =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>());

    const float kNaN = std::numeric_limits<float>::quiet_NaN();

    auto nested0 = ColumnFloat32::create();
    nested0->insert_value(1.5f);
    nested0->insert_value(0.0f);
    auto null_map0 = ColumnUInt8::create();
    null_map0->insert_value(0);
    null_map0->insert_value(1);
    ColumnPtr col0 = ColumnNullable::create(std::move(nested0), std::move(null_map0));

    auto nested1 = ColumnFloat32::create();
    nested1->insert_value(kNaN);
    nested1->insert_value(7.5f);
    ColumnPtr col1 = std::move(nested1);

    coalesce_expr->add_child(std::make_shared<MockChildVExpr>(
            col0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>())));
    coalesce_expr->add_child(
            std::make_shared<MockChildVExpr>(col1, std::make_shared<DataTypeFloat32>()));

    VExprContext context(coalesce_expr);
    ColumnPtr result;
    auto st = coalesce_expr->execute_column(&context, /*block=*/nullptr,
                                            /*selector=*/nullptr, /*count=*/2, result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result->size(), 2);

    auto get_f32 = [&](size_t row, bool* is_null) -> float {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(result.get())) {
            *is_null = nullable->is_null_at(row);
            return assert_cast<const ColumnFloat32&>(nullable->get_nested_column()).get_data()[row];
        }
        *is_null = false;
        return assert_cast<const ColumnFloat32*>(result.get())->get_data()[row];
    };

    bool is_null = false;
    float v0 = get_f32(0, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(std::isnan(v0)) << "row0 must not be polluted by NaN, got " << v0;
    EXPECT_FLOAT_EQ(v0, 1.5f);

    float v1 = get_f32(1, &is_null);
    EXPECT_FALSE(is_null);
    EXPECT_FLOAT_EQ(v1, 7.5f);
}

// Regression: verify that the non-floating (integer) path is unaffected by the Float branch change.
TEST_F(VConditionExprCoalesceTest, Int32_NormalPathStillWorks) {
    auto coalesce_node = make_coalesce_node(TPrimitiveType::INT, /*is_nullable=*/true);
    auto coalesce_expr = VectorizedCoalesceExpr::create_shared(coalesce_node);
    coalesce_expr->data_type() =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    auto nested0 = ColumnInt32::create();
    nested0->insert_value(11);
    nested0->insert_value(0);
    auto null_map0 = ColumnUInt8::create();
    null_map0->insert_value(0);
    null_map0->insert_value(1);
    ColumnPtr col0 = ColumnNullable::create(std::move(nested0), std::move(null_map0));

    auto nested1 = ColumnInt32::create();
    nested1->insert_value(99);
    nested1->insert_value(22);
    ColumnPtr col1 = std::move(nested1);

    coalesce_expr->add_child(std::make_shared<MockChildVExpr>(
            col0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())));
    coalesce_expr->add_child(
            std::make_shared<MockChildVExpr>(col1, std::make_shared<DataTypeInt32>()));

    VExprContext context(coalesce_expr);
    ColumnPtr result;
    auto st = coalesce_expr->execute_column(&context, /*block=*/nullptr,
                                            /*selector=*/nullptr, /*count=*/2, result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result->size(), 2);

    auto get_int = [&](size_t row, bool* is_null) -> int32_t {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(result.get())) {
            *is_null = nullable->is_null_at(row);
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column()).get_data()[row];
        }
        *is_null = false;
        return assert_cast<const ColumnInt32*>(result.get())->get_data()[row];
    };

    bool is_null = false;
    EXPECT_EQ(get_int(0, &is_null), 11);
    EXPECT_FALSE(is_null);
    EXPECT_EQ(get_int(1, &is_null), 22);
    EXPECT_FALSE(is_null);
}

} // namespace doris
