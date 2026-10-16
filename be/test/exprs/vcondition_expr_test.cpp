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

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vcase_expr.h"
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

    Status execute_column(VExprContext* /*context*/, const Block* /*block*/, Selector* /*selector*/,
                          size_t /*count*/, ColumnPtr& result_column) const override {
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

TEST_F(VConditionExprCoalesceTest, FilePreservesSixFieldsAndNulls) {
    auto type = std::make_shared<DataTypeFile>();
    auto nullable_type = make_nullable(type);
    auto node = make_coalesce_node(TPrimitiveType::STRING, true);
    node.type.types.clear();
    type->to_thrift(node.type);
    auto expression = VectorizedCoalesceExpr::create_shared(node);
    expression->data_type() = nullable_type;
    const std::string bytes("\0\xff\x80\n", 4);
    File fields {Field::create_field<TYPE_STRING>("urn:first"),
                 Field::create_field<TYPE_BIGINT>(3),
                 Field::create_field<TYPE_BIGINT>(4),
                 Field::create_field<TYPE_STRING>("application/octet-stream"),
                 Field::create_field<TYPE_STRING>("ETAG:opaque-2"),
                 Field::create_field<TYPE_VARBINARY>(StringView(bytes))};
    auto first = Field::create_field<TYPE_FILE>(fields);
    fields[0] = Field::create_field<TYPE_STRING>("urn:empty");
    fields[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    auto empty = Field::create_field<TYPE_FILE>(fields);
    fields[0] = Field::create_field<TYPE_STRING>("urn:no-inline");
    fields[5] = Field();
    auto no_inline = Field::create_field<TYPE_FILE>(fields);
    const std::vector<Field> expected {first, empty, no_inline, Field()};
    for (size_t source = 0; source < 3; ++source) {
        auto column = nullable_type->create_column();
        for (size_t row = 0; row < expected.size(); ++row) {
            column->insert(row == source ? expected[row] : Field());
        }
        expression->add_child(std::make_shared<MockChildVExpr>(std::move(column), nullable_type));
    }
    VExprContext context(expression);
    ColumnPtr result;
    auto status =
            expression->execute_column_impl(&context, nullptr, nullptr, expected.size(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(expected.size(), result->size());
    EXPECT_TRUE(result->is_null_at(3));
    for (size_t row = 0; row < 3; ++row) {
        const auto value = (*result)[row];
        const auto& actual_fields = value.get<TYPE_FILE>();
        const auto& expected_fields = expected[row].get<TYPE_FILE>();
        for (size_t child = 0; child < 5; ++child) {
            EXPECT_EQ(expected_fields[child], actual_fields[child]);
        }
        EXPECT_EQ(expected_fields[5].is_null(), actual_fields[5].is_null());
        if (!expected_fields[5].is_null()) {
            EXPECT_EQ(expected_fields[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
                      actual_fields[5].get<TYPE_VARBINARY>().to_string_ref().to_string());
        }
    }
}

TEST_F(VConditionExprCoalesceTest, FileNonNullableResultUsesConstantFallback) {
    auto type = std::make_shared<DataTypeFile>();
    auto nullable_type = make_nullable(type);
    auto node = make_coalesce_node(TPrimitiveType::STRING, false);
    node.type.types.clear();
    type->to_thrift(node.type);
    auto expression = VectorizedCoalesceExpr::create_shared(node);
    expression->data_type() = type;
    File fields(6);
    fields[0] = Field::create_field<TYPE_STRING>("urn:present");
    auto first = nullable_type->create_column();
    first->insert(Field::create_field<TYPE_FILE>(fields));
    first->insert_default();
    fields[0] = Field::create_field<TYPE_STRING>("urn:fallback");
    auto fallback = type->create_column();
    fallback->insert(Field::create_field<TYPE_FILE>(fields));
    expression->add_child(std::make_shared<MockChildVExpr>(std::move(first), nullable_type));
    expression->add_child(
            std::make_shared<MockChildVExpr>(ColumnConst::create(std::move(fallback), 2), type));
    VExprContext context(expression);
    ColumnPtr result;
    auto status = expression->execute_column_impl(&context, nullptr, nullptr, 2, result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(result->is_nullable());
    ASSERT_EQ(2, result->size());
    EXPECT_EQ("urn:present", (*result)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>());
    EXPECT_EQ("urn:fallback", (*result)[1].get<TYPE_FILE>()[0].get<TYPE_STRING>());
}

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

TEST(VCaseExprTest, FileBranchesPreserveValuesAndNulls) {
    const auto file_type = std::make_shared<DataTypeFile>();
    const auto file_value = [](const std::string& uri) {
        const auto bytes = uri + std::string("\0\xff", 2);
        return Field::create_field<TYPE_FILE>(
                File {Field::create_field<TYPE_STRING>(uri), Field(), Field(), Field(),
                      Field::create_field<TYPE_STRING>("ETAG:" + uri),
                      Field::create_field<TYPE_VARBINARY>(StringView(bytes))});
    };
    struct Scenario {
        bool nullable;
        bool constant;
        bool has_else;
        bool null_branch;
    };
    const Scenario scenarios[] {{false, false, true, false}, {false, true, true, false},
                                {true, false, true, false},  {true, true, true, false},
                                {true, true, true, true},    {true, false, false, false}};
    constexpr size_t rows = 4;
    const uint8_t indices[] {0, 1, 1, 0};
    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(::testing::Message() << "nullable=" << scenario.nullable << " constant="
                                          << scenario.constant << " has_else=" << scenario.has_else
                                          << " null_branch=" << scenario.null_branch);
        const auto result_type = scenario.nullable ? make_nullable(file_type) : file_type;
        TExprNode node;
        node.__set_node_type(TExprNodeType::CASE_EXPR);
        node.__set_type(file_type->to_thrift());
        node.__set_is_nullable(scenario.nullable);
        node.case_expr.__set_has_else_expr(scenario.has_else);
        VCaseExpr expression(node);

        std::vector<ColumnPtr> columns {nullptr};
        if (scenario.has_else) {
            auto otherwise = file_type->create_column();
            for (size_t row = 0; row < rows; ++row) {
                otherwise->insert(file_value("urn:left"));
            }
            columns[0] = std::move(otherwise);
        }
        auto branch = result_type->create_column();
        for (size_t row = 0; row < (scenario.constant ? 1 : rows); ++row) {
            if (scenario.nullable && (scenario.null_branch || (!scenario.constant && row == 1))) {
                branch->insert_default();
            } else {
                branch->insert(file_value("urn:right"));
            }
        }
        if (scenario.constant) {
            columns.push_back(ColumnConst::create(std::move(branch), rows));
        } else {
            columns.push_back(std::move(branch));
        }
        auto result = expression._execute_update_result<uint8_t>(indices, columns, rows);
        columns.clear();
        ASSERT_EQ(result->size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            const bool expected_null =
                    indices[row] == 0 ? !scenario.has_else
                                      : scenario.nullable && (scenario.null_branch ||
                                                              (!scenario.constant && row == 1));
            ASSERT_EQ(result->is_null_at(row), expected_null) << row;
            if (expected_null) {
                continue;
            }
            const std::string uri = indices[row] == 0 ? "urn:left" : "urn:right";
            const auto value = (*result)[row];
            const auto& file = value.get<TYPE_FILE>();
            EXPECT_EQ(file[0].get<TYPE_STRING>(), uri);
            EXPECT_TRUE(file[1].is_null());
            EXPECT_TRUE(file[2].is_null());
            EXPECT_TRUE(file[3].is_null());
            EXPECT_EQ(file[4].get<TYPE_STRING>(), "ETAG:" + uri);
            EXPECT_EQ(file[5].get<TYPE_VARBINARY>().str(), uri + std::string("\0\xff", 2));
        }
    }
}

} // namespace doris
