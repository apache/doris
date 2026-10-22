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

#include "exprs/vcase_expr.h"

#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <limits>
#include <string>
#include <type_traits>

#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/type_limit.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

template <typename Index, PrimitiveType PT>
struct CaseSelectionTypes {
    using IndexType = Index;
    using ColumnType = ColumnVector<PT>;
    using DataType = typename PrimitiveTypeTraits<PT>::DataType;
};

using CaseSelectionTestTypes = ::testing::Types<
        CaseSelectionTypes<uint8_t, TYPE_INT>, CaseSelectionTypes<uint8_t, TYPE_BIGINT>,
        CaseSelectionTypes<uint16_t, TYPE_INT>, CaseSelectionTypes<uint16_t, TYPE_BIGINT>,
        CaseSelectionTypes<uint8_t, TYPE_FLOAT>, CaseSelectionTypes<uint8_t, TYPE_DOUBLE>,
        CaseSelectionTypes<uint16_t, TYPE_FLOAT>, CaseSelectionTypes<uint16_t, TYPE_DOUBLE>,
        CaseSelectionTypes<uint8_t, TYPE_DATE>, CaseSelectionTypes<uint8_t, TYPE_DATETIME>,
        CaseSelectionTypes<uint8_t, TYPE_DATEV2>, CaseSelectionTypes<uint8_t, TYPE_DATETIMEV2>,
        CaseSelectionTypes<uint8_t, TYPE_TIMESTAMP_NS>,
        CaseSelectionTypes<uint8_t, TYPE_TIMESTAMPTZ>, CaseSelectionTypes<uint16_t, TYPE_DATE>,
        CaseSelectionTypes<uint16_t, TYPE_DATETIME>, CaseSelectionTypes<uint16_t, TYPE_DATEV2>,
        CaseSelectionTypes<uint16_t, TYPE_DATETIMEV2>,
        CaseSelectionTypes<uint16_t, TYPE_TIMESTAMP_NS>,
        CaseSelectionTypes<uint16_t, TYPE_TIMESTAMPTZ>>;

template <typename T>
class VCaseSelectionTest : public ::testing::Test {
protected:
    using Index = typename T::IndexType;
    using Column = typename T::ColumnType;
    using Value = typename Column::value_type;
    using Bits = std::conditional_t<sizeof(Value) == 4, uint32_t, uint64_t>;

    void check_selection(size_t rows, size_t branches, bool constant, bool mixed = false) {
        SCOPED_TRACE(::testing::Message()
                     << "rows=" << rows << " branches=" << branches << " constant=" << constant);
        TExprNode node;
        node.__set_node_type(TExprNodeType::CASE_EXPR);
        node.__set_type(typename T::DataType().to_thrift());
        node.__set_is_nullable(false);
        node.case_expr.__set_has_else_expr(true);
        VCaseExpr expr(node);
        const auto values = [] {
            if constexpr (std::is_floating_point_v<Value>) {
                // Arithmetic masking corrupts unselected infinities/NaNs, and adding to +0 loses -0.
                return std::array<Value, 9> {Value(1.25),
                                             Value(-2.5),
                                             Value(0.0),
                                             Value(-0.0),
                                             std::numeric_limits<Value>::infinity(),
                                             -std::numeric_limits<Value>::infinity(),
                                             std::numeric_limits<Value>::quiet_NaN(),
                                             std::numeric_limits<Value>::denorm_min(),
                                             std::numeric_limits<Value>::max()};
            } else {
                return std::array<Value, 3> {type_limit<Value>::min(), Column::default_value(),
                                             type_limit<Value>::max()};
            }
        }();
        std::vector<Index> indices(rows);
        for (size_t row = 0; row < rows; ++row) {
            indices[row] = row % branches;
        }
        std::vector<ColumnPtr> columns;
        for (size_t branch = 0; branch < branches; ++branch) {
            const bool branch_const = constant || (mixed && branch % 2 == 0);
            auto column = Column::create(branch_const ? 1 : rows);
            for (size_t row = 0; row < column->size(); ++row) {
                column->get_data()[row] = values[(row / branches + branch) % values.size()];
            }
            if (branch_const) {
                columns.push_back(ColumnConst::create(std::move(column), rows));
            } else {
                columns.push_back(std::move(column));
            }
        }
        auto result = expr.template _execute_update_result_impl<Index, Column>(indices.data(),
                                                                               columns, rows);
        const auto& actual = assert_cast<const Column&>(*result).get_data();
        ASSERT_EQ(actual.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            const bool branch_const = constant || (mixed && indices[row] % 2 == 0);
            const auto expected =
                    values[((branch_const ? 0 : row / branches) + indices[row]) % values.size()];
            // Compare bits to include NaN payloads, signed zero and subnormal values.
            ASSERT_EQ(std::bit_cast<Bits>(actual[row]), std::bit_cast<Bits>(expected)) << row;
        }
    }
};

TYPED_TEST_SUITE(VCaseSelectionTest, CaseSelectionTestTypes);

TYPED_TEST(VCaseSelectionTest, ValuesAndVectorTails) {
    for (size_t rows : {0, 1, 3, 7, 8, 15, 16, 31, 32, 33, 4095, 4096, 4099}) {
        this->check_selection(rows, 9, false);
    }
}

TYPED_TEST(VCaseSelectionTest, ConstantBranches) {
    for (size_t rows : {1, 31, 4099}) {
        this->check_selection(rows, 9, true);
    }
}

TYPED_TEST(VCaseSelectionTest, MixedConstantAndFullBranches) {
    for (size_t rows : {0, 1, 31, 4099}) {
        this->check_selection(rows, 9, false, true);
    }
}

TYPED_TEST(VCaseSelectionTest, MaximumAndWideBranchIndices) {
    // 255 columns still use uint8_t; 257 columns exercise indices beyond the uint8_t range.
    const size_t branches = sizeof(typename TypeParam::IndexType) == 1 ? 255 : 257;
    this->check_selection(4099, branches, false);
    this->check_selection(4099, branches, true);
}

// A VARBINARY result is selected like a string one. Values keep NUL and non-UTF-8 bytes, a
// constant branch repeats its value, and without ELSE a row that no branch selects is NULL.
class VCaseVarbinaryTest : public ::testing::Test {
protected:
    static TExprNode case_node(bool nullable, bool has_else) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::CASE_EXPR);
        node.__set_type(DataTypeVarbinary().to_thrift());
        node.__set_is_nullable(nullable);
        node.case_expr.__set_has_else_expr(has_else);
        return node;
    }

    static MutableColumnPtr varbinary_column(const std::vector<std::string>& values) {
        auto column = ColumnVarbinary::create();
        for (const auto& value : values) {
            column->insert_data(value.data(), value.size());
        }
        return column;
    }

    static std::string value_at(const IColumn& column, size_t row) {
        auto ref = column.get_data_at(row);
        return {ref.data, ref.size};
    }
};

TEST_F(VCaseVarbinaryTest, ResultWithElse) {
    VCaseExpr expr(case_node(false, true));
    const std::vector<std::string> else_values {"e0", "e1", "e2", "e3", "e4", "e5"};
    const std::vector<std::string> then_values {
            std::string("a\0b", 3), "\xff\xfe", "", "x", "\x80", "y"};
    std::vector<ColumnPtr> columns {varbinary_column(else_values), varbinary_column(then_values),
                                    ColumnConst::create(varbinary_column({"c"}), 6)};
    const std::vector<uint8_t> indices {1, 0, 2, 1, 0, 2};

    auto result = expr._execute_update_result<uint8_t>(indices.data(), columns, 6);

    ASSERT_EQ(result->size(), 6);
    ASSERT_NE(dynamic_cast<const ColumnVarbinary*>(result.get()), nullptr);
    const std::vector<std::string> expected {std::string("a\0b", 3), "e1", "c", "x", "e4", "c"};
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(value_at(*result, row), expected[row]) << row;
    }
}

TEST_F(VCaseVarbinaryTest, NullableResultWithoutElse) {
    VCaseExpr expr(case_node(true, false));
    auto null_map = ColumnUInt8::create();
    for (uint8_t is_null : {0, 1, 0, 0, 1, 0}) {
        null_map->insert_value(is_null);
    }
    std::vector<ColumnPtr> columns {
            nullptr,
            ColumnNullable::create(varbinary_column({"n0", "n1", "\xff", "n3", "n4", "n5"}),
                                   std::move(null_map)),
            varbinary_column({"p0", "p1", "p2", std::string("\0", 1), "p4", "p5"})};
    const std::vector<uint8_t> indices {1, 1, 1, 2, 0, 0};

    auto result = expr._execute_update_result<uint8_t>(indices.data(), columns, 6);

    ASSERT_EQ(result->size(), 6);
    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    ASSERT_NE(dynamic_cast<const ColumnVarbinary*>(&nullable.get_nested_column()), nullptr);
    const std::vector<bool> expected_null {false, true, false, false, true, true};
    const std::vector<std::string> expected {"n0", "", "\xff", std::string("\0", 1), "", ""};
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(nullable.is_null_at(row), expected_null[row]) << row;
        if (!expected_null[row]) {
            EXPECT_EQ(value_at(nullable.get_nested_column(), row), expected[row]) << row;
        }
    }
}

namespace {

class SimpleCaseColumnExpr final : public VExpr {
public:
    SimpleCaseColumnExpr(DataTypePtr type, ColumnPtr column)
            : VExpr(std::move(type), true), _column(std::move(column)) {}

    const std::string& expr_name() const override {
        static const std::string name = "simple_case_test_column";
        return name;
    }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector* selector, size_t count,
                               ColumnPtr& result) const override {
        ++calls;
        if (fail) {
            return Status::InvalidArgument("simple case test branch error");
        }
        result = filter_column_with_selector(_column, selector, count);
        return Status::OK();
    }

    mutable size_t calls = 0;
    bool fail = false;

private:
    ColumnPtr _column;
};

VExprSPtr simple_case_literal(const DataTypePtr& type, const Field& value) {
    return VLiteral::create_shared(type, value);
}

VCaseExpr make_simple_case(const DataTypePtr& result_type, bool has_else) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(remove_nullable(result_type)->to_thrift());
    node.__set_is_nullable(result_type->is_nullable());
    node.case_expr.__set_has_case_expr(true);
    node.case_expr.__set_has_else_expr(has_else);
    return VCaseExpr(node);
}

} // namespace

TEST(VSimpleCaseTest, NullPayloadDuplicateKeysAndNullableResults) {
    auto type = std::make_shared<DataTypeInt32>();
    auto nullable_type = make_nullable(type);
    auto input = ColumnInt32::create();
    auto nulls = ColumnUInt8::create();
    // NULL payloads deliberately equal keys and must never match.
    for (int value : {1, 1, 2, 3, 2}) {
        input->insert_value(value);
    }
    for (uint8_t value : {0, 1, 0, 0, 1}) {
        nulls->insert_value(value);
    }
    auto input_expr = std::make_shared<SimpleCaseColumnExpr>(
            nullable_type, ColumnNullable::create(std::move(input), std::move(nulls)));
    for (bool has_else : {false, true}) {
        auto expr = make_simple_case(nullable_type, has_else);
        expr.add_child(input_expr);
        size_t branch = 0;
        for (int value : {1, 1, 2}) {
            expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(value)));
            expr.add_child(simple_case_literal(
                    nullable_type,
                    value == 2 ? Field() : Field::create_field<TYPE_INT>(branch == 0 ? 11 : 99)));
            ++branch;
        }
        if (has_else) {
            expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(33)));
        }
        ColumnPtr result;
        auto status = expr.execute_column(nullptr, nullptr, nullptr, 5, result);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(result->size(), 5);
        EXPECT_EQ((*result)[0].get<TYPE_INT>(), 11);
        EXPECT_TRUE(result->is_null_at(2));
        for (size_t row : {1, 3, 4}) {
            EXPECT_EQ(result->is_null_at(row), !has_else);
            if (has_else) {
                EXPECT_EQ((*result)[row].get<TYPE_INT>(), 33);
            }
        }
    }
    EXPECT_EQ(input_expr->calls, 2);
}

TEST(VSimpleCaseTest, ConstantInputsAndEmptyBatch) {
    auto type = std::make_shared<DataTypeInt32>();
    for (bool null_input : {false, true}) {
        auto input_type = null_input ? make_nullable(type) : type;
        auto input = input_type->create_column_const(
                7, null_input ? Field() : Field::create_field<TYPE_INT>(1));
        auto input_expr = std::make_shared<SimpleCaseColumnExpr>(input_type, input);
        for (bool has_else : {false, true}) {
            auto expr = make_simple_case(make_nullable(type), has_else);
            expr.add_child(input_expr);
            expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(1)));
            expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(11)));
            if (has_else) {
                expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(33)));
            }
            for (size_t count : {0, 7}) {
                ColumnPtr result;
                auto status = expr.execute_column(nullptr, nullptr, nullptr, count, result);
                ASSERT_TRUE(status.ok()) << status.to_string();
                ASSERT_EQ(result->size(), count);
                for (size_t row = 0; row < count; ++row) {
                    EXPECT_EQ(result->is_null_at(row), null_input && !has_else);
                    if (!result->is_null_at(row)) {
                        EXPECT_EQ((*result)[row].get<TYPE_INT>(), null_input ? 33 : 11);
                    }
                }
            }
        }
        // Empty batches do not execute children.
        EXPECT_EQ(input_expr->calls, 2);
    }
}

TEST(VSimpleCaseTest, WideBranchIndexAndSelector) {
    auto type = std::make_shared<DataTypeInt32>();
    auto input = ColumnInt32::create();
    for (int value : {1, 65533, -1, 0}) {
        input->insert_value(value);
    }
    auto expr = make_simple_case(type, true);
    auto input_expr = std::make_shared<SimpleCaseColumnExpr>(type, std::move(input));
    expr.add_child(input_expr);
    for (int branch = 0; branch < 65534; ++branch) {
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(branch)));
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(branch + 1000)));
    }
    expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(-99)));
    RuntimeState state;
    VExprContext context(nullptr);
    ASSERT_TRUE(expr.prepare(&state, RowDescriptor(), &context).ok());
    ASSERT_TRUE(
            expr.open(&state, &context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    Selector selector {1, 2, 0};
    ColumnPtr result;
    auto status = expr.execute_column(&context, nullptr, &selector, selector.size(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result->size(), 3);
    EXPECT_EQ(result->get_int(0), 66533);
    EXPECT_EQ(result->get_int(1), -99);
    EXPECT_EQ(result->get_int(2), 1001);
    EXPECT_EQ(input_expr->calls, 1);
    expr.close(&context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
}

TEST(VSimpleCaseTest, StringKeysAndResults) {
    auto type = std::make_shared<DataTypeString>();
    auto input = ColumnString::create();
    const std::array<std::string, 4> values {"", "a", std::string("a\0b", 3), "other"};
    for (const auto& value : values) {
        input->insert_data(value.data(), value.size());
    }
    auto expr = make_simple_case(type, true);
    expr.add_child(std::make_shared<SimpleCaseColumnExpr>(type, std::move(input)));
    for (size_t i = 0; i < 3; ++i) {
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_STRING>(values[i])));
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_STRING>(values[i])));
    }
    expr.add_child(simple_case_literal(type, Field::create_field<TYPE_STRING>("fallback")));
    ColumnPtr result;
    auto status = expr.execute_column(nullptr, nullptr, nullptr, values.size(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (size_t i = 0; i < values.size(); ++i) {
        const std::string expected = i < 3 ? values[i] : "fallback";
        EXPECT_EQ(result->get_data_at(i), StringRef(expected));
    }
}

TEST(VSimpleCaseTest, ShadowedThenStillExecutesAndUnmatchedThenIsSkipped) {
    auto type = std::make_shared<DataTypeInt32>();
    auto input = type->create_column_const(3, Field::create_field<TYPE_INT>(1));
    auto failing = std::make_shared<SimpleCaseColumnExpr>(type, input);
    failing->fail = true;
    for (int key : {2, 1}) {
        auto expr = make_simple_case(type, true);
        expr.add_child(std::make_shared<SimpleCaseColumnExpr>(type, input));
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(1)));
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(11)));
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(key)));
        expr.add_child(failing);
        expr.add_child(simple_case_literal(type, Field::create_field<TYPE_INT>(33)));
        ColumnPtr result;
        auto status = expr.execute_column(nullptr, nullptr, nullptr, 3, result);
        EXPECT_EQ(status.ok(), key == 2) << status.to_string();
    }
    EXPECT_EQ(failing->calls, 1);
}

using SimpleCaseNumericTypes =
        ::testing::Types<std::integral_constant<PrimitiveType, TYPE_TINYINT>,
                         std::integral_constant<PrimitiveType, TYPE_SMALLINT>,
                         std::integral_constant<PrimitiveType, TYPE_INT>,
                         std::integral_constant<PrimitiveType, TYPE_BIGINT>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL32>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL64>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL128I>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL256>>;

template <typename T>
class VSimpleCaseNumericTest : public ::testing::Test {};

TYPED_TEST_SUITE(VSimpleCaseNumericTest, SimpleCaseNumericTypes);

TYPED_TEST(VSimpleCaseNumericTest, InputDispatchAndFirstMatch) {
    constexpr PrimitiveType pt = TypeParam::value;
    using Type = typename PrimitiveTypeTraits<pt>::DataType;
    using Value = typename PrimitiveTypeTraits<pt>::CppType;
    const auto type = [] {
        if constexpr (is_decimal(pt)) {
            return std::make_shared<Type>(Type::max_precision(), 6);
        } else {
            return std::make_shared<Type>();
        }
    }();
    const auto field = [](int value) {
        return Field::create_field<pt>(Value(is_decimal(pt) ? value * 1000000 : value));
    };
    auto input = type->create_column();
    for (int value : {1, 2, 3, -1}) {
        input->insert(field(value));
    }
    auto result_type = std::make_shared<DataTypeInt32>();
    auto expr = make_simple_case(result_type, true);
    expr.add_child(std::make_shared<SimpleCaseColumnExpr>(type, std::move(input)));
    for (int value : {1, 2}) {
        expr.add_child(simple_case_literal(type, field(value)));
        expr.add_child(simple_case_literal(result_type, Field::create_field<TYPE_INT>(value * 11)));
    }
    expr.add_child(simple_case_literal(result_type, Field::create_field<TYPE_INT>(33)));
    ColumnPtr result;
    auto status = expr.execute_column(nullptr, nullptr, nullptr, 4, result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::array<int, 4> expected {11, 22, 33, 33};
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(result->get_int(row), expected[row]);
    }
}

TYPED_TEST(VSimpleCaseNumericTest, NullableMatchingAcrossVectorBoundaries) {
    constexpr PrimitiveType pt = TypeParam::value;
    using Type = typename PrimitiveTypeTraits<pt>::DataType;
    using Value = typename PrimitiveTypeTraits<pt>::CppType;
    const auto type = [] {
        if constexpr (is_decimal(pt)) {
            return std::make_shared<Type>(Type::max_precision(), 6);
        } else {
            return std::make_shared<Type>();
        }
    }();
    const auto field = [](int value) {
        return Field::create_field<pt>(Value(is_decimal(pt) ? value * 1000000 : value));
    };
    constexpr size_t rows = 257;
    auto result_type = std::make_shared<DataTypeInt32>();
    for (bool has_null : {false, true}) {
        auto input = type->create_column();
        auto nulls = ColumnUInt8::create();
        for (size_t row = 0; row < rows; ++row) {
            // Include NULL payloads equal to a key, a vector tail and unmatched rows.
            input->insert(field(row % 5));
            nulls->insert_value(has_null && row % 7 == 0);
        }
        auto expr = make_simple_case(result_type, true);
        expr.add_child(std::make_shared<SimpleCaseColumnExpr>(
                make_nullable(type), ColumnNullable::create(std::move(input), std::move(nulls))));
        const std::array<int, 3> keys {1, 1, 2};
        const std::array<int, 3> values {11, 99, 22};
        for (size_t branch = 0; branch < keys.size(); ++branch) {
            expr.add_child(simple_case_literal(type, field(keys[branch])));
            expr.add_child(simple_case_literal(result_type,
                                               Field::create_field<TYPE_INT>(values[branch])));
        }
        expr.add_child(simple_case_literal(result_type, Field::create_field<TYPE_INT>(33)));
        ColumnPtr result;
        auto status = expr.execute_column(nullptr, nullptr, nullptr, rows, result);
        ASSERT_TRUE(status.ok()) << status.to_string();
        for (size_t row = 0; row < rows; ++row) {
            const bool matches = !(has_null && row % 7 == 0) && (row % 5 == 1 || row % 5 == 2);
            EXPECT_EQ(result->get_int(row), matches ? static_cast<int>(row % 5) * 11 : 33)
                    << "row=" << row << ", has_null=" << has_null;
        }
    }
}

using SimpleCaseLookupTypes =
        ::testing::Types<std::integral_constant<PrimitiveType, TYPE_BOOLEAN>,
                         std::integral_constant<PrimitiveType, TYPE_TINYINT>,
                         std::integral_constant<PrimitiveType, TYPE_SMALLINT>,
                         std::integral_constant<PrimitiveType, TYPE_INT>,
                         std::integral_constant<PrimitiveType, TYPE_BIGINT>,
                         std::integral_constant<PrimitiveType, TYPE_LARGEINT>,
                         std::integral_constant<PrimitiveType, TYPE_FLOAT>,
                         std::integral_constant<PrimitiveType, TYPE_DOUBLE>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL32>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL64>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL128I>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMAL256>,
                         std::integral_constant<PrimitiveType, TYPE_DECIMALV2>,
                         std::integral_constant<PrimitiveType, TYPE_STRING>>;

template <typename T>
class VSimpleCaseLookupTest : public ::testing::Test {};

TYPED_TEST_SUITE(VSimpleCaseLookupTest, SimpleCaseLookupTypes);

void check_simple_case_lookup_result(const ColumnPtr& result, const std::vector<int>& expected) {
    ASSERT_EQ(result->size(), expected.size());
    for (size_t row = 0; row < result->size(); ++row) {
        EXPECT_EQ(result->get_int(row), expected[row]) << row;
    }
}

void check_simple_case_lookup_context(const VExprSPtr& expr,
                                      const std::shared_ptr<SimpleCaseColumnExpr>& shadowed,
                                      const std::vector<int>& expected) {
    const size_t rows = expected.size();
    RuntimeState state;
    VExprContext context(expr);
    auto status = context.prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = context.open(&state);
    ASSERT_TRUE(status.ok()) << status.to_string();
    VExprContextSPtr clone;
    status = context.clone(&state, clone);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (auto* active : {&context, clone.get()}) {
        ColumnPtr result;
        status = expr->execute_column(active, nullptr, nullptr, rows, result);
        ASSERT_TRUE(status.ok()) << status.to_string();
        check_simple_case_lookup_result(result, expected);
    }
    // Lookup must preserve non-short-circuit evaluation for duplicate keys,
    // even though the first branch still determines the selected result.
    EXPECT_EQ(shadowed->calls, 2);
    shadowed->fail = true;
    ColumnPtr result;
    status = expr->execute_column(&context, nullptr, nullptr, rows, result);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("simple case test branch error"), std::string::npos);
    EXPECT_EQ(shadowed->calls, 3);
}

template <PrimitiveType pt>
void check_simple_case_lookup(size_t branches, bool repeated_limbs = false) {
    SCOPED_TRACE(branches);
    using Type = typename PrimitiveTypeTraits<pt>::DataType;
    const auto type = [] {
        if constexpr (is_decimal(pt)) {
            return std::make_shared<Type>(Type::max_precision(), 0);
        } else {
            return std::make_shared<Type>();
        }
    }();
    auto result_type = std::make_shared<DataTypeInt32>();
    const auto key = [repeated_limbs](int value) {
        if constexpr (pt == TYPE_STRING) {
            // Exercise empty, embedded-NUL and non-inline string keys.
            std::string text = value == 0 ? "" : std::string(40, 'x') + std::to_string(value);
            if (value == 1) {
                text = std::string("a\0b", 3);
            }
            return Field::create_field<TYPE_STRING>(text);
        } else if constexpr (pt == TYPE_DECIMAL128I || pt == TYPE_DECIMAL256) {
            using Value = typename PrimitiveTypeTraits<pt>::CppType;
            using Native = typename Value::NativeType;
            Native raw(value);
            if (repeated_limbs) {
                // Equal limbs cancel out in the former XOR-based decimal hash.
                raw |= Native(value) << 64;
                if constexpr (pt == TYPE_DECIMAL256) {
                    raw |= Native(value) << 128;
                    raw |= Native(value) << 192;
                }
            }
            return Field::create_field<pt>(Value(raw));
        } else {
            return Field::create_field<pt>(value);
        }
    };
    const size_t rows = std::max(size_t(257), branches + 1);
    std::vector<int> expected(rows, -1);
    auto input = type->create_column();
    auto nulls = ColumnUInt8::create();
    for (size_t row = 0; row < rows; ++row) {
        const auto input_key = key(row % (branches + 1));
        input->insert(input_key);
        nulls->insert_value(row % 7 == 0);
        if (row % 7 != 0) {
            for (size_t branch = 0; branch < branches; ++branch) {
                if (key(branch == branches - 1 ? 1 : branch) == input_key) {
                    expected[row] = branch + 1;
                    break;
                }
            }
        }
    }
    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(result_type->to_thrift());
    node.__set_is_nullable(false);
    node.case_expr.__set_has_case_expr(true);
    node.case_expr.__set_has_else_expr(true);
    auto expr = VCaseExpr::create_shared(node);
    expr->add_child(std::make_shared<SimpleCaseColumnExpr>(
            make_nullable(type), ColumnNullable::create(std::move(input), std::move(nulls))));
    auto shadowed = std::make_shared<SimpleCaseColumnExpr>(
            result_type, result_type->create_column_const(rows, Field::create_field<TYPE_INT>(99)));
    for (size_t branch = 0; branch < branches; ++branch) {
        const int value = branch == branches - 1 ? 1 : branch;
        expr->add_child(simple_case_literal(type, key(value)));
        if (branch == branches - 1) {
            expr->add_child(shadowed);
        } else {
            expr->add_child(
                    simple_case_literal(result_type, Field::create_field<TYPE_INT>(branch + 1)));
        }
    }
    expr->add_child(simple_case_literal(result_type, Field::create_field<TYPE_INT>(-1)));
    check_simple_case_lookup_context(expr, shadowed, expected);
}

TYPED_TEST(VSimpleCaseLookupTest, NullableDuplicatesAndClonedContext) {
    constexpr auto pt = TypeParam::value;
    using Key = typename PrimitiveTypeTraits<pt>::CppType;
    constexpr size_t threshold = sizeof(Key) <= sizeof(uint32_t)   ? 64
                                 : sizeof(Key) <= sizeof(uint64_t) ? 32
                                                                   : 8;
    for (size_t branches : {threshold - 1, threshold, size_t(256), size_t(257), size_t(1024)}) {
        check_simple_case_lookup<pt>(branches);
    }
}

TYPED_TEST(VSimpleCaseLookupTest, RepeatedLimbWideDecimalKeys) {
    constexpr auto pt = TypeParam::value;
    if constexpr (pt == TYPE_DECIMAL128I || pt == TYPE_DECIMAL256) {
        for (size_t branches : {256, 1024}) {
            check_simple_case_lookup<pt>(branches, true);
        }
    }
}

} // namespace doris
