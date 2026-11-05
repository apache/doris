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
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/type_limit.h"

namespace doris {

static void check_spatial_case_materialization(PrimitiveType primitive, bool nullable,
                                               bool constant) {
    const auto spatial = std::make_shared<DataTypeSpatial>(
            primitive, "OGC:CRS84", primitive == TYPE_GEOGRAPHY ? "spherical" : "");
    const DataTypePtr result_type = nullable ? make_nullable(spatial) : spatial;
    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(result_type->to_thrift());
    node.__set_is_nullable(nullable);
    node.case_expr.__set_has_else_expr(true);
    VCaseExpr expr(node);
    const std::array<uint8_t, 6> selected {1, 2, 0, 1, 2, 0};
    std::vector<ColumnPtr> branches;
    std::vector<std::string> payloads;
    const size_t null_row = constant ? 0 : 1;
    for (size_t branch = 0; branch < 3; ++branch) {
        std::string wkb(
                "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@",
                21);
        wkb[11] = static_cast<char>(0xf0 - branch * 0x10);
        payloads.push_back(wkb);
        const DataTypePtr type = nullable && branch == 2 ? make_nullable(spatial) : spatial;
        auto column = type->create_column();
        for (size_t row = 0; row < (constant ? 1 : selected.size()); ++row) {
            if (nullable && branch == 2 && row == null_row) {
                column->insert_default();
            } else {
                column->insert_data(wkb.data(), wkb.size());
            }
        }
        if (constant) {
            branches.emplace_back(ColumnConst::create(std::move(column), selected.size()));
        } else {
            branches.emplace_back(std::move(column));
        }
    }
    auto result = expr._execute_update_result(selected.data(), branches, selected.size());
    ASSERT_EQ(result->size(), selected.size());
    for (size_t row = 0; row < selected.size(); ++row) {
        const bool is_null = nullable && selected[row] == 2 && (constant || row == 1);
        EXPECT_EQ(result->is_null_at(row), is_null) << row;
        if (!is_null) {
            EXPECT_EQ(result->get_data_at(row).to_string(), payloads[selected[row]]) << row;
        }
    }
}

TEST(VCaseSpatialTest, MaterializesGeometryAndGeographyBranches) {
    check_spatial_case_materialization(TYPE_GEOMETRY, false, false);
    check_spatial_case_materialization(TYPE_GEOGRAPHY, false, false);
}

TEST(VCaseSpatialTest, MaterializesNullableAndConstantBranches) {
    check_spatial_case_materialization(TYPE_GEOMETRY, true, false);
    check_spatial_case_materialization(TYPE_GEOGRAPHY, true, false);
    check_spatial_case_materialization(TYPE_GEOMETRY, true, true);
    check_spatial_case_materialization(TYPE_GEOGRAPHY, true, true);
}

template <typename Index, PrimitiveType PT>
struct CaseSelectionTypes {
    using IndexType = Index;
    using ColumnType = ColumnVector<PT>;
    using DataType = typename PrimitiveTypeTraits<PT>::DataType;
};

using CaseSelectionTestTypes = ::testing::Types<
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

    void check_selection(size_t rows, size_t branches, bool constant) {
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
            auto column = Column::create(constant ? 1 : rows);
            for (size_t row = 0; row < column->size(); ++row) {
                column->get_data()[row] = values[(row / branches + branch) % values.size()];
            }
            if (constant) {
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
            const auto expected =
                    values[((constant ? 0 : row / branches) + indices[row]) % values.size()];
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

} // namespace doris
