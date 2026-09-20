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
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/type_limit.h"

namespace doris {

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

namespace {

// CASE WHEN <cond> THEN <then> ELSE <else> END over nullable BIGINT branches, driven directly
// through _execute_impl so the WHEN column can carry arbitrary bytes.
class VCaseConditionBytesTest : public ::testing::Test {
protected:
    static VCaseExpr make_expr() {
        TExprNode node;
        node.__set_node_type(TExprNodeType::CASE_EXPR);
        node.__set_type(DataTypeInt64().to_thrift());
        node.__set_is_nullable(true);
        node.case_expr.__set_has_else_expr(true);
        return VCaseExpr(node);
    }

    static ColumnPtr nullable_bigint(const std::vector<int64_t>& values) {
        auto nested = ColumnInt64::create();
        for (const auto value : values) {
            nested->insert_value(value);
        }
        return ColumnNullable::create(std::move(nested), ColumnUInt8::create(values.size(), 0));
    }

    // Evaluates the CASE and returns which branch each row took: 1 = THEN, 0 = ELSE.
    static std::vector<int> run(const ColumnPtr& when_column, size_t rows) {
        auto expr = make_expr();
        std::vector<int64_t> then_values(rows);
        std::vector<int64_t> else_values(rows);
        for (size_t row = 0; row < rows; ++row) {
            then_values[row] = 1000 + static_cast<int64_t>(row);
            else_values[row] = -1000 - static_cast<int64_t>(row);
        }
        const std::vector<ColumnPtr> when_columns {when_column};
        std::vector<ColumnPtr> then_columns {nullable_bigint(else_values),
                                             nullable_bigint(then_values)};
        auto result = expr._execute_impl<uint8_t>(when_columns, then_columns, rows);
        EXPECT_EQ(result->size(), rows);
        const auto& nullable = assert_cast<const ColumnNullable&>(*result);
        const auto& data = assert_cast<const ColumnInt64&>(nullable.get_nested_column()).get_data();
        std::vector<int> branches(rows);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(nullable.is_null_at(row)) << row;
            if (data[row] == then_values[row]) {
                branches[row] = 1;
            } else if (data[row] == else_values[row]) {
                branches[row] = 0;
            } else {
                ADD_FAILURE() << "row " << row << " holds " << data[row]
                              << ", which belongs to no branch";
                branches[row] = -1;
            }
        }
        return branches;
    }
};

// A boolean produced by CAST(jsonb AS BOOLEAN) OR <expr> can carry a stale non-zero payload in
// rows whose CAST result was NULL. Any non-zero byte means TRUE; it must never be used as an
// arithmetic factor when selecting the branch, or the index runs past then_columns.
TEST_F(VCaseConditionBytesTest, NonCanonicalTrueBytesSelectTheThenBranch) {
    auto when = ColumnUInt8::create();
    const std::vector<uint8_t> bytes {1, 0, 0x41, 0xFE, 0, 1, 0x80, 2};
    for (const auto byte : bytes) {
        when->insert_value(byte);
    }
    const auto branches = run(std::move(when), bytes.size());
    for (size_t row = 0; row < bytes.size(); ++row) {
        EXPECT_EQ(branches[row], bytes[row] != 0 ? 1 : 0) << row;
    }
}

TEST_F(VCaseConditionBytesTest, NonCanonicalBytesUnderNullMap) {
    auto nested = ColumnUInt8::create();
    auto null_map = ColumnUInt8::create();
    const std::vector<uint8_t> bytes {0x41, 0x41, 1, 0, 0x7F, 0};
    const std::vector<uint8_t> nulls {1, 0, 0, 0, 1, 1};
    for (size_t row = 0; row < bytes.size(); ++row) {
        nested->insert_value(bytes[row]);
        null_map->insert_value(nulls[row]);
    }
    const auto branches =
            run(ColumnNullable::create(std::move(nested), std::move(null_map)), bytes.size());
    for (size_t row = 0; row < bytes.size(); ++row) {
        // NULL conditions never select the branch, whatever payload they carry.
        EXPECT_EQ(branches[row], (!nulls[row] && bytes[row] != 0) ? 1 : 0) << row;
    }
}

} // namespace

} // namespace doris
