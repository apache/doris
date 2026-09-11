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
#include <type_traits>

#include "core/data_type/data_type_number.h"

namespace doris {

template <typename Index, PrimitiveType PT>
struct CaseFloatTypes {
    using IndexType = Index;
    using ColumnType = ColumnVector<PT>;
    using DataType = DataTypeNumber<PT>;
};

using CaseFloatTestTypes =
        ::testing::Types<CaseFloatTypes<uint8_t, TYPE_FLOAT>, CaseFloatTypes<uint8_t, TYPE_DOUBLE>,
                         CaseFloatTypes<uint16_t, TYPE_FLOAT>,
                         CaseFloatTypes<uint16_t, TYPE_DOUBLE>>;

template <typename T>
class VCaseFloatTest : public ::testing::Test {
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
        // Arithmetic masking corrupts unselected infinities/NaNs, and adding to +0 loses -0.
        const std::array<Value, 9> values = {Value(1.25),
                                             Value(-2.5),
                                             Value(0.0),
                                             Value(-0.0),
                                             std::numeric_limits<Value>::infinity(),
                                             -std::numeric_limits<Value>::infinity(),
                                             std::numeric_limits<Value>::quiet_NaN(),
                                             std::numeric_limits<Value>::denorm_min(),
                                             std::numeric_limits<Value>::max()};
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

TYPED_TEST_SUITE(VCaseFloatTest, CaseFloatTestTypes);

TYPED_TEST(VCaseFloatTest, NonFiniteValuesAndVectorTails) {
    for (size_t rows : {0, 1, 3, 7, 8, 15, 16, 31, 32, 33, 4095, 4096, 4099}) {
        this->check_selection(rows, 9, false);
    }
}

TYPED_TEST(VCaseFloatTest, ConstantBranches) {
    for (size_t rows : {1, 31, 4099}) {
        this->check_selection(rows, 9, true);
    }
}

TYPED_TEST(VCaseFloatTest, MaximumAndWideBranchIndices) {
    // 255 columns still use uint8_t; 257 columns exercise indices beyond the uint8_t range.
    const size_t branches = sizeof(typename TypeParam::IndexType) == 1 ? 255 : 257;
    this->check_selection(4099, branches, false);
    this->check_selection(4099, branches, true);
}

} // namespace doris
