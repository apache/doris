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

#include "common/check.h"
#include "core/accurate_comparison.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/functions_comparison.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

/// Recursive equality for a value the planner cannot compare as one expression, for example a
/// STRUCT with a float leaf whose JSON text is not a lossless identity. The comparison is NULL
/// safe: two NULL values (including a NULL nested field) are equal, and a NULL and a non-NULL
/// value are not.
///
/// The planner emits this function to keep the typed search in one place. Expanding the fields in
/// the expression tree instead embeds the search cast in every field comparison, and each
/// occurrence parses the whole search string again; an N field STRUCT then converts N fields N+1
/// times per row. Column::compare_at already applies the nested NULL rule and compares every
/// column type structurally, so the function only has to unwrap the two top level null maps.
class FunctionComplexEquals : public IFunction {
public:
    static constexpr auto name = "complex_equals";
    static FunctionPtr create() { return std::make_shared<FunctionComplexEquals>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeBool>();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr left =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnPtr right =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        DORIS_CHECK(left->size() == input_rows_count);
        DORIS_CHECK(right->size() == input_rows_count);

        auto result_column = ColumnUInt8::create();
        auto& result_data = result_column->get_data();
        result_data.resize(input_rows_count);
        for (size_t row = 0; row != input_rows_count; ++row) {
            result_data[row] = equals_at(*left, row, *right, row) ? 1 : 0;
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    static bool equals_at(const IColumn& left, size_t left_row, const IColumn& right,
                          size_t right_row) {
        const auto* left_nullable = check_and_get_column<ColumnNullable>(left);
        const auto* right_nullable = check_and_get_column<ColumnNullable>(right);
        const bool left_is_null = left_nullable != nullptr && left_nullable->is_null_at(left_row);
        const bool right_is_null =
                right_nullable != nullptr && right_nullable->is_null_at(right_row);
        if (left_is_null || right_is_null) {
            return left_is_null && right_is_null;
        }
        const IColumn& left_nested =
                left_nullable != nullptr ? left_nullable->get_nested_column() : left;
        const IColumn& right_nested =
                right_nullable != nullptr ? right_nullable->get_nested_column() : right;
        return left_nested.compare_at(left_row, right_row, right_nested, 1) == 0;
    }
};

void register_function_comparison_equals(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEquals>();
    factory.register_function<FunctionNotEquals>();
    factory.register_function<FunctionComplexEquals>();
}
} // namespace doris
