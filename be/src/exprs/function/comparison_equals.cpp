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
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/cast/cast_base.h"
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
/// times per row. Normalize native MAP duplicates on private copies first; Column::compare_at
/// then applies the nested NULL rule and structural comparison. Top-level NULL maps are handled
/// separately so a NULL STRUCT differs from a non-NULL STRUCT whose fields are all NULL.
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
        const auto& [left_data, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_data, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        ColumnPtr left = left_data;
        ColumnPtr right = right_data;
        DORIS_CHECK(left->size() == (left_const ? 1 : input_rows_count));
        DORIS_CHECK(right->size() == (right_const ? 1 : input_rows_count));
        // A constant search may contain a large ARRAY; expanding it would copy the whole payload
        // once per source row. Normalize only its single stored row, then compare against row 0.
        RETURN_IF_ERROR(normalize_maps(left));
        RETURN_IF_ERROR(normalize_maps(right));

        auto result_column = ColumnUInt8::create();
        auto& result_data = result_column->get_data();
        result_data.resize(input_rows_count);
        for (size_t row = 0; row != input_rows_count; ++row) {
            result_data[row] =
                    equals_at(*left, left_const ? 0 : row, *right, right_const ? 0 : row) ? 1 : 0;
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    static bool contains_map(const IColumn& column) {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
            return contains_map(nullable->get_nested_column());
        }
        if (const auto* array = check_and_get_column<ColumnArray>(column)) {
            return contains_map(array->get_data());
        }
        if (const auto* st = check_and_get_column<ColumnStruct>(column)) {
            for (size_t i = 0; i < st->tuple_size(); ++i) {
                if (contains_map(st->get_column(i))) {
                    return true;
                }
            }
        }
        return check_and_get_column<ColumnMap>(column) != nullptr;
    }

    static Status normalize_maps(ColumnPtr& column) {
        // Native Arrow MAPs can have duplicate keys, including inside a STRUCT. Compare the
        // last-wins value that the sink writes without modifying a shared source column. Types
        // without MAPs need no copy at all; this includes large constant ARRAY searches.
        if (contains_map(*column)) {
            auto normalized = column->clone_resized(column->size());
            RETURN_IF_ERROR(CastWrapper::deduplicate_map_keys(*normalized));
            column = std::move(normalized);
        }
        return Status::OK();
    }

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
