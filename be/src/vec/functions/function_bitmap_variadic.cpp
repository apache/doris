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

#include <stddef.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// currently only bitmap_or and bitmap_or_count will call this function,
// other bitmap functions will use default implementation for nulls
#define BITMAP_OR_NULLABLE(nullable, input_rows_count, res, op)                                \
    const auto& nested_col_ptr = nullable->get_nested_column_ptr();                            \
    const auto* __restrict null_map_data = nullable->get_null_map_data().data();               \
    const auto& mid_data = assert_cast<const ColumnBitmap*>(nested_col_ptr.get())->get_data(); \
    for (size_t row = 0; row < input_rows_count; ++row) {                                      \
        if (!null_map_data[row]) {                                                             \
            res[row] op mid_data[row];                                                         \
        }                                                                                      \
    }

#define BITMAP_FUNCTION_VARIADIC(CLASS, FUNCTION_NAME, OP)                                        \
    struct CLASS {                                                                                \
        static constexpr auto name = #FUNCTION_NAME;                                              \
        using ResultDataType = DataTypeBitMap;                                                    \
        static Status vector_vector(ColumnPtr argument_columns[], size_t col_size,                \
                                    size_t input_rows_count, std::vector<BitmapValue>& res,       \
                                    IColumn* res_nulls) {                                         \
            std::vector<const ColumnUInt8::value_type*> null_map_datas(col_size);                 \
            int nullable_cols_count = 0;                                                          \
            ColumnUInt8::value_type* __restrict res_nulls_data = nullptr;                         \
            if (res_nulls) {                                                                      \
                res_nulls_data = assert_cast<ColumnUInt8*>(res_nulls)->get_data().data();         \
            }                                                                                     \
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[0])) {    \
                null_map_datas[nullable_cols_count++] = nullable->get_null_map_data().data();     \
                BITMAP_OR_NULLABLE(nullable, input_rows_count, res, =);                           \
            } else {                                                                              \
                const auto& mid_data =                                                            \
                        assert_cast<const ColumnBitmap*>(argument_columns[0].get())->get_data();  \
                for (size_t row = 0; row < input_rows_count; ++row) {                             \
                    res[row] = mid_data[row];                                                     \
                }                                                                                 \
            }                                                                                     \
            for (size_t col = 1; col < col_size; ++col) {                                         \
                if (auto* nullable =                                                              \
                            check_and_get_column<ColumnNullable>(*argument_columns[col])) {       \
                    null_map_datas[nullable_cols_count++] = nullable->get_null_map_data().data(); \
                    BITMAP_OR_NULLABLE(nullable, input_rows_count, res, OP);                      \
                } else {                                                                          \
                    const auto& col_data =                                                        \
                            assert_cast<const ColumnBitmap*>(argument_columns[col].get())         \
                                    ->get_data();                                                 \
                    for (size_t row = 0; row < input_rows_count; ++row) {                         \
                        res[row] OP col_data[row];                                                \
                    }                                                                             \
                }                                                                                 \
            }                                                                                     \
            if (res_nulls_data && nullable_cols_count == col_size) {                              \
                const auto* null_map_data = null_map_datas[0];                                    \
                for (size_t row = 0; row < input_rows_count; ++row) {                             \
                    res_nulls_data[row] = null_map_data[row];                                     \
                }                                                                                 \
                for (int i = 1; i < nullable_cols_count; ++i) {                                   \
                    const auto* null_map_data = null_map_datas[i];                                \
                    for (size_t row = 0; row < input_rows_count; ++row) {                         \
                        res_nulls_data[row] &= null_map_data[row];                                \
                    }                                                                             \
                }                                                                                 \
            }                                                                                     \
            return Status::OK();                                                                  \
        }                                                                                         \
    }

#define BITMAP_FUNCTION_COUNT_VARIADIC(CLASS, FUNCTION_NAME, OP)                                  \
    struct CLASS {                                                                                \
        static constexpr auto name = #FUNCTION_NAME;                                              \
        using ResultDataType = DataTypeInt64;                                                     \
        using TData = std::vector<BitmapValue>;                                                   \
        using ResTData = typename ColumnVector<Int64>::Container;                                 \
        static Status vector_vector(ColumnPtr argument_columns[], size_t col_size,                \
                                    size_t input_rows_count, ResTData& res, IColumn* res_nulls) { \
            TData vals;                                                                           \
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[0])) {    \
                vals.resize(input_rows_count);                                                    \
                BITMAP_OR_NULLABLE(nullable, input_rows_count, vals, =);                          \
            } else {                                                                              \
                vals = assert_cast<const ColumnBitmap*>(argument_columns[0].get())->get_data();   \
            }                                                                                     \
            for (size_t col = 1; col < col_size; ++col) {                                         \
                if (auto* nullable =                                                              \
                            check_and_get_column<ColumnNullable>(*argument_columns[col])) {       \
                    BITMAP_OR_NULLABLE(nullable, input_rows_count, vals, OP);                     \
                } else {                                                                          \
                    const auto& col_data =                                                        \
                            assert_cast<const ColumnBitmap*>(argument_columns[col].get())         \
                                    ->get_data();                                                 \
                    for (size_t row = 0; row < input_rows_count; ++row) {                         \
                        vals[row] OP col_data[row];                                               \
                    }                                                                             \
                }                                                                                 \
            }                                                                                     \
            for (size_t row = 0; row < input_rows_count; ++row) {                                 \
                res[row] = vals[row].cardinality();                                               \
            }                                                                                     \
            return Status::OK();                                                                  \
        }                                                                                         \
    }

BITMAP_FUNCTION_VARIADIC(BitmapOr, bitmap_or, |=);
BITMAP_FUNCTION_VARIADIC(BitmapAnd, bitmap_and, &=);
BITMAP_FUNCTION_VARIADIC(BitmapXor, bitmap_xor, ^=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapOrCount, bitmap_or_count, |=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapAndCount, bitmap_and_count, &=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapXorCount, bitmap_xor_count, ^=);

Status execute_bitmap_op_count_null_to_zero(
        FunctionContext* context, Block& block, const ColumnNumbers& arguments, size_t result,
        size_t input_rows_count,
        const std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)>&
                exec_impl_func);

template <typename Impl>
class FunctionBitMapVariadic : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionBitMapVariadic>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        using ResultDataType = typename Impl::ResultDataType;
        if (std::is_same_v<Impl, BitmapOr> || is_count()) {
            bool return_nullable = false;
            // result is nullable only when any columns is nullable for bitmap_or and bitmap_or_count
            for (size_t i = 0; i < arguments.size(); ++i) {
                if (arguments[i]->is_nullable()) {
                    return_nullable = true;
                    break;
                }
            }
            auto result_type = std::make_shared<ResultDataType>();
            return return_nullable ? make_nullable(result_type) : result_type;
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    bool use_default_implementation_for_nulls() const override {
        // result is null only when all columns is null for bitmap_or.
        // for count functions, result is always not null, and if the bitmap op result is null,
        // the count is 0
        return !static_cast<bool>(std::is_same_v<Impl, BitmapOr> || is_count());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        if (std::is_same_v<Impl, BitmapAndCount> || std::is_same_v<Impl, BitmapXorCount>) {
            auto impl_func = [&](FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) {
                return execute_impl_internal(context, block, arguments, result, input_rows_count);
            };
            return execute_bitmap_op_count_null_to_zero(context, block, arguments, result,
                                                        input_rows_count, impl_func);
        } else {
            return execute_impl_internal(context, block, arguments, result, input_rows_count);
        }
    }

    Status execute_impl_internal(FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) const {
        size_t argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
        }

        using ResultDataType = typename Impl::ResultDataType;  //DataTypeBitMap or DataTypeInt64
        using ResultType = typename ResultDataType::FieldType; //BitmapValue or Int64
        using ColVecResult =
                std::conditional_t<is_complex_v<ResultType>, ColumnComplexType<ResultType>,
                                   ColumnVector<ResultType>>;
        typename ColVecResult::MutablePtr col_res = nullptr;

        typename ColumnUInt8::MutablePtr col_res_nulls;
        auto& result_info = block.get_by_position(result);
        // special case for bitmap_or and bitmap_or_count
        if (!use_default_implementation_for_nulls() && result_info.type->is_nullable()) {
            col_res_nulls = ColumnUInt8::create(input_rows_count, 0);
        }

        col_res = ColVecResult::create();

        auto& vec_res = col_res->get_data();
        vec_res.resize(input_rows_count);

        RETURN_IF_ERROR(Impl::vector_vector(argument_columns.data(), argument_size,
                                            input_rows_count, vec_res, col_res_nulls));
        if (!use_default_implementation_for_nulls() && result_info.type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(col_res_nulls)));
        } else {
            block.replace_by_position(result, std::move(col_res));
        }
        return Status::OK();
    }

private:
    bool is_count() const {
        return (std::is_same_v<Impl, BitmapOrCount> || std::is_same_v<Impl, BitmapAndCount> ||
                std::is_same_v<Impl, BitmapXorCount>);
    }
};

using FunctionBitmapOr = FunctionBitMapVariadic<BitmapOr>;
using FunctionBitmapXor = FunctionBitMapVariadic<BitmapXor>;
using FunctionBitmapAnd = FunctionBitMapVariadic<BitmapAnd>;
using FunctionBitmapOrCount = FunctionBitMapVariadic<BitmapOrCount>;
using FunctionBitmapAndCount = FunctionBitMapVariadic<BitmapAndCount>;
using FunctionBitmapXorCount = FunctionBitMapVariadic<BitmapXorCount>;

void register_function_bitmap_variadic(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitmapOr>();
    factory.register_function<FunctionBitmapXor>();
    factory.register_function<FunctionBitmapAnd>();
    factory.register_function<FunctionBitmapOrCount>();
    factory.register_function<FunctionBitmapAndCount>();
    factory.register_function<FunctionBitmapXorCount>();
}
} // namespace doris::vectorized