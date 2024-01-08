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
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <tuple>
#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function_multi_same_args.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class DecimalV2Value;
} // namespace doris

namespace doris::vectorized {

template <template <typename, typename> class Op, typename Impl>
struct CompareMultiImpl {
    static constexpr auto name = Impl::name;

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) { return arguments[0]; }

    static ColumnPtr execute(Block& block, const ColumnNumbers& arguments,
                             size_t input_rows_count) {
        if (arguments.size() == 1) {
            return block.get_by_position(arguments.back()).column;
        }

        const auto& data_type = block.get_by_position(arguments.back()).type;
        MutableColumnPtr result_column = data_type->create_column();

        Columns cols(arguments.size());
        std::unique_ptr<bool[]> col_const = std::make_unique<bool[]>(arguments.size());
        for (int i = 0; i < arguments.size(); ++i) {
            std::tie(cols[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }
        // because now the string types does not support random position writing,
        // so insert into result data have two methods, one is for string types, one is for others type remaining
        if (result_column->is_column_string()) {
            result_column->reserve(input_rows_count);
            const auto& column_string = reinterpret_cast<const ColumnString&>(*cols[0]);
            auto& column_res = reinterpret_cast<ColumnString&>(*result_column);

            for (int i = 0; i < input_rows_count; ++i) {
                auto str_data = column_string.get_data_at(index_check_const(i, col_const[0]));
                for (int cmp_col = 1; cmp_col < arguments.size(); ++cmp_col) {
                    auto temp_data = assert_cast<const ColumnString&>(*cols[cmp_col])
                                             .get_data_at(index_check_const(i, col_const[cmp_col]));
                    str_data = Op<StringRef, StringRef>::apply(temp_data, str_data) ? temp_data
                                                                                    : str_data;
                }
                column_res.insert_data(str_data.data, str_data.size);
            }

        } else {
            if (col_const[0]) {
                for (int i = 0; i < input_rows_count; ++i) {
                    result_column->insert_range_from(*(cols[0]), 0, 1);
                }
            } else {
                result_column->insert_range_from(*(cols[0]), 0, input_rows_count);
            }
            WhichDataType which(data_type);

#define DISPATCH(TYPE, COLUMN_TYPE)                                                               \
    if (which.idx == TypeIndex::TYPE) {                                                           \
        for (int i = 1; i < arguments.size(); ++i) {                                              \
            if (col_const[i]) {                                                                   \
                insert_result_data<COLUMN_TYPE, true>(result_column, cols[i], input_rows_count);  \
            } else {                                                                              \
                insert_result_data<COLUMN_TYPE, false>(result_column, cols[i], input_rows_count); \
            }                                                                                     \
        }                                                                                         \
    }
            NUMERIC_TYPE_TO_COLUMN_TYPE(DISPATCH)
            DECIMAL_TYPE_TO_COLUMN_TYPE(DISPATCH)
            TIME_TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH
        }

        return result_column;
    }

private:
    template <typename ColumnType, bool ArgConst>
    static void insert_result_data(const MutableColumnPtr& result_column,
                                   const ColumnPtr& argument_column,
                                   const size_t input_rows_count) {
        auto* __restrict result_raw_data =
                reinterpret_cast<ColumnType*>(result_column.get())->get_data().data();
        auto* __restrict column_raw_data =
                reinterpret_cast<const ColumnType*>(argument_column.get())->get_data().data();

        if constexpr (std::is_same_v<ColumnType, ColumnDecimal128V2>) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                result_raw_data[i] =
                        Op<DecimalV2Value, DecimalV2Value>::apply(
                                column_raw_data[index_check_const(i, ArgConst)], result_raw_data[i])
                                ? column_raw_data[index_check_const(i, ArgConst)]
                                : result_raw_data[i];
            }
        } else if constexpr (std::is_same_v<ColumnType, ColumnDecimal32> ||
                             std::is_same_v<ColumnType, ColumnDecimal64> ||
                             std::is_same_v<ColumnType, ColumnDecimal128V3> ||
                             std::is_same_v<ColumnType, ColumnDecimal256>) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                using type = std::decay_t<decltype(result_raw_data[0].value)>;
                result_raw_data[i] =
                        Op<type, type>::apply(column_raw_data[index_check_const(i, ArgConst)].value,
                                              result_raw_data[i].value)
                                ? column_raw_data[index_check_const(i, ArgConst)]
                                : result_raw_data[i];
            }
        } else {
            for (size_t i = 0; i < input_rows_count; ++i) {
                using type = std::decay_t<decltype(result_raw_data[0])>;
                result_raw_data[i] =
                        Op<type, type>::apply(column_raw_data[index_check_const(i, ArgConst)],
                                              result_raw_data[i])
                                ? column_raw_data[index_check_const(i, ArgConst)]
                                : result_raw_data[i];
            }
        }
    }
};

struct FunctionFieldImpl {
    static constexpr auto name = "field";

    static DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) {
        return std::make_shared<DataTypeInt32>();
    }

    static ColumnPtr execute(Block& block, const ColumnNumbers& arguments,
                             size_t input_rows_count) {
        const auto& data_type = block.get_by_position(arguments[0]).type;
        auto result_column = ColumnInt32::create(input_rows_count, 0);
        auto& res_data = static_cast<ColumnInt32*>(result_column)->get_data();

        const auto& column_size = arguments.size();
        ColumnPtr argument_columns[column_size];
        for (int i = 0; i < column_size; ++i) {
            argument_columns[i] = block.get_by_position(arguments[i]).column;
        }

        bool arg_const;
        std::tie(argument_columns[0], arg_const) = unpack_if_const(argument_columns[0]);

        WhichDataType which(data_type);
        //TODO: maybe could use hashmap to save column data, not use for loop ervey time to test equals.
        if (which.is_string_or_fixed_string()) {
            const auto& column_string = assert_cast<const ColumnString&>(*argument_columns[0]);
            for (int row = 0; row < input_rows_count; ++row) {
                const auto& str_data = column_string.get_data_at(index_check_const(row, arg_const));
                for (int col = 1; col < column_size; ++col) {
                    auto [column, is_const] =
                            unpack_if_const(block.safe_get_by_position(col).column);
                    const auto& temp_data = assert_cast<const ColumnString&>(*column).get_data_at(
                            index_check_const(row, is_const));
                    if (EqualsOp<StringRef, StringRef>::apply(temp_data, str_data)) {
                        res_data[row] = col;
                        break;
                    }
                }
            }

        } else { //string or not
#define DISPATCH(TYPE, COLUMN_TYPE)                                                             \
    if (which.idx == TypeIndex::TYPE) {                                                         \
        for (int col = 1; col < arguments.size(); ++col) {                                      \
            if (arg_const) {                                                                    \
                insert_result_data<COLUMN_TYPE, true>(res_data, argument_columns[0],            \
                                                      argument_columns[col], input_rows_count,  \
                                                      col);                                     \
            } else {                                                                            \
                insert_result_data<COLUMN_TYPE, false>(res_data, argument_columns[0],           \
                                                       argument_columns[col], input_rows_count, \
                                                       col);                                    \
            }                                                                                   \
        }                                                                                       \
    }
            NUMERIC_TYPE_TO_COLUMN_TYPE(DISPATCH)
            DECIMAL_TYPE_TO_COLUMN_TYPE(DISPATCH)
            TIME_TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH
        }

        return result_column;
    }

private:
    template <typename ColumnType, bool ArgConst>
    static void insert_result_data(PaddedPODArray<Int32>& __restrict res_data,
                                   ColumnPtr first_column, ColumnPtr argument_column,
                                   const size_t input_rows_count, const int col) {
        auto [first_column_raw, first_column_is_const] = unpack_if_const(first_column);
        auto* __restrict first_raw_data =
                assert_cast<const ColumnType*>(first_column_raw.get())->get_data().data();

        auto [argument_column_raw, argument_column_is_const] = unpack_if_const(argument_column);
        const auto& arg_data =
                assert_cast<const ColumnType&>(*argument_column_raw).get_data().data()[0];
        if constexpr (std::is_same_v<ColumnType, ColumnDecimal128V2>) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                res_data[i] |= (!res_data[i] *
                                (EqualsOp<DecimalV2Value, DecimalV2Value>::apply(
                                        first_raw_data[index_check_const(i, ArgConst)], arg_data)) *
                                col);
            }
        } else if constexpr (std::is_same_v<ColumnType, ColumnDecimal32> ||
                             std::is_same_v<ColumnType, ColumnDecimal64> ||
                             std::is_same_v<ColumnType, ColumnDecimal128V3> ||
                             std::is_same_v<ColumnType, ColumnDecimal256>) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                using type = std::decay_t<decltype(first_raw_data[0].value)>;
                res_data[i] |= (!res_data[i] *
                                (EqualsOp<type, type>::apply(
                                        first_raw_data[index_check_const(i, ArgConst)].value,
                                        arg_data.value)) *
                                col);
            }
        } else {
            for (size_t i = 0; i < input_rows_count; ++i) {
                using type = std::decay_t<decltype(first_raw_data[0])>;
                res_data[i] |= (!res_data[i] *
                                (EqualsOp<type, type>::apply(
                                        first_raw_data[index_check_const(i, ArgConst)], arg_data)) *
                                col);
            }
        }
    }
};

struct LeastName {
    static constexpr auto name = "least";
};
struct GreastName {
    static constexpr auto name = "greatest";
};

using FunctionLeast = FunctionMultiSameArgs<CompareMultiImpl<LessOp, LeastName>>;
using FunctionGreaest = FunctionMultiSameArgs<CompareMultiImpl<GreaterOp, GreastName>>;
using FunctionField = FunctionMultiSameArgs<FunctionFieldImpl>;
void register_function_least_greast(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLeast>();
    factory.register_function<FunctionGreaest>();
    factory.register_function<FunctionField>();
}
}; // namespace doris::vectorized