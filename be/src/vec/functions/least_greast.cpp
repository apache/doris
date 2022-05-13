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

#include "udf/udf.h"
#include "vec/core/accurate_comparison.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/function_multi_same_args.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <template <typename, typename> class Op, typename Impl>
struct CompareMultiImpl {
    static constexpr auto name = Impl::name;

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) { return arguments[0]; }

    template <typename ColumnType>
    static void insert_result_data(MutableColumnPtr& result_column, ColumnPtr& argument_column,
                                   const size_t input_rows_count) {
        auto* __restrict result_raw_data =
                reinterpret_cast<ColumnType*>(result_column.get())->get_data().data();
        auto* __restrict column_raw_data =
                reinterpret_cast<const ColumnType*>(argument_column.get())->get_data().data();

        if constexpr (std::is_same_v<ColumnType, ColumnDecimal128>) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                result_raw_data[i] = Op<DecimalV2Value, DecimalV2Value>::apply(column_raw_data[i],
                                                                               result_raw_data[i])
                                             ? column_raw_data[i]
                                             : result_raw_data[i];
            }
        } else {
            for (size_t i = 0; i < input_rows_count; ++i) {
                using type = std::decay_t<decltype(result_raw_data[0])>;
                result_raw_data[i] = Op<type, type>::apply(column_raw_data[i], result_raw_data[i])
                                             ? column_raw_data[i]
                                             : result_raw_data[i];
            }
        }
    }

    static ColumnPtr execute(Block& block, const ColumnNumbers& arguments,
                             size_t input_rows_count) {
        if (arguments.size() == 1) return block.get_by_position(arguments.back()).column;

        const auto& data_type = block.get_by_position(arguments.back()).type;
        MutableColumnPtr result_column = data_type->create_column();

        Columns args;
        for (int i = 0; i < arguments.size(); ++i) {
            args.emplace_back(
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const());
        }
        // because now the string types does not support random position writing,
        // so insert into result data have two methods, one is for string types, one is for others type remaining
        bool is_string_result = result_column->is_column_string();
        if (is_string_result) {
            result_column->reserve(input_rows_count);
        } else {
            result_column->insert_range_from(*(args[0]), 0, input_rows_count);
        }

        if (is_string_result) {
            const auto& column_string = reinterpret_cast<const ColumnString&>(*args[0]);
            auto& column_res = reinterpret_cast<ColumnString&>(*result_column);

            for (int i = 0; i < input_rows_count; ++i) {
                auto str_data = column_string.get_data_at(i);
                for (int j = 1; j < arguments.size(); ++j) {
                    auto temp_data = reinterpret_cast<const ColumnString&>(*args[j]).get_data_at(i);
                    str_data = Op<StringRef, StringRef>::apply(temp_data, str_data) ? temp_data
                                                                                    : str_data;
                }
                column_res.insert_data(str_data.data, str_data.size);
            }

        } else {
            WhichDataType which(data_type);
#define DISPATCH(TYPE, COLUMN_TYPE)                                                    \
    if (which.idx == TypeIndex::TYPE) {                                                \
        for (int i = 1; i < arguments.size(); ++i) {                                   \
            insert_result_data<COLUMN_TYPE>(result_column, args[i], input_rows_count); \
        }                                                                              \
    }
            NUMERIC_TYPE_TO_COLUMN_TYPE(DISPATCH)
            DISPATCH(Decimal128, ColumnDecimal<Decimal128>)
            TIME_TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH
        }

        return result_column;
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

void register_function_least_greast(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLeast>();
    factory.register_function<FunctionGreaest>();
}
}; // namespace doris::vectorized