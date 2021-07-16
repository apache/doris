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

#include <fmt/format.h>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_set.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

template <bool negative, bool null_in_set>
struct FunctionInName;

template <>
struct FunctionInName<false, false> {
    static constexpr auto name = "in";
};

template <>
struct FunctionInName<true, false> {
    static constexpr auto name = "not_in";
};

template <>
struct FunctionInName<false, true> {
    static constexpr auto name = "in_null_in_set";
};

template <>
struct FunctionInName<true, true> {
    static constexpr auto name = "not_in_null_in_set";
};

template <bool negative, bool null_in_set>
class FunctionIn : public IFunction {
public:
    static constexpr auto name = FunctionInName<negative, null_in_set>::name;
    static FunctionPtr create() { return std::make_shared<FunctionIn>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) override {
        /// NOTE: after updating this code, check that FunctionIgnoreExceptNull returns the same type of column.

        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.get_by_position(arguments[1]).column;
        const ColumnSet* column_set = typeid_cast<const ColumnSet*>(&*column_set_ptr);
        if (!column_set) {
            return Status::RuntimeError(
                    fmt::format("Second argument for function '{}' must be Set; found {}",
                                get_name(), column_set_ptr->get_name()));
        }

        auto set = column_set->get_data();
        /// First argument may be a single column.
        const ColumnWithTypeAndName& left_arg = block.get_by_position(arguments[0]);

        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        vec_res.resize(left_arg.column->size());

        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create(left_arg.column->size());
        auto& vec_null_map_to = col_null_map_to->get_data();

        if (set->size() == 0) {
            if (negative)
                memset(vec_res.data(), 1, vec_res.size());
            else
                memset(vec_res.data(), 0, vec_res.size());
        } else {
            auto materialized_column = left_arg.column->convert_to_full_column_if_const();
            auto size = materialized_column->size();

            if (auto* nullable = check_and_get_column<ColumnNullable>(*materialized_column)) {
                const auto& nested_column = nullable->get_nested_column();
                const auto& null_map = nullable->get_null_map_column().get_data();

                for (size_t i = 0; i < size; ++i) {
                    vec_null_map_to[i] = null_map[i];
                    if (null_map[i]) {
                        continue;
                    }
                    const auto& ref_data = nested_column.get_data_at(i);
                    vec_res[i] = negative ^ set->find((void*)ref_data.data, ref_data.size);
                    if constexpr (null_in_set) {
                        vec_null_map_to[i] = negative == vec_res[i];
                    }
                }
            } else {
                /// For all rows
                for (size_t i = 0; i < size; ++i) {
                    const auto& ref_data = materialized_column->get_data_at(i);
                    vec_res[i] = negative ^ set->find((void*)ref_data.data, ref_data.size);
                    if constexpr (null_in_set) {
                        vec_null_map_to[i] = negative == vec_res[i];
                    } else {
                        vec_null_map_to[i] = 0;
                    }
                }
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(res), std::move(col_null_map_to)));
        return Status::OK();
    }
};

void register_function_in(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIn<false, false>>();
    factory.register_function<FunctionIn<true, false>>();
    factory.register_function<FunctionIn<true, true>>();
    factory.register_function<FunctionIn<false, true>>();
}

} // namespace doris::vectorized
