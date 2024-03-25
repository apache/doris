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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/status.h"
#include "olap/hll.h"
#include "util/simd/vstring_function.h" //place this header file at last to compile
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
template <typename Impl>
class FunctionOctVariadic : public IFunction {
public:
    static constexpr auto name = "oct";

    static FunctionPtr create() { return std::make_shared<FunctionOctVariadic>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;

        auto result_data_column = ColumnString::create();
        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();

        RETURN_IF_ERROR(
                Impl::vector(argument_column, input_rows_count, result_data, result_offset));
        block.replace_by_position(result, std::move(result_data_column));
        return Status::OK();
    }
};

struct OctIntImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeInt64>()};
    }

    static std::string_view oct(uint64_t num, char* ans) {
        static constexpr auto oct_table = "012345678";
        if (num == 0) {
            return {oct_table, 1};
        }

        int i = 0;
        while (num != 0) {
            ans[i++] = oct_table[num % 8];
            num = num / 8;
        }
        ans[i] = '\0';

        for (int k = 0, j = i - 1; k <= j; k++, j--) {
            std::swap(ans[j], ans[k]);
        }

        return {ans, static_cast<size_t>(i)};
    }

    static Status vector(ColumnPtr argument_column, size_t input_rows_count,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        const auto* str_col = check_and_get_column<ColumnVector<Int64>>(argument_column.get());
        auto& data = str_col->get_data();

        res_offsets.resize(input_rows_count);
        char ans[17];
        for (size_t i = 0; i < input_rows_count; ++i) {
            StringOP::push_value_string(oct(data[i], ans), i, res_data, res_offsets);
        }
        return Status::OK();
    }
};

void register_function_oct_variadic(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionOctVariadic<OctIntImpl>>();
}
} // namespace doris::vectorized
