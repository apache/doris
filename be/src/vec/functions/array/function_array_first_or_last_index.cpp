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
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

/**
 * support array_first_index and array_last_index for input lambda expr
 * eg. array_first_index(x -> x == 0, [0, 1, 0]) -> [1]
 *     array_last_index(x -> x == 0, [0, 1, 0]) -> [3]
 */
template <bool first>
class FunctionArrayFirstOrLastIndex : public IFunction {
public:
    static constexpr auto name = first ? "array_first_index" : "array_last_index";

    static FunctionPtr create() { return std::make_shared<FunctionArrayFirstOrLastIndex>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (src_column->is_nullable()) {
            auto nullable_array = assert_cast<const ColumnNullable*>(src_column.get());
            array_column = assert_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = assert_cast<const ColumnArray*>(src_column.get());
        }

        auto& src_nested_data = array_column->get_data();
        auto& src_offset = array_column->get_offsets();

        auto result_data_col = ColumnInt64::create(input_rows_count, 0);
        auto& result_data = result_data_col->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (array_null_map && array_null_map[i]) {
                continue;
            }

            // default index is 0 if such index is not found
            size_t res_index = 0;
            size_t start_index = src_offset[i - 1];
            size_t end_index = src_offset[i];
            for (size_t off = start_index; off < end_index; ++off) {
                if constexpr (first) {
                    if (!src_nested_data.is_null_at(off) && src_nested_data.get_bool(off)) {
                        res_index = off - start_index + 1;
                        break;
                    }
                } else {
                    size_t reverse_off = start_index + (end_index - 1 - off);
                    if (!src_nested_data.is_null_at(reverse_off) &&
                        src_nested_data.get_bool(reverse_off)) {
                        res_index = reverse_off - start_index + 1;
                        break;
                    }
                }
            }
            result_data[i] = res_index;
        }
        block.replace_by_position(result, std::move(result_data_col));
        return Status::OK();
    }
};

void register_function_array_first_or_last_index(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayFirstOrLastIndex<true>>();
    factory.register_function<FunctionArrayFirstOrLastIndex<false>>();
}

} // namespace doris::vectorized
