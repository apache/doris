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

#include <glog/logging.h>

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class VExprContext;

class ArrayFilterFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayFilterFunction);

public:
    ~ArrayFilterFunction() override = default;

    static constexpr auto name = "array_filter";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayFilterFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, const doris::vectorized::Block* block,
                          Selector* expr_selector, size_t output_count, ColumnPtr& result_column,
                          const DataTypePtr& result_type,
                          const VExprSPtrs& children) const override {
        ///* array_filter(array, array<boolean>) *///

        //1. child[0:end]->execute(src_block)

        DCHECK_EQ(children.size(), 2);
        ColumnPtr column_ptr_0;
        RETURN_IF_ERROR(children[0]->execute_column(context, block, expr_selector, output_count,
                                                    column_ptr_0));
        ColumnPtr column_ptr_1;
        RETURN_IF_ERROR(children[1]->execute_column(context, block, expr_selector, output_count,
                                                    column_ptr_1));

        //2. get first and second array column
        auto first_column = column_ptr_0->convert_to_full_column_if_const();

        auto second_column = column_ptr_1->convert_to_full_column_if_const();

        auto input_rows = first_column->size();
        auto first_outside_null_map = ColumnUInt8::create(input_rows, 0);
        auto first_arg_column = first_column;
        if (first_arg_column->is_nullable()) {
            first_arg_column =
                    assert_cast<const ColumnNullable*>(first_column.get())->get_nested_column_ptr();
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable*>(first_column.get())->get_null_map_column();
            VectorizedUtils::update_null_map(first_outside_null_map->get_data(),
                                             column_array_nullmap.get_data());
        }
        const auto& first_col_array = assert_cast<const ColumnArray&>(*first_arg_column);
        const auto& first_off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(first_col_array.get_offsets_column())
                        .get_data();
        const auto& first_nested_nullable_column =
                assert_cast<const ColumnNullable&>(*first_col_array.get_data_ptr());

        auto result_data_column = first_nested_nullable_column.clone_empty();
        auto result_offset_column = ColumnArray::ColumnOffsets::create();
        auto& result_offset_data = result_offset_column->get_data();
        vectorized::IColumn::Selector selector;
        selector.reserve(first_off_data.size());
        result_offset_data.reserve(input_rows);

        auto second_arg_column = second_column;
        auto second_outside_null_map = ColumnUInt8::create(input_rows, 0);
        if (second_arg_column->is_nullable()) {
            second_arg_column = assert_cast<const ColumnNullable*>(second_column.get())
                                        ->get_nested_column_ptr();
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable*>(second_column.get())->get_null_map_column();
            VectorizedUtils::update_null_map(second_outside_null_map->get_data(),
                                             column_array_nullmap.get_data());
        }
        const auto& second_col_array = assert_cast<const ColumnArray&>(*second_arg_column);
        const auto& second_off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                                              second_col_array.get_offsets_column())
                                              .get_data();
        const auto& second_nested_null_map_data =
                assert_cast<const ColumnNullable&>(*second_col_array.get_data_ptr())
                        .get_null_map_column()
                        .get_data();
        const auto& second_nested_column =
                assert_cast<const ColumnNullable&>(*second_col_array.get_data_ptr())
                        .get_nested_column();
        const auto& second_nested_data =
                assert_cast<const ColumnUInt8&>(second_nested_column).get_data();

        //3. get the idx of second column data is not null and not 0
        for (int row = 0; row < input_rows; ++row) {
            //first or second column is null, so current row is invalid data
            if (first_outside_null_map->get_data()[row] ||
                second_outside_null_map->get_data()[row]) {
                result_offset_data.push_back(result_offset_data.back());
            } else {
                unsigned long count = 0;
                auto first_offset_start = first_off_data[row - 1];
                auto first_offset_end = first_off_data[row];
                auto second_offset_start = second_off_data[row - 1];
                auto second_offset_end = second_off_data[row];
                auto move_off = second_offset_start;
                for (auto off = first_offset_start;
                     off < first_offset_end && move_off < second_offset_end; // not out range
                     ++off) {
                    if (!second_nested_null_map_data[move_off] && // not null
                        second_nested_data[move_off]) {           // not 0
                        count++;
                        selector.push_back(off);
                    }
                    move_off++;
                }
                result_offset_data.push_back(count + result_offset_data.back());
            }
        }
        first_nested_nullable_column.append_data_by_selector(result_data_column, selector);

        //4. return result column
        if (result_type->is_nullable()) {
            result_column =
                    ColumnNullable::create(ColumnArray::create(std::move(result_data_column),
                                                               std::move(result_offset_column)),
                                           std::move(first_outside_null_map));
        } else {
            DCHECK(!first_column->is_nullable());
            DCHECK(!second_column->is_nullable());
            result_column = ColumnArray::create(std::move(result_data_column),
                                                std::move(result_offset_column));
        }

        return Status::OK();
    }
};

void register_function_array_filter(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayFilterFunction>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
