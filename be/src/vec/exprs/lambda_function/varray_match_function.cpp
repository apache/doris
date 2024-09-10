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
#include <vector>

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
#include "vec/data_types/data_type.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/utils/util.hpp"

namespace doris {
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

///* bool array_match_all/any(array, array<boolean>) *///
template <bool MATCH_ALL>
class ArrayMatchFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayMatchFunction);

public:
    ~ArrayMatchFunction() override = default;

    static constexpr auto name = MATCH_ALL ? "array_match_all" : "array_match_any";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayMatchFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id, const DataTypePtr& result_type,
                          const VExprSPtrs& children) override {
        DCHECK(children.size() == 2);
        //1. child[0:end]->execute(src_block)
        doris::vectorized::ColumnNumbers arguments(children.size());
        for (int i = 0; i < children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(children[i]->execute(context, block, &column_id));
            arguments[i] = column_id;
        }

        //2. get second array<bool> column
        auto first_column =
                block->get_by_position(arguments[0]).column->convert_to_full_column_if_const();

        auto second_column =
                block->get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        int input_rows = first_column->size();
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
        const ColumnArray& first_col_array = assert_cast<const ColumnArray&>(*first_arg_column);
        const auto& first_off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(first_col_array.get_offsets_column())
                        .get_data();
        const auto& first_nested_nullable_column =
                assert_cast<const ColumnNullable&>(*first_col_array.get_data_ptr());

        // result is nullable bool column for every array column
        auto result_data_column = ColumnUInt8::create(input_rows, 1);
        auto result_null_column = ColumnUInt8::create(input_rows, 0);

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
        const ColumnArray& second_col_array = assert_cast<const ColumnArray&>(*second_arg_column);
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

        //3. calculate result column with second bool column
        for (int row = 0; row < input_rows; ++row) {
            //first or second column is null, so current row is invalid data
            if (first_outside_null_map->get_data()[row] ||
                second_outside_null_map->get_data()[row]) {
                result_null_column->get_data()[row] = 1;
                result_data_column->get_data()[row] = 0;
            } else {
                // has_null in current array
                bool has_null_elem = false;
                // res for current array
                bool res_for_array = MATCH_ALL;
                auto first_offset_start = first_off_data[row - 1];
                auto first_offset_end = first_off_data[row];
                auto second_offset_start = second_off_data[row - 1];
                auto second_offset_end = second_off_data[row];
                auto move_off = second_offset_start;
                for (auto off = first_offset_start;
                     off < first_offset_end && move_off < second_offset_end; // not out range
                     ++off) {
                    if (first_nested_nullable_column.is_null_at(off)) {
                        has_null_elem = true;
                    } else {
                        if (!second_nested_null_map_data[move_off] &&    // not null
                            second_nested_data[move_off] != MATCH_ALL) { // not match
                            res_for_array = !MATCH_ALL;
                            break;
                        } // default is MATCH_ALL
                    }
                    move_off++;
                }
                result_null_column->get_data()[row] = has_null_elem && res_for_array == MATCH_ALL;
                result_data_column->get_data()[row] = res_for_array;
            }
        }

        //4. insert the result column to block
        ColumnWithTypeAndName result_arr;
        if (result_type->is_nullable()) {
            result_arr = {ColumnNullable::create(std::move(result_data_column),
                                                 std::move(result_null_column)),
                          result_type, get_name() + "_result"};

        } else {
            DCHECK(!first_column->is_nullable());
            DCHECK(!second_column->is_nullable());
            result_arr = {std::move(result_data_column), result_type, get_name() + "_result"};
        }
        block->insert(std::move(result_arr));
        *result_column_id = block->columns() - 1;
        return Status::OK();
    }
};

void register_function_array_match(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayMatchFunction<true>>(); // MATCH_ALL = true means array_match_all
    factory.register_function<
            ArrayMatchFunction<false>>(); // MATCH_ALL = false means array_match_any
}
} // namespace doris::vectorized
