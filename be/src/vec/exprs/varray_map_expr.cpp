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

#include "vec/exprs/varray_map_expr.h"

#include <cstddef>
#include <memory>
#include <string_view>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vlambda_function_expr.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

VArrayMapExpr::VArrayMapExpr(const TExprNode& node) : VExpr(node) {}

Status VArrayMapExpr::execute(VExprContext* context, doris::vectorized::Block* block,
                              int* result_column_id) {
    ///* array_map(lambda,arg1,arg2,.....) *///

    //1. child[1:end]->execute(src_block)
    doris::vectorized::ColumnNumbers arguments(_children.size() - 1);
    for (int i = 1; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i - 1] = column_id;
    }

    // used for save column array outside null map
    auto outside_null_map = ColumnUInt8::create();
    // used for save column array nested column null map
    auto nested_null_map = ColumnUInt8::create();
    // offset column
    MutableColumnPtr array_column_offset;
    int nested_array_column_rows = 0;

    //2. get the result column from executed expr, and the needed is nested column of array
    Block lambda_block;
    for (int i = 0; i < arguments.size(); ++i) {
        auto& array_column_type_name = block->get_by_position(arguments[i]);
        auto& column_array = array_column_type_name.column;
        column_array = column_array->convert_to_full_column_if_const();
        auto type_array = array_column_type_name.type;

        if (type_array->is_nullable()) {
            // get the nullmap of nullable column
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable&>(*array_column_type_name.column)
                            .get_null_map_column_ptr();
            // get the array column from nullable column
            column_array = assert_cast<const ColumnNullable*>(array_column_type_name.column.get())
                                   ->get_nested_column_ptr();
            // get the nested type from nullable type
            type_array = assert_cast<const DataTypeNullable*>(array_column_type_name.type.get())
                                 ->get_nested_type();
            // need to union nullmap from all columns
            if (i == 0) {
                outside_null_map->get_data().resize_fill(column_array->size(), 0);
            }
            const auto& column_array_null_data =
                    assert_cast<const ColumnUInt8&>(*column_array_nullmap);
            VectorizedUtils::update_null_map(outside_null_map->get_data(),
                                             column_array_null_data.get_data());
        } else {
            if (i == 0) {
                outside_null_map->get_data().resize_fill(column_array->size(), 0);
            }
        }

        // here is the array column
        const ColumnArray& col_array = assert_cast<const ColumnArray&>(*column_array);
        const auto& type = assert_cast<const DataTypeArray&>(*type_array);
        if (i == 0) {
            nested_array_column_rows = col_array.get_data_ptr()->size();
            nested_null_map->get_data().resize_fill(nested_array_column_rows, 0);
            auto& off_data =
                    assert_cast<const ColumnArray::ColumnOffsets&>(col_array.get_offsets_column());
            array_column_offset = off_data.clone_resized(col_array.get_offsets_column().size());
        } else {
            // select array_map((x,y)->x+y,c_array1,[0,1,2,3]) from array_test2;
            // c_array1: [0,1,2,3,4,5,6,7,8,9]
            if (nested_array_column_rows != col_array.get_data_ptr()->size()) {
                return Status::InternalError(
                        "in array map function, the input column nested column data rows are not "
                        "equal, the first size is {}, but with {}th size is {}.",
                        nested_array_column_rows, i + 1, col_array.get_data_ptr()->size());
            }
        }

        // need union nullmap from all nested column
        const auto& nested_array_nullmap =
                assert_cast<const ColumnNullable&>(*col_array.get_data_ptr())
                        .get_null_map_column_ptr();
        const auto& nested_array_null_data = assert_cast<const ColumnUInt8&>(*nested_array_nullmap);
        VectorizedUtils::update_null_map(nested_null_map->get_data(),
                                         nested_array_null_data.get_data());

        // insert the data column to the new block
        ColumnWithTypeAndName data_column {col_array.get_data_ptr(), type.get_nested_type(),
                                           "R" + array_column_type_name.name};
        lambda_block.insert(std::move(data_column));
    }

    //3. child[0]->execute(new_block)
    RETURN_IF_ERROR(_children[0]->execute(context, &lambda_block, result_column_id));

    auto res_col = lambda_block.get_by_position(*result_column_id)
                           .column->convert_to_full_column_if_const();
    auto res_type = lambda_block.get_by_position(*result_column_id).type;
    auto res_name = lambda_block.get_by_position(*result_column_id).name;

    //4. get the result column after execution, reassemble it into a new array column, and return.
    ColumnWithTypeAndName result_arr;
    if (res_type->is_nullable()) {
        result_arr = {
                ColumnNullable::create(ColumnArray::create(res_col, std::move(array_column_offset)),
                                       std::move(outside_null_map)),
                _data_type, res_name};

    } else {
        result_arr = {ColumnNullable::create(
                              ColumnArray::create(
                                      ColumnNullable::create(res_col, std::move(nested_null_map)),
                                      std::move(array_column_offset)),
                              std::move(outside_null_map)),
                      _data_type, res_name};
    }
    block->insert(std::move(result_arr));
    *result_column_id = block->columns() - 1;
    return Status::OK();
}

} // namespace doris::vectorized
