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

#include "vec/functions/function_tokenize.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

     Status FunctionTokenize::execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                     size_t result, size_t /*input_rows_count*/) {
         DCHECK_EQ(arguments.size(), 2);

         /*const auto& [src_column, left_const] =
                 unpack_if_const(block.get_by_position(arguments[0]).column);
         const auto& [right_column, right_const] =
                 unpack_if_const(block.get_by_position(arguments[1]).column);

         DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
         auto dest_column_ptr = ColumnArray::create(make_nullable(src_column_type)->create_column(),
                                                    ColumnArray::ColumnOffsets::create());

         IColumn* dest_nested_column = &dest_column_ptr->get_data();
         auto& dest_offsets = dest_column_ptr->get_offsets();
         DCHECK(dest_nested_column != nullptr);
         dest_nested_column->reserve(0);
         dest_offsets.reserve(0);

         NullMapType* dest_nested_null_map = nullptr;
         ColumnNullable* dest_nullable_col = reinterpret_cast<ColumnNullable*>(dest_nested_column);
         dest_nested_column = dest_nullable_col->get_nested_column_ptr();
         dest_nested_null_map = &dest_nullable_col->get_null_map_column().get_data();*/

         /*if (auto col_left = check_and_get_column<ColumnString>(src_column.get())) {
             if (auto col_right = check_and_get_column<ColumnString>(right_column.get())) {
                 if (right_const) {
                     _execute_constant(*col_left, col_right->get_data_at(0), *dest_nested_column,
                                       dest_offsets, dest_nested_null_map);
                 } else {
                     _execute_vector(*col_left, *col_right, *dest_nested_column, dest_offsets,
                                     dest_nested_null_map);
                 }

                 block.replace_by_position(result, std::move(dest_column_ptr));
                 return Status::OK();
             }
         }*/
         return Status::RuntimeError("unimplements function {}", get_name());
    }
} // namespace doris::vectorized