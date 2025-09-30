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

#include "cast_base.h"
#include "vec/columns/column_struct.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized::CastWrapper {
#include "common/compile_check_begin.h"
// check struct value type and get to_type value
// TODO: need handle another type to cast struct
WrapperType create_struct_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                  const DataTypeStruct& to_type) {
    // support CAST AS Struct from string
    if (is_string_type(from_type->get_primitive_type())) {
        if (context->enable_strict_mode()) {
            return cast_from_string_to_complex_type_strict_mode;
        } else {
            return cast_from_string_to_complex_type;
        }
    }

    // only support CAST AS Struct from struct or string types
    const auto* from = check_and_get_data_type<DataTypeStruct>(from_type.get());
    if (!from) {
        return CastWrapper::create_unsupport_wrapper(
                fmt::format("CAST AS Struct can only be performed between struct types or from "
                            "String. Left type: {}, right type: {}",
                            from_type->get_name(), to_type.get_name()));
    }

    const auto& from_element_types = from->get_elements();
    const auto& to_element_types = to_type.get_elements();
    // only support CAST AS Struct from struct type with same number of elements
    if (from_element_types.size() != to_element_types.size()) {
        return CastWrapper::create_unsupport_wrapper(
                fmt::format("CAST AS Struct can only be performed between struct types with "
                            "the same number of elements. Left type: {}, right type: {}",
                            from_type->get_name(), to_type.get_name()));
    }

    auto element_wrappers = get_element_wrappers(context, from_element_types, to_element_types);
    return [element_wrappers, from_element_types, to_element_types](
                   FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t /*input_rows_count*/,
                   const NullMap::value_type* null_map = nullptr) -> Status {
        auto& from_column = block.get_by_position(arguments.front()).column;
        const auto* from_col_struct = check_and_get_column<ColumnStruct>(from_column.get());
        if (!from_col_struct) {
            return Status::RuntimeError("Illegal column {} for function CAST AS Struct",
                                        from_column->get_name());
        }

        size_t elements_num = to_element_types.size();
        Columns converted_columns(elements_num);
        for (size_t i = 0; i < elements_num; ++i) {
            ColumnWithTypeAndName from_element_column {from_col_struct->get_column_ptr(i),
                                                       from_element_types[i], ""};
            ColumnNumbers element_arguments {block.columns()};
            block.insert(from_element_column);

            auto element_result = block.columns();
            block.insert({to_element_types[i], ""});

            RETURN_IF_ERROR(element_wrappers[i](context, block, element_arguments, element_result,
                                                from_col_struct->get_column(i).size(), null_map));
            converted_columns[i] = block.get_by_position(element_result).column;
        }

        block.get_by_position(result).column = ColumnStruct::create(converted_columns);
        return Status::OK();
    };
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized::CastWrapper