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
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_array.h"

namespace doris::vectorized::CastWrapper {
#include "common/compile_check_begin.h"
WrapperType create_array_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                 const DataTypeArray& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        if (context->enable_strict_mode()) {
            return cast_from_string_to_complex_type_strict_mode;
        } else {
            return cast_from_string_to_complex_type;
        }
    }

    const auto* from_type = check_and_get_data_type<DataTypeArray>(from_type_untyped.get());

    if (!from_type) {
        return CastWrapper::create_unsupport_wrapper(
                "CAST AS Array can only be performed between same-dimensional Array, String "
                "types");
    }

    DataTypePtr from_nested_type = from_type->get_nested_type();

    /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
    bool from_empty_array = from_nested_type->get_primitive_type() == INVALID_TYPE;

    if (from_type->get_number_of_dimensions() != to_type.get_number_of_dimensions() &&
        !from_empty_array) {
        return CastWrapper::create_unsupport_wrapper(
                "CAST AS Array can only be performed between same-dimensional array types");
    }

    const DataTypePtr& to_nested_type = to_type.get_nested_type();

    /// Prepare nested type conversion
    const auto nested_function =
            prepare_unpack_dictionaries(context, from_nested_type, to_nested_type);

    return [nested_function, from_nested_type, to_nested_type](
                   FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t /*input_rows_count*/,
                   const NullMap::value_type* null_map = nullptr) -> Status {
        ColumnPtr from_column = block.get_by_position(arguments.front()).column;

        const auto* from_col_array = check_and_get_column<ColumnArray>(from_column.get());

        if (from_col_array) {
            /// create columns for converting nested column containing original and result columns
            ColumnWithTypeAndName from_nested_column {from_col_array->get_data_ptr(),
                                                      from_nested_type, ""};

            /// convert nested column
            ColumnNumbers new_arguments {block.columns()};
            block.insert(from_nested_column);

            uint32_t nested_result = block.columns();
            block.insert({to_nested_type, ""});
            RETURN_IF_ERROR(nested_function(context, block, new_arguments, nested_result,
                                            from_col_array->get_data_ptr()->size(), null_map));
            auto nested_result_column = block.get_by_position(nested_result).column;

            /// set converted nested column to result
            block.get_by_position(result).column =
                    ColumnArray::create(nested_result_column, from_col_array->get_offsets_ptr());
        } else {
            return Status::RuntimeError("Illegal column {} for function CAST AS Array",
                                        from_column->get_name());
        }
        return Status::OK();
    };
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized::CastWrapper