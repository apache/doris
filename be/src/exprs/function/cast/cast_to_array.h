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

#include "core/column/column_array.h"
#include "core/data_type/data_type_array.h"
#include "exprs/function/cast/cast_base.h"

namespace doris::CastWrapper {
inline bool has_null_literal_leaf(const DataTypePtr& type) {
    const auto type_without_nullable = remove_nullable(type);
    if (const auto* array_type =
                check_and_get_data_type<DataTypeArray>(type_without_nullable.get())) {
        return has_null_literal_leaf(array_type->get_nested_type());
    }
    return type_without_nullable->is_null_literal();
}

WrapperType create_array_wrapper(const CastOptions& options, const DataTypePtr& from_type_untyped,
                                 const DataTypeArray& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        if (options.strict_mode) {
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

    if (from_type->get_number_of_dimensions() != to_type.get_number_of_dimensions() &&
        !has_null_literal_leaf(from_nested_type)) {
        return CastWrapper::create_unsupport_wrapper(
                "CAST AS Array can only be performed between same-dimensional array types");
    }

    DataTypePtr to_nested_type = to_type.get_nested_type();

    /// Prepare nested type conversion
    const auto nested_function = bind_cast_executor(options, from_nested_type, to_nested_type);

    return [nested_function, from_nested_type, to_nested_type](
                   RuntimeState* state, const ColumnWithTypeAndName& source_column,
                   ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                   const NullMap::value_type* parent_null_map = nullptr) -> Status {
        const auto* from_col_array = check_and_get_column<ColumnArray>(source_column.column.get());

        if (from_col_array) {
            /// The elements of a row that is NULL are hidden payloads, so the NULL of the row has to
            /// be inherited by the elements that belong to it before they are cast.
            auto child_mask = build_child_null_mask(parent_null_map, &from_col_array->get_offsets(),
                                                    from_col_array->get_data_ptr());
            ColumnWithTypeAndName from_nested_column {child_mask.column, from_nested_type, ""};
            ColumnWithTypeAndName nested_result {nullptr, to_nested_type, ""};
            RETURN_IF_ERROR(nested_function(state, from_nested_column, nested_result,
                                            child_mask.column->size(), child_mask.null_map));
            target_column.column =
                    ColumnArray::create(nested_result.column, from_col_array->get_offsets_ptr());
        } else {
            return Status::RuntimeError("Illegal column {} for function CAST AS Array",
                                        source_column.column->get_name());
        }
        return Status::OK();
    };
}
} // namespace doris::CastWrapper
