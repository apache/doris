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
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_map.h"

namespace doris::vectorized::CastWrapper {
#include "common/compile_check_begin.h"
//TODO(Amory) . Need support more cast for key , value for map
WrapperType create_map_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                               const DataTypeMap& to_type) {
    if (is_string_type(from_type->get_primitive_type())) {
        if (context->enable_strict_mode()) {
            return cast_from_string_to_complex_type_strict_mode;
        } else {
            return cast_from_string_to_complex_type;
        }
    }
    const auto* from = check_and_get_data_type<DataTypeMap>(from_type.get());
    if (!from) {
        return CastWrapper::create_unsupport_wrapper(
                fmt::format("CAST AS Map can only be performed between Map types or from "
                            "String. from type: {}, to type: {}",
                            from_type->get_name(), to_type.get_name()));
    }
    DataTypes from_kv_types;
    DataTypes to_kv_types;
    from_kv_types.reserve(2);
    to_kv_types.reserve(2);
    from_kv_types.push_back(from->get_key_type());
    from_kv_types.push_back(from->get_value_type());
    to_kv_types.push_back(to_type.get_key_type());
    to_kv_types.push_back(to_type.get_value_type());

    auto kv_wrappers = get_element_wrappers(context, from_kv_types, to_kv_types);
    return [kv_wrappers, from_kv_types, to_kv_types](
                   FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t /*input_rows_count*/,
                   const NullMap::value_type* null_map = nullptr) -> Status {
        auto& from_column = block.get_by_position(arguments.front()).column;
        const auto* from_col_map = check_and_get_column<ColumnMap>(from_column.get());
        if (!from_col_map) {
            return Status::RuntimeError("Illegal column {} for function CAST AS MAP",
                                        from_column->get_name());
        }

        Columns converted_columns(2);
        ColumnsWithTypeAndName columnsWithTypeAndName(2);
        columnsWithTypeAndName[0] = {from_col_map->get_keys_ptr(), from_kv_types[0], ""};
        columnsWithTypeAndName[1] = {from_col_map->get_values_ptr(), from_kv_types[1], ""};

        for (size_t i = 0; i < 2; ++i) {
            ColumnNumbers element_arguments {block.columns()};
            block.insert(columnsWithTypeAndName[i]);
            auto element_result = block.columns();
            block.insert({to_kv_types[i], ""});
            RETURN_IF_ERROR(kv_wrappers[i](context, block, element_arguments, element_result,
                                           columnsWithTypeAndName[i].column->size(), null_map));
            converted_columns[i] = block.get_by_position(element_result).column;
        }

        block.get_by_position(result).column = ColumnMap::create(
                converted_columns[0], converted_columns[1], from_col_map->get_offsets_ptr());
        return Status::OK();
    };
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized::CastWrapper