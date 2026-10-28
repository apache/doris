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

#include <array>

#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_map.h"
#include "exprs/function/cast/cast_base.h"

namespace doris::CastWrapper {
//TODO(Amory) . Need support more cast for key , value for map
WrapperType create_map_wrapper(const CastOptions& options, const DataTypePtr& from_type,
                               const DataTypeMap& to_type) {
    if (is_string_type(from_type->get_primitive_type())) {
        return options.strict_mode ? cast_from_string_to_complex_type_strict_mode
                                   : cast_from_string_to_complex_type;
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

    auto kv_wrappers = get_element_wrappers(options, from_kv_types, to_kv_types);
    /// A key or value whose type does not change is passed through, so it needs neither a child mask
    /// nor a call into its (identity) wrapper.
    std::array<bool, 2> unchanged_kv = {from_kv_types[0]->equals(*to_kv_types[0]),
                                        from_kv_types[1]->equals(*to_kv_types[1])};
    return [kv_wrappers, from_kv_types, to_kv_types, unchanged_kv](
                   RuntimeState* state, const ColumnWithTypeAndName& source_column,
                   ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                   const NullMap::value_type* parent_null_map = nullptr) -> Status {
        const auto* from_col_map = check_and_get_column<ColumnMap>(source_column.column.get());
        if (!from_col_map) {
            return Status::RuntimeError("Illegal column {} for function CAST AS MAP",
                                        source_column.column->get_name());
        }

        Columns converted_columns(2);
        ColumnsWithTypeAndName columnsWithTypeAndName(2);
        columnsWithTypeAndName[0] = {from_col_map->get_keys_ptr(), from_kv_types[0], ""};
        columnsWithTypeAndName[1] = {from_col_map->get_values_ptr(), from_kv_types[1], ""};

        const auto& offsets = from_col_map->get_offsets();
        // Keys and values share the rows of their parent, so the parent mask is scanned once.
        const NullMap::value_type* effective_parent_null_map =
                (parent_null_map != nullptr && has_masked_row(parent_null_map, offsets.size()))
                        ? parent_null_map
                        : nullptr;
        for (size_t i = 0; i < 2; ++i) {
            if (unchanged_kv[i]) {
                converted_columns[i] = columnsWithTypeAndName[i].column;
                continue;
            }
            /// Keys and values are flattened, so the NULL of a map row has to be inherited by the
            /// entries that belong to it before they are cast.
            auto child_mask = build_child_null_mask(effective_parent_null_map, &offsets,
                                                    columnsWithTypeAndName[i].column);
            ColumnWithTypeAndName child_column {child_mask.column, from_kv_types[i], ""};
            ColumnWithTypeAndName element_result {nullptr, to_kv_types[i], ""};
            RETURN_IF_ERROR(kv_wrappers[i](state, child_column, element_result,
                                           child_mask.column->size(), child_mask.null_map));
            converted_columns[i] = std::move(element_result.column);
        }

        auto map_column = ColumnMap::create(converted_columns[0], converted_columns[1],
                                            from_col_map->get_offsets_ptr());
        static_cast<void>(map_column->deduplicate_keys());
        target_column.column = std::move(map_column);
        return Status::OK();
    };
}
} // namespace doris::CastWrapper
