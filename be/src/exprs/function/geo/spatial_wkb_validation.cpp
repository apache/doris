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

#include "exprs/function/geo/spatial_wkb_validation.h"

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/function/geo/wkb_parse.h"

namespace doris {

static bool has_spatial_leaf(const DataTypePtr& type) {
    const auto& nested = remove_nullable(type);
    switch (nested->get_primitive_type()) {
    case TYPE_GEOMETRY:
    case TYPE_GEOGRAPHY:
        return true;
    case TYPE_ARRAY:
        return has_spatial_leaf(assert_cast<const DataTypeArray&>(*nested).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*nested);
        return has_spatial_leaf(map.get_key_type()) || has_spatial_leaf(map.get_value_type());
    }
    case TYPE_STRUCT:
        for (const auto& element : assert_cast<const DataTypeStruct&>(*nested).get_elements()) {
            if (has_spatial_leaf(element)) {
                return true;
            }
        }
        return false;
    default:
        return false;
    }
}

static Status validate_spatial_range(const IColumn& input, const DataTypePtr& type, size_t start,
                                     size_t end) {
    if (!has_spatial_leaf(type) || start == end) {
        return Status::OK();
    }
    if (const auto* constant = check_and_get_column<ColumnConst>(input)) {
        return validate_spatial_range(constant->get_data_column(), type, 0, 1);
    }
    const auto* nullable = check_and_get_column<ColumnNullable>(input);
    const IColumn& column = nullable ? nullable->get_nested_column() : input;
    const auto& nested_type = remove_nullable(type);
    for (size_t row = start; row < end; ++row) {
        // NULL parents can contain default or stale child payloads; only visible values are WKB.
        if (nullable && nullable->is_null_at(row)) {
            continue;
        }
        switch (nested_type->get_primitive_type()) {
        case TYPE_GEOMETRY:
        case TYPE_GEOGRAPHY: {
            const auto value = column.get_data_at(row);
            const auto& spatial = assert_cast<const DataTypeSpatial&>(*nested_type);
            RETURN_IF_ERROR(WkbParse::validate_spatial_wkb(
                    value.data, value.size, spatial.requires_geographic_coordinates(), row));
            break;
        }
        case TYPE_ARRAY: {
            const auto& array = assert_cast<const ColumnArray&>(column);
            const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
            RETURN_IF_ERROR(validate_spatial_range(array.get_data(), array_type.get_nested_type(),
                                                   array.offset_at(row), array.get_offsets()[row]));
            break;
        }
        case TYPE_MAP: {
            const auto& map = assert_cast<const ColumnMap&>(column);
            const auto& map_type = assert_cast<const DataTypeMap&>(*nested_type);
            RETURN_IF_ERROR(validate_spatial_range(map.get_keys(), map_type.get_key_type(),
                                                   map.offset_at(row), map.get_offsets()[row]));
            RETURN_IF_ERROR(validate_spatial_range(map.get_values(), map_type.get_value_type(),
                                                   map.offset_at(row), map.get_offsets()[row]));
            break;
        }
        case TYPE_STRUCT: {
            const auto& structure = assert_cast<const ColumnStruct&>(column);
            const auto& elements = assert_cast<const DataTypeStruct&>(*nested_type).get_elements();
            for (size_t i = 0; i < elements.size(); ++i) {
                RETURN_IF_ERROR(
                        validate_spatial_range(structure.get_column(i), elements[i], row, row + 1));
            }
            break;
        }
        default:
            break;
        }
    }
    return Status::OK();
}

Status validate_spatial_wkb_inputs(const Block& block, const ColumnNumbers& arguments) {
    for (const auto argument : arguments) {
        const auto& entry = block.get_by_position(argument);
        RETURN_IF_ERROR(validate_spatial_range(*entry.column, entry.type, 0, entry.column->size()));
    }
    return Status::OK();
}

Status append_spatial_wkb(const ColumnPtr& source, IColumn& destination,
                          const DataTypePtr& spatial_type, const NullMap::value_type* null_map) {
    auto validation_column = source;
    auto validation_type = remove_nullable(spatial_type);
    if (null_map != nullptr) {
        auto null_column = ColumnUInt8::create();
        null_column->get_data().assign(null_map, null_map + source->size());
        validation_column = ColumnNullable::create(source, ColumnPtr(std::move(null_column)));
        validation_type = make_nullable(validation_type);
    } else if (source->is_nullable()) {
        validation_type = make_nullable(validation_type);
    }
    Block validation_block;
    validation_block.insert({validation_column, validation_type, "spatial_wkb"});
    RETURN_IF_ERROR(validate_spatial_wkb_inputs(validation_block, {0}));
    for (size_t row = 0; row < validation_column->size(); ++row) {
        if (validation_column->is_null_at(row)) {
            destination.insert_default();
        } else {
            const auto value = validation_column->get_data_at(row);
            destination.insert_data(value.data, value.size);
        }
    }
    return Status::OK();
}

} // namespace doris
