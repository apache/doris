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

#include "exec/sink/writer/iceberg/iceberg_write_validator.h"

#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"

namespace doris::iceberg {
namespace {

using Path = std::vector<std::string_view>;

struct ValidationLocation {
    size_t row = 0;
    std::optional<size_t> ordinal;
    std::string_view ordinal_kind;
};

enum class NonNullReason {
    SCHEMA_REQUIRED,
    MAP_KEY,
};

std::string path_to_string(const Path& path) {
    std::string result;
    for (size_t i = 0; i < path.size(); ++i) {
        if (i > 0) {
            result.push_back('.');
        }
        result.append(path[i]);
    }
    return result;
}

Status null_violation(const Path& path, const ValidationLocation& location, NonNullReason reason) {
    std::string position = fmt::format("row {}", location.row);
    if (location.ordinal.has_value()) {
        position += fmt::format(", {} {}", location.ordinal_kind, *location.ordinal);
    }
    return Status::DataQualityError("Iceberg {} '{}' contains NULL at {}",
                                    reason == NonNullReason::MAP_KEY ? "map key" : "required field",
                                    path_to_string(path), position);
}

bool requires_validation(const NestedField& field) {
    if (field.is_required()) {
        return true;
    }
    switch (field.field_type()->type_id()) {
    case TypeID::STRUCT: {
        const auto& fields = field.field_type()->as_struct_type()->fields();
        return std::any_of(fields.begin(), fields.end(),
                           [](const NestedField& child) { return requires_validation(child); });
    }
    case TypeID::LIST:
        return requires_validation(field.field_type()->as_list_type()->element_field());
    case TypeID::MAP:
        // Iceberg map keys are required independently of the map field's nullability.
        return true;
    default:
        return false;
    }
}

template <typename Offsets>
Status validate_collection_offsets(const Offsets& offsets, size_t position, size_t row,
                                   std::string_view container_name, const Path& path, size_t* start,
                                   size_t* end) {
    if (position >= offsets.size()) {
        return Status::InternalError(
                "Iceberg {} field '{}' has {} offsets, but position {} is required", container_name,
                path_to_string(path), offsets.size(), position);
    }
    *start = position == 0 ? 0 : offsets[position - 1];
    *end = offsets[position];
    if (*start > *end) {
        return Status::InternalError(
                "Iceberg {} field '{}' has invalid offsets at row {}: [{}, {}]", container_name,
                path_to_string(path), row, *start, *end);
    }
    return Status::OK();
}

Status validate_value(const NestedField& field, const IColumn& column, const DataTypePtr& type,
                      size_t position, const ValidationLocation& location, Path& path,
                      NonNullReason reason) {
    if (position >= column.size()) {
        return Status::InternalError(
                "Iceberg write column '{}' has {} values, but position {} "
                "is required",
                path_to_string(path), column.size(), position);
    }

    if (column.is_null_at(position)) {
        if (reason == NonNullReason::MAP_KEY || field.is_required()) {
            return null_violation(path, location, reason);
        }
        // An optional STRUCT/LIST/MAP is a NULL value in its own right. Its children and
        // elements are masked by the parent NULL and must not be inspected.
        return Status::OK();
    }

    const IColumn* data_column = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        data_column = &nullable->get_nested_column();
    }

    const auto primitive_type = remove_nullable(type)->get_primitive_type();
    switch (field.field_type()->type_id()) {
    case TypeID::STRUCT: {
        if (primitive_type != TYPE_STRUCT) {
            return Status::InternalError("Iceberg STRUCT field '{}' does not match Doris type {}",
                                         path_to_string(path), type->get_name());
        }
        const auto* struct_column = check_and_get_column<ColumnStruct>(*data_column);
        const auto* struct_type = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());
        if (struct_column == nullptr) {
            return Status::InternalError("Iceberg STRUCT field '{}' is not stored as a STRUCT",
                                         path_to_string(path));
        }
        const auto& fields = field.field_type()->as_struct_type()->fields();
        if (struct_column->tuple_size() != fields.size() ||
            struct_type->get_elements().size() != fields.size()) {
            return Status::InternalError(
                    "Iceberg STRUCT field '{}' has {} schema fields, {} Doris type fields, and "
                    "{} column fields",
                    path_to_string(path), fields.size(), struct_type->get_elements().size(),
                    struct_column->tuple_size());
        }
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!requires_validation(fields[i])) {
                continue;
            }
            path.emplace_back(fields[i].field_name());
            Status status = validate_value(fields[i], struct_column->get_column(i),
                                           struct_type->get_element(i), position, location, path,
                                           NonNullReason::SCHEMA_REQUIRED);
            path.pop_back();
            RETURN_IF_ERROR(status);
        }
        return Status::OK();
    }
    case TypeID::LIST: {
        if (primitive_type != TYPE_ARRAY) {
            return Status::InternalError("Iceberg LIST field '{}' does not match Doris type {}",
                                         path_to_string(path), type->get_name());
        }
        const auto* array_column = check_and_get_column<ColumnArray>(*data_column);
        const auto* array_type = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        if (array_column == nullptr) {
            return Status::InternalError("Iceberg LIST field '{}' is not stored as an ARRAY",
                                         path_to_string(path));
        }
        const auto& offsets = array_column->get_offsets();
        size_t start = 0;
        size_t end = 0;
        RETURN_IF_ERROR(validate_collection_offsets(offsets, position, location.row, "LIST", path,
                                                    &start, &end));
        if (end > array_column->get_data().size()) {
            return Status::InternalError(
                    "Iceberg LIST field '{}' has invalid offsets at row {}: [{}, {}], "
                    "element count {}",
                    path_to_string(path), location.row, start, end,
                    array_column->get_data().size());
        }
        const auto& element = field.field_type()->as_list_type()->element_field();
        if (!requires_validation(element)) {
            return Status::OK();
        }
        path.emplace_back(element.field_name());
        for (size_t i = start; i < end; ++i) {
            ValidationLocation element_location {
                    .row = location.row,
                    .ordinal = i - start,
                    .ordinal_kind = "element",
            };
            Status status =
                    validate_value(element, array_column->get_data(), array_type->get_nested_type(),
                                   i, element_location, path, NonNullReason::SCHEMA_REQUIRED);
            if (!status.ok()) {
                path.pop_back();
                return status;
            }
        }
        path.pop_back();
        return Status::OK();
    }
    case TypeID::MAP: {
        if (primitive_type != TYPE_MAP) {
            return Status::InternalError("Iceberg MAP field '{}' does not match Doris type {}",
                                         path_to_string(path), type->get_name());
        }
        const auto* map_column = check_and_get_column<ColumnMap>(*data_column);
        const auto* map_type = assert_cast<const DataTypeMap*>(remove_nullable(type).get());
        if (map_column == nullptr) {
            return Status::InternalError("Iceberg MAP field '{}' is not stored as a MAP",
                                         path_to_string(path));
        }
        const auto& offsets = map_column->get_offsets();
        size_t start = 0;
        size_t end = 0;
        RETURN_IF_ERROR(validate_collection_offsets(offsets, position, location.row, "MAP", path,
                                                    &start, &end));
        if (end > map_column->get_keys().size() || end > map_column->get_values().size()) {
            return Status::InternalError(
                    "Iceberg MAP field '{}' has invalid offsets at row {}: [{}, {}], "
                    "key count {}, value count {}",
                    path_to_string(path), location.row, start, end, map_column->get_keys().size(),
                    map_column->get_values().size());
        }

        const auto& map_type_desc = field.field_type()->as_map_type();
        path.emplace_back(map_type_desc->key_field().field_name());
        for (size_t i = start; i < end; ++i) {
            ValidationLocation entry_location {
                    .row = location.row,
                    .ordinal = i - start,
                    .ordinal_kind = "map entry",
            };
            Status status = validate_value(map_type_desc->key_field(), map_column->get_keys(),
                                           map_type->get_key_type(), i, entry_location, path,
                                           NonNullReason::MAP_KEY);
            if (!status.ok()) {
                path.pop_back();
                return status;
            }
        }
        path.pop_back();

        const auto& value = map_type_desc->value_field();
        if (!requires_validation(value)) {
            return Status::OK();
        }
        path.emplace_back(value.field_name());
        for (size_t i = start; i < end; ++i) {
            ValidationLocation entry_location {
                    .row = location.row,
                    .ordinal = i - start,
                    .ordinal_kind = "map entry",
            };
            Status status =
                    validate_value(value, map_column->get_values(), map_type->get_value_type(), i,
                                   entry_location, path, NonNullReason::SCHEMA_REQUIRED);
            if (!status.ok()) {
                path.pop_back();
                return status;
            }
        }
        path.pop_back();
        return Status::OK();
    }
    default:
        return Status::OK();
    }
}

} // namespace

Status validate_write_block(const Schema& schema, const Block& block) {
    if (block.columns() != schema.columns().size()) {
        return Status::InternalError(
                "Iceberg write block has {} columns, but pinned schema has {} fields",
                block.columns(), schema.columns().size());
    }

    Path path;
    path.reserve(8);
    for (size_t field_index = 0; field_index < schema.columns().size(); ++field_index) {
        const auto& field = schema.columns()[field_index];
        if (!requires_validation(field)) {
            continue;
        }
        const auto& column_with_name = block.get_by_position(field_index);
        ColumnPtr column = column_with_name.column->convert_to_full_column_if_const();
        if (!column) {
            return Status::InternalError("Iceberg write block column {} is null", field_index);
        }
        if (column->size() < block.rows()) {
            return Status::InternalError(
                    "Iceberg write block column '{}' has {} values, but block has {} rows",
                    field.field_name(), column->size(), block.rows());
        }

        path.emplace_back(field.field_name());
        if (field.field_type()->is_primitive_type() && field.is_required()) {
            if (column->has_null()) {
                for (size_t row = 0; row < block.rows(); ++row) {
                    if (!column->is_null_at(row)) {
                        continue;
                    }
                    ValidationLocation location {
                            .row = row,
                            .ordinal = std::nullopt,
                            .ordinal_kind = {},
                    };
                    Status status = null_violation(path, location, NonNullReason::SCHEMA_REQUIRED);
                    path.pop_back();
                    return status;
                }
            }
            path.pop_back();
            continue;
        }
        for (size_t row = 0; row < block.rows(); ++row) {
            ValidationLocation location {
                    .row = row,
                    .ordinal = std::nullopt,
                    .ordinal_kind = {},
            };
            Status status = validate_value(field, *column, column_with_name.type, row, location,
                                           path, NonNullReason::SCHEMA_REQUIRED);
            if (!status.ok()) {
                path.pop_back();
                return status;
            }
        }
        path.pop_back();
    }
    return Status::OK();
}

} // namespace doris::iceberg
