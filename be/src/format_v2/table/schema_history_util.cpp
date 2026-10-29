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

#include "format_v2/table/schema_history_util.h"

#include <algorithm>
#include <ranges>
#include <string>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "util/string_util.h"

namespace doris::format {
namespace {

DataTypePtr nullable_like_original(const DataTypePtr& original, DataTypePtr nested) {
    return original != nullptr && original->is_nullable() ? make_nullable(nested) : nested;
}

const schema::external::TField* find_child_field_by_name(
        const std::vector<schema::external::TFieldPtr>& fields, const std::string& name) {
    for (const auto& field_ptr : fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field == nullptr) {
            continue;
        }
        if (field->__isset.name && to_lower(field->name) == to_lower(name)) {
            return field;
        }
        if (field->__isset.name_mapping &&
            std::ranges::any_of(field->name_mapping, [&](const std::string& alias) {
                return to_lower(alias) == to_lower(name);
            })) {
            return field;
        }
    }
    return nullptr;
}

void annotate_column_from_field(ColumnDefinition* column, const schema::external::TField& field);

void annotate_struct_children(ColumnDefinition* column,
                              const schema::external::TStructField& struct_field) {
    DORIS_CHECK(column != nullptr);
    if (!struct_field.__isset.fields) {
        return;
    }
    for (auto& child : column->children) {
        const auto* child_field = find_child_field_by_name(struct_field.fields, child.name);
        if (child_field != nullptr) {
            annotate_column_from_field(&child, *child_field);
        }
    }
}

void annotate_column_from_field(ColumnDefinition* column, const schema::external::TField& field) {
    DORIS_CHECK(column != nullptr);
    if (field.__isset.id) {
        column->identifier = Field::create_field<TYPE_INT>(field.id);
    }
    column->name_mapping =
            field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {};
    column->has_name_mapping =
            field.__isset.name_mapping_is_authoritative && field.name_mapping_is_authoritative;
    column->timestamp_is_adjusted_to_utc =
            field.__isset.timestamp_is_adjusted_to_utc
                    ? std::make_optional(field.timestamp_is_adjusted_to_utc)
                    : std::nullopt;
    if (!field.__isset.nestedField) {
        return;
    }
    if (field.nestedField.__isset.struct_field) {
        annotate_struct_children(column, field.nestedField.struct_field);
    } else if (field.nestedField.__isset.array_field) {
        if (column->children.empty() || !field.nestedField.array_field.__isset.item_field) {
            return;
        }
        const auto* item_field = get_field_ptr(field.nestedField.array_field.item_field);
        if (item_field != nullptr) {
            annotate_column_from_field(&column->children.front(), *item_field);
        }
    } else if (field.nestedField.__isset.map_field) {
        if (!column->children.empty() && field.nestedField.map_field.__isset.key_field) {
            const auto* key_field = get_field_ptr(field.nestedField.map_field.key_field);
            if (key_field != nullptr) {
                annotate_column_from_field(&column->children.front(), *key_field);
            }
        }
        if (column->children.size() > 1 && field.nestedField.map_field.__isset.value_field) {
            const auto* value_field = get_field_ptr(field.nestedField.map_field.value_field);
            if (value_field != nullptr) {
                annotate_column_from_field(&column->children[1], *value_field);
            }
        }
    }
}

} // namespace

const schema::external::TSchema* find_history_schema(const TFileScanRangeParams* params,
                                                     int64_t schema_id) {
    if (params == nullptr || !params->__isset.history_schema_info) {
        return nullptr;
    }
    for (const auto& schema : params->history_schema_info) {
        if (schema.__isset.schema_id && schema.schema_id == schema_id) {
            return &schema;
        }
    }
    return nullptr;
}

const schema::external::TSchema* find_current_schema(const TFileScanRangeParams* params) {
    if (params == nullptr || !params->__isset.history_schema_info ||
        params->history_schema_info.empty()) {
        return nullptr;
    }
    if (params->__isset.current_schema_id) {
        if (const auto* schema = find_history_schema(params, params->current_schema_id);
            schema != nullptr) {
            return schema;
        }
    }
    return &params->history_schema_info.front();
}

ColumnDefinition build_schema_identity_from_external_field(const schema::external::TField& field) {
    ColumnDefinition identity;
    if (field.__isset.id) {
        identity.identifier = Field::create_field<TYPE_INT>(field.id);
    }
    identity.name = field.__isset.name ? field.name : "";
    identity.name_mapping =
            field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {};
    identity.has_name_mapping =
            field.__isset.name_mapping_is_authoritative && field.name_mapping_is_authoritative;
    identity.timestamp_is_adjusted_to_utc =
            field.__isset.timestamp_is_adjusted_to_utc
                    ? std::make_optional(field.timestamp_is_adjusted_to_utc)
                    : std::nullopt;
    if (!field.__isset.nestedField) {
        return identity;
    }
    if (field.nestedField.__isset.struct_field && field.nestedField.struct_field.__isset.fields) {
        for (const auto& child_ptr : field.nestedField.struct_field.fields) {
            if (const auto* child = get_field_ptr(child_ptr); child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
            }
        }
    } else if (field.nestedField.__isset.array_field &&
               field.nestedField.array_field.__isset.item_field) {
        if (const auto* child = get_field_ptr(field.nestedField.array_field.item_field);
            child != nullptr) {
            identity.children.push_back(build_schema_identity_from_external_field(*child));
            identity.children.back().name = "element";
        }
    } else if (field.nestedField.__isset.map_field) {
        if (field.nestedField.map_field.__isset.key_field) {
            if (const auto* child = get_field_ptr(field.nestedField.map_field.key_field);
                child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
                identity.children.back().name = "key";
            }
        }
        if (field.nestedField.map_field.__isset.value_field) {
            if (const auto* child = get_field_ptr(field.nestedField.map_field.value_field);
                child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
                identity.children.back().name = "value";
            }
        }
    }
    return identity;
}

bool can_map_by_history_schema(const TFileScanRangeParams* params, int64_t split_schema_id) {
    if (split_schema_id < 0 || params == nullptr || !params->__isset.current_schema_id ||
        !params->__isset.history_schema_info) {
        return false;
    }
    return find_history_schema(params, split_schema_id) != nullptr;
}

Status annotate_file_schema_from_history(const TFileScanRangeParams* params,
                                         int64_t split_schema_id,
                                         std::vector<ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    const auto* schema = find_history_schema(params, split_schema_id);
    DORIS_CHECK(schema != nullptr);
    if (!schema->__isset.root_field || !schema->root_field.__isset.fields) {
        return Status::OK();
    }
    for (auto& column : *file_schema) {
        const auto* field = find_child_field_by_name(schema->root_field.fields, column.name);
        if (field != nullptr) {
            annotate_column_from_field(&column, *field);
        }
    }
    return Status::OK();
}

Status apply_file_timestamp_semantics(ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK(column->type != nullptr);
    const auto primitive = remove_nullable(column->type)->get_primitive_type();
    // Validate before changing children: silently skipping a malformed parent leaves stale types.
    if ((primitive == TYPE_ARRAY && column->children.size() != 1) ||
        (primitive == TYPE_MAP && column->children.size() != 2)) {
        return Status::InvalidArgument("Invalid {} column '{}': {} children",
                                       column->type->get_name(), column->name,
                                       column->children.size());
    }
    if (column->timestamp_is_adjusted_to_utc.has_value() &&
        (primitive == TYPE_DATETIMEV2 || primitive == TYPE_TIMESTAMPTZ)) {
        const auto target =
                *column->timestamp_is_adjusted_to_utc ? TYPE_TIMESTAMPTZ : TYPE_DATETIMEV2;
        column->type = DataTypeFactory::instance().create_data_type(
                target, column->type->is_nullable(), 0, column->type->get_scale());
        return Status::OK();
    }

    std::vector<DataTypePtr> child_types;
    child_types.reserve(column->children.size());
    for (auto& child : column->children) {
        RETURN_IF_ERROR(apply_file_timestamp_semantics(&child));
        child_types.push_back(child.type);
    }
    if (primitive == TYPE_ARRAY) {
        column->type = nullable_like_original(column->type,
                                              std::make_shared<DataTypeArray>(child_types.front()));
    } else if (primitive == TYPE_MAP) {
        column->type = nullable_like_original(
                column->type, std::make_shared<DataTypeMap>(make_nullable(child_types[0]),
                                                            make_nullable(child_types[1])));
    } else if (primitive == TYPE_STRUCT) {
        Strings child_names;
        child_names.reserve(column->children.size());
        for (const auto& child : column->children) {
            child_names.push_back(child.name);
        }
        column->type = nullable_like_original(
                column->type, std::make_shared<DataTypeStruct>(child_types, child_names));
    }
    return Status::OK();
}

} // namespace doris::format
