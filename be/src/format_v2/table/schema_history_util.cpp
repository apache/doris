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

#include "core/field.h"
#include "util/string_util.h"

namespace doris::format {
namespace {

const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
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

} // namespace doris::format
