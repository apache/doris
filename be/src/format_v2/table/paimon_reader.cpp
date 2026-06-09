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

#include "format_v2/table/paimon_reader.h"

#include <cstring>
#include <string>

#include "format/table/deletion_vector_reader.h"
#include "util/string_util.h"

namespace doris::paimon {
namespace {

const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
}

const schema::external::TSchema* find_schema(const TFileScanRangeParams* params,
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

const schema::external::TField* find_child_field_by_name(
        const std::vector<schema::external::TFieldPtr>& fields, const std::string& name) {
    for (const auto& field_ptr : fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field != nullptr && field->__isset.name && to_lower(field->name) == to_lower(name)) {
            return field;
        }
    }
    return nullptr;
}

void annotate_column_from_field(format::ColumnDefinition* column,
                                const schema::external::TField& field);

void annotate_struct_children(format::ColumnDefinition* column,
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

void annotate_column_from_field(format::ColumnDefinition* column,
                                const schema::external::TField& field) {
    DORIS_CHECK(column != nullptr);
    if (field.__isset.id) {
        column->identifier = Field::create_field<TYPE_INT>(field.id);
    }
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

Status PaimonReader::prepare_split(const format::SplitReadOptions& options) {
    _split_schema_id = -1;
    const auto& paimon_params = options.current_range.table_format_params.paimon_params;
    if (paimon_params.__isset.schema_id) {
        _split_schema_id = paimon_params.schema_id;
    }
    return format::TableReader::prepare_split(options);
}

format::TableColumnMappingMode PaimonReader::mapping_mode() const {
    if (_split_schema_id < 0 || _scan_params == nullptr ||
        !_scan_params->__isset.current_schema_id || !_scan_params->__isset.history_schema_info) {
        return format::TableColumnMappingMode::BY_NAME;
    }
    return find_schema(_scan_params, _split_schema_id) == nullptr
                   ? format::TableColumnMappingMode::BY_NAME
                   : format::TableColumnMappingMode::BY_FIELD_ID;
}

Status PaimonReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (mapping_mode() != format::TableColumnMappingMode::BY_FIELD_ID) {
        return Status::OK();
    }
    const auto* schema = find_schema(_scan_params, _split_schema_id);
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

Status PaimonReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                 DeleteFileDesc* desc, bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    const auto& table_desc = t_desc.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }
    const auto& deletion_file = table_desc.deletion_file;

    const std::string key_prefix = "paimon_dv:";
    desc->key.resize(key_prefix.size() + deletion_file.path.size() + sizeof(deletion_file.offset));
    char* key_data = desc->key.data();
    memcpy(key_data, key_prefix.data(), key_prefix.size());
    key_data += key_prefix.size();
    memcpy(key_data, deletion_file.path.data(), deletion_file.path.size());
    key_data += deletion_file.path.size();
    memcpy(key_data, &deletion_file.offset, sizeof(deletion_file.offset));
    desc->path = deletion_file.path;
    desc->start_offset = deletion_file.offset;
    desc->size = deletion_file.length + 4;
    desc->file_size = -1;
    desc->format = DeleteFileDesc::Format::PAIMON;
    *has_delete_file = true;
    return Status::OK();
}

} // namespace doris::paimon
