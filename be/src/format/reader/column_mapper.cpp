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

#include "format/reader/column_mapper.h"

#include <algorithm>
#include <set>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "format/reader/expr/cast.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"

namespace doris::reader {
namespace {

std::string append_field_path(std::string_view parent, std::string_view child) {
    if (parent.empty()) {
        return std::string(child);
    }
    std::string path(parent);
    path.push_back('.');
    path.append(child);
    return path;
}

bool is_type(const DataTypePtr& type, PrimitiveType primitive_type) {
    return type != nullptr && remove_nullable(type)->get_primitive_type() == primitive_type;
}

bool is_struct_type(const DataTypePtr& type) {
    return is_type(type, PrimitiveType::TYPE_STRUCT);
}

bool is_array_type(const DataTypePtr& type) {
    return is_type(type, PrimitiveType::TYPE_ARRAY);
}

bool is_map_type(const DataTypePtr& type) {
    return is_type(type, PrimitiveType::TYPE_MAP);
}

bool contains_struct_type(const DataTypePtr& type) {
    DataTypePtr nested_type = remove_nullable(type);
    if (nested_type == nullptr) {
        return false;
    }
    switch (nested_type->get_primitive_type()) {
    case PrimitiveType::TYPE_STRUCT:
        return true;
    case PrimitiveType::TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        return contains_struct_type(array_type->get_nested_type());
    }
    case PrimitiveType::TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        return contains_struct_type(map_type->get_key_type()) ||
               contains_struct_type(map_type->get_value_type());
    }
    default:
        return false;
    }
}

bool mapping_needs_materialization(const ColumnMapping& mapping) {
    return !mapping.is_trivial || !mapping.child_mappings.empty() ||
           !mapping.file_child_index.has_value() || mapping.default_expr != nullptr ||
           mapping.is_constant;
}

VExprContextSPtr create_slot_ref_context(ColumnId file_column_id, size_t source_column_index,
                                         const DataTypePtr& type, const std::string& name) {
    return VExprContext::create_shared(
            TableSlotRef::create_shared(file_column_id, source_column_index, -1, type, name));
}

VExprContextSPtr create_cast_context(ColumnId file_column_id, size_t source_column_index,
                                     const DataTypePtr& file_type, const DataTypePtr& table_type,
                                     const std::string& name) {
    auto expr = Cast::create_shared(table_type);
    expr->add_child(
            TableSlotRef::create_shared(file_column_id, source_column_index, -1, file_type, name));
    return VExprContext::create_shared(expr);
}

} // namespace

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER = "_last_updated_sequence_number";

Status TableColumnMapper::create_mapping(const std::vector<TableColumn>& projected_columns,
                                         const std::map<std::string, Field>& partition_values,
                                         const std::vector<SchemaField>& file_schema) {
    // 真实实现会做 field id/name matching、类型转换、复杂列 child mapping、缺失列
    // default/partition/generated 表达式构造。
    _mappings.clear();
    std::map<ColumnId, size_t> source_column_indexes;
    size_t next_source_column_index = 0;
    for (const auto& table_column : projected_columns) {
        ColumnMapping mapping;
        size_t source_column_index = next_source_column_index;
        const auto* file_field = _find_file_field(table_column, file_schema);
        if (file_field != nullptr) {
            auto [it, inserted] =
                    source_column_indexes.emplace(file_field->id, next_source_column_index);
            source_column_index = it->second;
            if (inserted) {
                ++next_source_column_index;
            }
        }
        RETURN_IF_ERROR(_create_column_mapping(table_column, std::nullopt, file_schema,
                                               partition_values, "", source_column_index,
                                               &mapping));
        _mappings.push_back(std::move(mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::_create_column_mapping(
        const TableColumn& table_column, std::optional<size_t> table_child_index,
        const std::vector<SchemaField>& file_schema,
        const std::map<std::string, Field>& partition_values, std::string_view field_path,
        size_t source_column_index, ColumnMapping* mapping) {
    if (mapping == nullptr) {
        return Status::InvalidArgument("column mapping output is null");
    }
    mapping->table_column_id = table_column.id;
    mapping->table_type = table_column.type;
    if (table_child_index.has_value()) {
        mapping->table_child_index = *table_child_index;
    }

    size_t file_field_index = 0;
    const auto* file_field =
            _find_file_field(table_column, file_schema, field_path, &file_field_index);
    if (file_field != nullptr) {
        mapping->file_column_id = file_field->id;
        mapping->file_child_index = file_field_index;
        mapping->file_type = file_field->type;

        if (is_struct_type(table_column.type) && !table_column.children.empty()) {
            if (!is_struct_type(file_field->type)) {
                return Status::NotSupported(
                        "table struct column '{}' (id={}) maps to non-struct file column "
                        "(id={}, type={})",
                        table_column.name, table_column.id, file_field->id,
                        file_field->type->get_name());
            }
            std::string matched_field_path = append_field_path(field_path, file_field->name);
            RETURN_IF_ERROR(_create_struct_child_mappings(
                    table_column, *file_field, partition_values, matched_field_path, mapping));
            mapping->is_trivial = mapping->child_mappings.empty();
        } else if (is_array_type(table_column.type) && !table_column.children.empty()) {
            if (!is_array_type(file_field->type)) {
                return Status::NotSupported(
                        "table array column '{}' (id={}) maps to non-array file column "
                        "(id={}, type={})",
                        table_column.name, table_column.id, file_field->id,
                        file_field->type->get_name());
            }
            std::string matched_field_path = append_field_path(field_path, file_field->name);
            RETURN_IF_ERROR(_create_array_child_mapping(table_column, *file_field, partition_values,
                                                        matched_field_path, mapping));
            mapping->is_trivial = mapping->child_mappings.empty() &&
                                  _is_same_type(mapping->table_type, mapping->file_type);
            if (!mapping->is_trivial && mapping->child_mappings.empty()) {
                return Status::NotSupported(
                        "array column mapping with type conversion is not supported yet: table "
                        "column '{}' (id={}, type={}) vs file column (id={}, type={})",
                        table_column.name, mapping->table_column_id,
                        mapping->table_type->get_name(), mapping->file_column_id.value(),
                        mapping->file_type->get_name());
            }
        } else if (is_map_type(table_column.type) && !table_column.children.empty()) {
            if (!is_map_type(file_field->type)) {
                return Status::NotSupported(
                        "table map column '{}' (id={}) maps to non-map file column "
                        "(id={}, type={})",
                        table_column.name, table_column.id, file_field->id,
                        file_field->type->get_name());
            }
            std::string matched_field_path = append_field_path(field_path, file_field->name);
            RETURN_IF_ERROR(_create_map_child_mappings(table_column, *file_field, partition_values,
                                                       matched_field_path, mapping));
            mapping->is_trivial = mapping->child_mappings.empty() &&
                                  _is_same_type(mapping->table_type, mapping->file_type);
            if (!mapping->is_trivial && mapping->child_mappings.empty()) {
                return Status::NotSupported(
                        "map column mapping with type conversion is not supported yet: table "
                        "column '{}' (id={}, type={}) vs file column (id={}, type={})",
                        table_column.name, mapping->table_column_id,
                        mapping->table_type->get_name(), mapping->file_column_id.value(),
                        mapping->file_type->get_name());
            }
        } else {
            mapping->is_trivial = _is_same_type(mapping->table_type, mapping->file_type);
            if (!mapping->is_trivial) {
                if (!field_path.empty()) {
                    return Status::NotSupported(
                            "nested column mapping with type conversion is not supported yet: "
                            "table column '{}' (id={}, type={}) vs file column (id={}, type={})",
                            table_column.name, mapping->table_column_id,
                            mapping->table_type->get_name(), mapping->file_column_id.value(),
                            mapping->file_type->get_name());
                }
                mapping->projection = create_cast_context(*mapping->file_column_id,
                                                          source_column_index, mapping->file_type,
                                                          mapping->table_type, file_field->name);
            }
        }

        if (field_path.empty() && mapping->projection == nullptr) {
            mapping->projection = create_slot_ref_context(
                    *mapping->file_column_id, source_column_index, mapping->file_type,
                    file_field->name);
        }
        return Status::OK();
    }

    if (table_column.is_partition_key && partition_values.count(table_column.name) > 0) {
        mapping->default_expr = VExprContext::create_shared(TableLiteral::create_shared(
                mapping->table_type, partition_values.at(table_column.name)));
    } else if (table_column.default_expr != nullptr) {
        mapping->is_constant = true;
        mapping->default_expr = table_column.default_expr;
    } else if (table_column.name == ROW_LINEAGE_ROW_ID) {
        mapping->virtual_column_type = TableVirtualColumnType::ROW_ID;
    } else if (table_column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
        mapping->virtual_column_type = TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER;
    } else {
        if (table_column.is_partition_key) {
            return Status::InvalidArgument(
                    "Table column '{}' (id={}) does not have a matching partition value",
                    table_column.name, table_column.id);
        }
        if (!_options.allow_missing_columns) {
            return Status::InvalidArgument(
                    "Table column '{}' (id={}) does not have a matching file column",
                    table_column.name, table_column.id);
        }
    }
    return Status::OK();
}

Status TableColumnMapper::create_scan_request(const std::map<int32_t, TableFilter>& table_filters,
                                              const std::vector<TableColumn>& projected_columns,
                                              FileScanRequest* file_request) {
    // 真实实现会把 table projection/filter 转换成 file-local projection/filter。
    if (file_request == nullptr) {
        return Status::InvalidArgument("file scan request output is null");
    }
    file_request->predicate_columns.clear();
    file_request->non_predicate_columns.clear();
    file_request->projected_columns.clear();
    file_request->local_filters.clear();
    file_request->reader_expression_map.clear();
    std::set<ColumnId> projected_file_column_set;
    for (const auto& table_column : projected_columns) {
        const auto* mapping = _find_mapping(table_column.id);
        if (mapping != nullptr && mapping->file_column_id.has_value()) {
            if (projected_file_column_set.insert(*mapping->file_column_id).second) {
                file_request->projected_columns.push_back(*mapping->file_column_id);
                if (table_filters.count(table_column.id) == 0) {
                    file_request->non_predicate_columns.push_back(*mapping->file_column_id);
                }
            }
        }
    }
    RETURN_IF_ERROR(localize_filters(table_filters, file_request));
    return Status::OK();
}

Status TableColumnMapper::localize_filters(const std::map<int32_t, TableFilter>& table_filters,
                                           FileScanRequest* file_request) const {
    // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
    // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
    for (const auto& it : table_filters) {
        const auto* mapping = _find_mapping(it.first);
        if (mapping == nullptr || !mapping->file_column_id.has_value()) {
            continue;
        }
        if (!it.second.can_be_localized()) {
            // TODO: Rewrite table filter to reader_expression_map
            // file_request->reader_expression_map.emplace_back(mapping->table_column_id, it.second.conjunct);
        } else {
            FileLocalFilter local_filter;
            local_filter.file_column_id = *mapping->file_column_id;
            local_filter.conjunct = it.second.conjunct;
            local_filter.predicates = it.second.predicates;
            file_request->local_filters.push_back(std::move(local_filter));
        }
        if (std::find(file_request->predicate_columns.begin(),
                      file_request->predicate_columns.end(),
                      *mapping->file_column_id) == file_request->predicate_columns.end()) {
            file_request->predicate_columns.push_back(*mapping->file_column_id);
        }
    }
    return Status::OK();
}

const SchemaField* TableColumnMapper::_find_file_field(
        const TableColumn& table_column, const std::vector<SchemaField>& file_schema) const {
    return _find_file_field(table_column, file_schema, "", nullptr);
}

const SchemaField* TableColumnMapper::_find_file_field(const TableColumn& table_column,
                                                       const std::vector<SchemaField>& file_schema,
                                                       std::string_view field_path,
                                                       size_t* field_index) const {
    auto set_field_index = [field_index](size_t index) {
        if (field_index != nullptr) {
            *field_index = index;
        }
    };
    if (_options.mode == TableColumnMappingMode::BY_FIELD_ID) {
        for (size_t i = 0; i < file_schema.size(); ++i) {
            const auto& field = file_schema[i];
            if (field.field_id.has_value() && field.field_id.value() == table_column.id) {
                set_field_index(i);
                return &field;
            }
        }
        for (size_t i = 0; i < file_schema.size(); ++i) {
            const auto& field = file_schema[i];
            std::string candidate_path = append_field_path(field_path, field.name);
            auto mapped_field_id = _mapped_field_id_by_name(candidate_path);
            if (mapped_field_id.has_value() && mapped_field_id.value() == table_column.id) {
                set_field_index(i);
                return &field;
            }
        }
        if (_options.name_mapping.empty()) {
            for (size_t i = 0; i < file_schema.size(); ++i) {
                const auto& field = file_schema[i];
                if (!field.field_id.has_value() && field.id == table_column.id) {
                    set_field_index(i);
                    return &field;
                }
            }
        }
        return nullptr;
    }
    for (size_t i = 0; i < file_schema.size(); ++i) {
        const auto& field = file_schema[i];
        if (_options.mode == TableColumnMappingMode::BY_NAME && field.name == table_column.name) {
            set_field_index(i);
            return &field;
        }
    }
    return nullptr;
}

const SchemaField* TableColumnMapper::find_file_field_by_table_column_id(
        ColumnId table_column_id, const std::vector<SchemaField>& file_schema) const {
    for (const auto& field : file_schema) {
        if (field.field_id.has_value() && field.field_id.value() == table_column_id) {
            return &field;
        }
    }
    for (const auto& field : file_schema) {
        auto mapped_field_id = _mapped_field_id_by_name(field.name);
        if (mapped_field_id.has_value() && mapped_field_id.value() == table_column_id) {
            return &field;
        }
    }
    if (_options.name_mapping.empty()) {
        for (const auto& field : file_schema) {
            if (!field.field_id.has_value() && field.id == table_column_id) {
                return &field;
            }
        }
    }
    return nullptr;
}

std::optional<ColumnId> TableColumnMapper::_mapped_field_id_by_name(
        std::string_view field_path) const {
    auto it = _options.name_mapping.find(std::string(field_path));
    if (it != _options.name_mapping.end()) {
        return it->second;
    }
    return std::nullopt;
}

Status TableColumnMapper::_create_struct_child_mappings(
        const TableColumn& table_column, const SchemaField& file_field,
        const std::map<std::string, Field>& partition_values, std::string_view field_path,
        ColumnMapping* mapping) {
    for (size_t i = 0; i < table_column.children.size(); ++i) {
        ColumnMapping child_mapping;
        RETURN_IF_ERROR(_create_column_mapping(table_column.children[i], i, file_field.children,
                                               partition_values, field_path, i, &child_mapping));
        mapping->child_mappings.push_back(std::move(child_mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::_create_array_child_mapping(
        const TableColumn& table_column, const SchemaField& file_field,
        const std::map<std::string, Field>& partition_values, std::string_view field_path,
        ColumnMapping* mapping) {
    if (table_column.children.size() != 1) {
        return Status::InvalidArgument("array column '{}' (id={}) must have one element child",
                                       table_column.name, table_column.id);
    }
    if (!contains_struct_type(table_column.type)) {
        return Status::OK();
    }
    if (file_field.children.empty()) {
        return Status::NotSupported(
                "array column '{}' (id={}) requires file element schema for nested mapping",
                table_column.name, table_column.id);
    }

    ColumnMapping element_mapping;
    RETURN_IF_ERROR(_create_column_mapping(table_column.children[0], 0, file_field.children,
                                           partition_values, field_path, 0, &element_mapping));
    if (!element_mapping.file_child_index.has_value()) {
        return Status::NotSupported("array column '{}' (id={}) cannot map element field id {}",
                                    table_column.name, table_column.id,
                                    table_column.children[0].id);
    }
    if (mapping_needs_materialization(element_mapping)) {
        mapping->child_mappings.push_back(std::move(element_mapping));
    }
    return Status::OK();
}

Status TableColumnMapper::_create_map_child_mappings(
        const TableColumn& table_column, const SchemaField& file_field,
        const std::map<std::string, Field>& partition_values, std::string_view field_path,
        ColumnMapping* mapping) {
    if (table_column.children.size() != 2) {
        return Status::InvalidArgument("map column '{}' (id={}) must have key and value children",
                                       table_column.name, table_column.id);
    }
    if (!contains_struct_type(table_column.type)) {
        return Status::OK();
    }
    if (file_field.children.size() < 2) {
        return Status::NotSupported(
                "map column '{}' (id={}) requires file key/value schema for nested mapping",
                table_column.name, table_column.id);
    }

    std::vector<ColumnMapping> child_mappings;
    child_mappings.reserve(2);
    bool needs_materialization = false;
    for (size_t i = 0; i < table_column.children.size(); ++i) {
        ColumnMapping child_mapping;
        RETURN_IF_ERROR(_create_column_mapping(table_column.children[i], i, file_field.children,
                                               partition_values, field_path, i, &child_mapping));
        if (!child_mapping.file_child_index.has_value()) {
            return Status::NotSupported("map column '{}' (id={}) cannot map child field id {}",
                                        table_column.name, table_column.id,
                                        table_column.children[i].id);
        }
        needs_materialization =
                needs_materialization || mapping_needs_materialization(child_mapping);
        child_mappings.push_back(std::move(child_mapping));
    }
    if (needs_materialization) {
        mapping->child_mappings = std::move(child_mappings);
    }
    return Status::OK();
}

} // namespace doris::reader
