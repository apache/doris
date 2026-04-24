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

#include "format/table/hive_reader.h"

#include <vector>

#include "common/status.h"
#include "format/table/hive/hive_orc_nested_column_utils.h"
#include "format/table/hive/hive_parquet_nested_column_utils.h"
#include "format/table/nested_column_access_helper.h"
#include "runtime/runtime_state.h"

namespace doris {

Status HiveOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(
            _extract_partition_values(*ctx->range, ctx->tuple_descriptor, _fill_partition_values));
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::SYNTHESIZED &&
                   desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
            this->register_synthesized_column_handler(
                    desc.name,
                    [iter = std::move(topn_row_id_column_iter), this, &desc](
                            Block* block, size_t rows) -> Status {
                        return fill_topn_row_id(iter, desc.name, block, rows);
                    });
            continue;
        }
    }

    // Get file type (available because _create_file_reader() runs before this hook)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));
    bool is_hive_col_name = OrcReader::is_hive1_col_name(orc_type_ptr);

    // Build table_info_node based on config
    if (get_state()->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(ctx->tuple_descriptor, orc_type_ptr,
                                                        ctx->table_info_node, _is_file_slot));
    } else {
        ctx->table_info_node = std::make_shared<StructNode>();
        std::map<std::string, const SlotDescriptor*> slot_map;
        for (const auto& slot : ctx->tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name_lower_case(), slot);
        }

        for (size_t idx = 0; idx < get_scan_params().column_idxs.size(); idx++) {
            auto table_column_name = ctx->column_names[idx];
            auto file_index = get_scan_params().column_idxs[idx];

            if (file_index >= orc_type_ptr->getSubtypeCount()) {
                ctx->table_info_node->add_not_exist_children(table_column_name);
            } else {
                auto field_node = std::make_shared<Node>();
                RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(
                        slot_map[table_column_name]->type(), orc_type_ptr->getSubtype(file_index),
                        field_node));
                ctx->table_info_node->add_children(
                        table_column_name, orc_type_ptr->getFieldName(file_index), field_node);
            }
            slot_map.erase(table_column_name);
        }
        for (const auto& [partition_col_name, _] : slot_map) {
            ctx->table_info_node->add_not_exist_children(partition_col_name);
        }
    }

    // Compute column_ids
    auto column_id_result = ColumnIdResult();
    if (get_state()->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        column_id_result = _create_column_ids(orc_type_ptr, ctx->tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids_by_top_level_col_index(orc_type_ptr, ctx->tuple_descriptor);
    }
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    // _is_acid is false by default, no need to set explicitly
    return Status::OK();
}

ColumnIdResult HiveOrcReader::_create_column_ids(const orc::Type* orc_type,
                                                 const TupleDescriptor* tuple_descriptor) {
    // map top-level table column name (lower-cased) -> orc::Type*
    std::unordered_map<std::string, const orc::Type*> table_col_name_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;

        std::string table_col_name = to_lower(orc_type->getFieldName(i));
        table_col_name_to_orc_type_map[table_col_name] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level orc field
    auto process_access_paths = [](const orc::Type* orc_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                orc_field, access_paths, out_ids,
                [](const orc::Type* type) { return type->getColumnId(); },
                [](const orc::Type* type) { return type->getMaximumColumnId(); },
                HiveOrcNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = table_col_name_to_orc_type_map.find(slot->col_name_lower_case());
        if (it == table_col_name_to_orc_type_map.end()) {
            // Column not found in file
            continue;
        }
        const orc::Type* orc_field = it->second;

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        // complex types
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveOrcReader::_create_column_ids_by_top_level_col_index(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor) {
    // map top-level table column position -> orc::Type*
    std::unordered_map<uint64_t, const orc::Type*> table_col_pos_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;

        table_col_pos_to_orc_type_map[i] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level orc field
    auto process_access_paths = [](const orc::Type* orc_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                orc_field, access_paths, out_ids,
                [](const orc::Type* type) { return type->getColumnId(); },
                [](const orc::Type* type) { return type->getMaximumColumnId(); },
                HiveOrcNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = table_col_pos_to_orc_type_map.find(slot->col_pos());
        if (it == table_col_pos_to_orc_type_map.end()) {
            // Column not found in file
            continue;
        }
        const orc::Type* orc_field = it->second;

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        const auto& all_access_paths = slot->all_access_paths();
        // complex types
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

Status HiveParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(
            _extract_partition_values(*ctx->range, ctx->tuple_descriptor, _fill_partition_values));
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::SYNTHESIZED &&
                   desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
            this->register_synthesized_column_handler(
                    desc.name,
                    [iter = std::move(topn_row_id_column_iter), this, &desc](
                            Block* block, size_t rows) -> Status {
                        return fill_topn_row_id(iter, desc.name, block, rows);
                    });
            continue;
        }
    }

    // Get file metadata schema (available because _open_file() runs before this hook)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node based on config
    if (get_state()->query_options().hive_parquet_use_column_names) {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(ctx->tuple_descriptor, *field_desc,
                                                            ctx->table_info_node, _is_file_slot));
    } else {
        ctx->table_info_node = std::make_shared<StructNode>();
        std::map<std::string, const SlotDescriptor*> slot_map;
        for (const auto& slot : ctx->tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name_lower_case(), slot);
        }

        auto parquet_fields_schema = field_desc->get_fields_schema();
        for (size_t idx = 0; idx < get_scan_params().column_idxs.size(); idx++) {
            auto table_column_name = ctx->column_names[idx];
            auto file_index = get_scan_params().column_idxs[idx];

            if (file_index >= parquet_fields_schema.size()) {
                ctx->table_info_node->add_not_exist_children(table_column_name);
            } else {
                auto field_node = std::make_shared<Node>();
                RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                        slot_map[table_column_name]->type(), parquet_fields_schema[file_index],
                        field_node));
                ctx->table_info_node->add_children(
                        table_column_name, parquet_fields_schema[file_index].name, field_node);
            }
            slot_map.erase(table_column_name);
        }
        for (const auto& [partition_col_name, _] : slot_map) {
            ctx->table_info_node->add_not_exist_children(partition_col_name);
        }
    }

    // Compute column_ids for lazy materialization
    auto column_id_result = ColumnIdResult();
    if (get_state()->query_options().hive_parquet_use_column_names) {
        column_id_result = _create_column_ids(field_desc, ctx->tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids_by_top_level_col_index(field_desc, ctx->tuple_descriptor);
    }
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    _filter_groups = true;
    return Status::OK();
}

ColumnIdResult HiveParquetReader::_create_column_ids(const FieldDescriptor* field_desc,
                                                     const TupleDescriptor* tuple_descriptor) {
    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // map top-level table column name (lower-cased) -> FieldSchema*
    std::unordered_map<std::string, const FieldSchema*> table_col_name_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        table_col_name_to_field_schema_map[field_schema->lower_case_name] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level parquet field
    auto process_access_paths = [](const FieldSchema* parquet_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                parquet_field, access_paths, out_ids,
                [](const FieldSchema* field) { return field->get_column_id(); },
                [](const FieldSchema* field) { return field->get_max_column_id(); },
                HiveParquetNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = table_col_name_to_field_schema_map.find(slot->col_name_lower_case());
        if (it == table_col_name_to_field_schema_map.end()) {
            // Column not found in file
            continue;
        }
        auto field_schema = it->second;

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);

            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        // complex types
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveParquetReader::_create_column_ids_by_top_level_col_index(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor) {
    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // map top-level table column position -> FieldSchema*
    std::unordered_map<uint64_t, const FieldSchema*> table_col_pos_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        table_col_pos_to_field_schema_map[i] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level parquet field
    auto process_access_paths = [](const FieldSchema* parquet_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                parquet_field, access_paths, out_ids,
                [](const FieldSchema* field) { return field->get_column_id(); },
                [](const FieldSchema* field) { return field->get_max_column_id(); },
                HiveParquetNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = table_col_pos_to_field_schema_map.find(slot->col_pos());
        if (it == table_col_pos_to_field_schema_map.end()) {
            // Column not found in file
            continue;
        }
        auto field_schema = it->second;

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);

            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        // complex types
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

} // namespace doris
