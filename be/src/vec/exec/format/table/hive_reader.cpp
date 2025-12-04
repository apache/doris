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

#include "hive_reader.h"

#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/table/hive/hive_orc_nested_column_utils.h"
#include "vec/exec/format/table/hive/hive_parquet_nested_column_utils.h"
#include "vec/exec/format/table/nested_column_access_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status HiveReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
};

Status HiveOrcReader::init_reader(
        const std::vector<std::string>& read_table_col_names, const VExprContextSPtrs& conjuncts,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());

    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
    bool is_hive_col_name = OrcReader::is_hive1_col_name(orc_type_ptr);

    if (_state->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        // Directly use the table column name to match the file column name, but pay attention to the case issue.
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(tuple_descriptor, orc_type_ptr,
                                                        table_info_node_ptr, _is_file_slot));
    } else {
        // hive1 / use index
        std::map<std::string, const SlotDescriptor*> slot_map; // table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name_lower_case(), slot);
        }

        // For top-level columns, use indexes to match, and for sub-columns, still use name to match columns.
        for (size_t idx = 0; idx < _params.column_idxs.size(); idx++) {
            auto table_column_name = read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            if (file_index >= orc_type_ptr->getSubtypeCount()) {
                table_info_node_ptr->add_not_exist_children(table_column_name);
            } else {
                auto field_node = std::make_shared<Node>();
                // For sub-columns, still use name to match columns.
                RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(
                        slot_map[table_column_name]->type(), orc_type_ptr->getSubtype(file_index),
                        field_node));
                table_info_node_ptr->add_children(
                        table_column_name, orc_type_ptr->getFieldName(file_index), field_node);
            }
            slot_map.erase(table_column_name);
        }
        for (const auto& [partition_col_name, _] : slot_map) {
            table_info_node_ptr->add_not_exist_children(partition_col_name);
        }
    }

    auto column_id_result = ColumnIdResult();
    if (_state->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        column_id_result = _create_column_ids(orc_type_ptr, tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids_by_top_level_col_index(orc_type_ptr, tuple_descriptor);
    }

    const auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    return orc_reader->init_reader(&read_table_col_names, conjuncts, false, tuple_descriptor,
                                   row_descriptor, not_single_slot_filter_conjuncts,
                                   slot_id_to_filter_conjuncts, table_info_node_ptr, column_ids,
                                   filter_column_ids);
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

Status HiveParquetReader::init_reader(
        const std::vector<std::string>& read_table_col_names, const VExprContextSPtrs& conjuncts,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    if (_state->query_options().hive_parquet_use_column_names) {
        // Directly use the table column name to match the file column name, but pay attention to the case issue.
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(tuple_descriptor, *field_desc,
                                                            table_info_node_ptr, _is_file_slot));
    } else {                                                   // use idx
        std::map<std::string, const SlotDescriptor*> slot_map; //table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name_lower_case(), slot);
        }

        // For top-level columns, use indexes to match, and for sub-columns, still use name to match columns.
        auto parquet_fields_schema = field_desc->get_fields_schema();
        for (size_t idx = 0; idx < _params.column_idxs.size(); idx++) {
            auto table_column_name = read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            if (file_index >= parquet_fields_schema.size()) {
                // Non-partitioning columns, which may be columns added later.
                table_info_node_ptr->add_not_exist_children(table_column_name);
            } else {
                // Non-partitioning columns, columns that exist in both the table and the file.
                auto field_node = std::make_shared<Node>();
                // for sub-columns, still use name to match columns.
                RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                        slot_map[table_column_name]->type(), parquet_fields_schema[file_index],
                        field_node));
                table_info_node_ptr->add_children(
                        table_column_name, parquet_fields_schema[file_index].name, field_node);
            }

            slot_map.erase(table_column_name);
        }
        /*
         * `_params.column_idxs` only have `isIsFileSlot()`, so we need add `partition slot`.
         * eg:
         * Table : A, B, C, D     (D: partition column)
         * Parquet file : A, B
         * Column C is obtained by add column.
         *
         * sql : select * from table;
         * slot : A, B, C, D
         * _params.column_idxs: 0, 1, 2 (There is no 3, because column D is the partition column)
         *
         */
        for (const auto& [partition_col_name, _] : slot_map) {
            table_info_node_ptr->add_not_exist_children(partition_col_name);
        }
    }

    auto column_id_result = ColumnIdResult();
    if (_state->query_options().hive_parquet_use_column_names) {
        column_id_result = _create_column_ids(field_desc, tuple_descriptor);
    } else {
        column_id_result = _create_column_ids_by_top_level_col_index(field_desc, tuple_descriptor);
    }

    const auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    RETURN_IF_ERROR(init_row_filters());

    return parquet_reader->init_reader(
            read_table_col_names, conjuncts, tuple_descriptor, row_descriptor, colname_to_slot_id,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts, table_info_node_ptr,
            true, column_ids, filter_column_ids);
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

#include "common/compile_check_end.h"
} // namespace doris::vectorized
