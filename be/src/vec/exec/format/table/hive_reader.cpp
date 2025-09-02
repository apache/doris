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

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status HiveReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
};

Status HiveOrcReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
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
            slot_map.emplace(slot->col_name(), slot);
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
                        slot_map[table_column_name]->get_data_type_ptr(),
                        orc_type_ptr->getSubtype(file_index), field_node));
                table_info_node_ptr->add_children(
                        table_column_name, orc_type_ptr->getFieldName(file_index), field_node);
            }
            slot_map.erase(table_column_name);
        }
        for (const auto& [partition_col_name, _] : slot_map) {
            table_info_node_ptr->add_not_exist_children(partition_col_name);
        }
    }

    return orc_reader->init_reader(&read_table_col_names, table_col_name_to_value_range, conjuncts,
                                   false, tuple_descriptor, row_descriptor,
                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts,
                                   table_info_node_ptr);
}

Status HiveParquetReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
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
                                                            table_info_node_ptr,_is_file_slot));
    } else {                                                   // use idx
        std::map<std::string, const SlotDescriptor*> slot_map; //table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name(), slot);
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
                        slot_map[table_column_name]->get_data_type_ptr(),
                        parquet_fields_schema[file_index], field_node));
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

    return parquet_reader->init_reader(read_table_col_names, table_col_name_to_value_range,
                                       conjuncts, tuple_descriptor, row_descriptor,
                                       colname_to_slot_id, not_single_slot_filter_conjuncts,
                                       slot_id_to_filter_conjuncts, table_info_node_ptr);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
