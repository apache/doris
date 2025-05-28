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
        const TSchemaInfoNode& table_col_id_table_name_map,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {

    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());

    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
    bool is_hive_col_name =  OrcReader::is_hive1_col_name(orc_type_ptr);
    auto root = std::make_shared<TableSchemaChange::tableNode>();


    if (_state->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        RETURN_IF_ERROR(TableSchemaChangeHelper::orc_use_name(tuple_descriptor, orc_type_ptr, root));

    } else {
        std::map<std::string, const SlotDescriptor*> slot_map;//table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name(), slot);
        }

        for (size_t idx = 0 ; idx < _params.column_idxs.size() ; idx++) {
            auto table_column_name =  read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            auto field_node = std::make_shared<TableSchemaChange::tableNode>();
            if (file_index >= orc_type_ptr->getSubtypeCount()) {
                field_node->exists_in_file = false;
            }else {
                field_node->exists_in_file = true;
                field_node->file_name = orc_type_ptr->getFieldName(file_index);
                RETURN_IF_ERROR(TableSchemaChangeHelper::orc_subcolumn_use_name(
                        slot_map[table_column_name]->type(), orc_type_ptr->getSubtype(file_index),field_node));
            }
            root->children.emplace(table_column_name,field_node);
        }
    }
    orc_reader->table_info_node_ptr = root;

    return orc_reader->init_reader(&read_table_col_names, {},
                                   table_col_name_to_value_range, conjuncts, false, tuple_descriptor,
                                   row_descriptor, not_single_slot_filter_conjuncts,
                                   slot_id_to_filter_conjuncts);
}

Status HiveParquetReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const TSchemaInfoNode& table_col_id_table_name_map,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    FieldDescriptor field_desc = parquet_reader->get_file_metadata_schema();
    auto parquet_fields_schema  = field_desc.get_fields_schema();

    auto root = std::make_shared<TableSchemaChange::tableNode>();
    if (_state->query_options().hive_parquet_use_column_names) {
        RETURN_IF_ERROR(TableSchemaChangeHelper::parquet_use_name(tuple_descriptor, field_desc, root));
    } else { // use idx  and   hive1
        std::map<std::string, const SlotDescriptor*> slot_map;//table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name(), slot);
        }

        for (size_t idx = 0 ; idx < _params.column_idxs.size() ; idx++) {
            auto table_column_name =  read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            auto field_node = std::make_shared<TableSchemaChange::tableNode>();
            if (file_index >= parquet_fields_schema.size()) {
                field_node->exists_in_file = false;
            }else {
                field_node->exists_in_file = true;
                field_node->file_name = parquet_fields_schema[file_index].name;
                RETURN_IF_ERROR(TableSchemaChangeHelper::parquet_subcolumn_use_name
                        (slot_map[table_column_name]->type(),field_node, parquet_fields_schema[file_index]));
            }
            root->children.emplace(table_column_name,field_node);
        }

    }



    parquet_reader->set_table_info_node_ptr(root);
    return parquet_reader->init_reader(
            read_table_col_names, {}, table_col_name_to_value_range,
            conjuncts, tuple_descriptor, row_descriptor, colname_to_slot_id,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
