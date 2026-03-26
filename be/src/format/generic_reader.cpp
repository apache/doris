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

#include "format/generic_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include "runtime/descriptors.h"
#include "util/string_util.h"

namespace doris {

Status GenericReader::_prepare_fill_columns(const std::vector<ColumnDescriptor>& column_descs,
                                            const TFileScanRangeParams& params,
                                            const TFileRangeDesc& range,
                                            const TupleDescriptor* tuple_descriptor,
                                            const RowDescriptor* row_descriptor,
                                            RuntimeState* state) {
    // 1. Extract partition values from range
    _fill_partition_values.clear();
    if (range.__isset.columns_from_path_keys) {
        std::unordered_map<std::string, const SlotDescriptor*> name_to_slot;
        for (auto* slot : tuple_descriptor->slots()) {
            name_to_slot[slot->col_name()] = slot;
        }
        for (size_t i = 0; i < range.columns_from_path_keys.size(); i++) {
            const auto& key = range.columns_from_path_keys[i];
            const auto& value = range.columns_from_path[i];
            auto slot_it = name_to_slot.find(key);
            if (slot_it != name_to_slot.end()) {
                _fill_partition_values.emplace(key, std::make_tuple(value, slot_it->second));
            }
        }
    }

    return Status::OK();
}

Status GenericReader::_init_common_reader_states(
        std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        const TFileScanRangeParams& params, const TFileRangeDesc& range,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
    _column_descs = &column_descs;
    _fill_col_name_to_block_idx = col_name_to_block_idx;
    RETURN_IF_ERROR(_prepare_fill_columns(column_descs, params, range, tuple_descriptor,
                                          row_descriptor, state));

    // Extract column names for reading: REGULAR + GENERATED columns need file reading
    for (auto& desc : column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

Status GenericReader::on_before_init_reader(
        std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
        std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids,
        const TFileScanRangeParams& params, const TFileRangeDesc& range,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
    RETURN_IF_ERROR(_init_common_reader_states(column_descs, column_names, params, range,
                                               tuple_descriptor, row_descriptor, state,
                                               col_name_to_block_idx));

    // Build default table_info_node from file column names (case-insensitive matching).
    // Subclasses (OrcReader, ParquetReader, Hive, Iceberg, etc.) override on_before_init_reader
    // and build their own table_info_node AFTER calling this base method.
    // For simple readers (CSV, JSON, etc.) that do NOT override, we build it here.
    std::unordered_map<std::string, DataTypePtr> file_columns;
    RETURN_IF_ERROR(get_columns(&file_columns));

    // lowercase file name → original file name
    std::unordered_map<std::string, std::string> lower_to_native;
    for (const auto& [name, _] : file_columns) {
        lower_to_native[doris::to_lower(name)] = name;
    }

    // Auto-compute missing columns for simple readers (CSV/JSON/Arrow/etc.).
    // Parquet/ORC readers overwrite these in _do_init_reader by calling set_fill_column_data.
    if (_column_descs) {
        for (const auto& desc : *_column_descs) {
            if (desc.category != ColumnCategory::REGULAR &&
                desc.category != ColumnCategory::GENERATED) {
                continue;
            }
            // Skip columns already handled as partition columns to avoid double-fill.
            if (_fill_partition_values.contains(desc.name)) {
                continue;
            }
            if (!lower_to_native.contains(doris::to_lower(desc.name))) {
                _fill_missing_defaults[desc.name] = desc.default_expr;
                _fill_missing_cols.insert(desc.name);
            }
        }
    }

    auto info_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = lower_to_native.find(slot->col_name_lower_case());
        if (it != lower_to_native.end()) {
            info_node->add_children(slot->col_name(), it->second,
                                    TableSchemaChangeHelper::ConstNode::get_instance());
        } else {
            info_node->add_not_exist_children(slot->col_name());
        }
    }
    table_info_node = info_node;

    return Status::OK();
}

} // namespace doris
