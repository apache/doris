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

#include "table_format_reader.h"

namespace doris::vectorized {

TableFormatReader::TableFormatReader(std::unique_ptr<GenericReader> file_format_reader)
        : _file_format_reader(std::move(file_format_reader)) {}

Status TableSchemaChangeHelper::init_schema_info(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<int32_t, std::string>& table_id_to_name,
        const std::unordered_map<std::string, ColumnValueRangeType>*
                table_col_name_to_value_range) {
    bool exist_schema = true;
    std::map<int32_t, std::string> file_id_to_name;
    RETURN_IF_ERROR(get_file_col_id_to_name(exist_schema, file_id_to_name));
    if (!exist_schema) {
        file_id_to_name.clear();
        for (const auto& [table_col_id, table_col_name] : table_id_to_name) {
            file_id_to_name.emplace(table_col_id, table_col_name);
        }
    }

    /** This is to compare the table schema from FE (table_id_to_name) with
    * the current file schema (file_id_to_name) , generate two maps for future use:
    * 1. table column name to file column name.
    * 2. file column name to table column name.
    * For example, file has a column 'col1',
    * after this file was written, iceberg changed the column name to 'col1_new'.
    * The two maps would contain:
    * 1. col1_new -> col1
     * 2. col1 -> col1_new
    */
    for (const auto& [file_col_id, file_col_name] : file_id_to_name) {
        if (table_id_to_name.find(file_col_id) == table_id_to_name.end()) {
            continue;
        }

        auto& table_col_name = table_id_to_name.at(file_col_id);
        _table_col_to_file_col.emplace(table_col_name, file_col_name);
        _file_col_to_table_col.emplace(file_col_name, table_col_name);
        if (table_col_name != file_col_name) {
            _has_schema_change = true;
        }
    }

    /** Generate _all_required_col_names and _not_in_file_col_names.
     *
     * _all_required_col_names is all the columns required by user sql.
     * If the column name has been modified after the data file was written,
     * put the old name in data file to _all_required_col_names.
     *
     * _not_in_file_col_names is all the columns required by user sql but not in the data file.
     * e.g. New columns added after this data file was written.
     * The columns added with names used by old dropped columns should consider as a missing column,
     * which should be in _not_in_file_col_names.
     */
    _all_required_col_names.clear();
    _not_in_file_col_names.clear();
    for (auto table_col_name : read_table_col_names) {
        auto iter = _table_col_to_file_col.find(table_col_name);
        if (iter == _table_col_to_file_col.end()) {
            _all_required_col_names.emplace_back(table_col_name);
            _not_in_file_col_names.emplace_back(table_col_name);
        } else {
            _all_required_col_names.emplace_back(iter->second);
        }
    }

    /** Generate _new_colname_to_value_range, by replacing the column name in
    * _colname_to_value_range with column name in data file.
    */
    for (auto& it : *table_col_name_to_value_range) {
        auto iter = _table_col_to_file_col.find(it.first);
        if (iter == _table_col_to_file_col.end()) {
            _new_colname_to_value_range.emplace(it.first, it.second);
        } else {
            _new_colname_to_value_range.emplace(iter->second, it.second);
        }
    }
    return Status::OK();
}

Status TableSchemaChangeHelper::get_next_block_before(Block* block) const {
    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _table_col_to_file_col.find(col.name);
            if (iter != _table_col_to_file_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }
    return Status::OK();
}

Status TableSchemaChangeHelper::get_next_block_after(Block* block) const {
    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _file_col_to_table_col.find(col.name);
            if (iter != _file_col_to_table_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }
    return Status::OK();
}

} // namespace doris::vectorized