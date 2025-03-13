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

#pragma once

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
class TFileRangeDesc;
namespace vectorized {
class Block;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class TableFormatReader : public GenericReader {
public:
    TableFormatReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeState* state,
                      RuntimeProfile* profile, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx)
            : _file_format_reader(std::move(file_format_reader)),
              _state(state),
              _profile(profile),
              _params(params),
              _range(range),
              _io_ctx(io_ctx) {
        if (range.table_format_params.__isset.table_level_row_count) {
            _table_level_row_count = range.table_format_params.table_level_row_count;
        } else {
            _table_level_row_count = -1;
        }
    }
    ~TableFormatReader() override = default;
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) final {
        if (_push_down_agg_type == TPushAggOp::type::COUNT && _table_level_row_count >= 0) {
            auto rows =
                    std::min(_table_level_row_count, (int64_t)_state->query_options().batch_size);
            _table_level_row_count -= rows;
            auto mutate_columns = block->mutate_columns();
            for (auto& col : mutate_columns) {
                col->resize(rows);
            }
            block->set_columns(std::move(mutate_columns));
            *read_rows = rows;
            if (_table_level_row_count == 0) {
                *eof = true;
            }

            return Status::OK();
        }
        return get_next_block_inner(block, read_rows, eof);
    }

    virtual Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) = 0;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) final {
        return _file_format_reader->get_columns(name_to_type, missing_cols);
    }

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override {
        return _file_format_reader->get_parsed_schema(col_names, col_types);
    }

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) final {
        return _file_format_reader->set_fill_columns(partition_columns, missing_columns);
    }

    bool fill_all_columns() const override { return _file_format_reader->fill_all_columns(); }

    virtual Status init_row_filters() = 0;

protected:
    std::string _table_format;                          // hudi, iceberg, paimon
    std::unique_ptr<GenericReader> _file_format_reader; // parquet, orc
    RuntimeState* _state = nullptr;                     // for query options
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    io::IOContext* _io_ctx = nullptr;
    int64_t _table_level_row_count = -1; // for optimization of count(*) push down
    void _collect_profile_before_close() override {
        if (_file_format_reader != nullptr) {
            _file_format_reader->collect_profile_before_close();
        }
    }
};

class TableSchemaChangeHelper {
public:
    /** Get the mapping from the unique ID of the column in the current file to the file column name.
     * Iceberg/Hudi/Paimon usually maintains field IDs to support schema changes. If you cannot obtain this
     * information (maybe the old version does not have this information), you need to set `exist_schema` = `false`.
     */
    virtual Status get_file_col_id_to_name(bool& exist_schema,
                                           std::map<int, std::string>& file_col_id_to_name) = 0;

    virtual ~TableSchemaChangeHelper() = default;

    // table_id_to_name  : table column unique id to table name map
    Status init(const std::vector<std::string>& read_table_col_names,
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

    /**  To support  schema evolution. We change the column name in block to
     * make it match with the column name in file before reading data. and
     * set the name back to table column name before return this block.
     */
    Status get_next_block_before(Block* block) const {
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

    // Set the name back to table column name before return this block.
    Status get_next_block_after(Block* block) const {
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

protected:
    // copy from _colname_to_value_range with new column name that is in parquet/orc file
    std::unordered_map<std::string, ColumnValueRangeType> _new_colname_to_value_range;
    // all the columns required by user sql.
    std::vector<std::string> _all_required_col_names;
    // col names in table but not in parquet,orc file
    std::vector<std::string> _not_in_file_col_names;
    bool _has_schema_change = false;
    // file column name to table column name map
    std::unordered_map<std::string, std::string> _file_col_to_table_col;
    // table column name to file column name map.
    std::unordered_map<std::string, std::string> _table_col_to_file_col;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
