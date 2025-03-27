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
#include "exec/olap_common.h"
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

protected:
    /** table_id_to_name  : table column unique id to table name map */
    Status init_schema_info(const std::vector<std::string>& read_table_col_names,
                            const std::unordered_map<int32_t, std::string>& table_id_to_name,
                            const std::unordered_map<std::string, ColumnValueRangeType>*
                                    table_col_name_to_value_range);

    /** To support schema evolution. We change the column name in block to
     * make it match with the column name in file before reading data. and
     * set the name back to table column name before return this block.
     */
    Status get_next_block_before(Block* block) const;

    /** Set the name back to table column name before return this block.*/
    Status get_next_block_after(Block* block) const;

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
