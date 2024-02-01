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

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "table_format_reader.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_dictionary.h"

namespace tparquet {
class KeyValue;
} // namespace tparquet

namespace doris {
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;
class TIcebergDeleteFileDesc;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
struct TypeDescriptor;

namespace vectorized {
class Block;
class ColumnString;
class GenericReader;
class ShardedKVCache;
class VExprContext;

class IcebergTableReader : public TableFormatReader {
    ENABLE_FACTORY_CREATOR(IcebergTableReader);

public:
    struct PositionDeleteRange {
        std::vector<std::string> data_file_path;
        std::vector<std::pair<int, int>> range;
    };

    IcebergTableReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                       RuntimeState* state, const TFileScanRangeParams& params,
                       const TFileRangeDesc& range, ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                       int64_t push_down_count);
    ~IcebergTableReader() override = default;

    Status init_row_filters(const TFileRangeDesc& range) override;

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) override;

    bool fill_all_columns() const override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_reader(
            const std::vector<std::string>& file_col_names,
            const std::unordered_map<int, std::string>& col_id_name_map,
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);

    enum { DATA, POSITION_DELETE, EQUALITY_DELETE };

private:
    struct IcebergProfile {
        RuntimeProfile::Counter* num_delete_files;
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
        RuntimeProfile::Counter* delete_rows_sort_time;
    };

    Status _position_delete(const std::vector<TIcebergDeleteFileDesc>& delete_files);

    /**
     * https://iceberg.apache.org/spec/#position-delete-files
     * The rows in the delete file must be sorted by file_path then position to optimize filtering rows while scanning.
     * Sorting by file_path allows filter pushdown by file in columnar storage formats.
     * Sorting by position allows filtering rows while scanning, to avoid keeping deletes in memory.
     */
    void _sort_delete_rows(std::vector<std::vector<int64_t>*>& delete_rows_array,
                           int64_t num_delete_rows);

    PositionDeleteRange _get_range(const ColumnDictI32& file_path_column);

    PositionDeleteRange _get_range(const ColumnString& file_path_column);

    Status _gen_col_name_maps(std::vector<tparquet::KeyValue> parquet_meta_kv);
    void _gen_file_col_names();
    void _gen_new_colname_to_value_range();
    std::string _delet_file_cache_key(const std::string& path) { return "delete_" + path; }

    RuntimeProfile* _profile;
    RuntimeState* _state;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    // owned by scan node
    ShardedKVCache* _kv_cache;
    IcebergProfile _iceberg_profile;
    std::vector<int64_t> _delete_rows;
    // col names from _file_slot_descs
    std::vector<std::string> _file_col_names;
    // file column name to table column name map. For iceberg schema evolution.
    std::unordered_map<std::string, std::string> _file_col_to_table_col;
    // table column name to file column name map. For iceberg schema evolution.
    std::unordered_map<std::string, std::string> _table_col_to_file_col;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    // copy from _colname_to_value_range with new column name that is in parquet file, to support schema evolution.
    std::unordered_map<std::string, ColumnValueRangeType> _new_colname_to_value_range;
    // column id to name map. Collect from FE slot descriptor.
    std::unordered_map<int, std::string> _col_id_name_map;
    // col names in the parquet file
    std::vector<std::string> _all_required_col_names;
    // col names in table but not in parquet file
    std::vector<std::string> _not_in_file_col_names;

    io::IOContext* _io_ctx;
    bool _has_schema_change = false;
    bool _has_iceberg_schema = false;

    int64_t _remaining_push_down_count;
};
} // namespace vectorized
} // namespace doris
