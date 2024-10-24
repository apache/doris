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

#include <gen_cpp/parquet_types.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "io/file_factory.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/obj_lru_cache.h"
#include "util/runtime_profile.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vparquet_column_reader.h"
#include "vparquet_group_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;
class TupleDescriptor;

namespace io {
class FileSystem;
struct IOContext;
} // namespace io
namespace vectorized {
class Block;
class FileMetaData;
class PageIndex;
class ShardedKVCache;
class VExprContext;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {

class ParquetReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ParquetReader);

public:
    struct Statistics {
        int32_t filtered_row_groups = 0;
        int32_t read_row_groups = 0;
        int64_t filtered_group_rows = 0;
        int64_t filtered_page_rows = 0;
        int64_t lazy_read_filtered_rows = 0;
        int64_t read_rows = 0;
        int64_t filtered_bytes = 0;
        int64_t read_bytes = 0;
        int64_t column_read_time = 0;
        int64_t parse_meta_time = 0;
        int64_t parse_footer_time = 0;
        int64_t open_file_time = 0;
        int64_t open_file_num = 0;
        int64_t row_group_filter_time = 0;
        int64_t page_index_filter_time = 0;
        int64_t read_page_index_time = 0;
        int64_t parse_page_index_time = 0;
        int64_t predicate_filter_time = 0;
    };

    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, cctz::time_zone* ctz,
                  io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache = nullptr,
                  bool enable_lazy_mat = true);

    ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                  io::IOContext* io_ctx, RuntimeState* state, bool enable_lazy_mat = true);

    ~ParquetReader() override;
    // for test
    void set_file_reader(io::FileReaderSPtr file_reader) { _file_reader = file_reader; }

    Status open();

    Status init_reader(
            const std::vector<std::string>& all_column_names,
            const std::vector<std::string>& missing_column_names,
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts,
            bool filter_groups = true, const bool hive_use_column_names = true);

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status close() override;

    RowRange get_whole_range() { return _whole_range; }

    // set the delete rows in current parquet file
    void set_delete_rows(const std::vector<int64_t>* delete_rows) { _delete_rows = delete_rows; }

    int64_t size() const { return _file_reader->size(); }

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

    Statistics& statistics() { return _statistics; }

    const tparquet::FileMetaData* get_meta_data() const { return _t_metadata; }

    // Only for iceberg reader to sanitize invalid column names
    void iceberg_sanitize(const std::vector<std::string>& read_columns);

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) override;

    std::vector<tparquet::KeyValue> get_metadata_key_values();
    void set_table_to_file_col_map(std::unordered_map<std::string, std::string>& map) {
        _table_col_to_file_col = map;
    }

protected:
    void _collect_profile_before_close() override;

private:
    struct ParquetProfile {
        RuntimeProfile::Counter* filtered_row_groups = nullptr;
        RuntimeProfile::Counter* to_read_row_groups = nullptr;
        RuntimeProfile::Counter* filtered_group_rows = nullptr;
        RuntimeProfile::Counter* filtered_page_rows = nullptr;
        RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* filtered_bytes = nullptr;
        RuntimeProfile::Counter* raw_rows_read = nullptr;
        RuntimeProfile::Counter* to_read_bytes = nullptr;
        RuntimeProfile::Counter* column_read_time = nullptr;
        RuntimeProfile::Counter* parse_meta_time = nullptr;
        RuntimeProfile::Counter* parse_footer_time = nullptr;
        RuntimeProfile::Counter* open_file_time = nullptr;
        RuntimeProfile::Counter* open_file_num = nullptr;
        RuntimeProfile::Counter* row_group_filter_time = nullptr;
        RuntimeProfile::Counter* page_index_filter_time = nullptr;
        RuntimeProfile::Counter* read_page_index_time = nullptr;
        RuntimeProfile::Counter* parse_page_index_time = nullptr;

        RuntimeProfile::Counter* file_read_time = nullptr;
        RuntimeProfile::Counter* file_read_calls = nullptr;
        RuntimeProfile::Counter* file_meta_read_calls = nullptr;
        RuntimeProfile::Counter* file_read_bytes = nullptr;
        RuntimeProfile::Counter* decompress_time = nullptr;
        RuntimeProfile::Counter* decompress_cnt = nullptr;
        RuntimeProfile::Counter* decode_header_time = nullptr;
        RuntimeProfile::Counter* decode_value_time = nullptr;
        RuntimeProfile::Counter* decode_dict_time = nullptr;
        RuntimeProfile::Counter* decode_level_time = nullptr;
        RuntimeProfile::Counter* decode_null_map_time = nullptr;
        RuntimeProfile::Counter* skip_page_header_num = nullptr;
        RuntimeProfile::Counter* parse_page_header_num = nullptr;
        RuntimeProfile::Counter* predicate_filter_time = nullptr;
    };

    Status _open_file();
    void _init_profile();
    void _close_internal();
    Status _next_row_group_reader();
    RowGroupReader::PositionDeleteContext _get_position_delete_ctx(
            const tparquet::RowGroup& row_group,
            const RowGroupReader::RowGroupIndex& row_group_index);
    Status _init_row_groups(const bool& is_filter_groups);
    void _init_system_properties();
    void _init_file_description();
    // Page Index Filter
    bool _has_page_index(const std::vector<tparquet::ColumnChunk>& columns, PageIndex& page_index);
    Status _process_page_index(const tparquet::RowGroup& row_group,
                               std::vector<RowRange>& candidate_row_ranges);

    // Row Group Filter
    bool _is_misaligned_range_group(const tparquet::RowGroup& row_group);
    Status _process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& column_meta,
                                       bool* filter_group);
    Status _process_row_group_filter(const tparquet::RowGroup& row_group, bool* filter_group);
    void _init_chunk_dicts();
    Status _process_dict_filter(bool* filter_group);
    void _init_bloom_filter();
    Status _process_bloom_filter(bool* filter_group);
    int64_t _get_column_start_offset(const tparquet::ColumnMetaData& column_init_column_readers);
    std::string _meta_cache_key(const std::string& path) { return "meta_" + path; }
    std::vector<io::PrefetchRange> _generate_random_access_ranges(
            const RowGroupReader::RowGroupIndex& group, size_t* avg_io_size);
    void _collect_profile();

    static SortOrder _determine_sort_order(const tparquet::SchemaElement& parquet_schema);

private:
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;

    // the following fields are for parquet meta data cache.
    // if _meta_cache is not null, the _file_metadata will be got from _meta_cache,
    // and it is owned by _meta_cache_handle.
    // if _meta_cache is null, _file_metadata will be managed by _file_metadata_ptr,
    // which will be released when deconstructing.
    // ATTN: these fields must be before _file_reader, to make sure they will be released
    // after _file_reader. Otherwise, there may be heap-use-after-free bug.
    ObjLRUCache::CacheHandle _meta_cache_handle;
    std::unique_ptr<FileMetaData> _file_metadata_ptr;
    FileMetaData* _file_metadata = nullptr;
    const tparquet::FileMetaData* _t_metadata = nullptr;

    io::FileReaderSPtr _file_reader = nullptr;
    std::unique_ptr<RowGroupReader> _current_group_reader;
    // read to the end of current reader
    bool _row_group_eof = true;
    int32_t _total_groups; // num of groups(stripes) of a parquet(orc) file
    // table column name to file column name map. For iceberg schema evolution.
    std::unordered_map<std::string, std::string> _table_col_to_file_col;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range = nullptr;
    std::vector<std::string> _read_columns;
    RowRange _whole_range = RowRange(0, 0);
    const std::vector<int64_t>* _delete_rows = nullptr;
    int64_t _delete_rows_index = 0;

    // Used for column lazy read.
    RowGroupReader::LazyReadContext _lazy_read_ctx;

    std::list<RowGroupReader::RowGroupIndex> _read_row_groups;
    // parquet file reader object
    size_t _batch_size;
    int64_t _range_start_offset;
    int64_t _range_size;
    cctz::time_zone* _ctz = nullptr;

    std::unordered_map<int, tparquet::OffsetIndex> _col_offsets;
    const std::vector<std::string>* _column_names = nullptr;

    std::vector<std::string> _missing_cols;
    Statistics _statistics;
    ParquetColumnReader::Statistics _column_statistics;
    ParquetProfile _parquet_profile;
    bool _closed = false;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _state = nullptr;
    // Cache to save some common part such as file footer.
    // Maybe null if not used
    FileMetaCache* _meta_cache = nullptr;
    bool _enable_lazy_mat = true;
    bool _enable_filter_by_min_max = true;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
    const std::unordered_map<std::string, int>* _colname_to_slot_id = nullptr;
    const VExprContextSPtrs* _not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
    bool _hive_use_column_names = false;
    std::unordered_map<tparquet::Type::type, bool> _ignored_stats;
};
} // namespace doris::vectorized
