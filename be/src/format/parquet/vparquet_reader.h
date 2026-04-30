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
#include "format/parquet/parquet_common.h"
#include "format/parquet/parquet_predicate.h"
#include "format/parquet/vparquet_column_reader.h"
#include "format/parquet/vparquet_group_reader.h"
#include "format/table/table_format_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "io/file_factory.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "runtime/runtime_profile.h"
#include "storage/olap_scan_common.h"
#include "util/obj_lru_cache.h"

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
class Block;
class FileMetaData;
class PageIndex;
class ShardedKVCache;
class VExprContext;
struct RowLineageColumns;
} // namespace doris

namespace doris {

/// Parquet-specific initialization context.
/// Extends ReaderInitContext with predicate pushdown fields.
struct ParquetInitContext final : public ReaderInitContext {
    // Safe defaults for standalone readers (delete file readers, push handler)
    // that don't have conjuncts/predicates. Dereferenced by _do_init_reader.
    static inline const VExprContextSPtrs EMPTY_CONJUNCTS {};
    static inline phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>
            EMPTY_SLOT_PREDICATES {};

    const VExprContextSPtrs* conjuncts = &EMPTY_CONJUNCTS;
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>*
            slot_id_to_predicates = &EMPTY_SLOT_PREDICATES;
    const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;
    bool filter_groups = true;
};

class ParquetReader : public TableFormatReader {
    ENABLE_FACTORY_CREATOR(ParquetReader);

public:
    struct ReaderStatistics {
        int32_t filtered_row_groups = 0;
        int32_t filtered_row_groups_by_min_max = 0;
        int32_t filtered_row_groups_by_bloom_filter = 0;
        int32_t read_row_groups = 0;
        int64_t filtered_group_rows = 0;
        int64_t filtered_page_rows = 0;
        int64_t lazy_read_filtered_rows = 0;
        int64_t read_rows = 0;
        int64_t filtered_bytes = 0;
        int64_t column_read_time = 0;
        int64_t parse_meta_time = 0;
        int64_t parse_footer_time = 0;
        int64_t file_footer_read_calls = 0;
        int64_t file_footer_hit_cache = 0;
        int64_t file_reader_create_time = 0;
        int64_t open_file_num = 0;
        int64_t row_group_filter_time = 0;
        int64_t page_index_filter_time = 0;
        int64_t read_page_index_time = 0;
        int64_t parse_page_index_time = 0;
        int64_t predicate_filter_time = 0;
        int64_t dict_filter_rewrite_time = 0;
        int64_t bloom_filter_read_time = 0;
    };

    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                  io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache = nullptr,
                  bool enable_lazy_mat = true);

    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                  std::shared_ptr<io::IOContext> io_ctx_holder, RuntimeState* state,
                  FileMetaCache* meta_cache = nullptr, bool enable_lazy_mat = true);

    ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                  io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache = nullptr,
                  bool enable_lazy_mat = true);

    ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                  std::shared_ptr<io::IOContext> io_ctx_holder, RuntimeState* state,
                  FileMetaCache* meta_cache = nullptr, bool enable_lazy_mat = true);

    ~ParquetReader() override;
#ifdef BE_TEST
    // for unit test
    void set_file_reader(io::FileReaderSPtr file_reader);
#endif

    // Override to build table_info_node from Parquet file metadata using by_parquet_name.
    // Subclasses (HiveParquetReader, etc.) call GenericReader::on_before_init_reader directly,
    // so this override only applies to plain ParquetReader (TVF, load).
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    void set_batch_size(size_t batch_size) override;

    Status close() override;

    // set the delete rows in current parquet file
    void set_delete_rows(const std::vector<int64_t>* delete_rows) { _delete_rows = delete_rows; }

    int64_t size() const { return _file_reader->size(); }

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

    Status init_schema_reader() override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;

    ReaderStatistics& reader_statistics() { return _reader_statistics; }

    const tparquet::FileMetaData* get_meta_data() const { return _t_metadata; }

    Status get_file_metadata_schema(const FieldDescriptor** ptr);

    void set_create_row_id_column_iterator_func(
            std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()> create_func) {
        _create_topn_row_id_column_iterator = create_func;
    }

    /// Access current batch row positions (delegates to RowGroupReader).
    /// Used by IcebergReaderMixin to build $row_id column.
    const std::vector<segment_v2::rowid_t>& current_batch_row_positions() const {
        return _current_group_reader->current_batch_row_positions();
    }

    Status fill_topn_row_id(
            std::shared_ptr<segment_v2::RowIdColumnIteratorV2> _row_id_column_iterator,
            std::string col_name, Block* block, size_t rows) {
        int col_pos = block->get_position_by_name(col_name);
        DCHECK(col_pos >= 0);
        if (col_pos < 0) {
            return Status::InternalError("Column {} not found in block", col_name);
        }
        auto col = block->get_by_position(col_pos).column->assume_mutable();
        const auto& row_ids = this->current_batch_row_positions();
        RETURN_IF_ERROR(
                _row_id_column_iterator->read_by_rowids(row_ids.data(), row_ids.size(), col));

        return Status::OK();
    }

    bool count_read_rows() override { return true; }

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override;

    bool supports_count_pushdown() const override { return true; }

    int64_t get_total_rows() const override;

    bool has_delete_operations() const override {
        return _delete_rows != nullptr && !_delete_rows->empty();
    }

    /// Disable row-group range filtering (needed when reading delete files
    /// whose TFileRangeDesc has size=-1).
    void set_filter_groups(bool v) { _filter_groups = v; }

protected:
    // ---- Unified init_reader(ReaderInitContext*) overrides ----
    Status _open_file_reader(ReaderInitContext* ctx) override;
    Status _do_init_reader(ReaderInitContext* ctx) override;

    void _collect_profile_before_close() override;

    // Core block reading implementation
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    // Parquet fills partition/missing columns per-batch internally via RowGroupReader,
    // so suppress TableFormatReader's default on_after_read_block fill.
    Status on_after_read_block(Block* /*block*/, size_t* /*read_rows*/) override {
        return Status::OK();
    }

    // Protected accessors so CRTP mixin subclasses can reach private members
    io::IOContext* get_io_ctx() const { return _io_ctx; }
    std::unordered_map<std::string, uint32_t>*& col_name_to_block_idx_ref() {
        return _col_name_to_block_idx;
    }
    RuntimeProfile* get_profile() const { return _profile; }
    RuntimeState* get_state() const { return _state; }
    const TFileScanRangeParams& get_scan_params() const { return _scan_params; }
    const TFileRangeDesc& get_scan_range() const { return _scan_range; }
    const TupleDescriptor* get_tuple_descriptor() const { return _tuple_descriptor; }
    const RowDescriptor* get_row_descriptor() const { return _row_descriptor; }
    const FileMetaData* get_file_metadata() const { return _file_metadata; }

private:
    struct ParquetProfile {
        RuntimeProfile::Counter* filtered_row_groups = nullptr;
        RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
        RuntimeProfile::Counter* filtered_row_groups_by_bloom_filter = nullptr;
        RuntimeProfile::Counter* to_read_row_groups = nullptr;
        RuntimeProfile::Counter* total_row_groups = nullptr;
        RuntimeProfile::Counter* filtered_group_rows = nullptr;
        RuntimeProfile::Counter* filtered_page_rows = nullptr;
        RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* filtered_bytes = nullptr;
        RuntimeProfile::Counter* raw_rows_read = nullptr;
        RuntimeProfile::Counter* column_read_time = nullptr;
        RuntimeProfile::Counter* parse_meta_time = nullptr;
        RuntimeProfile::Counter* parse_footer_time = nullptr;
        RuntimeProfile::Counter* file_reader_create_time = nullptr;
        RuntimeProfile::Counter* open_file_num = nullptr;
        RuntimeProfile::Counter* row_group_filter_time = nullptr;
        RuntimeProfile::Counter* page_index_read_calls = nullptr;
        RuntimeProfile::Counter* page_index_filter_time = nullptr;
        RuntimeProfile::Counter* read_page_index_time = nullptr;
        RuntimeProfile::Counter* parse_page_index_time = nullptr;
        RuntimeProfile::Counter* file_footer_read_calls = nullptr;
        RuntimeProfile::Counter* file_footer_hit_cache = nullptr;
        RuntimeProfile::Counter* decompress_time = nullptr;
        RuntimeProfile::Counter* decompress_cnt = nullptr;
        RuntimeProfile::Counter* page_read_counter = nullptr;
        RuntimeProfile::Counter* page_cache_write_counter = nullptr;
        RuntimeProfile::Counter* page_cache_compressed_write_counter = nullptr;
        RuntimeProfile::Counter* page_cache_decompressed_write_counter = nullptr;
        RuntimeProfile::Counter* page_cache_hit_counter = nullptr;
        RuntimeProfile::Counter* page_cache_missing_counter = nullptr;
        RuntimeProfile::Counter* page_cache_compressed_hit_counter = nullptr;
        RuntimeProfile::Counter* page_cache_decompressed_hit_counter = nullptr;
        RuntimeProfile::Counter* decode_header_time = nullptr;
        RuntimeProfile::Counter* read_page_header_time = nullptr;
        RuntimeProfile::Counter* decode_value_time = nullptr;
        RuntimeProfile::Counter* decode_dict_time = nullptr;
        RuntimeProfile::Counter* decode_level_time = nullptr;
        RuntimeProfile::Counter* decode_null_map_time = nullptr;
        RuntimeProfile::Counter* skip_page_header_num = nullptr;
        RuntimeProfile::Counter* parse_page_header_num = nullptr;
        RuntimeProfile::Counter* predicate_filter_time = nullptr;
        RuntimeProfile::Counter* dict_filter_rewrite_time = nullptr;
        RuntimeProfile::Counter* convert_time = nullptr;
        RuntimeProfile::Counter* bloom_filter_read_time = nullptr;
    };

    // ---- set_fill_columns sub-functions ----
    void _collect_predicate_columns_from_conjuncts(
            std::unordered_map<std::string, std::pair<uint32_t, int>>& predicate_columns);
    void _classify_columns_for_lazy_read(
            const std::unordered_map<std::string, std::pair<uint32_t, int>>& predicate_columns,
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns);

    Status _open_file();
    void _init_profile();
    void _close_internal();
    Status _next_row_group_reader();
    RowGroupReader::PositionDeleteContext _get_position_delete_ctx(
            const tparquet::RowGroup& row_group,
            const RowGroupReader::RowGroupIndex& row_group_index);
    void _init_system_properties();
    void _init_file_description();

    // At the beginning of reading next row group, index should be loaded and used to filter data efficiently.
    Status _process_page_index_filter(
            const tparquet::RowGroup& row_group,
            const RowGroupReader::RowGroupIndex& row_group_index,
            const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
            RowRanges* candidate_row_ranges);

    // check this range contain this row group.
    bool _is_misaligned_range_group(const tparquet::RowGroup& row_group) const;

    // Row Group min-max Filter
    Status _process_column_stat_filter(
            const tparquet::RowGroup& row_group,
            const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
            bool* filter_group, bool* filtered_by_min_max, bool* filtered_by_bloom_filter);

    /*
     * 1. row group min-max filter
     * 2. row group bloom filter
     * 3. page index min-max filter
     *
     * return Status && row_ranges (lines to be read)
     */
    Status _process_min_max_bloom_filter(
            const RowGroupReader::RowGroupIndex& row_group_index,
            const tparquet::RowGroup& row_group,
            const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
            RowRanges* row_ranges);

    int64_t _get_column_start_offset(
            const tparquet::ColumnMetaData& column_init_column_readers) const;
    std::string _meta_cache_key(const std::string& path) { return "meta_" + path; }
    std::vector<io::PrefetchRange> _generate_random_access_ranges(
            const RowGroupReader::RowGroupIndex& group, size_t* avg_io_size);
    void _collect_profile();

    Status _set_read_one_line_impl() override { return Status::OK(); }

    bool _exists_in_file(const std::string& expr_name) const;
    bool _type_matches(const int cid) const;
    void _init_read_columns(const std::vector<std::string>& column_names);

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
    const tparquet::FileMetaData* _t_metadata = nullptr;

    // _tracing_file_reader wraps _file_reader.
    // _file_reader is original file reader.
    // _tracing_file_reader is tracing file reader with io context.
    // If io_ctx is null, _tracing_file_reader will be the same as file_reader.
    io::FileReaderSPtr _file_reader = nullptr;
    io::FileReaderSPtr _tracing_file_reader = nullptr;
    std::unique_ptr<RowGroupReader> _current_group_reader;

    RowGroupReader::RowGroupIndex _current_row_group_index {-1, 0, 0};
    // read to the end of current reader
    bool _row_group_eof = true;
    size_t _total_groups = 0; // num of groups(stripes) of a parquet(orc) file

    std::shared_ptr<ConditionCacheContext> _condition_cache_ctx;

    // Through this node, you can find the file column based on the table column.
    std::shared_ptr<TableSchemaChangeHelper::Node> _table_info_node_ptr =
            TableSchemaChangeHelper::ConstNode::get_instance();

    //sequence in file, need to read
    std::vector<std::string> _read_table_columns;
    std::vector<std::string> _read_file_columns;
    // The set of file columns to be read; only columns within this set will be filtered using the min-max predicate.
    std::set<std::string> _read_table_columns_set;
    // Deleted rows will be marked by Iceberg/Paimon. So we should filter deleted rows when reading it.
    const std::vector<int64_t>* _delete_rows = nullptr;
    int64_t _delete_rows_index = 0;

    // parquet file reader object
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    size_t _batch_size;
    // Bytes-per-row estimate from the previous batch, used to pre-shrink _batch_size
    // before reading so that oversized blocks are prevented from the current call onward.
    // Zero means no prior data (first batch).
    size_t _load_bytes_per_row = 0;
    int64_t _range_start_offset;
    int64_t _range_size;
    const cctz::time_zone* _ctz = nullptr;

    std::unordered_map<int, tparquet::OffsetIndex> _col_offsets;

    ReaderStatistics _reader_statistics;
    ParquetColumnReader::ColumnStatistics _column_statistics;
    ParquetProfile _parquet_profile;
    bool _closed = false;
    io::IOContext* _io_ctx = nullptr;
    std::shared_ptr<io::IOContext> _io_ctx_holder;
    RuntimeState* _state = nullptr;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
    const FileMetaData* _file_metadata = nullptr;
    // Pointer to external column name to block index mapping (from FileScanner)
    std::unordered_map<std::string, uint32_t>* _col_name_to_block_idx = nullptr;
    bool _enable_lazy_mat = true;
    bool _enable_filter_by_min_max = true;
    bool _enable_filter_by_bloom_filter = true;
    const std::unordered_map<std::string, int>* _colname_to_slot_id = nullptr;
    const VExprContextSPtrs* _not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
    std::unordered_map<tparquet::Type::type, bool> _ignored_stats;
    size_t get_batch_size() const override { return _batch_size; }

protected:
    // Used for column lazy read. Protected so Iceberg/Paimon subclasses can
    // register synthesized columns in on_before_init_reader.
    RowGroupReader::LazyReadContext _lazy_read_ctx;
    bool _filter_groups = true;

    std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()>
            _create_topn_row_id_column_iterator;

private:
    std::set<uint64_t> _column_ids;
    std::set<uint64_t> _filter_column_ids;

    std::vector<std::unique_ptr<MutilColumnBlockPredicate>> _push_down_predicates;
    Arena _arena;
};

} // namespace doris
