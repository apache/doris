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

#include <butil/macros.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory> // unique_ptr
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace vectorized {
class Block;
class IOlapColumnDataAccessor;
class OlapBlockDataConvertor;
} // namespace vectorized

// TODO(lingbin): Should be a conf that can be dynamically adjusted, or a member in the context
const uint32_t MAX_SEGMENT_SIZE = static_cast<uint32_t>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE *
                                                        OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE);
class DataDir;
class MemTracker;
class ShortKeyIndexBuilder;
class PrimaryKeyIndexBuilder;
class KeyCoder;
struct RowsetWriterContext;

namespace io {
class FileWriter;
} // namespace io

namespace segment_v2 {

extern const char* k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    bool enable_unique_key_merge_on_write = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;

    RowsetWriterContext* rowset_ctx = nullptr;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
};

using TabletSharedPtr = std::shared_ptr<Tablet>;

using IndicatorMapsVertical = std::map<uint32_t, const vectorized::UInt8*>; // TODO(baohan)

class SegmentWriter {
public:
    explicit SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                           TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet, DataDir* data_dir,
                           uint32_t max_row_per_segment, const SegmentWriterOptions& opts,
                           std::shared_ptr<MowContext> mow_context);
    ~SegmentWriter();

    Status init();

    // for vertical compaction
    Status init(const std::vector<uint32_t>& col_ids, bool has_key);

    template <typename RowType>
    Status append_row(const RowType& row);

    Status append_block(const vectorized::Block* block, size_t row_pos, size_t num_rows);
    Status append_block_with_partial_content(const vectorized::Block* block, size_t row_pos,
                                             size_t num_rows);

    int64_t max_row_to_add(size_t row_avg_size_in_bytes);

    uint64_t estimate_segment_size();
    size_t try_get_inverted_index_file_size();

    size_t get_inverted_index_file_size() const { return _inverted_index_file_size; }
    uint32_t num_rows_written() const { return _num_rows_written; }
    int64_t num_rows_filtered() const { return _num_rows_filtered; }
    uint32_t row_count() const { return _row_count; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size);

    uint32_t get_segment_id() { return _segment_id; }

    Status finalize_columns_data();
    Status finalize_columns_index(uint64_t* index_size);
    Status finalize_footer(uint64_t* segment_file_size);

    void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                          TabletSchemaSPtr tablet_schema);
    Slice min_encoded_key();
    Slice max_encoded_key();

    bool is_unique_key() { return _tablet_schema->keys_type() == UNIQUE_KEYS; }

    void clear();

    void set_mow_context(std::shared_ptr<MowContext> mow_context);
    Status fill_missing_columns(vectorized::Block* full_block,
                                const PartialUpdateReadPlan& read_plan,
                                const std::vector<uint32_t>& cids_full_read,
                                const std::vector<uint32_t>& cids_point_read,
                                const std::vector<bool>& use_default_or_null_flag,
                                bool has_default_or_nullable, const size_t& segment_start_pos,
                                bool is_unique_key_replace_if_not_null);

    std::shared_ptr<IndicatorMaps> get_indicator_maps() const { return _indicator_maps; }

private:
    DISALLOW_COPY_AND_ASSIGN(SegmentWriter);
    Status _create_writers(const TabletSchema& tablet_schema, const std::vector<uint32_t>& col_ids,
                           std::function<Status(uint32_t, const TabletColumn&)> writer_creator);
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bitmap_index();
    Status _write_inverted_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _maybe_invalid_row_cache(const std::string& key);
    std::string _encode_keys(const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
                             size_t pos, bool null_first = true);
    // used for unique-key with merge on write and segment min_max key
    std::string _full_encode_keys(
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
            bool null_first = true);
    // used for unique-key with merge on write
    void _encode_seq_column(const vectorized::IOlapColumnDataAccessor* seq_column, size_t pos,
                            string* encoded_keys);
    void set_min_max_key(const Slice& key);
    void set_min_key(const Slice& key);
    void set_max_key(const Slice& key);
    bool _should_create_writers_with_dynamic_block(size_t num_columns_in_block);
    void _serialize_block_to_row_column(vectorized::Block& block);

    void _calc_indicator_maps(uint32_t row_pos, uint32_t num_rows,
                              const IndicatorMapsVertical& indicator_maps_vertical);

private:
    uint32_t _segment_id;
    TabletSchemaSPtr _tablet_schema;
    BaseTabletSPtr _tablet;
    DataDir* _data_dir;
    uint32_t _max_row_per_segment;
    SegmentWriterOptions _opts;

    // Not owned. owned by RowsetWriter
    io::FileWriter* _file_writer;

    SegmentFooterPB _footer;
    size_t _num_key_columns;
    size_t _num_short_key_columns;
    size_t _inverted_index_file_size;
    std::unique_ptr<ShortKeyIndexBuilder> _short_key_index_builder;
    std::unique_ptr<PrimaryKeyIndexBuilder> _primary_key_index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<MemTracker> _mem_tracker;

    std::unique_ptr<vectorized::OlapBlockDataConvertor> _olap_data_convertor;
    // used for building short key index or primary key index during vectorized write.
    std::vector<const KeyCoder*> _key_coders;
    const KeyCoder* _seq_coder = nullptr;
    std::vector<uint16_t> _key_index_size;
    size_t _short_key_row_pos = 0;

    std::vector<uint32_t> _column_ids;
    bool _has_key = true;
    // _num_rows_written means row count already written in this current column group
    uint32_t _num_rows_written = 0;
    // number of rows filtered in strict mode partial update
    int64_t _num_rows_filtered = 0;
    // _row_count means total row count of this segment
    // In vertical compaction row count is recorded when key columns group finish
    //  and _num_rows_written will be updated in value column group
    uint32_t _row_count = 0;

    bool _is_first_row = true;
    faststring _min_key;
    faststring _max_key;

    std::shared_ptr<MowContext> _mow_context;
    // group every rowset-segment row id to speed up reader
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;

    // For partial update, during publish version we may generate a new block to handle
    // the data loss problem due to concurrent update. We need to re-calculate the values
    // read from the old rows because there may be new rows with same keys written successfully
    // during the period of flush phase and publish phase of the current write. Columns to read from
    // the segements in old versions for this purpose include `missing_cols` and part of the
    // `including_cols` if the property `enable_unique_key_replace_if_not_null` is turned on for all conflict rows.
    // However, in publish version, the input block(s) has been trasnformed to segment(s) and we can't
    // get the information about the locations of indicator values DIRECTLY. So we keep the information of
    // the locations of indicator values of `including_cols` in `_indicator_maps` and pass it to publish phase
    // through TabletTxnInfo
    std::shared_ptr<IndicatorMaps> _indicator_maps = nullptr;

    // record row locations here and used when memtable flush
};

} // namespace segment_v2
} // namespace doris
