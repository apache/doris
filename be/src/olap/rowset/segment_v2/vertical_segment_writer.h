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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory> // unique_ptr
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h" // Status
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
class InvertedIndexFileWriter;

struct VerticalSegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    bool enable_unique_key_merge_on_write = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;

    RowsetWriterContext* rowset_ctx = nullptr;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
};

struct RowsInBlock {
    const vectorized::Block* block;
    size_t row_pos;
    size_t num_rows;
};

class VerticalSegmentWriter {
public:
    explicit VerticalSegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                                   TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                                   DataDir* data_dir, uint32_t max_row_per_segment,
                                   const VerticalSegmentWriterOptions& opts,
                                   std::shared_ptr<MowContext> mow_context);
    ~VerticalSegmentWriter();

    VerticalSegmentWriter(const VerticalSegmentWriter&) = delete;
    const VerticalSegmentWriter& operator=(const VerticalSegmentWriter&) = delete;

    Status init();

    // Add one block to batch, memory is owned by the caller.
    // The batched blocks will be flushed in write_batch.
    // Once write_batch is called, no more blocks shoud be added.
    Status batch_block(const vectorized::Block* block, size_t row_pos, size_t num_rows);
    Status write_batch();

    [[nodiscard]] std::string data_dir_path() const {
        return _data_dir == nullptr ? "" : _data_dir->path();
    }
    [[nodiscard]] size_t inverted_index_file_size() const { return _inverted_index_file_size; }
    [[nodiscard]] uint32_t num_rows_written() const { return _num_rows_written; }

    // for partial update
    [[nodiscard]] int64_t num_rows_updated() const { return _num_rows_updated; }
    [[nodiscard]] int64_t num_rows_deleted() const { return _num_rows_deleted; }
    [[nodiscard]] int64_t num_rows_new_added() const { return _num_rows_new_added; }
    [[nodiscard]] int64_t num_rows_filtered() const { return _num_rows_filtered; }
    [[nodiscard]] uint32_t row_count() const { return _row_count; }
    [[nodiscard]] uint32_t segment_id() const { return _segment_id; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size);

    Status finalize_columns_index(uint64_t* index_size);
    Status finalize_footer(uint64_t* segment_file_size);

    Slice min_encoded_key();
    Slice max_encoded_key();

    void clear();

private:
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column);
    Status _create_column_writer(uint32_t cid, const TabletColumn& column);
    size_t _calculate_inverted_index_file_size();
    uint64_t _estimated_remaining_size();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bitmap_index();
    Status _write_inverted_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _maybe_invalid_row_cache(const std::string& key) const;
    std::string _encode_keys(const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
                             size_t pos);
    // used for unique-key with merge on write and segment min_max key
    std::string _full_encode_keys(
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos);
    // used for unique-key with merge on write
    void _encode_seq_column(const vectorized::IOlapColumnDataAccessor* seq_column, size_t pos,
                            string* encoded_keys);
    void _set_min_max_key(const Slice& key);
    void _set_min_key(const Slice& key);
    void _set_max_key(const Slice& key);
    void _serialize_block_to_row_column(vectorized::Block& block);
    Status _append_block_with_partial_content(RowsInBlock& data);
    Status _fill_missing_columns(vectorized::MutableColumns& mutable_full_columns,
                                 const std::vector<bool>& use_default_or_null_flag,
                                 bool has_default_or_nullable, const size_t& segment_start_pos,
                                 const vectorized::Block* block);

private:
    uint32_t _segment_id;
    TabletSchemaSPtr _tablet_schema;
    BaseTabletSPtr _tablet;
    DataDir* _data_dir = nullptr;
    VerticalSegmentWriterOptions _opts;

    // Not owned. owned by RowsetWriter
    io::FileWriter* _file_writer = nullptr;
    std::unique_ptr<InvertedIndexFileWriter> _inverted_index_file_writer;

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

    // _num_rows_written means row count already written in this current column group
    uint32_t _num_rows_written = 0;

    /** for partial update stats **/
    int64_t _num_rows_updated = 0;
    int64_t _num_rows_new_added = 0;
    int64_t _num_rows_deleted = 0;
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
    PartialUpdateReadPlan _rssid_to_rid;
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;

    std::vector<RowsInBlock> _batched_blocks;
};

} // namespace segment_v2
} // namespace doris
