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
#include <utility>
#include <vector>

#include "common/status.h" // Status
#include "storage/index/index_file_writer.h"
#include "storage/key/row_key_encoder.h"
#include "storage/olap_define.h"
#include "storage/partial_update_info.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/segment_index_file_cache_loader.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
class Block;
class IOlapColumnDataAccessor;
class OlapBlockDataConvertor;

class DataDir;
class MemTracker;
class ShortKeyIndexBuilder;
class PrimaryKeyIndexBuilder;
class KeyCoder;
struct RowsetWriterContext;

namespace io {
class FileWriter;
class FileSystem;
} // namespace io
namespace segment_v2 {
class IndexFileWriter;

struct VerticalSegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    bool enable_unique_key_merge_on_write = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;

    RowsetWriterContext* rowset_ctx = nullptr;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
};

class DerivedColumnGenerator;
// Matches block_transform.h: at most one derived column (the row-store column)
// for each flush, held as a {cid, generator} pair; null generator means none.
using DerivedColumn = std::pair<uint32_t, std::shared_ptr<const DerivedColumnGenerator>>;

struct RowsInBlock {
    const Block* block;
    size_t row_pos;
    size_t num_rows;
};

class VerticalSegmentWriter {
public:
    explicit VerticalSegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                                   TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                                   DataDir* data_dir, const VerticalSegmentWriterOptions& opts,
                                   IndexFileWriter* index_file_writer);
    ~VerticalSegmentWriter();

    VerticalSegmentWriter(const VerticalSegmentWriter&) = delete;
    const VerticalSegmentWriter& operator=(const VerticalSegmentWriter&) = delete;

    Status init();

    // Add one block to batch, memory is owned by the caller.
    // The batched blocks will be flushed in write_batch.
    // Once write_batch is called, no more blocks shoud be added.
    Status batch_block(const Block* block, size_t row_pos, size_t num_rows);
    Status write_batch();

    void set_derived_column(DerivedColumn derived_column) {
        _derived_column = std::move(derived_column);
    }

    [[nodiscard]] std::string data_dir_path() const {
        return _data_dir == nullptr ? "" : _data_dir->path();
    }

    [[nodiscard]] uint32_t num_rows_written() const { return _num_rows_written; }

    [[nodiscard]] uint32_t row_count() const { return _row_count; }
    [[nodiscard]] uint32_t segment_id() const { return _segment_id; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size,
                    SegmentIndexFileCacheInfo* index_file_cache_info = nullptr);

    Status finalize_columns_index(uint64_t* index_size);
    Status finalize_footer(uint64_t* segment_file_size,
                           SegmentIndexFileCacheInfo* index_file_cache_info = nullptr);

    Slice min_encoded_key();
    Slice max_encoded_key();

    void clear();

    Status close_inverted_index(int64_t* inverted_index_file_size) {
        // no inverted index
        if (_index_file_writer == nullptr) {
            *inverted_index_file_size = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(_index_file_writer->begin_close());
        *inverted_index_file_size = _index_file_writer->get_index_file_total_size();
        return Status::OK();
    }

private:
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                           const ColumnWriterOptions& opts);
    Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                                 const TabletSchemaSPtr& schema);
    uint64_t _estimated_remaining_size();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_inverted_index();
    Status _write_ann_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _set_min_max_key(const Slice& key);
    void _set_min_key(const Slice& key);
    void _set_max_key(const Slice& key);
    Status _append_generated_column(const DerivedColumnGenerator& generator, const Block& block,
                                    size_t row_pos, size_t num_rows, uint32_t cid);
    bool _is_partial_update_load() const;
    Status _write_partial_update_batch();
    Status _write_fixed_partial_update_batch(const RowsInBlock& data);
    Status _write_flexible_partial_update_batch(const RowsInBlock& data);
    Status _precreate_flexible_partial_update_writers(uint32_t num_key_columns);
    Status _write_partial_update_column(const RowsInBlock& data, uint32_t cid,
                                        IOlapColumnDataAccessor*& retained_column);
    Status _generate_key_index(RowsInBlock& data,
                               std::vector<IOlapColumnDataAccessor*>& key_columns,
                               IOlapColumnDataAccessor* seq_column,
                               std::map<uint32_t, IOlapColumnDataAccessor*>& cid_to_column);
    Status _generate_primary_key_index(
            const std::vector<IOlapColumnDataAccessor*>& primary_key_columns,
            IOlapColumnDataAccessor* seq_column, size_t num_rows, bool need_sort);
    Status _generate_short_key_index(std::vector<IOlapColumnDataAccessor*>& key_columns,
                                     size_t num_rows, const std::vector<size_t>& short_key_pos);
    Status _check_column_writer_disk_capacity(size_t cid);
    Status _finalize_column_writer_and_update_meta(size_t cid);

    bool _is_mow() {
        return _tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write;
    }
    bool _is_mow_with_cluster_key() {
        return _is_mow() && !_tablet_schema->cluster_key_uids().empty();
    }

private:
    uint32_t _segment_id;
    TabletSchemaSPtr _tablet_schema;
    BaseTabletSPtr _tablet;
    DataDir* _data_dir = nullptr;
    VerticalSegmentWriterOptions _opts;

    // Not owned. owned by RowsetWriter
    io::FileWriter* _file_writer = nullptr;
    // Not owned. owned by RowsetWriter or SegmentFlusher
    IndexFileWriter* _index_file_writer = nullptr;

    SegmentFooterPB _footer;
    SegmentIndexFileCacheInfo _index_file_cache_info;
    size_t _num_short_key_columns;

    std::unique_ptr<ShortKeyIndexBuilder> _short_key_index_builder;
    std::unique_ptr<PrimaryKeyIndexBuilder> _primary_key_index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<MemTracker> _mem_tracker;

    std::unique_ptr<OlapBlockDataConvertor> _olap_data_convertor;
    // used for building short key index or primary key index during vectorized write.
    // NOTE: must stay declared after _tablet_schema and _opts, the constructor
    // init list reads both through _is_mow().
    RowKeyEncoder _key_encoder;
    size_t _short_key_row_pos = 0;

    // _num_rows_written means row count already written in this current column group
    uint32_t _num_rows_written = 0;

    // _row_count means total row count of this segment
    // In vertical compaction row count is recorded when key columns group finish
    //  and _num_rows_written will be updated in value column group
    uint32_t _row_count = 0;

    bool _is_first_row = true;
    faststring _min_key;
    faststring _max_key;

    // group every rowset-segment row id to speed up reader

    std::vector<RowsInBlock> _batched_blocks;

    // the derived column the transform chain hands off to this writer's bounded pump
    DerivedColumn _derived_column;
};

} // namespace segment_v2
} // namespace doris
