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

#include <cstdint>
#include <memory> // unique_ptr
#include <string>
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "olap/tablet_schema.h"
#include "vec/core/block.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {

// TODO(lingbin): Should be a conf that can be dynamically adjusted, or a member in the context
const uint32_t MAX_SEGMENT_SIZE = static_cast<uint32_t>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE *
                                                        OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE);
class DataDir;
class MemTracker;
class RowBlock;
class RowCursor;
class TabletSchema;
class TabletColumn;
class ShortKeyIndexBuilder;
class PrimaryKeyIndexBuilder;
class KeyCoder;

namespace io {
class FileWriter;
} // namespace io

namespace segment_v2 {

class ColumnWriter;

extern const char* k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    bool enable_unique_key_merge_on_write = false;
};

class SegmentWriter {
public:
    explicit SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                           TabletSchemaSPtr tablet_schema, DataDir* data_dir,
                           uint32_t max_row_per_segment, const SegmentWriterOptions& opts);
    ~SegmentWriter();

    Status init();

    template <typename RowType>
    Status append_row(const RowType& row);

    Status append_block(const vectorized::Block* block, size_t row_pos, size_t num_rows);

    int64_t max_row_to_add(size_t row_avg_size_in_bytes);

    uint64_t estimate_segment_size();

    uint32_t num_rows_written() const { return _row_count; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size);

    static void init_column_meta(ColumnMetaPB* meta, uint32_t* column_id,
                                 const TabletColumn& column, TabletSchemaSPtr tablet_schema);

    uint32_t get_segment_id() const { return _segment_id; }

    Slice min_encoded_key();
    Slice max_encoded_key();

    DataDir* get_data_dir() { return _data_dir; }
    bool is_unique_key() { return _tablet_schema->keys_type() == UNIQUE_KEYS; }

private:
    DISALLOW_COPY_AND_ASSIGN(SegmentWriter);
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bitmap_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    std::string _encode_keys(const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
                             size_t pos, bool null_first = true);

private:
    uint32_t _segment_id;
    TabletSchemaSPtr _tablet_schema;
    DataDir* _data_dir;
    uint32_t _max_row_per_segment;
    SegmentWriterOptions _opts;

    // Not owned. owned by RowsetWriter
    io::FileWriter* _file_writer;

    SegmentFooterPB _footer;
    size_t _num_key_columns;
    std::unique_ptr<ShortKeyIndexBuilder> _short_key_index_builder;
    std::unique_ptr<PrimaryKeyIndexBuilder> _primary_key_index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<MemTracker> _mem_tracker;
    uint32_t _row_count = 0;

    vectorized::OlapBlockDataConvertor _olap_data_convertor;
    // used for building short key index or primary key index during vectorized write.
    std::vector<const KeyCoder*> _key_coders;
    std::vector<uint16_t> _key_index_size;
    size_t _short_key_row_pos = 0;
};

} // namespace segment_v2
} // namespace doris
