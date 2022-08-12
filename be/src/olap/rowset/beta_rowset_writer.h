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

#include "olap/rowset/rowset_writer.h"

namespace doris {
namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

namespace io {
class FileWriter;
} // namespace io

class BetaRowsetWriter : public RowsetWriter {
public:
    BetaRowsetWriter();

    ~BetaRowsetWriter() override;

    Status init(const RowsetWriterContext& rowset_writer_context) override;

    Status add_row(const RowCursor& row) override { return _add_row(row); }
    // For Memtable::flush()
    Status add_row(const ContiguousRow& row) override { return _add_row(row); }

    Status add_block(const vectorized::Block* block) override;

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override;

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                               const SchemaMapping& schema_mapping) override;

    Status flush() override;

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_memtable(MemTable* memtable, int64_t* flush_size) override;
    Status flush_single_memtable(const vectorized::Block* block) override;

    RowsetSharedPtr build() override;

    // build a tmp rowset for load segment to calc delete_bitmap
    // for this segment
    RowsetSharedPtr build_tmp() override;

    Version version() override { return _context.version; }

    int64_t num_rows() const override { return _num_rows_written; }

    RowsetId rowset_id() override { return _context.rowset_id; }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const override {
        *segment_num_rows = _segment_num_rows;
        return Status::OK();
    }

private:
    template <typename RowType>
    Status _add_row(const RowType& row);
    Status _add_block(const vectorized::Block* block,
                      std::unique_ptr<segment_v2::SegmentWriter>* writer);

    Status _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer);

    Status _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer);
    void _build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta);

private:
    RowsetWriterContext _context;
    std::shared_ptr<RowsetMeta> _rowset_meta;

    std::atomic<int32_t> _num_segment;
    /// When flushing the memtable in the load process, we do not use this writer but an independent writer.
    /// Because we want to flush memtables in parallel.
    /// In other processes, such as merger or schema change, we will use this unified writer for data writing.
    std::unique_ptr<segment_v2::SegmentWriter> _segment_writer;
    mutable SpinLock _lock; // lock to protect _wblocks.
    std::vector<io::FileWriterPtr> _file_writers;

    // counters and statistics maintained during data write
    std::atomic<int64_t> _num_rows_written;
    std::atomic<int64_t> _total_data_size;
    std::atomic<int64_t> _total_index_size;
    // TODO rowset Zonemap

    bool _is_pending = false;
    bool _already_built = false;

    // for unique key table with merge-on-write
    std::vector<KeyBoundsPB> _segments_encoded_key_bounds;
    // record rows number of every segment
    std::vector<uint32_t> _segment_num_rows;
};

} // namespace doris
