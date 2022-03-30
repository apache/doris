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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H

#include "olap/rowset/rowset_writer.h"
#include "vector"

namespace doris {

namespace fs {
class WritableBlock;
}

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

class BetaRowsetWriter : public RowsetWriter {
public:
    BetaRowsetWriter();

    ~BetaRowsetWriter() override;

    OLAPStatus init(const RowsetWriterContext& rowset_writer_context) override;

    OLAPStatus add_row(const RowCursor& row) override { return _add_row(row); }
    // For Memtable::flush()
    OLAPStatus add_row(const ContiguousRow& row) override { return _add_row(row); }

    // add rowset by create hard link
    OLAPStatus add_rowset(RowsetSharedPtr rowset) override;

    OLAPStatus add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                   const SchemaMapping& schema_mapping) override;

    OLAPStatus flush() override;

    // Return the file size flushed to disk in "flush_size"
    OLAPStatus flush_single_memtable(MemTable* memtable, int64_t* flush_size) override;

    RowsetSharedPtr build() override;

    Version version() override { return _context.version; }

    int64_t num_rows() override { return _num_rows_written; }

    RowsetId rowset_id() override { return _context.rowset_id; }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

private:
    template <typename RowType>
    OLAPStatus _add_row(const RowType& row);

    OLAPStatus _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer);

    OLAPStatus _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer);

private:
    RowsetWriterContext _context;
    std::shared_ptr<RowsetMeta> _rowset_meta;

    std::atomic<int32_t> _num_segment;
    /// When flushing the memtable in the load process, we do not use this writer but an independent writer.
    /// Because we want to flush memtables in parallel.
    /// In other processes, such as merger or schema change, we will use this unified writer for data writing.
    std::unique_ptr<segment_v2::SegmentWriter> _segment_writer;
    mutable SpinLock _lock; // lock to protect _wblocks.
    // TODO(lingbin): it is better to wrapper in a Batch?
    std::vector<std::unique_ptr<fs::WritableBlock>> _wblocks;

    // counters and statistics maintained during data write
    std::atomic<int64_t> _num_rows_written;
    std::atomic<int64_t> _total_data_size;
    std::atomic<int64_t> _total_index_size;
    // TODO rowset Zonemap

    bool _is_pending = false;
    bool _already_built = false;
};

} // namespace doris

#endif //DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_WRITER_H
