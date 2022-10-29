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
#include "vec/olap/vgeneric_iterators.h"

namespace doris {
namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

namespace io {
class FileWriter;
} // namespace io

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

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

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) override;

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

    int64_t num_rows() const override { return _raw_num_rows_written; }

    RowsetId rowset_id() override { return _context.rowset_id; }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const override {
        std::lock_guard<SpinLock> l(_lock);
        *segment_num_rows = _segment_num_rows;
        return Status::OK();
    }

    void compact_segments(SegCompactionCandidatesSharedPtr segments);

private:
    template <typename RowType>
    Status _add_row(const RowType& row);
    Status _add_block(const vectorized::Block* block,
                      std::unique_ptr<segment_v2::SegmentWriter>* writer);
    Status _add_block_for_segcompaction(const vectorized::Block* block,
                                        std::unique_ptr<segment_v2::SegmentWriter>* writer);

    Status _do_create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer,
                                     bool is_segcompaction, int64_t begin, int64_t end);
    Status _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer);
    Status _create_segment_writer_for_segcompaction(
            std::unique_ptr<segment_v2::SegmentWriter>* writer, uint64_t begin, uint64_t end);

    Status _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer,
                                 int64_t* flush_size = nullptr);
    void _build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta);
    Status _segcompaction_if_necessary();
    Status _segcompaction_ramaining_if_necessary();
    vectorized::VMergeIterator* _get_segcompaction_reader(SegCompactionCandidatesSharedPtr segments,
                                                          std::shared_ptr<Schema> schema,
                                                          OlapReaderStatistics* stat,
                                                          uint64_t* merged_row_stat);
    std::unique_ptr<segment_v2::SegmentWriter> _create_segcompaction_writer(uint64_t begin,
                                                                            uint64_t end);
    Status _delete_original_segments(uint32_t begin, uint32_t end);
    Status _rename_compacted_segments(int64_t begin, int64_t end);
    Status _rename_compacted_segment_plain(uint64_t seg_id);
    Status _load_noncompacted_segments(std::vector<segment_v2::SegmentSharedPtr>* segments);
    Status _find_longest_consecutive_small_segment(SegCompactionCandidatesSharedPtr segments);
    Status _get_segcompaction_candidates(SegCompactionCandidatesSharedPtr& segments, bool is_last);
    Status _wait_flying_segcompaction();
    bool _is_segcompacted() { return (_num_segcompacted > 0) ? true : false; }
    void _clear_statistics_for_deleting_segments_unsafe(uint64_t begin, uint64_t end);
    Status _check_correctness(std::unique_ptr<OlapReaderStatistics> stat, uint64_t merged_row_stat,
                              uint64_t row_count, uint64_t begin, uint64_t end);
    bool _check_and_set_is_doing_segcompaction();

    Status _do_compact_segments(SegCompactionCandidatesSharedPtr segments);

private:
    RowsetWriterContext _context;
    std::shared_ptr<RowsetMeta> _rowset_meta;

    std::atomic<int32_t> _num_segment;
    std::atomic<int32_t> _segcompacted_point; // segemnts before this point have
                                              // already been segment compacted
    std::atomic<int32_t> _num_segcompacted;   // index for segment compaction
    /// When flushing the memtable in the load process, we do not use this writer but an independent writer.
    /// Because we want to flush memtables in parallel.
    /// In other processes, such as merger or schema change, we will use this unified writer for data writing.
    std::unique_ptr<segment_v2::SegmentWriter> _segment_writer;

    mutable SpinLock _lock; // protect following vectors.
    // record rows number of every segment
    std::vector<uint32_t> _segment_num_rows;
    std::vector<io::FileWriterPtr> _file_writers;
    // for unique key table with merge-on-write
    std::vector<KeyBoundsPB> _segments_encoded_key_bounds;

    io::FileWriterPtr _segcompaction_file_writer;

    // counters and statistics maintained during add_rowset
    std::atomic<int64_t> _num_rows_written;
    std::atomic<int64_t> _total_data_size;
    std::atomic<int64_t> _total_index_size;
    // TODO rowset Zonemap

    // written rows by add_block/add_row (not effected by segcompaction)
    std::atomic<int64_t> _raw_num_rows_written;

    struct Statistics {
        int64_t row_num;
        int64_t data_size;
        int64_t index_size;
        KeyBoundsPB key_bounds;
    };
    std::map<uint32_t, Statistics> _segid_statistics_map;
    std::mutex _segid_statistics_map_mutex;

    bool _is_pending = false;
    bool _already_built = false;

    // ensure only one inflight segcompaction task for each rowset
    std::atomic<bool> _is_doing_segcompaction;
    // enforce compare-and-swap on _is_doing_segcompaction
    std::mutex _is_doing_segcompaction_lock;
    std::condition_variable _segcompacting_cond;

    std::atomic<ErrorCode> _segcompaction_status;

    fmt::memory_buffer vlog_buffer;
};

} // namespace doris
