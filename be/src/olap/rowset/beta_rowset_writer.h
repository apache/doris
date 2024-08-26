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

#include <fmt/format.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/delta_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_creator.h"
#include "segment_v2/segment.h"
#include "util/spinlock.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

class SegmentFileCollection {
public:
    ~SegmentFileCollection();

    Status add(int seg_id, io::FileWriterPtr&& writer);

    // Return `nullptr` if no file writer matches `seg_id`
    io::FileWriter* get(int seg_id) const;

    // Close all file writers
    Status close();

    // Get segments file size in segment id order.
    // `seg_id_offset` is the offset of the segment id relative to the subscript of `_file_writers`,
    // for more details, see `Tablet::create_transient_rowset_writer`.
    Result<std::vector<size_t>> segments_file_size(int seg_id_offset);

    const std::unordered_map<int, io::FileWriterPtr>& get_file_writers() const {
        return _file_writers;
    }

private:
    mutable SpinLock _lock;
    std::unordered_map<int /* seg_id */, io::FileWriterPtr> _file_writers;
    bool _closed {false};
};

// Collect the size of the inverted index files
class InvertedIndexFilesInfo {
public:
    // Get inverted index file info in segment id order.
    // Return the info of inverted index files from seg_id_offset to the last one.
    Result<std::vector<InvertedIndexFileInfo>> get_inverted_files_info(int seg_id_offset) {
        std::lock_guard lock(_lock);

        Status st;
        std::vector<InvertedIndexFileInfo> inverted_files_info(_inverted_index_files_info.size());
        bool succ = std::all_of(
                _inverted_index_files_info.begin(), _inverted_index_files_info.end(),
                [&](auto&& it) {
                    auto&& [seg_id, info] = it;

                    int idx = seg_id - seg_id_offset;
                    if (idx >= inverted_files_info.size()) [[unlikely]] {
                        auto err_msg = fmt::format(
                                "invalid seg_id={} num_inverted_files_info={} seg_id_offset={}",
                                seg_id, inverted_files_info.size(), seg_id_offset);
                        DCHECK(false) << err_msg;
                        st = Status::InternalError(err_msg);
                        return false;
                    }

                    auto& finfo = inverted_files_info[idx];
                    if (finfo.has_index_size() || finfo.index_info_size() > 0) [[unlikely]] {
                        // File size should not been set
                        auto err_msg = fmt::format("duplicate seg_id={}", seg_id);
                        DCHECK(false) << err_msg;
                        st = Status::InternalError(err_msg);
                        return false;
                    }
                    finfo = info;
                    return true;
                });

        if (succ) {
            return inverted_files_info;
        }

        return ResultError(st);
    }

    void add_file_info(int seg_id, InvertedIndexFileInfo file_info) {
        std::lock_guard lock(_lock);
        _inverted_index_files_info.emplace(seg_id, file_info);
    }

private:
    std::unordered_map<int /* seg_id */, InvertedIndexFileInfo> _inverted_index_files_info;
    mutable SpinLock _lock;
};

class BaseBetaRowsetWriter : public RowsetWriter {
public:
    BaseBetaRowsetWriter();

    ~BaseBetaRowsetWriter() override;

    Status init(const RowsetWriterContext& rowset_writer_context) override;

    Status add_block(const vectorized::Block* block) override;

    // Declare these interface in `BaseBetaRowsetWriter`
    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override;
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) override;

    Status create_file_writer(uint32_t segment_id, io::FileWriterPtr& writer,
                              FileType file_type = FileType::SEGMENT_FILE) override;

    Status add_segment(uint32_t segment_id, const SegmentStatistics& segstat,
                       TabletSchemaSPtr flush_schema) override;

    Status flush() override;

    Status flush_memtable(vectorized::Block* block, int32_t segment_id,
                          int64_t* flush_size) override;

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block) override;

    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) override;

    PUniqueId load_id() override { return _context.load_id; }

    Version version() override { return _context.version; }

    int64_t num_rows() const override { return _segment_creator.num_rows_written(); }

    // for partial update
    int64_t num_rows_updated() const override { return _segment_creator.num_rows_updated(); }
    int64_t num_rows_deleted() const override { return _segment_creator.num_rows_deleted(); }
    int64_t num_rows_new_added() const override { return _segment_creator.num_rows_new_added(); }
    int64_t num_rows_filtered() const override { return _segment_creator.num_rows_filtered(); }

    RowsetId rowset_id() override { return _context.rowset_id; }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const override {
        std::lock_guard l(_segid_statistics_map_mutex);
        *segment_num_rows = _segment_num_rows;
        return Status::OK();
    }

    int32_t allocate_segment_id() override { return _segment_creator.allocate_segment_id(); };

    void set_segment_start_id(int32_t start_id) override {
        _segment_creator.set_segment_start_id(start_id);
        _segment_start_id = start_id;
    }

    int64_t delete_bitmap_ns() override { return _delete_bitmap_ns; }

    int64_t segment_writer_ns() override { return _segment_writer_ns; }

    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() override {
        return _context.partial_update_info;
    }

    bool is_partial_update() override {
        return _context.partial_update_info && _context.partial_update_info->is_partial_update;
    }

    const std::unordered_map<int, io::FileWriterPtr>& get_file_writers() const {
        return _seg_files.get_file_writers();
    }

    InvertedIndexFilesInfo& get_inverted_index_files_info() { return _idx_files_info; }

private:
    void update_rowset_schema(TabletSchemaSPtr flush_schema);
    // build a tmp rowset for load segment to calc delete_bitmap
    // for this segment
protected:
    Status _generate_delete_bitmap(int32_t segment_id);
    Status _build_rowset_meta(RowsetMeta* rowset_meta, bool check_segment_num = false);
    Status _create_file_writer(const std::string& path, io::FileWriterPtr& file_writer);
    virtual Status _close_file_writers();
    virtual Status _check_segment_number_limit();
    virtual int64_t _num_seg() const;
    // build a tmp rowset for load segment to calc delete_bitmap for this segment
    Status _build_tmp(RowsetSharedPtr& rowset_ptr);

    uint64_t get_rowset_num_rows() {
        std::lock_guard l(_segid_statistics_map_mutex);
        return std::accumulate(_segment_num_rows.begin(), _segment_num_rows.end(), uint64_t(0));
    }

    std::atomic<int32_t> _num_segment; // number of consecutive flushed segments
    roaring::Roaring _segment_set;     // bitmap set to record flushed segment id
    std::mutex _segment_set_mutex;     // mutex for _segment_set
    int32_t _segment_start_id;         // basic write start from 0, partial update may be different

    SegmentFileCollection _seg_files;

    // record rows number of every segment already written, using for rowid
    // conversion when compaction in unique key with MoW model
    std::vector<uint32_t> _segment_num_rows;
    // for unique key table with merge-on-write
    std::vector<KeyBoundsPB> _segments_encoded_key_bounds;

    // counters and statistics maintained during add_rowset
    std::atomic<int64_t> _num_rows_written;
    std::atomic<int64_t> _total_data_size;
    std::atomic<int64_t> _total_index_size;
    // TODO rowset Zonemap

    std::map<uint32_t, SegmentStatistics> _segid_statistics_map;
    mutable std::mutex _segid_statistics_map_mutex;

    bool _is_pending = false;
    bool _already_built = false;

    SegmentCreator _segment_creator;

    fmt::memory_buffer vlog_buffer;

    std::shared_ptr<MowContext> _mow_context;

    int64_t _delete_bitmap_ns = 0;
    int64_t _segment_writer_ns = 0;

    // map<segment_id, inverted_index_file_info>
    InvertedIndexFilesInfo _idx_files_info;
};

class SegcompactionWorker;

// `StorageEngine` mixin for `BaseBetaRowsetWriter`
class BetaRowsetWriter : public BaseBetaRowsetWriter {
public:
    BetaRowsetWriter(StorageEngine& engine);

    ~BetaRowsetWriter() override;

    Status build(RowsetSharedPtr& rowset) override;

    Status add_segment(uint32_t segment_id, const SegmentStatistics& segstat,
                       TabletSchemaSPtr flush_schema) override;

    Status flush_segment_writer_for_segcompaction(
            std::unique_ptr<segment_v2::SegmentWriter>* writer, uint64_t index_size,
            KeyBoundsPB& key_bounds);

    bool is_segcompacted() const { return _num_segcompacted > 0; }

private:
    // segment compaction
    friend class SegcompactionWorker;
    Status _close_file_writers() override;
    Status _check_segment_number_limit() override;
    int64_t _num_seg() const override;
    Status _wait_flying_segcompaction();
    Status _create_segment_writer_for_segcompaction(
            std::unique_ptr<segment_v2::SegmentWriter>* writer, int64_t begin, int64_t end);
    Status _segcompaction_if_necessary();
    Status _segcompaction_rename_last_segments();
    Status _load_noncompacted_segment(segment_v2::SegmentSharedPtr& segment, int32_t segment_id);
    Status _find_longest_consecutive_small_segment(SegCompactionCandidatesSharedPtr& segments);
    bool _check_and_set_is_doing_segcompaction();
    Status _rename_compacted_segments(int64_t begin, int64_t end);
    Status _rename_compacted_segment_plain(uint64_t seg_id);
    Status _rename_compacted_indices(int64_t begin, int64_t end, uint64_t seg_id);
    void _clear_statistics_for_deleting_segments_unsafe(uint64_t begin, uint64_t end);

    StorageEngine& _engine;

    std::atomic<int32_t> _segcompacted_point {0}; // segemnts before this point have
                                                  // already been segment compacted
    std::atomic<int32_t> _num_segcompacted {0};   // index for segment compaction

    std::shared_ptr<SegcompactionWorker> _segcompaction_worker;

    // ensure only one inflight segcompaction task for each rowset
    std::atomic<bool> _is_doing_segcompaction {false};
    // enforce condition variable on _is_doing_segcompaction
    std::mutex _is_doing_segcompaction_lock;
    std::condition_variable _segcompacting_cond;

    std::atomic<int> _segcompaction_status {ErrorCode::OK};
};

} // namespace doris
