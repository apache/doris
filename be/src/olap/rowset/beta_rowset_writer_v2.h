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
#include <gen_cpp/olap_file.pb.h>
#include <stddef.h>
#include <stdint.h>

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

#include "brpc/controller.h"
#include "brpc/stream.h"
#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
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

class LoadStreamStub;

class BetaRowsetWriterV2 : public RowsetWriter {
public:
    BetaRowsetWriterV2(const std::vector<std::shared_ptr<LoadStreamStub>>& streams);

    ~BetaRowsetWriterV2() override;

    Status init(const RowsetWriterContext& rowset_writer_context) override;

    Status add_block(const vectorized::Block* block) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>("add_block is not implemented");
    }

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>("add_rowset is not implemented");
    }

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "add_rowset_for_linked_schema_change is not implemented");
    }

    Status create_file_writer(uint32_t segment_id, io::FileWriterPtr& writer,
                              FileType file_type = FileType::SEGMENT_FILE) override;

    Status flush() override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>("flush is not implemented");
    }

    Status flush_memtable(vectorized::Block* block, int32_t segment_id,
                          int64_t* flush_size) override;

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block) override;

    Status build(RowsetSharedPtr& rowset) override {
        return Status::NotSupported("BetaRowsetWriterV2::build is not supported");
    };

    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) override {
        LOG(FATAL) << "not implemeted";
        return nullptr;
    }

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
        std::lock_guard<SpinLock> l(_lock);
        *segment_num_rows = _segment_num_rows;
        return Status::OK();
    }

    Status add_segment(uint32_t segment_id, const SegmentStatistics& segstat,
                       TabletSchemaSPtr flush_schema) override;

    int32_t allocate_segment_id() override { return _segment_creator.allocate_segment_id(); };

    int32_t next_segment_id() { return _segment_creator.next_segment_id(); };

    int64_t delete_bitmap_ns() override { return _delete_bitmap_ns; }

    int64_t segment_writer_ns() override { return _segment_writer_ns; }

    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() override {
        return _context.partial_update_info;
    }

    bool is_partial_update() override {
        return _context.partial_update_info && _context.partial_update_info->is_partial_update;
    }

private:
    mutable SpinLock _lock; // protect following vectors.
    // record rows number of every segment already written, using for rowid
    // conversion when compaction in unique key with MoW model
    std::vector<uint32_t> _segment_num_rows;

    // for unique key table with merge-on-write
    std::vector<KeyBoundsPB> _segments_encoded_key_bounds;

    SegmentFileCollection _seg_files;

    SegmentCreator _segment_creator;

    InvertedIndexFilesInfo _idx_files_info;

    fmt::memory_buffer vlog_buffer;

    std::vector<std::shared_ptr<LoadStreamStub>> _streams;

    int64_t _delete_bitmap_ns = 0;
    int64_t _segment_writer_ns = 0;
};

} // namespace doris
