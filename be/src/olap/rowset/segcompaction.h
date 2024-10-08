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

#include <memory>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/merger.h"
#include "olap/simple_rowid_conversion.h"
#include "olap/tablet.h"
#include "segment_v2/segment.h"

namespace doris {
class Schema;

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2
namespace vectorized {
class RowSourcesBuffer;
class VerticalBlockReader;
} // namespace vectorized
struct OlapReaderStatistics;

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

class BetaRowsetWriter;

class SegcompactionWorker {
    friend class BetaRowsetWriter;

public:
    explicit SegcompactionWorker(BetaRowsetWriter* writer);

    ~SegcompactionWorker() {
        DCHECK(_seg_compact_mem_tracker != nullptr);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_seg_compact_mem_tracker);
        if (_rowid_conversion) {
            _rowid_conversion.reset();
        }
    }

    void compact_segments(SegCompactionCandidatesSharedPtr segments);

    bool need_convert_delete_bitmap();

    void convert_segment_delete_bitmap(DeleteBitmapPtr src_delete_bitmap, uint32_t src_seg_id,
                                       uint32_t dest_seg_id);
    void convert_segment_delete_bitmap(DeleteBitmapPtr src_delete_bitmap, uint32_t src_begin,
                                       uint32_t src_end, uint32_t dest_seg_id);
    DeleteBitmapPtr get_converted_delete_bitmap() { return _converted_delete_bitmap; }

    io::FileWriterPtr& get_file_writer() { return _file_writer; }

    // set the cancel flag, tasks already started will not be cancelled.
    bool cancel();

    void init_mem_tracker(int64_t txn_id);

private:
    Status _create_segment_writer_for_segcompaction(
            std::unique_ptr<segment_v2::SegmentWriter>* writer, uint32_t begin, uint32_t end);
    Status _get_segcompaction_reader(SegCompactionCandidatesSharedPtr segments,
                                     TabletSharedPtr tablet, std::shared_ptr<Schema> schema,
                                     OlapReaderStatistics* stat,
                                     vectorized::RowSourcesBuffer& row_sources_buf, bool is_key,
                                     std::vector<uint32_t>& return_columns,
                                     std::unique_ptr<vectorized::VerticalBlockReader>* reader);
    std::unique_ptr<segment_v2::SegmentWriter> _create_segcompaction_writer(uint32_t begin,
                                                                            uint32_t end);
    Status _delete_original_segments(uint32_t begin, uint32_t end);
    Status _check_correctness(OlapReaderStatistics& reader_stat, Merger::Statistics& merger_stat,
                              uint32_t begin, uint32_t end);
    Status _do_compact_segments(SegCompactionCandidatesSharedPtr segments);

private:
    //TODO(zhengyu): current impl depends heavily on the access to feilds of BetaRowsetWriter
    // Currently cloud storage engine doesn't need segcompaction
    BetaRowsetWriter* _writer = nullptr;
    io::FileWriterPtr _file_writer;

    // for unique key mow table
    std::unique_ptr<SimpleRowIdConversion> _rowid_conversion = nullptr;
    DeleteBitmapPtr _converted_delete_bitmap;
    std::shared_ptr<MemTrackerLimiter> _seg_compact_mem_tracker = nullptr;

    // the state is not mutable when 1)actual compaction operation started or 2) cancelled
    std::atomic<bool> _is_compacting_state_mutable = true;
};
} // namespace doris
