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
#include <stdint.h>

#include <ctime> // time
#include <memory>
#include <sstream>

#include "common/status.h"
#include "olap/merger.h"
#include "olap/rowset/rowset_writer.h"
#include "segment_v2/segment.h"
#include "segment_v2/segment_writer.h"
#include "vec/olap/vertical_block_reader.h"

namespace doris {
using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

class BetaRowsetWriter;

class SegcompactionWorker {
    friend class BetaRowsetWriter;

public:
    SegcompactionWorker(BetaRowsetWriter* writer) { _writer = writer; }

    void compact_segments(SegCompactionCandidatesSharedPtr segments);

    io::FileWriterPtr& get_file_writer() { return _file_writer; }

private:
    Status _create_segment_writer_for_segcompaction(
            std::unique_ptr<segment_v2::SegmentWriter>* writer, uint64_t begin, uint64_t end);
    Status _get_segcompaction_reader(SegCompactionCandidatesSharedPtr segments,
                                     TabletSharedPtr tablet, std::shared_ptr<Schema> schema,
                                     OlapReaderStatistics* stat,
                                     vectorized::RowSourcesBuffer& row_sources_buf, bool is_key,
                                     std::vector<uint32_t>& return_columns,
                                     std::unique_ptr<vectorized::VerticalBlockReader>* reader);
    std::unique_ptr<segment_v2::SegmentWriter> _create_segcompaction_writer(uint64_t begin,
                                                                            uint64_t end);
    Status _delete_original_segments(uint32_t begin, uint32_t end);
    Status _check_correctness(OlapReaderStatistics& reader_stat, Merger::Statistics& merger_stat,
                              uint64_t begin, uint64_t end);
    Status _do_compact_segments(SegCompactionCandidatesSharedPtr segments);

private:
    //TODO(zhengyu): current impl depends heavily on the access to feilds of BetaRowsetWriter
    BetaRowsetWriter* _writer;
    io::FileWriterPtr _file_writer;
};
} // namespace doris
