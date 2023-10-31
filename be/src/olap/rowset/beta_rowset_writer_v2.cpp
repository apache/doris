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

#include "olap/rowset/beta_rowset_writer_v2.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <memory>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/integral_types.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/stream_sink_file_writer.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/common/schema_util.h" // LocalSchemaChangeRecorder
#include "vec/core/block.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetWriterV2::BetaRowsetWriterV2(const std::vector<std::shared_ptr<LoadStreamStub>>& streams)
        : _next_segment_id(0),
          _num_segment(0),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0),
          _streams(streams) {}

BetaRowsetWriterV2::~BetaRowsetWriterV2() = default;

Status BetaRowsetWriterV2::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    _context.segment_collector = std::make_shared<SegmentCollectorT<BetaRowsetWriterV2>>(this);
    _context.file_writer_creator = std::make_shared<FileWriterCreatorT<BetaRowsetWriterV2>>(this);
    static_cast<void>(_segment_creator.init(_context));
    return Status::OK();
}

Status BetaRowsetWriterV2::create_file_writer(uint32_t segment_id, io::FileWriterPtr& file_writer) {
    auto partition_id = _context.partition_id;
    auto index_id = _context.index_id;
    auto tablet_id = _context.tablet_id;
    auto load_id = _context.load_id;

    auto stream_writer = std::make_unique<io::StreamSinkFileWriter>(_streams);
    stream_writer->init(load_id, partition_id, index_id, tablet_id, segment_id);
    file_writer = std::move(stream_writer);
    return Status::OK();
}

Status BetaRowsetWriterV2::add_segment(uint32_t segment_id, SegmentStatistics& segstat) {
    for (const auto& stream : _streams) {
        RETURN_IF_ERROR(stream->add_segment(_context.partition_id, _context.index_id,
                                            _context.tablet_id, segment_id, segstat));
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_memtable(vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    TabletSchemaSPtr flush_schema;
    {
        SCOPED_RAW_TIMER(&_segment_writer_ns);
        RETURN_IF_ERROR(
                _segment_creator.flush_single_block(block, segment_id, flush_size, flush_schema));
    }
    // delete bitmap and seg compaction are done on the destination BE.
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_single_block(const vectorized::Block* block) {
    return _segment_creator.flush_single_block(block);
}

} // namespace doris
