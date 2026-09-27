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

#include "storage/rowset/beta_rowset_writer_v2.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <memory>
#include <sstream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "exec/sink/load_stream_stub.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/stream_sink_file_writer.h"
#include "storage/data_dir.h"
#include "storage/index/global_point/global_point_index_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment/segment.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "util/hash_util.hpp"
#include "util/slice.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetWriterV2::BetaRowsetWriterV2(const std::vector<std::shared_ptr<LoadStreamStub>>& streams)
        : _segment_creator(_context, _seg_files, _idx_files), _streams(streams) {}

BetaRowsetWriterV2::~BetaRowsetWriterV2() = default;

Status BetaRowsetWriterV2::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    // Row-binlog writer or a schema carrying ROW_LSN_COL needs allocated LSN.
    _context._need_allocate_lsn =
            _context.write_binlog_opt().enable ||
            (_context.tablet_schema != nullptr && _context.tablet_schema->row_lsn_col_idx() >= 0);
    _context.segment_collector = std::make_shared<SegmentCollectorT<BetaRowsetWriterV2>>(this);
    _context.file_writer_creator = std::make_shared<FileWriterCreatorT<BetaRowsetWriterV2>>(this);
    RETURN_IF_ERROR(_init_global_point_index_builders());
    return Status::OK();
}

Status BetaRowsetWriterV2::_init_global_point_index_builders() {
    if (!config::enable_global_point_index_sink_build || _context.tablet_schema == nullptr ||
        _context.write_binlog_opt().enable) {
        return Status::OK();
    }
    for (const auto* tablet_index : _context.tablet_schema->global_point_indexes()) {
        if (tablet_index->col_unique_ids().empty()) {
            continue;
        }
        int32_t col_unique_id = tablet_index->col_unique_ids()[0];
        // The receiver sizes its accumulator the same way; the parts must have equal size.
        auto sizing = segment_v2::compute_global_point_index_sizing(
                tablet_index->get_global_point_fpp(),
                _context.exact_row_count_for_global_point_index);
        auto builder = std::make_unique<segment_v2::GlobalPointIndexBuilder>(
                col_unique_id, tablet_index->index_id());
        RETURN_IF_ERROR(builder->init(sizing.bloom_bytes, sizing.per_bloom_fpp));
        _context.global_point_index_builders[col_unique_id] = builder.get();
        _global_point_index_builders[col_unique_id] = std::move(builder);
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::send_point_query_indexes() {
    if (_point_query_indexes_sent || _global_point_index_builders.empty()) {
        return Status::OK();
    }
    _point_query_indexes_sent = true;

    for (auto& [col_unique_id, builder] : _global_point_index_builders) {
        const char* body_data = builder->body();
        size_t body_len = builder->body_size();

        PGlobalPointIndexPart part;
        part.set_column_unique_id(col_unique_id);
        part.set_index_id(builder->index_id());
        part.set_fpp(builder->bloom_fpp());
        part.set_hash_strategy(static_cast<int32_t>(segment_v2::HASH_MURMUR3_X64_64));
        part.set_num_bits(builder->num_bits());
        part.set_body_size(static_cast<int64_t>(body_len));
        part.set_total_rows(builder->total_rows());
        part.set_body_crc32(HashUtil::zlib_crc_hash(body_data, static_cast<uint32_t>(body_len), 0));

        Slice body(body_data, body_len);
        for (const auto& stream : _streams) {
            // A replica that does not get the part drops the index of this rowset on its own.
            auto st = stream->add_point_query_index(_context.partition_id, _context.index_id,
                                                    _context.tablet_id, part, {&body, 1});
            if (!st.ok()) {
                LOG(WARNING) << "failed to send GLOBAL_POINT bloom of column " << col_unique_id
                             << " for tablet " << _context.tablet_id << " to stream "
                             << stream->stream_id() << ": " << st;
            }
        }
    }

    // A sender holds one bloom per indexed column for every tablet it writes, so free them early.
    _global_point_index_builders.clear();
    _context.global_point_index_builders.clear();
    return Status::OK();
}

Status BetaRowsetWriterV2::create_file_writer(uint32_t segment_id, io::FileWriterPtr& file_writer,
                                              FileType file_type) {
    auto partition_id = _context.partition_id;
    auto index_id = _context.index_id;
    auto tablet_id = _context.tablet_id;
    auto load_id = _context.load_id;
    auto stream_writer = std::make_unique<io::StreamSinkFileWriter>(_streams);
    stream_writer->init(load_id, partition_id, index_id, tablet_id, segment_id, file_type);
    file_writer = std::move(stream_writer);
    return Status::OK();
}

Status BetaRowsetWriterV2::add_segment(uint32_t segment_id, const SegmentStatistics& segstat) {
    bool ok = false;
    for (const auto& stream : _streams) {
        auto st = stream->add_segment(_context.partition_id, _context.index_id, _context.tablet_id,
                                      segment_id, segstat);
        if (!st.ok()) {
            LOG(WARNING) << "failed to add segment " << segment_id << " to stream "
                         << stream->stream_id();
        }
        ok = ok || st.ok();
    }
    if (!ok) {
        return Status::InternalError("failed to add segment {} of tablet {} to any replicas",
                                     segment_id, _context.tablet_id);
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_memtable(Block* block, int32_t segment_id, int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_segment_writer_ns);
        RETURN_IF_ERROR(_segment_creator.flush_single_block(block, segment_id, flush_size));
    }
    // delete bitmap and seg compaction are done on the destination BE.
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_single_block(const Block* block) {
    return _segment_creator.flush_single_block(block);
}

Status BetaRowsetWriterV2::flush_single_block(const Block* block, int32_t segment_id) {
    return _segment_creator.flush_single_block(block, segment_id);
}

} // namespace doris
