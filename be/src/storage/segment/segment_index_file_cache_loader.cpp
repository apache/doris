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

#include "storage/segment/segment_index_file_cache_loader.h"

#include <bvar/bvar.h>

#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "storage/rowset/rowset_writer_context.h"
#include "util/slice.h"

namespace doris::segment_v2 {

namespace {

bvar::Adder<int64_t> g_segment_index_file_cache_load_total("segment_index_file_cache_load_total");
bvar::Adder<int64_t> g_segment_index_file_cache_load_failed("segment_index_file_cache_load_failed");
bvar::Adder<int64_t> g_segment_index_file_cache_load_bytes("segment_index_file_cache_load_bytes");

bool enable_cloud_index_only_file_cache() {
    return config::is_cloud_mode() && config::enable_file_cache &&
           config::enable_file_cache_write_index_file_only;
}

const char* reason_to_string(SegmentIndexFileCacheLoadReason reason) {
    switch (reason) {
    case SegmentIndexFileCacheLoadReason::LOAD:
        return "load";
    case SegmentIndexFileCacheLoadReason::CUMULATIVE_COMPACTION:
        return "cumulative_compaction";
    case SegmentIndexFileCacheLoadReason::BASE_COMPACTION:
        return "base_compaction";
    case SegmentIndexFileCacheLoadReason::SCHEMA_CHANGE:
        return "schema_change";
    }
    return "unknown";
}

SegmentIndexFileCacheLoadReason reason_from_context(const RowsetWriterContext& context) {
    if (context.write_type == DataWriteType::TYPE_SCHEMA_CHANGE) {
        return SegmentIndexFileCacheLoadReason::SCHEMA_CHANGE;
    }
    if (context.write_type == DataWriteType::TYPE_COMPACTION) {
        if (context.compaction_type == ReaderType::READER_BASE_COMPACTION ||
            context.compaction_type == ReaderType::READER_FULL_COMPACTION) {
            return SegmentIndexFileCacheLoadReason::BASE_COMPACTION;
        }
        return SegmentIndexFileCacheLoadReason::CUMULATIVE_COMPACTION;
    }
    return SegmentIndexFileCacheLoadReason::LOAD;
}

Status read_range_to_file_cache(io::FileReaderSPtr reader, uint64_t offset, uint64_t size,
                                const io::IOContext& io_ctx) {
    const auto read_size = static_cast<size_t>(size);
    size_t bytes_read = 0;
    RETURN_IF_ERROR(reader->read_at(offset, Slice(static_cast<const char*>(nullptr), read_size),
                                    &bytes_read, &io_ctx));
    if (bytes_read != read_size) {
        return Status::InternalError(
                "short dry-run read when preloading segment index to file cache, offset={}, "
                "expected={}, actual={}",
                offset, read_size, bytes_read);
    }
    return Status::OK();
}

} // namespace

Status SegmentIndexFileCacheLoader::preload_segment_index_to_file_cache(
        const RowsetWriterContext& context, uint32_t segment_id, const std::string& segment_path,
        const SegmentIndexFileCacheInfo& info) {
    if (!enable_cloud_index_only_file_cache() || context.is_local_rowset()) {
        return Status::OK();
    }

    for (const auto& range : info.index_ranges) {
        auto st = load_segment_index_to_file_cache({
                .fs = context.fs(),
                .segment_path = segment_path,
                .rowset_id = context.rowset_id,
                .tablet_id = context.tablet_id,
                .segment_id = segment_id,
                .range = range,
                .segment_file_size = info.segment_file_size,
                .reason = reason_from_context(context),
        });
        if (!st.ok()) {
            g_segment_index_file_cache_load_failed << 1;
            LOG(WARNING) << "failed to preload segment index to file cache, tablet_id="
                         << context.tablet_id << ", rowset_id=" << context.rowset_id
                         << ", segment_id=" << segment_id << ", segment_path=" << segment_path
                         << ", index_start=" << range.offset << ", index_size=" << range.size
                         << ", segment_file_size=" << info.segment_file_size << ", status=" << st;
        }
    }
    return Status::OK();
}

Status SegmentIndexFileCacheLoader::preload_segment_indexes_to_file_cache(
        const RowsetWriterContext& context,
        const std::vector<SegmentIndexFileCachePreloadTask>& tasks) {
    TEST_SYNC_POINT_CALLBACK("SegmentIndexFileCacheLoader::preload_segment_indexes_to_file_cache",
                             &context, &tasks);
    for (const auto& task : tasks) {
        RETURN_IF_ERROR(preload_segment_index_to_file_cache(
                context, task.segment_id, context.segment_path(task.segment_id), task.info));
    }
    return Status::OK();
}

Status SegmentIndexFileCacheLoader::load_segment_index_to_file_cache(
        const SegmentIndexFileCacheLoadContext& ctx) {
    if (!enable_cloud_index_only_file_cache()) {
        return Status::OK();
    }
    const auto& range = ctx.range;
    if (range.empty()) {
        return Status::OK();
    }
    if (!range.is_valid_for(ctx.segment_file_size)) {
        return Status::InvalidArgument(
                "invalid segment index cache range, path={}, index_start={}, index_size={}, "
                "segment_file_size={}",
                ctx.segment_path, range.offset, range.size, ctx.segment_file_size);
    }
    if (ctx.fs == nullptr) {
        return Status::InternalError("file system is null");
    }

    io::IOContext io_ctx {
            .reader_type = ReaderType::READER_QUERY,
            .is_index_data = true,
            .is_dryrun = true,
            .is_warmup = false,
            .table_name = "",
            .partition_name = "",
    };
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "SegmentIndexFileCacheLoader::load_segment_index_to_file_cache", Status::OK(), &ctx,
            &io_ctx);

    io::FileReaderOptions reader_opts;
    reader_opts.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
    reader_opts.is_doris_table = true;
    reader_opts.file_size = static_cast<int64_t>(ctx.segment_file_size);
    reader_opts.tablet_id = ctx.tablet_id;

    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(ctx.fs->open_file(ctx.segment_path, &reader, &reader_opts));

    RETURN_IF_ERROR(read_range_to_file_cache(reader, range.offset, range.size, io_ctx));
    g_segment_index_file_cache_load_total << 1;
    g_segment_index_file_cache_load_bytes << static_cast<int64_t>(range.size);

    VLOG_DEBUG << "preloaded segment index to file cache, tablet_id=" << ctx.tablet_id
               << ", rowset_id=" << ctx.rowset_id << ", segment_id=" << ctx.segment_id
               << ", segment_path=" << ctx.segment_path << ", index_start=" << range.offset
               << ", index_size=" << range.size << ", segment_file_size=" << ctx.segment_file_size
               << ", reason=" << reason_to_string(ctx.reason);
    return Status::OK();
}

} // namespace doris::segment_v2
