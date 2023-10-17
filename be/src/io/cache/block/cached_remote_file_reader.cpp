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

#include "io/cache/block/cached_remote_file_reader.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <string.h>

#include <algorithm>
#include <list>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/bit_util.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

namespace doris {
namespace io {

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               const FileReaderOptions& opts)
        : _remote_file_reader(std::move(remote_file_reader)) {
    _is_doris_table = opts.is_doris_table;
    if (_is_doris_table) {
        _cache_key = IFileCache::hash(path().filename().native());
        _cache = FileCacheFactory::instance()->get_by_path(_cache_key);
    } else {
        // Use path and modification time to build cache key
        std::string unique_path = fmt::format("{}:{}", path().native(), opts.mtime);
        _cache_key = IFileCache::hash(unique_path);
        if (opts.cache_base_path.empty()) {
            // if cache path is not specified by session variable, chose randomly.
            _cache = FileCacheFactory::instance()->get_by_path(_cache_key);
        } else {
            // from query session variable: file_cache_base_path
            _cache = FileCacheFactory::instance()->get_by_path(opts.cache_base_path);
            if (_cache == nullptr) {
                LOG(WARNING) << "Can't get cache from base path: " << opts.cache_base_path
                             << ", using random instead.";
                _cache = FileCacheFactory::instance()->get_by_path(_cache_key);
            }
        }
    }
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    static_cast<void>(close());
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::_align_size(size_t offset,
                                                              size_t read_size) const {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left, align_right;
    if (_is_doris_table) {
        // when the cache is read_only, we don't need to prefetch datas into cache, so we just read what we need
        if (IFileCache::read_only()) [[unlikely]] {
            return std::make_pair(offset, read_size);
        }
        align_left = (left / config::file_cache_max_file_segment_size) *
                     config::file_cache_max_file_segment_size;
        align_right = (right / config::file_cache_max_file_segment_size + 1) *
                      config::file_cache_max_file_segment_size;
    } else {
        size_t segment_size =
                std::min(std::max(read_size, (size_t)config::file_cache_min_file_segment_size),
                         (size_t)config::file_cache_max_file_segment_size);
        segment_size = BitUtil::next_power_of_two(segment_size);
        align_left = (left / segment_size) * segment_size;
        align_right = (right / segment_size + 1) * segment_size;
    }
    align_right = align_right < size() ? align_right : size();
    size_t align_size = align_right - align_left;
    return std::make_pair(align_left, align_size);
}

Status CachedRemoteFileReader::_read_from_cache(size_t offset, Slice result, size_t* bytes_read,
                                                const IOContext* io_ctx) {
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    ReadStatistics stats;
    // session variable chooses to close file cache for this query
    if (!io_ctx->read_file_cache) {
        SCOPED_RAW_TIMER(&stats.remote_read_timer);
        RETURN_IF_ERROR(_remote_file_reader->read_at(offset, result, bytes_read, io_ctx));
        DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
        if (io_ctx->file_cache_stats) {
            stats.bytes_read += bytes_req;
            _update_state(stats, io_ctx->file_cache_stats);
        }
        return Status::OK();
    }
    auto [align_left, align_size] = _align_size(offset, bytes_req);
    CacheContext cache_context(io_ctx);
    FileBlocksHolder holder = _cache->get_or_set(_cache_key, align_left, align_size, cache_context);
    std::vector<FileBlockSPtr> empty_segments;
    for (auto& segment : holder.file_segments) {
        switch (segment->state()) {
        case FileBlock::State::EMPTY:
            segment->get_or_set_downloader();
            if (segment->is_downloader()) {
                empty_segments.push_back(segment);
            }
            stats.hit_cache = false;
            break;
        case FileBlock::State::SKIP_CACHE:
            empty_segments.push_back(segment);
            stats.hit_cache = false;
            stats.skip_cache = true;
            break;
        case FileBlock::State::DOWNLOADING:
            stats.hit_cache = false;
            break;
        case FileBlock::State::DOWNLOADED:
            break;
        }
    }
    stats.bytes_read += bytes_req;
    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_segments.empty()) {
        empty_start = empty_segments.front()->range().left;
        empty_end = empty_segments.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        std::unique_ptr<char[]> buffer(new char[size]);
        {
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                         &size, io_ctx));
        }
        for (auto& segment : empty_segments) {
            if (segment->state() == FileBlock::State::SKIP_CACHE) {
                continue;
            }
            SCOPED_RAW_TIMER(&stats.local_write_timer);
            char* cur_ptr = buffer.get() + segment->range().left - empty_start;
            size_t segment_size = segment->range().size();
            RETURN_IF_ERROR(segment->append(Slice(cur_ptr, segment_size)));
            RETURN_IF_ERROR(segment->finalize_write());
            stats.bytes_write_into_file_cache += segment_size;
        }
        // copy from memory directly
        size_t right_offset = offset + result.size - 1;
        if (empty_start <= right_offset && empty_end >= offset) {
            size_t copy_left_offset = offset < empty_start ? empty_start : offset;
            size_t copy_right_offset = right_offset < empty_end ? right_offset : empty_end;
            char* dst = result.data + (copy_left_offset - offset);
            char* src = buffer.get() + (copy_left_offset - empty_start);
            size_t copy_size = copy_right_offset - copy_left_offset + 1;
            memcpy(dst, src, copy_size);
        }
    }

    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    *bytes_read = 0;
    for (auto& segment : holder.file_segments) {
        if (current_offset > end_offset) {
            break;
        }
        size_t left = segment->range().left;
        size_t right = segment->range().right;
        if (right < offset) {
            continue;
        }
        size_t read_size =
                end_offset > right ? right - current_offset + 1 : end_offset - current_offset + 1;
        if (empty_start <= left && right <= empty_end) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }
        FileBlock::State segment_state;
        int64_t wait_time = 0;
        static int64_t MAX_WAIT_TIME = 10;
        if (segment->state() != FileBlock::State::DOWNLOADED) {
            do {
                {
                    SCOPED_RAW_TIMER(&stats.remote_read_timer);
                    segment_state = segment->wait();
                }
                if (segment_state == FileBlock::State::DOWNLOADED) {
                    break;
                }
                if (segment_state != FileBlock::State::DOWNLOADING) {
                    return Status::InternalError(
                            "File Cache State is {}, the cache downloader encounters an error, "
                            "please "
                            "retry it",
                            segment_state);
                }
            } while (++wait_time < MAX_WAIT_TIME);
        }
        if (UNLIKELY(wait_time) == MAX_WAIT_TIME) {
            return Status::InternalError("Waiting too long for the download to complete");
        }
        size_t file_offset = current_offset - left;
        {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            RETURN_IF_ERROR(segment->read_at(
                    Slice(result.data + (current_offset - offset), read_size), file_offset));
        }
        *bytes_read += read_size;
        current_offset = right + 1;
    }
    DCHECK(*bytes_read == bytes_req);
    DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
    if (io_ctx->file_cache_stats) {
        _update_state(stats, io_ctx->file_cache_stats);
    }
    return Status::OK();
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    DCHECK(!closed());
    DCHECK(io_ctx);
    if (offset > size()) {
        return Status::InvalidArgument(
                fmt::format("offset exceeds file size(offset: {), file size: {}, path: {})", offset,
                            size(), path().native()));
    }
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }
    Status cache_st = _read_from_cache(offset, result, bytes_read, io_ctx);
    if (UNLIKELY(!cache_st.ok())) {
        if (config::file_cache_wait_sec_after_fail > 0) {
            // only for debug, wait and retry to load data from file cache
            // return error if failed again
            LOG(WARNING) << "Failed to read data from file cache, and wait "
                         << config::file_cache_wait_sec_after_fail
                         << " seconds to reload data: " << cache_st.to_string();
            sleep(config::file_cache_wait_sec_after_fail);
            cache_st = _read_from_cache(offset, result, bytes_read, io_ctx);
        } else {
            // fail over to remote file reader, and return the status of remote read
            LOG(WARNING) << "Failed to read data from file cache, and fail over to remote file: "
                         << cache_st.to_string();
            return _remote_file_reader->read_at(offset, result, bytes_read, io_ctx);
        }
    }
    return cache_st;
}

void CachedRemoteFileReader::_update_state(const ReadStatistics& read_stats,
                                           FileCacheStatistics* statis) const {
    if (statis == nullptr) {
        return;
    }
    if (read_stats.hit_cache) {
        statis->num_local_io_total++;
        statis->bytes_read_from_local += read_stats.bytes_read;
    } else {
        statis->num_remote_io_total++;
        statis->bytes_read_from_remote += read_stats.bytes_read;
    }
    statis->remote_io_timer += read_stats.remote_read_timer;
    statis->local_io_timer += read_stats.local_read_timer;
    statis->num_skip_cache_io_total += read_stats.skip_cache;
    statis->bytes_write_into_cache += read_stats.bytes_write_into_file_cache;
    statis->write_cache_io_timer += read_stats.local_write_timer;
}

} // namespace io
} // namespace doris
