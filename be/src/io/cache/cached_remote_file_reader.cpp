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

#include "io/cache/cached_remote_file_reader.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <string.h>

#include <algorithm>
#include <list>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/file_block.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "util/bit_util.h"
#include "util/concurrency_stats.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::io {

bvar::Adder<uint64_t> s3_read_counter("cached_remote_reader_s3_read");
bvar::LatencyRecorder g_skip_cache_num("cached_remote_reader_skip_cache_num");
bvar::Adder<uint64_t> g_skip_cache_sum("cached_remote_reader_skip_cache_sum");
bvar::Adder<uint64_t> g_skip_local_cache_io_sum_bytes(
        "cached_remote_reader_skip_local_cache_io_sum_bytes");
bvar::LatencyRecorder g_read_at_req_bytes("cached_remote_reader_read_at_req_bytes");
bvar::Adder<uint64_t> g_read_at_impl_bytes_req_total("cached_remote_reader",
                                                     "read_at_impl_bytes_req_total");
bvar::PerSecond<bvar::Adder<uint64_t>> g_read_at_impl_bytes_req_per_second(
        "cached_remote_reader", "read_at_impl_bytes_req_per_second",
        &g_read_at_impl_bytes_req_total, 10);
bvar::Adder<uint64_t> g_bytes_write_into_file_cache_total("cached_remote_reader",
                                                          "bytes_write_into_file_cache_total");
bvar::PerSecond<bvar::Adder<uint64_t>> g_bytes_write_into_file_cache_per_second(
        "cached_remote_reader", "bytes_write_into_file_cache_per_second",
        &g_bytes_write_into_file_cache_total, 10);
bvar::LatencyRecorder g_read_at_wait_block_download(
        "cached_remote_reader_read_at_wait_block_download");
bvar::LatencyRecorder g_read_at_impl_cost("cached_remote_reader_read_at_impl_cost");

bvar::Adder<int64_t> g_cached_remote_reader_read_active("cached_remote_reader", "read_active");
bvar::Adder<int64_t> g_cached_remote_reader_blocking_active("cached_remote_reader",
                                                            "blocking_active");
bvar::Adder<int64_t> g_cached_remote_reader_get_or_set_active("cached_remote_reader",
                                                              "get_or_set_active");

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               const FileReaderOptions& opts)
        : _remote_file_reader(std::move(remote_file_reader)) {
    _is_doris_table = opts.is_doris_table;
    if (_is_doris_table) {
        _cache_hash = BlockFileCache::hash(path().filename().native());
        _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
        if (config::enable_read_cache_file_directly) {
            _cache_file_readers = _cache->get_blocks_by_key(_cache_hash);
        }
    } else {
        // Use path and modification time to build cache key
        std::string unique_path = fmt::format("{}:{}", path().native(), opts.mtime);
        _cache_hash = BlockFileCache::hash(unique_path);
        if (opts.cache_base_path.empty()) {
            // if cache path is not specified by session variable, chose randomly.
            _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
        } else {
            // from query session variable: file_cache_base_path
            _cache = FileCacheFactory::instance()->get_by_path(opts.cache_base_path);
            if (_cache == nullptr) {
                LOG(WARNING) << "Can't get cache from base path: " << opts.cache_base_path
                             << ", using random instead.";
                _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
            }
        }
    }
}

void CachedRemoteFileReader::_insert_file_reader(FileBlockSPtr file_block) {
    if (config::enable_read_cache_file_directly) {
        std::lock_guard lock(_mtx);
        DCHECK(file_block->state() == FileBlock::State::DOWNLOADED);
        _cache_file_readers.emplace(file_block->offset(), std::move(file_block));
    }
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    {
        std::unique_lock l(_parallel_mtx);
        _parallel_cv.wait(l, [this] { return _parallel_ref == 0; });
    }
    static_cast<void>(close());
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::s_align_size(size_t offset, size_t read_size,
                                                               size_t file_length) {
    const static size_t block_size = config::file_cache_each_block_size;

    // Calculate the original read range [start, end)
    size_t start_pos = offset;
    size_t end_pos = offset + read_size;

    // Align start position to the previous block boundary
    size_t aligned_start = (start_pos / block_size) * block_size;

    // Align end position to the next block boundary
    size_t aligned_end = ((end_pos - 1) / block_size + 1) * block_size;

    // Ensure we don't exceed file boundaries
    aligned_end = std::min(aligned_end, file_length);

    size_t aligned_size = aligned_end - aligned_start;

    if (aligned_size > config::file_cache_tail_read_extra_bytes_threshold) {
        return {aligned_start, aligned_size};
    }

    // Special case: if aligned size is smaller than a block and we're not at file start,
    // extend backwards to include a full block
    if (aligned_size < block_size && aligned_start > 0) {
        auto factor = config::file_cache_tail_read_extra_factor;
        auto extra = std::min(block_size * factor, aligned_start);
        aligned_start -= extra;
        aligned_size += extra;
    }

    return {aligned_start, aligned_size};
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    g_cached_remote_reader_read_active << 1;
    Defer _ = [&]() { g_cached_remote_reader_read_active << -1; };
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().cached_remote_reader_read_at);

    g_read_at_req_bytes << result.size;
    const bool is_dryrun = io_ctx->is_dryrun;
    DCHECK(!closed());
    DCHECK(io_ctx);
    if (offset > size()) {
        return Status::InvalidArgument(
                fmt::format("offset exceeds file size(offset: {}, file size: {}, path: {})", offset,
                            size(), path().native()));
    }
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    g_read_at_impl_bytes_req_total << bytes_req;
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }
    ReadStatistics stats;
    MonotonicStopWatch watch;
    watch.start();
    auto defer_func = [&](int*) {
        g_bytes_write_into_file_cache_total << stats.bytes_write_into_file_cache;
        g_read_at_impl_cost << watch.elapsed_time_microseconds();
        if (io_ctx->file_cache_stats && !is_dryrun) {
            // update stats in io_ctx, for query profile
            _update_stats(stats, io_ctx->file_cache_stats, io_ctx->is_inverted_index);
            // update stats increment in this reading procedure for file cache metrics
            FileCacheStatistics fcache_stats_increment;
            _update_stats(stats, &fcache_stats_increment, io_ctx->is_inverted_index);
            io::FileCacheProfile::instance().update(&fcache_stats_increment);
        }
    };
    std::unique_ptr<int, decltype(defer_func)> defer((int*)0x01, std::move(defer_func));
    stats.bytes_read += bytes_req;
    if (config::enable_read_cache_file_directly) {
        // read directly
        SCOPED_RAW_TIMER(&stats.read_cache_file_directly_timer);
        size_t need_read_size = bytes_req;
        std::shared_lock lock(_mtx);
        if (!_cache_file_readers.empty()) {
            // find the last offset > offset.
            auto iter = _cache_file_readers.upper_bound(offset);
            if (iter != _cache_file_readers.begin()) {
                iter--;
            }
            size_t cur_offset = offset;
            while (need_read_size != 0 && iter != _cache_file_readers.end()) {
                if (iter->second->offset() > cur_offset ||
                    iter->second->range().right < cur_offset) {
                    break;
                }
                size_t file_offset = cur_offset - iter->second->offset();
                size_t reserve_bytes =
                        std::min(need_read_size, iter->second->range().size() - file_offset);
                if (is_dryrun) [[unlikely]] {
                    g_skip_local_cache_io_sum_bytes << reserve_bytes;
                } else {
                    SCOPED_RAW_TIMER(&stats.local_read_timer);
                    if (!iter->second
                                 ->read(Slice(result.data + (cur_offset - offset), reserve_bytes),
                                        file_offset)
                                 .ok()) {
                        break;
                    }
                }
                need_read_size -= reserve_bytes;
                cur_offset += reserve_bytes;
                iter++;
            }
            if (need_read_size == 0) {
                *bytes_read = bytes_req;
                stats.hit_cache = true;
                return Status::OK();
            }
        }
    }
    // read from cache or remote
    auto [align_left, align_size] = s_align_size(offset, bytes_req, size());
    if (config::file_cache_num_parallel_prefetch > 0 && !is_dryrun && _is_doris_table) {
        auto off = align_left + align_size;
        auto pool = ExecEnv::GetInstance()->scanner_scheduler()->get_remote_scan_thread_pool();

        // Prefetch multiple blocks ahead
        for (int i = 0; i < config::file_cache_num_parallel_prefetch; ++i) {
            if (off >= _remote_file_reader->size()) {
                break; // No more data to prefetch
            }

            auto ioctx = *io_ctx;
            ioctx.is_dryrun = true;
            ioctx.query_id = nullptr;
            ioctx.file_cache_stats = nullptr;
            ioctx.file_reader_stats = nullptr;

            {
                std::unique_lock l(_parallel_mtx);
                _parallel_ref++;
            }

            auto st = pool->submit_scan_task(vectorized::SimplifiedScanTask(
                    [ioctx, off, this] {
                        size_t bytesread;
                        Slice r((char*)0x0, std::min<int64_t>(config::file_cache_each_block_size,
                                                              _remote_file_reader->size() - off));
                        (void)read_at_impl(off, r, &bytesread, &ioctx);
                        std::unique_lock l(_parallel_mtx);
                        _parallel_ref--;
                        _parallel_cv.notify_one();
                    },
                    nullptr));

            if (!st.ok()) {
                std::unique_lock l(_parallel_mtx);
                _parallel_ref--;
            }

            // Move to next block
            off += config::file_cache_each_block_size;
        }
    }
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    MonotonicStopWatch sw;
    sw.start();

    g_cached_remote_reader_get_or_set_active << 1;
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->increment();
    auto holder = std::make_shared<FileBlocksHolder>(
            _cache->get_or_set(_cache_hash, align_left, align_size, cache_context));
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->decrement();
    g_cached_remote_reader_get_or_set_active << -1;

    stats.cache_get_or_set_timer += sw.elapsed_time();
    std::vector<FileBlockSPtr> empty_blocks;
    for (auto& block : holder->file_blocks) {
        switch (block->state()) {
        case FileBlock::State::EMPTY:
            block->get_or_set_downloader();
            if (block->is_downloader()) {
                empty_blocks.push_back(block);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::EMPTY");
            }
            stats.hit_cache = false;
            break;
        case FileBlock::State::SKIP_CACHE:
            empty_blocks.push_back(block);
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
    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_blocks.empty()) {
        empty_start = empty_blocks.front()->range().left;
        empty_end = empty_blocks.back()->range().right;
        size_t size = empty_end - empty_start + 1;

        // For small requests that don't hit cache, read directly and write cache asynchronously
        if (!stats.hit_cache && config::enable_async_write_back_file_cache &&
            bytes_req < config::file_cache_each_block_size *
                                config::file_cache_async_write_back_threshold_factor) {
            return _read_small_request_with_async_write_back(holder, offset, result, bytes_req,
                                                             io_ctx, std::move(empty_blocks),
                                                             empty_start, size, bytes_read, &stats);
        }

        // Synchronous path for large requests or other cases
        std::unique_ptr<char[]> buffer(new char[size]);
        {
            s3_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                         &size, io_ctx));
        }
        {
            SCOPED_RAW_TIMER(&stats.local_write_timer);
            SCOPED_CONCURRENCY_COUNT(
                    ConcurrencyStatsManager::instance().cached_remote_reader_write_back);
            _write_to_file_cache(empty_blocks, buffer.get(), empty_start);
        }
        for (auto& block : empty_blocks) {
            if (block->state() != FileBlock::State::SKIP_CACHE) {
                stats.bytes_write_into_file_cache += block->range().size();
            }
        }
        // copy from memory directly
        size_t right_offset = offset + bytes_req - 1;
        if (empty_start <= right_offset && empty_end >= offset && !is_dryrun) {
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
    for (auto& block : holder->file_blocks) {
        if (current_offset > end_offset) {
            break;
        }
        size_t left = block->range().left;
        size_t right = block->range().right;
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
        FileBlock::State block_state = block->state();
        int64_t wait_time = 0;
        static int64_t max_wait_time = 10;
        TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::max_wait_time", &max_wait_time);
        if (block_state != FileBlock::State::DOWNLOADED && !is_dryrun) {
            g_cached_remote_reader_blocking_active << 1;
            Defer _ = [&]() { g_cached_remote_reader_blocking_active << -1; };
            SCOPED_CONCURRENCY_COUNT(
                    ConcurrencyStatsManager::instance().cached_remote_reader_blocking);
            MonotonicStopWatch watch2;
            watch2.start();
            do {
                SCOPED_RAW_TIMER(&stats.cache_block_download_wait_timer);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::DOWNLOADING");
                block_state = block->wait();
                if (block_state != FileBlock::State::DOWNLOADING) {
                    break;
                }
            } while (++wait_time < max_wait_time);
            g_read_at_wait_block_download << watch2.elapsed_time_microseconds();
        }
        if (wait_time == max_wait_time) [[unlikely]] {
            LOG_WARNING("Waiting too long for the download to complete");
        }
        {
            Status st;
            /*
             * If block_state == EMPTY, the thread reads the data from remote.
             * If block_state == DOWNLOADED, when the cache file is deleted by the other process,
             * the thread reads the data from remote too.
             */
            if (block_state == FileBlock::State::DOWNLOADED) {
                if (is_dryrun) [[unlikely]] {
                    g_skip_local_cache_io_sum_bytes << read_size;
                } else {
                    size_t file_offset = current_offset - left;
                    SCOPED_RAW_TIMER(&stats.local_read_timer);
                    SCOPED_CONCURRENCY_COUNT(
                            ConcurrencyStatsManager::instance().cached_remote_reader_local_read);
                    st = block->read(Slice(result.data + (current_offset - offset), read_size),
                                     file_offset);
                }
            }
            if (!is_dryrun && (!st || block_state != FileBlock::State::DOWNLOADED)) {
                LOG(WARNING) << "Read data failed from file cache downloaded by others. err="
                             << st.msg() << ", block state=" << block_state;
                size_t bytes_read {0};
                stats.hit_cache = false;
                s3_read_counter << 1;
                SCOPED_RAW_TIMER(&stats.remote_read_timer);
                RETURN_IF_ERROR(_remote_file_reader->read_at(
                        current_offset, Slice(result.data + (current_offset - offset), read_size),
                        &bytes_read));
                DCHECK(bytes_read == read_size);
            }
        }
        *bytes_read += read_size;
        current_offset = right + 1;
    }
    DCHECK(*bytes_read == bytes_req);
    return Status::OK();
}

void CachedRemoteFileReader::_write_to_file_cache(const std::vector<FileBlockSPtr>& empty_blocks,
                                                  const char* buffer, size_t empty_start) {
    for (auto& block : empty_blocks) {
        if (block->state() == FileBlock::State::SKIP_CACHE) {
            continue;
        }
        char* cur_ptr = const_cast<char*>(buffer) + block->range().left - empty_start;
        size_t block_size = block->range().size();
        Status st = block->append(Slice(cur_ptr, block_size));
        if (st.ok()) {
            st = block->finalize();
        }
        if (!st.ok()) {
            LOG_EVERY_N(WARNING, 100) << "Write data to file cache failed. err=" << st.msg();
        } else {
            _insert_file_reader(block);
        }
    }
}

Status CachedRemoteFileReader::_read_small_request_with_async_write_back(
        std::shared_ptr<FileBlocksHolder> holder, size_t offset, Slice result, size_t bytes_req,
        const IOContext* io_ctx, std::vector<FileBlockSPtr>&& empty_blocks, size_t empty_start,
        size_t size, size_t* bytes_read, ReadStatistics* stats) {
    // Update statistics
    stats->num_small_request_with_async_write_back++;
    stats->small_request_with_async_write_back_bytes += bytes_req;
    SCOPED_RAW_TIMER(&stats->small_request_with_async_write_back_timer);

    // Submit async task to write cache in background
    auto* engine = dynamic_cast<CloudStorageEngine*>(&ExecEnv::GetInstance()->storage_engine());
    if (engine) {
        auto& downloader = engine->file_cache_block_downloader();

        // Prepare IOContext for async task (clear pointers to avoid dangling references)
        IOContext async_io_ctx;
        if (io_ctx) {
            async_io_ctx = *io_ctx;
        }

        // Capture necessary data for async cache write
        auto async_cache_write_task = [reader = shared_from_this(), holder = std::move(holder),
                                       empty_blocks = std::move(empty_blocks), empty_start, size,
                                       async_io_ctx]() mutable {
            TUniqueId query_id = TUniqueId();
            FileCacheStatistics file_cache_stats;
            FileReaderStats file_reader_stats;
            async_io_ctx.query_id = &query_id;
            async_io_ctx.file_cache_stats = &file_cache_stats;
            async_io_ctx.file_reader_stats = &file_reader_stats;

            // Read from remote
            std::unique_ptr<char[]> buffer(new char[size]);
            size_t read_size = size;
            Status st = reader->_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                             &read_size, &async_io_ctx);
            if (!st.ok()) {
                LOG_EVERY_N(WARNING, 100)
                        << "Async cache write: read from remote failed. err=" << st.msg();
                return;
            }

            // Write to file cache using the extracted helper function
            reader->_write_to_file_cache(empty_blocks, buffer.get(), empty_start);
        };

        Status submit_st = downloader.submit_io_task(std::move(async_cache_write_task));
        if (!submit_st.ok()) {
            LOG_EVERY_N(WARNING, 100)
                    << "Failed to submit async cache write task: " << submit_st.msg();
        }
    }

    // Read directly from remote to user buffer
    {
        s3_read_counter << 1;
        SCOPED_RAW_TIMER(&stats->remote_read_timer);
        size_t direct_read_size = bytes_req;
        RETURN_IF_ERROR(_remote_file_reader->read_at(offset, result, &direct_read_size, io_ctx));
        DCHECK(direct_read_size == bytes_req);
        *bytes_read = bytes_req;
    }
    return Status::OK();
}

void CachedRemoteFileReader::_update_stats(const ReadStatistics& read_stats,
                                           FileCacheStatistics* statis,
                                           bool is_inverted_index) const {
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

    statis->read_cache_file_directly_timer += read_stats.read_cache_file_directly_timer;
    statis->cache_get_or_set_timer += read_stats.cache_get_or_set_timer;
    statis->lock_wait_timer += read_stats.lock_wait_timer;
    statis->get_timer += read_stats.get_timer;
    statis->set_timer += read_stats.set_timer;
    statis->cache_block_download_wait_timer += read_stats.cache_block_download_wait_timer;
    statis->num_small_request_with_async_write_back +=
            read_stats.num_small_request_with_async_write_back;
    statis->small_request_with_async_write_back_bytes +=
            read_stats.small_request_with_async_write_back_bytes;
    statis->small_request_with_async_write_back_timer +=
            read_stats.small_request_with_async_write_back_timer;

    if (is_inverted_index) {
        if (read_stats.hit_cache) {
            statis->inverted_index_num_local_io_total++;
            statis->inverted_index_bytes_read_from_local += read_stats.bytes_read;
        } else {
            statis->inverted_index_num_remote_io_total++;
            statis->inverted_index_bytes_read_from_remote += read_stats.bytes_read;
        }
        statis->inverted_index_local_io_timer += read_stats.local_read_timer;
        statis->inverted_index_remote_io_timer += read_stats.remote_read_timer;
    }

    g_skip_cache_num << read_stats.skip_cache;
    g_skip_cache_sum << read_stats.skip_cache;
}

Status CachedRemoteFileReader::prefetch(size_t offset, size_t size, const IOContext* io_ctx) {
    if (!_cache) {
        VLOG_DEBUG << "Prefetch: No cache available, skipping";
        return Status::OK();
    }

    if (offset >= this->size()) {
        VLOG_DEBUG << fmt::format("Prefetch: Invalid offset {} >= file size {}, skipping", offset,
                                  this->size());
        return Status::OK();
    }

    size_t adjusted_size = std::min(size, this->size() - offset);
    if (adjusted_size == 0) {
        return Status::OK();
    }

    IOContext ctx_copy;
    if (io_ctx) {
        ctx_copy = *io_ctx;
    }

    VLOG_DEBUG << fmt::format("Prefetch: Submitting range [offset={}, size={}] for file {}", offset,
                              adjusted_size, path().native());

    return _submit_prefetch_task(offset, adjusted_size, std::move(ctx_copy));
}

Status CachedRemoteFileReader::prefetch_batch(const std::vector<std::pair<size_t, size_t>>& ranges,
                                              const IOContext* io_ctx) {
    if (!_cache) {
        VLOG_DEBUG << "Prefetch: No cache available, skipping batch";
        return Status::OK();
    }

    if (ranges.empty()) {
        return Status::OK();
    }

    IOContext ctx_copy;
    if (io_ctx) {
        ctx_copy = *io_ctx;
    }

    VLOG_DEBUG << fmt::format("Prefetch: Submitting batch of {} ranges for file {}", ranges.size(),
                              path().native());

    size_t submitted = 0;
    for (const auto& [offset, size] : ranges) {
        if (offset >= this->size()) {
            VLOG_DEBUG << fmt::format("Prefetch: Skipping invalid range [offset={}, size={}]",
                                      offset, size);
            continue;
        }

        size_t adjusted_size = std::min(size, this->size() - offset);
        if (adjusted_size == 0) {
            continue;
        }

        Status st = _submit_prefetch_task(offset, adjusted_size, ctx_copy);
        if (st.ok()) {
            submitted++;
        } else {
            VLOG(1) << fmt::format("Prefetch: Failed to submit range [offset={}, size={}]: {}",
                                   offset, size, st.to_string());
        }
    }

    VLOG_DEBUG << fmt::format("Prefetch: Successfully submitted {}/{} ranges", submitted,
                              ranges.size());

    return Status::OK();
}

Status CachedRemoteFileReader::_submit_prefetch_task(size_t offset, size_t size, IOContext io_ctx) {
    auto* engine = dynamic_cast<CloudStorageEngine*>(&ExecEnv::GetInstance()->storage_engine());
    if (!engine) {
        VLOG_DEBUG << "Prefetch: Not in cloud mode, skipping";
        return Status::OK();
    }

    auto& downloader = engine->file_cache_block_downloader();

    std::weak_ptr<CachedRemoteFileReader> weak_this = shared_from_this();

    auto prefetch_task = [weak_this, offset, size, io_ctx = std::move(io_ctx)]() {
        auto reader = weak_this.lock();
        if (!reader) {
            VLOG_DEBUG << "Prefetch: Reader destroyed before prefetch executed";
            return;
        }

        std::vector<char> dummy_buffer(0);
        size_t bytes_read = 0;

        auto io_ctx_copy = io_ctx;
        io_ctx_copy.is_dryrun = true;

        Status st = reader->read_at(offset, Slice(dummy_buffer.data(), size), &bytes_read,
                                    &io_ctx_copy);

        if (st.ok()) {
            VLOG_DEBUG << fmt::format(
                    "Prefetch: Successfully cached range [offset={}, size={}] for file {}", offset,
                    size, reader->path().native());
        } else {
            VLOG(1) << fmt::format("Prefetch: Failed for range [offset={}, size={}] on file {}: {}",
                                   offset, size, reader->path().native(), st.to_string());
        }
    };

    Status st = downloader.submit_io_task(std::move(prefetch_task));
    if (!st.ok()) {
        VLOG(1) << fmt::format("Prefetch: Failed to submit task to threadpool: {}", st.to_string());
        return st;
    }

    return Status::OK();
}

Status CachedRemoteFileReader::prefetch_blocks_in_parallel(size_t start_offset, size_t end_offset,
                                                           const IOContext* io_ctx) {
    if (!_cache) {
        VLOG_DEBUG << "prefetch_blocks_in_parallel: No cache available, skipping";
        return Status::OK();
    }

    if (start_offset >= size() || end_offset > size() || start_offset >= end_offset) {
        VLOG_DEBUG << fmt::format(
                "prefetch_blocks_in_parallel: Invalid range [{}, {}) for file size {}",
                start_offset, end_offset, size());
        return Status::OK();
    }

    // Get the s3_parallel_read_thread_pool from ExecEnv
    ThreadPool* thread_pool = ExecEnv::GetInstance()->s3_parallel_read_thread_pool();
    if (!thread_pool) {
        VLOG(1) << "prefetch_blocks_in_parallel: s3_parallel_read_thread_pool not available";
        return Status::InternalError("s3_parallel_read_thread_pool not available");
    }

    // Calculate which file cache blocks are covered by the range [start_offset, end_offset)
    const size_t block_size = config::file_cache_each_block_size;
    size_t first_block_start = (start_offset / block_size) * block_size;
    size_t last_block_start = (end_offset - 1) / block_size * block_size;

    VLOG_DEBUG << fmt::format(
            "prefetch_blocks_in_parallel: Processing range [{}, {}) covering blocks [{}, {}] for "
            "file {}",
            start_offset, end_offset, first_block_start, last_block_start, path().native());

    // Prepare IOContext for async tasks
    IOContext ctx_copy;
    if (io_ctx) {
        ctx_copy = *io_ctx;
    }
    // Set as dryrun to avoid allocating unnecessary result buffer
    ctx_copy.is_dryrun = true;
    // Clear pointers to avoid dangling references in async context
    ctx_copy.query_id = nullptr;
    ctx_copy.file_cache_stats = nullptr;
    ctx_copy.file_reader_stats = nullptr;

    std::weak_ptr<CachedRemoteFileReader> weak_this = shared_from_this();

    // Submit a prefetch task for each file cache block in parallel
    size_t num_submitted = 0;
    for (size_t block_start = first_block_start; block_start <= last_block_start;
         block_start += block_size) {
        // Calculate the actual size for this block (considering file boundaries)
        size_t block_end = std::min(block_start + block_size, size());
        size_t read_size = block_end - block_start;

        // Create a prefetch task for this block
        auto prefetch_task = [weak_this, block_start, read_size, ctx_copy]() {
            auto reader = weak_this.lock();
            if (!reader) {
                VLOG_DEBUG << "prefetch_blocks_in_parallel: Reader destroyed before task executed";
                return;
            }

            // Perform a dryrun read to populate the cache
            std::vector<char> dummy_buffer(0);
            size_t bytes_read = 0;
            Status st = reader->read_at(block_start, Slice(dummy_buffer.data(), read_size),
                                        &bytes_read, &ctx_copy);

            if (st.ok()) {
                VLOG_DEBUG << fmt::format(
                        "prefetch_blocks_in_parallel: Successfully prefetched block at offset {} "
                        "size {} for file {}",
                        block_start, read_size, reader->path().native());
            } else {
                VLOG(1) << fmt::format(
                        "prefetch_blocks_in_parallel: Failed to prefetch block at offset {} size "
                        "{} "
                        "for file {}: {}",
                        block_start, read_size, reader->path().native(), st.to_string());
            }
        };

        // Submit to s3_parallel_read_thread_pool
        Status submit_st = thread_pool->submit_func(std::move(prefetch_task));
        if (submit_st.ok()) {
            num_submitted++;
        } else {
            VLOG(1) << fmt::format(
                    "prefetch_blocks_in_parallel: Failed to submit task for block at offset {}: {}",
                    block_start, submit_st.to_string());
        }
    }

    VLOG_DEBUG << fmt::format(
            "prefetch_blocks_in_parallel: Successfully submitted {} prefetch tasks for file {}",
            num_submitted, path().native());

    return Status::OK();
}

} // namespace doris::io
