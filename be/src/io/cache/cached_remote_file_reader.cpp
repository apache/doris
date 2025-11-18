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

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cloud/cloud_warm_up_manager.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/peer_file_cache_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "olap/storage_policy.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "service/backend_options.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

namespace doris::io {

bvar::Adder<uint64_t> s3_read_counter("cached_remote_reader_s3_read");
bvar::Adder<uint64_t> peer_read_counter("cached_remote_reader_peer_read");
bvar::LatencyRecorder g_skip_cache_num("cached_remote_reader_skip_cache_num");
bvar::Adder<uint64_t> g_skip_cache_sum("cached_remote_reader_skip_cache_sum");
bvar::Adder<uint64_t> g_skip_local_cache_io_sum_bytes(
        "cached_remote_reader_skip_local_cache_io_sum_bytes");
bvar::Adder<uint64_t> g_read_cache_direct_whole_num("cached_remote_reader_cache_direct_whole_num");
bvar::Adder<uint64_t> g_read_cache_direct_partial_num(
        "cached_remote_reader_cache_direct_partial_num");
bvar::Adder<uint64_t> g_read_cache_indirect_num("cached_remote_reader_cache_indirect_num");
bvar::Adder<uint64_t> g_read_cache_direct_whole_bytes(
        "cached_remote_reader_cache_direct_whole_bytes");
bvar::Adder<uint64_t> g_read_cache_direct_partial_bytes(
        "cached_remote_reader_cache_direct_partial_bytes");
bvar::Adder<uint64_t> g_read_cache_indirect_bytes("cached_remote_reader_cache_indirect_bytes");
bvar::Adder<uint64_t> g_read_cache_indirect_total_bytes(
        "cached_remote_reader_cache_indirect_total_bytes");
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_bytes_1min_window(
        "cached_remote_reader_indirect_bytes_1min_window", &g_read_cache_indirect_bytes, 60);
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_total_bytes_1min_window(
        "cached_remote_reader_indirect_total_bytes_1min_window", &g_read_cache_indirect_total_bytes,
        60);
bvar::Adder<uint64_t> g_failed_get_peer_addr_counter(
        "cached_remote_reader_failed_get_peer_addr_counter");

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               const FileReaderOptions& opts)
        : _remote_file_reader(std::move(remote_file_reader)) {
    _is_doris_table = opts.is_doris_table;
    if (_is_doris_table) {
        _cache_hash = BlockFileCache::hash(path().filename().native());
        _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
        if (config::enable_read_cache_file_directly) {
            // this is designed for and test in doris table, external table need extra tests
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
    if (_is_doris_table && config::enable_read_cache_file_directly) {
        std::lock_guard lock(_mtx);
        DCHECK(file_block->state() == FileBlock::State::DOWNLOADED);
        file_block->_owned_by_cached_reader = true;
        _cache_file_readers.emplace(file_block->offset(), std::move(file_block));
    }
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    for (auto& it : _cache_file_readers) {
        it.second->_owned_by_cached_reader = false;
    }
    static_cast<void>(close());
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::s_align_size(size_t offset, size_t read_size,
                                                               size_t length) {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left =
            (left / config::file_cache_each_block_size) * config::file_cache_each_block_size;
    size_t align_right =
            (right / config::file_cache_each_block_size + 1) * config::file_cache_each_block_size;
    align_right = align_right < length ? align_right : length;
    size_t align_size = align_right - align_left;
    if (align_size < config::file_cache_each_block_size && align_left != 0) {
        align_size += config::file_cache_each_block_size;
        align_left -= config::file_cache_each_block_size;
    }
    return std::make_pair(align_left, align_size);
}

namespace {
std::optional<int64_t> extract_tablet_id(const std::string& file_path) {
    return StorageResource::parse_tablet_id_from_path(file_path);
}

// Get peer connection info from tablet_id
std::pair<std::string, int> get_peer_connection_info(const std::string& file_path) {
    std::string host = "";
    int port = 0;

    // Try to get tablet_id from actual path and lookup tablet info
    if (auto tablet_id = extract_tablet_id(file_path)) {
        auto& manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
        if (auto tablet_info = manager.get_balanced_tablet_info(*tablet_id)) {
            host = tablet_info->first;
            port = tablet_info->second;
        } else {
            LOG_WARNING("get peer connection info not found")
                    .tag("tablet_id", *tablet_id)
                    .tag("file_path", file_path);
        }
    } else {
        LOG_WARNING("parse tablet id from path failed")
                .tag("tablet_id", "null")
                .tag("file_path", file_path);
    }

    DBUG_EXECUTE_IF("PeerFileCacheReader::_fetch_from_peer_cache_blocks", {
        host = dp->param<std::string>("host", "127.0.0.1");
        port = dp->param("port", 9060);
        LOG_WARNING("debug point PeerFileCacheReader::_fetch_from_peer_cache_blocks")
                .tag("host", host)
                .tag("port", port);
    });

    return {host, port};
}

// Execute peer read with fallback to S3
// file_size is the size of the file
// used to calculate the rightmost boundary value of the block, due to inaccurate current block meta information.
Status execute_peer_read(const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start,
                         size_t& size, std::unique_ptr<char[]>& buffer,
                         const std::string& file_path, size_t file_size, bool is_doris_table,
                         ReadStatistics& stats, const IOContext* io_ctx) {
    auto [host, port] = get_peer_connection_info(file_path);
    VLOG_DEBUG << "PeerFileCacheReader read from peer, host=" << host << ", port=" << port
               << ", file_path=" << file_path;

    if (host.empty() || port == 0) {
        g_failed_get_peer_addr_counter << 1;
        LOG_EVERY_N(WARNING, 100) << "PeerFileCacheReader host or port is empty"
                                  << ", host=" << host << ", port=" << port
                                  << ", file_path=" << file_path;
        return Status::InternalError<false>("host or port is empty");
    }
    SCOPED_RAW_TIMER(&stats.peer_read_timer);
    peer_read_counter << 1;
    PeerFileCacheReader peer_reader(file_path, is_doris_table, host, port);
    auto st = peer_reader.fetch_blocks(empty_blocks, empty_start, Slice(buffer.get(), size), &size,
                                       file_size, io_ctx);
    if (!st.ok()) {
        LOG_WARNING("PeerFileCacheReader read from peer failed")
                .tag("host", host)
                .tag("port", port)
                .tag("error", st.msg());
    }
    stats.from_peer_cache = true;
    return st;
}

// Execute S3 read
Status execute_s3_read(size_t empty_start, size_t& size, std::unique_ptr<char[]>& buffer,
                       ReadStatistics& stats, const IOContext* io_ctx,
                       FileReaderSPtr remote_file_reader) {
    s3_read_counter << 1;
    SCOPED_RAW_TIMER(&stats.remote_read_timer);
    stats.from_peer_cache = false;
    return remote_file_reader->read_at(empty_start, Slice(buffer.get(), size), &size, io_ctx);
}

} // anonymous namespace

Status CachedRemoteFileReader::_execute_remote_read(const std::vector<FileBlockSPtr>& empty_blocks,
                                                    size_t empty_start, size_t& size,
                                                    std::unique_ptr<char[]>& buffer,
                                                    ReadStatistics& stats,
                                                    const IOContext* io_ctx) {
    DBUG_EXECUTE_IF("CachedRemoteFileReader.read_at_impl.change_type", {
        // Determine read type from debug point or default to S3
        std::string read_type = "s3";
        read_type = dp->param<std::string>("type", "s3");
        LOG_WARNING("CachedRemoteFileReader.read_at_impl.change_type")
                .tag("path", path().native())
                .tag("off", empty_start)
                .tag("size", size)
                .tag("type", read_type);
        // Execute appropriate read strategy
        if (read_type == "s3") {
            return execute_s3_read(empty_start, size, buffer, stats, io_ctx, _remote_file_reader);
        } else {
            return execute_peer_read(empty_blocks, empty_start, size, buffer, path().native(),
                                     this->size(), _is_doris_table, stats, io_ctx);
        }
    });

    if (!doris::config::is_cloud_mode() || !_is_doris_table || io_ctx->is_warmup ||
        !doris::config::enable_cache_read_from_peer) {
        return execute_s3_read(empty_start, size, buffer, stats, io_ctx, _remote_file_reader);
    } else {
        // first try peer read, if peer failed, fallback to S3
        // peer timeout is 5 seconds
        // TODO(dx): here peer and s3 reader need to get data in parallel, and take the one that is correct and returns first
        // ATTN: Save original size before peer read, as it may be modified by fetch_blocks, read peer ref size
        size_t original_size = size;
        auto st = execute_peer_read(empty_blocks, empty_start, size, buffer, path().native(),
                                    this->size(), _is_doris_table, stats, io_ctx);
        if (!st.ok()) {
            // Restore original size for S3 fallback, as peer read may have modified it
            size = original_size;
            // Fallback to S3
            return execute_s3_read(empty_start, size, buffer, stats, io_ctx, _remote_file_reader);
        }
        return st;
    }
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    size_t already_read = 0;
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
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    ReadStatistics stats;
    stats.bytes_read += bytes_req;
    MonotonicStopWatch read_at_sw;
    read_at_sw.start();
    auto defer_func = [&](int*) {
        if (config::print_stack_when_cache_miss) {
            if (io_ctx->file_cache_stats == nullptr && !stats.hit_cache && !io_ctx->is_warmup) {
                LOG_INFO("[verbose] {}", Status::InternalError<true>("not hit cache"));
            }
        }
        if (!stats.hit_cache && config::read_cluster_cache_opt_verbose_log) {
            LOG_INFO(
                    "[verbose] not hit cache, path: {}, offset: {}, size: {}, cost: {} ms, warmup: "
                    "{}",
                    path().native(), offset, bytes_req, read_at_sw.elapsed_time_milliseconds(),
                    io_ctx->is_warmup);
        }
        if (io_ctx->file_cache_stats && !is_dryrun) {
            // update stats in io_ctx, for query profile
            _update_stats(stats, io_ctx->file_cache_stats, io_ctx->is_inverted_index);
            // update stats increment in this reading procedure for file cache metrics
            FileCacheStatistics fcache_stats_increment;
            _update_stats(stats, &fcache_stats_increment, io_ctx->is_inverted_index);
            io::FileCacheMetrics::instance().update(&fcache_stats_increment);
        }
    };
    std::unique_ptr<int, decltype(defer_func)> defer((int*)0x01, std::move(defer_func));
    if (_is_doris_table && config::enable_read_cache_file_directly) {
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
                                 .ok()) { //TODO: maybe read failed because block evict, should handle error
                        break;
                    }
                }
                _cache->add_need_update_lru_block(iter->second);
                need_read_size -= reserve_bytes;
                cur_offset += reserve_bytes;
                already_read += reserve_bytes;
                iter++;
            }
            if (need_read_size == 0) {
                *bytes_read = bytes_req;
                stats.hit_cache = true;
                g_read_cache_direct_whole_num << 1;
                g_read_cache_direct_whole_bytes << bytes_req;
                return Status::OK();
            } else {
                g_read_cache_direct_partial_num << 1;
                g_read_cache_direct_partial_bytes << already_read;
            }
        }
    }
    // read from cache or remote
    g_read_cache_indirect_num << 1;
    size_t indirect_read_bytes = 0;
    auto [align_left, align_size] =
            s_align_size(offset + already_read, bytes_req - already_read, size());
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    auto tablet_id = get_tablet_id(path().string());
    cache_context.tablet_id = tablet_id.value_or(0);
    MonotonicStopWatch sw;
    sw.start();
    FileBlocksHolder holder =
            _cache->get_or_set(_cache_hash, align_left, align_size, cache_context);
    stats.cache_get_or_set_timer += sw.elapsed_time();
    std::vector<FileBlockSPtr> empty_blocks;
    for (auto& block : holder.file_blocks) {
        switch (block->state()) {
        case FileBlock::State::EMPTY:
            VLOG_DEBUG << fmt::format("Block EMPTY path={} hash={}:{}:{} offset={} cache_path={}",
                                      path().native(), _cache_hash.to_string(), _cache_hash.high(),
                                      _cache_hash.low(), block->offset(), block->get_cache_file());
            block->get_or_set_downloader();
            if (block->is_downloader()) {
                empty_blocks.push_back(block);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::EMPTY");
            }
            stats.hit_cache = false;
            break;
        case FileBlock::State::SKIP_CACHE:
            VLOG_DEBUG << fmt::format(
                    "Block SKIP_CACHE path={} hash={}:{}:{} offset={} cache_path={}",
                    path().native(), _cache_hash.to_string(), _cache_hash.high(), _cache_hash.low(),
                    block->offset(), block->get_cache_file());
            empty_blocks.push_back(block);
            stats.hit_cache = false;
            stats.skip_cache = true;
            break;
        case FileBlock::State::DOWNLOADING:
            stats.hit_cache = false;
            break;
        case FileBlock::State::DOWNLOADED:
            _insert_file_reader(block);
            break;
        }
    }
    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_blocks.empty()) {
        empty_start = empty_blocks.front()->range().left;
        empty_end = empty_blocks.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        std::unique_ptr<char[]> buffer(new char[size]);

        // Determine read type and execute remote read
        RETURN_IF_ERROR(
                _execute_remote_read(empty_blocks, empty_start, size, buffer, stats, io_ctx));

        for (auto& block : empty_blocks) {
            if (block->state() == FileBlock::State::SKIP_CACHE) {
                continue;
            }
            SCOPED_RAW_TIMER(&stats.local_write_timer);
            char* cur_ptr = buffer.get() + block->range().left - empty_start;
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
            stats.bytes_write_into_file_cache += block_size;
        }
        // copy from memory directly
        size_t right_offset = offset + bytes_req - 1;
        if (empty_start <= right_offset && empty_end >= offset + already_read && !is_dryrun) {
            size_t copy_left_offset = std::max(offset + already_read, empty_start);
            size_t copy_right_offset = std::min(right_offset, empty_end);
            char* dst = result.data + (copy_left_offset - offset);
            char* src = buffer.get() + (copy_left_offset - empty_start);
            size_t copy_size = copy_right_offset - copy_left_offset + 1;
            memcpy(dst, src, copy_size);
            indirect_read_bytes += copy_size;
        }
    }

    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    *bytes_read = 0;
    for (auto& block : holder.file_blocks) {
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
        if (block_state != FileBlock::State::DOWNLOADED) {
            do {
                SCOPED_RAW_TIMER(&stats.remote_wait_timer);
                SCOPED_RAW_TIMER(&stats.remote_read_timer);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::DOWNLOADING");
                block_state = block->wait();
                if (block_state != FileBlock::State::DOWNLOADING) {
                    break;
                }
            } while (++wait_time < max_wait_time);
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
                    st = block->read(Slice(result.data + (current_offset - offset), read_size),
                                     file_offset);
                    indirect_read_bytes += read_size;
                }
            }
            if (!st || block_state != FileBlock::State::DOWNLOADED) {
                LOG(WARNING) << "Read data failed from file cache downloaded by others. err="
                             << st.msg() << ", block state=" << block_state;
                size_t nest_bytes_read {0};
                stats.hit_cache = false;
                stats.from_peer_cache = false;
                s3_read_counter << 1;
                SCOPED_RAW_TIMER(&stats.remote_read_timer);
                RETURN_IF_ERROR(_remote_file_reader->read_at(
                        current_offset, Slice(result.data + (current_offset - offset), read_size),
                        &nest_bytes_read));
                indirect_read_bytes += read_size;
                DCHECK(nest_bytes_read == read_size);
            }
        }
        *bytes_read += read_size;
        current_offset = right + 1;
    }
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << *bytes_read;

    DCHECK(*bytes_read == bytes_req);
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
        if (read_stats.from_peer_cache) {
            statis->num_peer_io_total++;
            statis->bytes_read_from_peer += read_stats.bytes_read;
            statis->peer_io_timer += read_stats.peer_read_timer;
        } else {
            statis->num_remote_io_total++;
            statis->bytes_read_from_remote += read_stats.bytes_read;
            statis->remote_io_timer += read_stats.remote_read_timer;
        }
    }
    statis->remote_wait_timer += read_stats.remote_wait_timer;
    statis->local_io_timer += read_stats.local_read_timer;
    statis->num_skip_cache_io_total += read_stats.skip_cache;
    statis->bytes_write_into_cache += read_stats.bytes_write_into_file_cache;
    statis->write_cache_io_timer += read_stats.local_write_timer;

    statis->read_cache_file_directly_timer += read_stats.read_cache_file_directly_timer;
    statis->cache_get_or_set_timer += read_stats.cache_get_or_set_timer;
    statis->lock_wait_timer += read_stats.lock_wait_timer;
    statis->get_timer += read_stats.get_timer;
    statis->set_timer += read_stats.set_timer;

    if (is_inverted_index) {
        if (read_stats.hit_cache) {
            statis->inverted_index_num_local_io_total++;
            statis->inverted_index_bytes_read_from_local += read_stats.bytes_read;
        } else {
            if (read_stats.from_peer_cache) {
                statis->inverted_index_num_peer_io_total++;
                statis->inverted_index_bytes_read_from_peer += read_stats.bytes_read;
                statis->inverted_index_peer_io_timer += read_stats.peer_read_timer;
            } else {
                statis->inverted_index_num_remote_io_total++;
                statis->inverted_index_bytes_read_from_remote += read_stats.bytes_read;
                statis->inverted_index_remote_io_timer += read_stats.remote_read_timer;
            }
        }
        statis->inverted_index_local_io_timer += read_stats.local_read_timer;
    }

    g_skip_cache_sum << read_stats.skip_cache;
}

} // namespace doris::io
