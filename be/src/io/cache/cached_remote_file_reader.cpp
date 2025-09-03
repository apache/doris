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

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/file_block.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
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
bvar::LatencyRecorder g_peer_file_reader_latency("peer_file_reader_latency");
bvar::Adder<uint64_t> peer_file_reader_read_counter("peer_file_reader", "read_at");
bvar::PerSecond<bvar::Adder<uint64_t>> peer_get_request_qps("peer_file_reader", "peer_get_request",
                                                            &peer_file_reader_read_counter);
bvar::Adder<uint64_t> peer_bytes_read_total("peer_file_reader", "bytes_read");

// Controller to race two fetchers (e.g., S3 vs peer BE) and signal on first success
struct FetchRangeCntl : std::enable_shared_from_this<FetchRangeCntl> {
    using TaskFn = std::function<void(std::shared_ptr<FetchRangeCntl>)>;
    std::mutex mtx;
    std::condition_variable cv;
    bool done {false};
    Status st {Status::Error<INTERNAL_ERROR>("uninitialized")};
    std::atomic<int> finished {0};
    int total {2};

    void fail_one() {
        int now = finished.fetch_add(1) + 1;
        if (now >= total) {
            std::unique_lock<std::mutex> lock(mtx);
            if (!done) {
                // all sources failed
                done = true;
                cv.notify_all();
            }
        }
    }

    bool is_done() const { return done; }

    // Atomically commit the first successful result into dst and signal success.
    bool commit_and_succeed(const char* src, size_t size, char* dst) {
        std::unique_lock<std::mutex> lock(mtx);
        if (done) {
            return false;
        }
        std::memcpy(dst, src, size);
        st = Status::OK();
        done = true;
        cv.notify_all();
        return true;
    }

    void run_and_wait(TaskFn f1, TaskFn f2) {
        auto self = shared_from_this();
        auto runner1 = [self, fn = std::move(f1)] { fn(self); };
        auto runner2 = [self, fn = std::move(f2)] { fn(self); };
        bool ok1 = true, ok2 = true;
        std::thread t1, t2;
        try {
            t1 = std::thread(runner1);
        } catch (...) {
            ok1 = false;
            self->fail_one();
        }
        try {
            t2 = std::thread(runner2);
        } catch (...) {
            ok2 = false;
            self->fail_one();
        }
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return done; });
        }
        if (ok1 && t1.joinable()) {
            t1.join();
        }
        if (ok2 && t2.joinable()) {
            t2.join();
        }
    }
};

// Helper to race two fetch sources and commit the first success into dst
static Status race_fetch_into_buffer(
        size_t empty_start, size_t size, char* dst, const IOContext* io_ctx, ReadStatistics* stats,
        const std::function<Status(size_t, Slice, size_t*, const IOContext*)>& s3_read_fn,
        const std::function<Status(size_t, Slice, size_t*, const IOContext*)>& peer_read_fn) {
    auto cntl = std::make_shared<FetchRangeCntl>();
    auto s3_fetch = [&, size, empty_start, dst, io_ctx](std::shared_ptr<FetchRangeCntl> c) {
        if (c->is_done()) {
            return;
        }
        size_t read_sz = size;
        std::unique_ptr<char[]> local_buf(new char[size]);
        MonotonicStopWatch t;
        t.start();
        Status st = s3_read_fn(empty_start, Slice(local_buf.get(), read_sz), &read_sz, io_ctx);
        auto elapsed = t.elapsed_time();
        if (st.ok() && read_sz == size) {
            if (c->commit_and_succeed(local_buf.get(), size, dst)) {
                s3_read_counter << 1;
                stats->remote_read_timer += elapsed;
            }
        } else {
            c->fail_one();
        }
    };
    auto peer_fetch = [&, size, empty_start, dst, io_ctx](std::shared_ptr<FetchRangeCntl> c) {
        if (c->is_done()) {
            return;
        }
        size_t read_sz = size;
        std::unique_ptr<char[]> local_buf(new char[size]);
        MonotonicStopWatch t;
        t.start();
        Status st = peer_read_fn(empty_start, Slice(local_buf.get(), read_sz), &read_sz, io_ctx);
        auto elapsed = t.elapsed_time();
        if (st.ok() && read_sz == size) {
            if (c->commit_and_succeed(local_buf.get(), size, dst)) {
                peer_read_counter << 1;
                stats->remote_read_timer += elapsed;
            }
        } else {
            c->fail_one();
        }
    };

    cntl->run_and_wait(std::move(s3_fetch), std::move(peer_fetch));
    if (!cntl->st.ok()) {
        LOG(INFO) << "dx debug run_and_wait failed, try to read from s3, err=" << cntl->st.msg();
        s3_read_counter << 1;
        SCOPED_RAW_TIMER(&stats->remote_read_timer);
        RETURN_IF_ERROR(s3_read_fn(empty_start, Slice(dst, size), &size, io_ctx));
    }
    return Status::OK();
}

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

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
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
    LIMIT_REMOTE_SCAN_IO(bytes_read);
    ReadStatistics stats;
    auto defer_func = [&](int*) {
        if (io_ctx->file_cache_stats && !is_dryrun) {
            // update stats in io_ctx, for query profile
            _update_stats(stats, io_ctx->file_cache_stats, io_ctx->is_inverted_index);
            // update stats increment in this reading procedure for file cache metrics
            FileCacheStatistics fcache_stats_increment;
            _update_stats(stats, &fcache_stats_increment, io_ctx->is_inverted_index);
            io::FileCacheMetrics::instance().update(&fcache_stats_increment);
        }
    };
    std::unique_ptr<int, decltype(defer_func)> defer1((int*)0x01, std::move(defer_func));
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
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    MonotonicStopWatch sw;
    sw.start();
    FileBlocksHolder holder =
            _cache->get_or_set(_cache_hash, align_left, align_size, cache_context);
    stats.cache_get_or_set_timer += sw.elapsed_time();
    std::vector<FileBlockSPtr> empty_blocks;
    for (auto& block : holder.file_blocks) {
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
            // LOG(INFO) << "dx debug set cache miss downloading";
            // empty_blocks.push_back(block);
            stats.hit_cache = false;
            break;
        case FileBlock::State::DOWNLOADED:
            // LOG(INFO) << "dx debug set cache miss downloaded";
            // empty_blocks.push_back(block);
            break;
        }
    }
    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_blocks.empty()) {
        empty_start = empty_blocks.front()->range().left;
        empty_end = empty_blocks.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        LOG(INFO) << "dx debug empty_blocks size=" << empty_blocks.size()
                  << " start=" << empty_start << " end=" << empty_end << " size=" << size;
        std::unique_ptr<char[]> buffer(new char[size]);

        std::string type = "s3";
        DBUG_EXECUTE_IF("CachedRemoteFileReader.read_at_impl.change_type", {
            type = dp->param<std::string>("type", "s3");
            LOG_WARNING("CachedRemoteFileReader.read_at_impl.change_type")
                    .tag("path", path().native())
                    .tag("off", offset)
                    .tag("size", size)
                    .tag("type", type);
        });

        if (type == "s3") {
            // Direct S3 read
            s3_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                         &size, io_ctx));
        } else if (type == "peer") {
            // Direct peer read
            peer_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_fetch_from_peer_cache_blocks(
                    empty_blocks, empty_start, Slice(buffer.get(), size), &size, io_ctx));
        } else if (type == "winner") {
            // Race between S3 and peer - let the faster one win
            auto s3_read_fn = [this](size_t off, Slice s, size_t* n, const IOContext* ctx) {
                DBUG_EXECUTE_IF("CachedRemoteFileReader.read_at_impl.s3_read_fn_failed", {
                    LOG_WARNING("debug point read_at_impl.s3_read_fn_failed")
                            .tag("path", path().native())
                            .tag("off", off)
                            .tag("size", s.size);
                    return Status::InternalError("debug point read_at_impl.s3_read_fn_failed");
                });
                return _remote_file_reader->read_at(off, s, n, ctx);
            };

            auto peer_read_fn = [this, &empty_blocks](size_t off, Slice s, size_t* n,
                                                      const IOContext* ctx) {
                DBUG_EXECUTE_IF("CachedRemoteFileReader.read_at_impl.peer_read_fn_failed", {
                    LOG_WARNING("debug point read_at_impl.peer_read_fn_failed")
                            .tag("path", path().native())
                            .tag("off", off)
                            .tag("size", s.size);
                    return Status::InternalError("debug point read_at_impl.peer_read_fn_failed");
                });
                return _fetch_from_peer_cache_blocks(empty_blocks, off, s, n, ctx);
            };

            RETURN_IF_ERROR(race_fetch_into_buffer(empty_start, size, buffer.get(), io_ctx, &stats,
                                                   s3_read_fn, peer_read_fn));
        } else {
            // Default fallback to S3
            LOG(WARNING) << "Unknown type: " << type << ", falling back to S3 read";
            s3_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                         &size, io_ctx));
        }

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
                }
            }
            if (!st || block_state != FileBlock::State::DOWNLOADED) {
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

Status CachedRemoteFileReader::_fetch_from_peer_cache_blocks(
        const std::vector<FileBlockSPtr>& blocks, size_t off, Slice s, size_t* n,
        const IOContext* ctx) {
    LOG(INFO) << "enter _fetch_from_peer_cache_blocks, off=" << off << " n=" << *n;
    *n = 0;
    if (blocks.empty()) {
        return Status::OK();
    }
    if (!_is_doris_table) {
        return Status::NotSupported("peer cache fetch only supports doris table segments");
    }

    PFetchPeerDataRequest req;
    req.set_type(PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
    req.set_path(path().filename().native());
    for (const auto& blk : blocks) {
        auto* cb = req.add_cache_req();
        cb->set_block_offset(static_cast<int64_t>(blk->range().left));
        cb->set_block_size(static_cast<int64_t>(blk->range().size()));
    }

    // todo(dx), use peer host and port to fetch data
    std::string host = "127.0.0.1";
    int port = 9060;
    DBUG_EXECUTE_IF("CachedRemoteFileReader::_fetch_from_peer_cache_blocks", {
        host = dp->param<std::string>("host", "127.0.0.1");
        port = dp->param("port", 9060);
        LOG_WARNING("debug point CachedRemoteFileReader::_fetch_from_peer_cache_blocks")
                .tag("host", host)
                .tag("port", port);
    });
    auto dns_cache = ExecEnv::GetInstance()->dns_cache();
    if (dns_cache == nullptr) {
        LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
    } else if (!is_valid_ip(host)) {
        Status status = dns_cache->get(host, &host);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get ip from host " << host << ": " << status.to_string();
            return Status::InternalError("failed to get ip from host {}", host);
        }
    }
    std::string brpc_addr = get_host_port(host, port);
    Status st = Status::OK();
    std::shared_ptr<PBackendService_Stub> brpc_stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_new_client_no_cache(
                    brpc_addr);
    if (!brpc_stub) {
        LOG(WARNING) << "failed to get brpc stub " << brpc_addr;
        st = Status::RpcError("Address {} is wrong", brpc_addr);
        return st;
    }
    int64_t begin_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    Defer defer {[&]() {
        int64_t end_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        g_peer_file_reader_latency << (end_ts - begin_ts);
    }};

    brpc::Controller cntl;
    cntl.set_timeout_ms(60000);
    PFetchPeerDataResponse resp;
    peer_file_reader_read_counter << 1;
    brpc_stub->fetch_peer_data(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError(cntl.ErrorText());
    }
    if (resp.has_status()) {
        Status st = Status::create(resp.status());
        if (!st.ok()) return st;
    }

    size_t filled = 0;
    for (const auto& data : resp.datas()) {
        if (data.data().empty()) {
            LOG(WARNING) << "peer cache read empty data" << data.block_offset();
            return Status::InternalError("peer cache read empty data");
        }
        int64_t block_off = data.block_offset();
        size_t rel = block_off > static_cast<int64_t>(off)
                             ? static_cast<size_t>(block_off - static_cast<int64_t>(off))
                             : 0;
        size_t can_copy = std::min(s.size - rel, static_cast<size_t>(data.data().size()));
        LOG(INFO) << "peer cache read data=" << data.block_offset()
                  << " size=" << data.data().size() << " off=" << rel << " can_copy=" << can_copy;
        std::memcpy(s.data + rel, data.data().data(), can_copy);
        filled += can_copy;
    }
    LOG(INFO) << "peer cache read filled=" << filled;
    peer_bytes_read_total << filled;
    *n = filled;
    if (filled == s.size) {
        return Status::OK();
    }
    return Status::InternalError("peer cache read incomplete: need={}, got={}", s.size, filled);
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

} // namespace doris::io
