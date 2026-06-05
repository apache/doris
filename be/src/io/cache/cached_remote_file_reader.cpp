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
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
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

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/file_block.h"
#include "io/cache/peer_file_cache_reader.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "service/backend_options.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/bthread_utils.h"
#include "util/client_cache.h"
#include "util/concurrency_stats.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

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
bvar::Adder<uint64_t> g_read_cache_self_heal_on_not_found(
        "cached_remote_reader_self_heal_on_not_found");
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_bytes_1min_window(
        "cached_remote_reader_indirect_bytes_1min_window", &g_read_cache_indirect_bytes, 60);
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_total_bytes_1min_window(
        "cached_remote_reader_indirect_total_bytes_1min_window", &g_read_cache_indirect_total_bytes,
        60);
bvar::Adder<uint64_t> g_failed_get_peer_addr_counter(
        "cached_remote_reader_failed_get_peer_addr_counter");

static std::atomic<int> g_active_peer_races {0};
bvar::PassiveStatus<int> g_active_peer_races_bvar(
        "peer_race_active_count",
        [](void*) { return g_active_peer_races.load(std::memory_order_relaxed); }, nullptr);
// Cross-CG peer read race statistics
bvar::Adder<uint64_t> g_peer_race_peer_win("peer_race_peer_win");
bvar::Adder<uint64_t> g_peer_race_s3_win("peer_race_s3_win");
bvar::Adder<uint64_t> g_peer_race_both_fail("peer_race_both_fail");
bvar::Adder<uint64_t> g_peer_cross_compute_group_read("peer_cross_compute_group_read");
bvar::Adder<uint64_t> g_peer_same_compute_group_read("peer_same_compute_group_read");
bvar::Adder<uint64_t> g_peer_lazy_fetch_triggered("peer_lazy_fetch_triggered");

static bool use_remote_only_on_cache_miss(const IOContext* io_ctx) {
    if (io_ctx->file_cache_miss_policy == FileCacheMissPolicy::REMOTE_ONLY_ON_MISS) {
        return true;
    }
    auto* limiter = io_ctx->remote_scan_cache_write_limiter;
    return limiter != nullptr && limiter->remote_only_on_miss();
}

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               const FileReaderOptions& opts)
        : _is_doris_table(opts.is_doris_table),
          _tablet_id(opts.tablet_id),
          _storage_resource_id(opts.storage_resource_id),
          _remote_file_reader(std::move(remote_file_reader)) {
    DCHECK(!_is_doris_table || _tablet_id > 0);
    if (_is_doris_table) {
        _init_doris_table_cache();
    } else {
        _init_external_table_cache(opts);
    }
}

void CachedRemoteFileReader::_init_doris_table_cache() {
    _cache_hash = BlockFileCache::hash(path().filename().native());
    _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
    if (_can_read_cache_file_directly()) {
        // this is designed for and test in doris table, external table need extra tests
        _cache_file_readers = _cache->get_blocks_by_key(_cache_hash);
    }
}

void CachedRemoteFileReader::_init_external_table_cache(const FileReaderOptions& opts) {
    // Use path and modification time to build cache key.
    std::string unique_path = fmt::format("{}:{}", path().native(), opts.mtime);
    _cache_hash = BlockFileCache::hash(unique_path);
    if (opts.cache_base_path.empty()) {
        // If cache path is not specified by session variable, choose randomly.
        _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
        return;
    }

    // From query session variable: file_cache_base_path.
    _cache = FileCacheFactory::instance()->get_by_path(opts.cache_base_path);
    if (_cache != nullptr) {
        return;
    }

    LOG(WARNING) << "Can't get cache from base path: " << opts.cache_base_path
                 << ", using random instead.";
    _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
}

bool CachedRemoteFileReader::_can_read_cache_file_directly() const {
    return _is_doris_table && config::enable_read_cache_file_directly;
}

bool CachedRemoteFileReader::_should_read_from_peer(const IOContext* io_ctx) const {
    return doris::config::is_cloud_mode() && _is_doris_table && _tablet_id > 0 &&
           !io_ctx->is_warmup && !io_ctx->bypass_peer_read &&
           doris::config::enable_cache_read_from_peer;
}

void CachedRemoteFileReader::_insert_file_reader(FileBlockSPtr file_block) {
    if (_can_read_cache_file_directly()) {
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
struct PeerFetchLayout {
    std::vector<size_t> block_offsets;
    std::vector<size_t> block_sizes;
    size_t total_size = 0;
};

bool is_fill_not_found(const Status& st, bool request_fill) {
    return request_fill && st.is<ErrorCode::NOT_FOUND>();
}

bool contains_file_block(const PeerFetchedBlockSet& fetched_blocks, const FileBlockSPtr& block) {
    return fetched_blocks.contains(block.get());
}

size_t clip_peer_block_size(const FileBlock::Range& range, size_t file_size) {
    if (range.left >= file_size) {
        return 0;
    }
    return std::min(file_size - range.left, range.size());
}

PeerFetchLayout build_peer_fetch_layout(const std::vector<FileBlockSPtr>& blocks,
                                        size_t file_size) {
    PeerFetchLayout layout;
    layout.block_offsets.reserve(blocks.size());
    layout.block_sizes.reserve(blocks.size());
    for (const auto& block : blocks) {
        const size_t block_size = clip_peer_block_size(block->range(), file_size);
        layout.block_offsets.push_back(layout.total_size);
        layout.block_sizes.push_back(block_size);
        layout.total_size += block_size;
    }
    return layout;
}

Status write_peer_payloads_into_block(const FileBlockSPtr& block,
                                      std::vector<const PeerFetchChunk*>& chunks,
                                      size_t* block_size) {
    if (block_size == nullptr) {
        return Status::InvalidArgument("peer block write requires non-null block_size");
    }
    *block_size = 0;
    if (chunks.empty()) {
        return Status::OK();
    }
    std::sort(chunks.begin(), chunks.end(),
              [](const PeerFetchChunk* lhs, const PeerFetchChunk* rhs) {
                  return lhs->block_offset < rhs->block_offset;
              });
    butil::IOBuf payload;
    for (const auto* chunk : chunks) {
        *block_size += chunk->payload.length();
        payload.append(chunk->payload);
    }
    DCHECK(*block_size != 0);
    return block->append_iobuf(payload);
}

void copy_peer_chunk_to_result(const PeerFetchChunk& chunk, size_t offset, size_t right_offset,
                               size_t already_read, Slice result, size_t& indirect_read_bytes,
                               SourceReadBreakdown& source_read_breakdown) {
    const size_t payload_size = chunk.payload.length();
    if (payload_size == 0) {
        return;
    }
    const size_t chunk_left = chunk.block_offset;
    const size_t chunk_right = chunk_left + payload_size - 1;
    const size_t copy_left_offset = std::max(offset + already_read, chunk_left);
    const size_t copy_right_offset = std::min(right_offset, chunk_right);
    if (copy_left_offset > copy_right_offset) {
        return;
    }
    const size_t copy_offset = copy_left_offset - chunk_left;
    const size_t copy_size = copy_right_offset - copy_left_offset + 1;
    char* dst = result.data + (copy_left_offset - offset);
    chunk.payload.copy_to(dst, copy_size, copy_offset);
    indirect_read_bytes += copy_size;
    source_read_breakdown.peer_bytes += copy_size;
}

// Execute peer read targeting a specific host:port.
Status execute_peer_read(const std::vector<FileBlockSPtr>& empty_blocks,
                         PeerFetchResult* peer_result, const std::string& file_path,
                         size_t file_size, bool is_doris_table, ReadStatistics& stats,
                         const IOContext* io_ctx, const std::string& host, int32_t port) {
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
    // Serial peer read: source BE has the data from rebalance warm-up; no fill needed.
    auto st = peer_reader.fetch_blocks(empty_blocks, peer_result, file_size, io_ctx,
                                       /*request_fill=*/false);
    if (!st.ok()) {
        LOG_WARNING("PeerFileCacheReader read from peer failed")
                .tag("host", host)
                .tag("port", port)
                .tag("error", st.msg());
    }
    stats.from_peer_cache = st.ok();
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

CloudWarmUpManager& get_warm_up_manager() {
    return ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
}

// Shared state for peer-vs-S3 winner race.
// Uses bthread primitives — never std::mutex/condition_variable in bthread context.
struct RaceState {
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    int winner = -1; // 0=peer won, 1=s3 won, -1=undecided, -2=both failed
    bool peer_done = false;
    bool s3_done = false;
    Status peer_status;
    Status s3_status;
    std::unique_ptr<char[]> s3_buf;
    PeerFetchResult peer_res;
    std::string peer_winner_cg_id; // compute_group_id of the winning peer candidate
    std::string peer_winner_host;  // host of the winning peer candidate
    int64_t peer_elapsed_ns = 0;   // wall-clock time of the entire peer path (including retries)
    int64_t peer_winner_io_ns = 0; // I/O time of the winning candidate only
};

// Peer race logic: try candidates sequentially until one succeeds or all fail.
// NOTE: Do NOT capture io_ctx here — it points into the caller's stack which may be destroyed
// when S3 wins the race and the caller returns before this bthread finishes.
void run_peer_race(std::shared_ptr<RaceState> race, std::vector<FileBlockSPtr> empty_blocks,
                   const std::string& file_path, size_t file_sz, bool is_doris,
                   std::shared_ptr<CloudWarmUpManager> manager,
                   std::vector<doris::PeerCandidate> candidates, int64_t tablet_id,
                   std::string resource_id, std::shared_ptr<ResourceContext> parent_resource_ctx) {
    std::unique_ptr<AttachTask> attach_task;
    if (parent_resource_ctx != nullptr) {
        attach_task = std::make_unique<AttachTask>(parent_resource_ctx);
    }

    bool all_tried = true;
    MonotonicStopWatch peer_sw;
    peer_sw.start();

    for (size_t i = 0; i < candidates.size(); ++i) {
        // Before issuing the next RPC, check if S3 already won.
        if (i > 0) {
            TEST_SYNC_POINT("run_peer_race::between_candidates");
            std::unique_lock<bthread::Mutex> lk(race->mtx);
            if (race->winner > 0) {
                // S3 already won — stop, but not all candidates were tried.
                all_tried = false;
                break;
            }
        }

        const auto& cand = candidates[i];
        peer_read_counter << 1;
        PeerFileCacheReader peer_reader(file_path, is_doris, cand.host, cand.brpc_port);
        PeerFetchResult local_peer_res;
        const bool request_fill =
                !config::peer_cache_fill_compute_group_id.empty() &&
                cand.compute_group_id == config::peer_cache_fill_compute_group_id &&
                !resource_id.empty() && !file_path.empty();
        MonotonicStopWatch cand_sw;
        cand_sw.start();
        auto st = peer_reader.fetch_blocks(empty_blocks, &local_peer_res, file_sz,
                                           /*ctx=*/nullptr, request_fill, tablet_id, resource_id);
        if (st.ok()) {
            manager->update_peer_candidate_on_success(tablet_id, cand.compute_group_id);
            std::unique_lock<bthread::Mutex> lk(race->mtx);
            if (race->winner < 0) {
                race->winner = 0;
                race->peer_res = std::move(local_peer_res);
                race->peer_winner_cg_id = cand.compute_group_id;
                race->peer_winner_host = cand.host;
                race->peer_elapsed_ns = peer_sw.elapsed_time();
                race->peer_winner_io_ns = cand_sw.elapsed_time();
            }
            race->peer_done = true;
            race->peer_status = Status::OK();
            race->cv.notify_all();
            return;
        }

        // Handle per-candidate failure.
        if (st.template is<ErrorCode::TOO_MANY_TASKS>()) {
            all_tried = false;
            break;
        }
        if (is_fill_not_found(st, request_fill)) {
            // Pull-through fill already told us this designated fill CG could not serve the block
            // in time. Do not serially retry additional candidates in the same race; let S3 win
            // instead of paying more peer RPC latency.
            manager->rotate_peer_candidate_on_cache_miss(tablet_id, cand.host, cand.brpc_port);
            all_tried = false;
            break;
        }
        if (st.template is<ErrorCode::NOT_FOUND>()) {
            manager->rotate_peer_candidate_on_cache_miss(tablet_id, cand.host, cand.brpc_port);
        } else {
            manager->update_peer_candidate_on_rpc_failure(tablet_id, cand.host, cand.brpc_port);
        }
    }

    if (all_tried) {
        manager->record_peer_all_miss(tablet_id);
    }
    std::unique_lock<bthread::Mutex> lk(race->mtx);
    race->peer_done = true;
    race->peer_status = Status::InternalError<false>("peer: all candidates failed");
    if (race->winner < 0 && race->s3_done) {
        race->winner = race->s3_status.ok() ? 1 : -2;
    }
    race->cv.notify_all();
}

// Apply hedge delay, then submit S3 read to the thread pool (or run inline).
void launch_s3_race(std::shared_ptr<RaceState> race, size_t empty_start, size_t span_size,
                    const IOContext* io_ctx, FileReaderSPtr remote_reader,
                    std::shared_ptr<ResourceContext> parent_resource_ctx,
                    std::shared_ptr<CachedRemoteFileReader> owner) {
    // Raw S3 read body.
    // `owner` keeps the CachedRemoteFileReader alive until the S3 task finishes,
    // preventing close() from being called on remote_reader while we are still reading.
    // Do NOT capture io_ctx: it points into the caller's stack/iterator which may be
    // destroyed when the query is cancelled before this background task runs. The S3
    // leg of the race is a best-effort background task whose result is discarded if the
    // peer wins; passing nullptr is safe because S3FileReader::read_at_impl ignores it.
    auto do_s3_read = [race, empty_start, span_size, remote_reader, owner]() {
        (void)owner;
        auto s3_buf = std::make_unique<char[]>(span_size);
        size_t read_size = span_size;
        s3_read_counter << 1;
        TEST_SYNC_POINT("CachedRemoteFileReader::_execute_winner_race::s3_before_read");
        auto st = remote_reader->read_at(empty_start, Slice(s3_buf.get(), span_size), &read_size,
                                         nullptr);
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        race->s3_done = true;
        race->s3_status = st;
        if (st.ok() && race->winner < 0) {
            race->winner = 1;
            race->s3_buf = std::move(s3_buf);
        }
        race->cv.notify_all();
    };

    // Hedge delay: give peer a head start, but wake up early if peer finishes.
    // Uses cv.wait_for() instead of bthread_usleep() so the calling thread is
    // unblocked as soon as the peer bthread signals completion, avoiding the
    // unconditional 20ms sleep that dominated latency on cache-miss-heavy queries.
    bool peer_already_won = false;
    if (config::peer_race_hedge_delay_ms > 0) {
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        if (!race->peer_done) {
            race->cv.wait_for(lk, static_cast<long>(config::peer_race_hedge_delay_ms) * 1000);
        }
        peer_already_won = (race->winner == 0);
        if (peer_already_won) {
            race->s3_done = true;
            race->s3_status = Status::InternalError<false>("skipped: peer won during hedge delay");
        }
    }

    if (!peer_already_won) {
        auto s3_fn = [do_s3_read, parent_resource_ctx]() mutable {
            std::unique_ptr<AttachTask> attach_task;
            if (parent_resource_ctx != nullptr) {
                attach_task = std::make_unique<AttachTask>(parent_resource_ctx);
            }
            do_s3_read();
        };
        auto* s3_pool = ExecEnv::GetInstance()->peer_race_s3_thread_pool();
        if (s3_pool == nullptr || !s3_pool->submit_func(s3_fn).ok()) {
            do_s3_read();
        }
    }
}

// Wait for the race to finish and populate the output accordingly.
Status collect_race_result(std::shared_ptr<RaceState> race, size_t span_size,
                           std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result,
                           ReadStatistics& stats, const IOContext* io_ctx) {
    {
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        while (race->winner < 0 && !(race->peer_done && race->s3_done)) {
            race->cv.wait(lk);
        }
    }
    g_active_peer_races.fetch_sub(1, std::memory_order_relaxed);

    const std::string self_cg_id =
            static_cast<CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info())
                    ->cloud_compute_group_id();

    if (race->winner == 0) {
        // Peer won.
        if (peer_result != nullptr) {
            *peer_result = std::move(race->peer_res);
        }
        stats.from_peer_cache = true;
        stats.peer_read_timer += race->peer_elapsed_ns;
        g_peer_race_peer_win << 1;
        const bool is_cross_cg =
                !race->peer_winner_cg_id.empty() && race->peer_winner_cg_id != self_cg_id;
        if (is_cross_cg) {
            g_peer_cross_compute_group_read << 1;
        } else {
            g_peer_same_compute_group_read << 1;
        }
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->num_peer_race_peer_win++;
            io_ctx->file_cache_stats->peer_hosts.insert(race->peer_winner_host);
            if (is_cross_cg) {
                io_ctx->file_cache_stats->num_cross_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_cross_cg_peer += span_size;
                io_ctx->file_cache_stats->cross_cg_peer_io_timer += race->peer_winner_io_ns;
            } else {
                io_ctx->file_cache_stats->num_same_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_same_cg_peer += span_size;
                io_ctx->file_cache_stats->same_cg_peer_io_timer += race->peer_winner_io_ns;
            }
        }
        return Status::OK();
    } else if (race->winner == 1) {
        // S3 won.
        buffer = std::move(race->s3_buf);
        stats.from_peer_cache = false;
        g_peer_race_s3_win << 1;
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->num_peer_race_s3_win++;
        }
        return Status::OK();
    }
    g_peer_race_both_fail << 1;
    return Status::InternalError<false>("peer race: both peer and s3 failed");
}

} // anonymous namespace

Status CachedRemoteFileReader::_execute_s3_fallback(size_t empty_start, size_t span_size,
                                                    std::unique_ptr<char[]>& buffer,
                                                    PeerFetchResult* peer_result,
                                                    ReadStatistics& stats,
                                                    const IOContext* io_ctx) {
    if (peer_result != nullptr) {
        peer_result->clear();
    }
    buffer.reset(new char[span_size]);
    size_t read_size = span_size;
    return execute_s3_read(empty_start, read_size, buffer, stats, io_ctx, _remote_file_reader);
}

Status CachedRemoteFileReader::_execute_sequential_peer_read(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start, size_t span_size,
        std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result, ReadStatistics& stats,
        const IOContext* io_ctx, const std::vector<doris::PeerCandidate>& candidates,
        int64_t tablet_id) {
    // candidates[0] already reflects last_successful_compute_group_id affinity:
    // get_peer_candidates() applies stable_partition before returning.
    if (candidates.empty()) {
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    auto& manager = get_warm_up_manager();
    PeerFetchResult serial_res;
    const int64_t timer_before = stats.peer_read_timer;
    auto st = execute_peer_read(empty_blocks, &serial_res, path().native(), this->size(),
                                _is_doris_table, stats, io_ctx, candidates[0].host,
                                candidates[0].brpc_port);
    if (st.ok()) {
        manager.update_peer_candidate_on_success(tablet_id, candidates[0].compute_group_id);
        if (peer_result != nullptr) {
            *peer_result = std::move(serial_res);
        }
        // Update profile counters for cross/same CG stats.
        const std::string self_cg_id =
                static_cast<CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info())
                        ->cloud_compute_group_id();
        const bool is_cross_cg = !candidates[0].compute_group_id.empty() &&
                                 candidates[0].compute_group_id != self_cg_id;
        if (is_cross_cg) {
            g_peer_cross_compute_group_read << 1;
        } else {
            g_peer_same_compute_group_read << 1;
        }
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->peer_hosts.insert(candidates[0].host);
            if (is_cross_cg) {
                io_ctx->file_cache_stats->num_cross_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_cross_cg_peer += span_size;
                io_ctx->file_cache_stats->cross_cg_peer_io_timer +=
                        stats.peer_read_timer - timer_before;
            } else {
                io_ctx->file_cache_stats->num_same_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_same_cg_peer += span_size;
                io_ctx->file_cache_stats->same_cg_peer_io_timer +=
                        stats.peer_read_timer - timer_before;
            }
        }
        return st;
    }
    // Track failure so affinity / eviction logic stays consistent with the race path.
    if (st.is<ErrorCode::TOO_MANY_TASKS>()) {
        // Server healthy but overloaded — don't penalize candidate.
    } else if (st.is<ErrorCode::NOT_FOUND>()) {
        manager.rotate_peer_candidate_on_cache_miss(tablet_id, candidates[0].host,
                                                    candidates[0].brpc_port);
    } else {
        manager.update_peer_candidate_on_rpc_failure(tablet_id, candidates[0].host,
                                                     candidates[0].brpc_port);
    }
    return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
}

Status CachedRemoteFileReader::_execute_remote_read(const std::vector<FileBlockSPtr>& empty_blocks,
                                                    size_t empty_start, size_t span_size,
                                                    std::unique_ptr<char[]>& buffer,
                                                    PeerFetchResult* peer_result,
                                                    ReadStatistics& stats,
                                                    const IOContext* io_ctx) {
    // --- Non-peer path: direct S3 ---
    if (!_should_read_from_peer(io_ctx)) {
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    // --- UT debug point: injected peer address ---
    DBUG_EXECUTE_IF("PeerFileCacheReader::_fetch_from_peer_cache_blocks", {
        std::string dp_host = dp->param<std::string>("host", "127.0.0.1");
        int32_t dp_port = dp->param("port", 9060);
        buffer.reset();
        DCHECK(peer_result != nullptr);
        peer_result->clear();
        auto st = execute_peer_read(empty_blocks, peer_result, path().native(), this->size(),
                                    _is_doris_table, stats, io_ctx, dp_host, dp_port);
        if (st.ok()) return st;
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    });

    // --- Resolve tablet and obtain peer candidates ---
    int64_t tablet_id = _tablet_id;
    auto& manager = get_warm_up_manager();
    auto candidates = manager.get_peer_candidates(tablet_id);
    if (candidates.empty()) {
        if (!manager.is_peer_cooldown(tablet_id)) {
            // Cold miss: trigger background FE fetch and fall back to S3.
            g_peer_lazy_fetch_triggered << 1;
            auto manager_ptr =
                    ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager_ptr();
            start_bthread([manager_ptr = std::move(manager_ptr), tablet_id]() {
                manager_ptr->fetch_candidates_from_fe(tablet_id);
            });
        }
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    // --- Dispatch: concurrent race or sequential fallback ---
    // Candidates are already sorted by last_successful_compute_group_id affinity
    // (stable_partition in get_peer_candidates), so the winner race peer bthread
    // naturally tries the most promising candidate first — whether same-CG or cross-CG.
    if (config::enable_peer_s3_race) {
        return _execute_winner_race(empty_blocks, empty_start, span_size, buffer, peer_result,
                                    stats, io_ctx, candidates, tablet_id);
    }
    return _execute_sequential_peer_read(empty_blocks, empty_start, span_size, buffer, peer_result,
                                         stats, io_ctx, candidates, tablet_id);
}

Status CachedRemoteFileReader::_execute_winner_race(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start, size_t span_size,
        std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result, ReadStatistics& stats,
        const IOContext* io_ctx, const std::vector<doris::PeerCandidate>& candidates,
        int64_t tablet_id) {
    // Reserve a race slot; degrade to sequential if at limit.
    if (g_active_peer_races.fetch_add(1, std::memory_order_relaxed) >=
        config::max_concurrent_peer_races) {
        g_active_peer_races.fetch_sub(1, std::memory_order_relaxed);
        return _execute_sequential_peer_read(empty_blocks, empty_start, span_size, buffer,
                                             peer_result, stats, io_ctx, candidates, tablet_id);
    }

    auto race = std::make_shared<RaceState>();
    auto manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager_ptr();

    // Capture context for child threads.
    const std::string file_path = path().native();
    const size_t file_sz = this->size();
    const bool is_doris = _is_doris_table;
    auto remote_reader = _remote_file_reader;
    std::shared_ptr<ResourceContext> parent_resource_ctx;
    auto* parent_thread_context = thread_context();
    if (parent_thread_context != nullptr && parent_thread_context->is_attach_task()) {
        parent_resource_ctx = parent_thread_context->resource_ctx();
    }

    // Launch peer bthread.
    start_bthread(
            [race, empty_blocks = std::move(empty_blocks), file_path, file_sz, is_doris,
             manager = std::move(manager), candidates = std::move(candidates), tablet_id,
             resource_id = _storage_resource_id, parent_resource_ctx]() mutable {
                run_peer_race(race, std::move(empty_blocks), file_path, file_sz, is_doris,
                              std::move(manager), std::move(candidates), tablet_id,
                              std::move(resource_id), parent_resource_ctx);
            },
            /*init_thread_ctx=*/true);

    // Launch S3 (with optional hedge delay).
    // Pass shared_from_this() so the background S3 task holds a reference to this
    // reader, preventing destruction (and close()) until the S3 task completes.
    launch_s3_race(race, empty_start, span_size, io_ctx, remote_reader, parent_resource_ctx,
                   shared_from_this());

    // Collect race result.
    return collect_race_result(race, span_size, buffer, peer_result, stats, io_ctx);
}

bool CachedRemoteFileReader::_try_read_from_cached_files_directly(
        size_t offset, Slice result, size_t bytes_req, bool is_dryrun, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t& already_read, size_t* bytes_read) {
    if (!_can_read_cache_file_directly()) {
        return false;
    }

    SCOPED_RAW_TIMER(&stats.read_cache_file_directly_timer);
    size_t need_read_size = bytes_req;
    std::shared_lock lock(_mtx);
    if (_cache_file_readers.empty()) {
        return false;
    }

    auto iter = _cache_file_readers.upper_bound(offset);
    if (iter != _cache_file_readers.begin()) {
        --iter;
    }

    size_t current_offset = offset;
    while (need_read_size != 0 && iter != _cache_file_readers.end()) {
        if (iter->second->offset() > current_offset ||
            iter->second->range().right < current_offset) {
            break;
        }

        size_t file_offset = current_offset - iter->second->offset();
        size_t reserve_bytes = std::min(need_read_size, iter->second->range().size() - file_offset);
        if (is_dryrun) [[unlikely]] {
            g_skip_local_cache_io_sum_bytes << reserve_bytes;
        } else {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            if (!iter->second
                         ->read(Slice(result.data + (current_offset - offset), reserve_bytes),
                                file_offset)
                         .ok()) { // TODO: maybe read failed because block evict, should handle error
                break;
            }
            source_read_breakdown.local_bytes += reserve_bytes;
        }

        _cache->add_need_update_lru_block(iter->second);
        need_read_size -= reserve_bytes;
        current_offset += reserve_bytes;
        already_read += reserve_bytes;
        ++iter;
    }

    if (need_read_size == 0) {
        *bytes_read = bytes_req;
        stats.hit_cache = true;
        g_read_cache_direct_whole_num << 1;
        g_read_cache_direct_whole_bytes << bytes_req;
        return true;
    }

    g_read_cache_direct_partial_num << 1;
    g_read_cache_direct_partial_bytes << already_read;
    return false;
}

std::vector<FileBlockSPtr> CachedRemoteFileReader::_collect_remote_read_blocks(
        const FileBlocksHolder& holder, ReadStatistics& stats) {
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
    return empty_blocks;
}

Status CachedRemoteFileReader::_read_remote_blocks_into_cache(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t offset, size_t bytes_req,
        size_t already_read, Slice result, bool is_dryrun, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, const IOContext* io_ctx,
        size_t& indirect_read_bytes, size_t& empty_start, size_t& empty_end,
        PeerFetchedBlockSet& peer_fetched_blocks) {
    empty_start = 0;
    empty_end = 0;
    peer_fetched_blocks.clear();
    if (empty_blocks.empty()) {
        return Status::OK();
    }

    empty_start = empty_blocks.front()->range().left;
    empty_end = empty_blocks.back()->range().right;
    const size_t span_read_size = empty_end - empty_start + 1;
    const auto peer_fetch_layout = build_peer_fetch_layout(empty_blocks, size());
    std::unique_ptr<char[]> buffer;
    PeerFetchResult peer_result;

    RETURN_IF_ERROR(_execute_remote_read(empty_blocks, empty_start, span_read_size, buffer,
                                         &peer_result, stats, io_ctx));

    std::vector<std::vector<const PeerFetchChunk*>> peer_chunks_by_block;
    if (stats.from_peer_cache) {
        // Peer returns sparse payloads; remember the exact sparse blocks that were filled.
        peer_fetched_blocks.reserve(empty_blocks.size());
        for (const auto& block : empty_blocks) {
            peer_fetched_blocks.insert(block.get());
        }
        peer_chunks_by_block.resize(empty_blocks.size());
        for (const auto& chunk : peer_result.chunks) {
            DCHECK_LT(chunk.block_index, empty_blocks.size());
            peer_chunks_by_block[chunk.block_index].push_back(&chunk);
        }
    }

    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().cached_remote_reader_write_back);
    for (size_t idx = 0; idx < empty_blocks.size(); ++idx) {
        auto& block = empty_blocks[idx];
        if (block->state() == FileBlock::State::SKIP_CACHE) {
            continue;
        }

        SCOPED_RAW_TIMER(&stats.local_write_timer);
        size_t block_size = block->range().size();
        Status st;
        if (stats.from_peer_cache) {
            block_size = peer_fetch_layout.block_sizes[idx];
            if (block_size == 0) {
                continue;
            }
            st = write_peer_payloads_into_block(block, peer_chunks_by_block[idx], &block_size);
        } else {
            char* current_ptr = buffer.get() + block->range().left - empty_start;
            st = block->append(Slice(current_ptr, block_size));
        }
        if (st.ok()) {
            st = block->finalize();
        }
        if (!st.ok()) {
            LOG(WARNING) << "write data to file cache failed, source="
                         << (stats.from_peer_cache ? "peer" : "remote")
                         << ", path=" << path().native() << ", tablet_id=" << _tablet_id
                         << ", file_size=" << size() << ", cache_hash=" << _cache_hash.to_string()
                         << ", write_block_size=" << block_size
                         << ", block=" << block->get_info_for_log()
                         << ", cache_file=" << block->get_cache_file() << ", err=" << st;
        } else {
            _insert_file_reader(block);
            stats.bytes_write_into_file_cache += block_size;
        }
    }

    const size_t right_offset = offset + bytes_req - 1;
    if (stats.from_peer_cache) {
        if (is_dryrun) {
            return Status::OK();
        }
        for (const auto& chunk : peer_result.chunks) {
            copy_peer_chunk_to_result(chunk, offset, right_offset, already_read, result,
                                      indirect_read_bytes, source_read_breakdown);
        }
        return Status::OK();
    }

    if (empty_start <= right_offset && empty_end >= offset + already_read && !is_dryrun) {
        size_t copy_left_offset = std::max(offset + already_read, empty_start);
        size_t copy_right_offset = std::min(right_offset, empty_end);
        char* dst = result.data + (copy_left_offset - offset);
        char* src = buffer.get() + (copy_left_offset - empty_start);
        size_t copy_size = copy_right_offset - copy_left_offset + 1;
        memcpy(dst, src, copy_size);
        indirect_read_bytes += copy_size;
        source_read_breakdown.remote_bytes += copy_size;
    }
    return Status::OK();
}

Status CachedRemoteFileReader::_read_remaining_blocks_from_cache(
        const FileBlocksHolder& holder, size_t offset, size_t bytes_req, Slice result,
        bool is_dryrun, size_t empty_start, size_t empty_end,
        const PeerFetchedBlockSet& peer_fetched_blocks, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t& indirect_read_bytes, size_t* bytes_read,
        const IOContext* io_ctx) {
    size_t current_offset = offset + *bytes_read;
    size_t end_offset = offset + bytes_req - 1;
    bool need_self_heal = false;
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
        if (!peer_fetched_blocks.empty() && contains_file_block(peer_fetched_blocks, block)) {
            // For sparse peer reads, skip only blocks fetched from peer. Other blocks inside the
            // enclosing span may still come from local cache.
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }
        if (peer_fetched_blocks.empty() && empty_start <= left && right <= empty_end) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }

        FileBlock::State block_state = block->state();
        int64_t wait_time = 0;
        static int64_t max_wait_time = 10;
        TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::max_wait_time", &max_wait_time);
        if (block_state != FileBlock::State::DOWNLOADED) {
            SCOPED_CONCURRENCY_COUNT(
                    ConcurrencyStatsManager::instance().cached_remote_reader_blocking);
            do {
                SCOPED_RAW_TIMER(&stats.remote_wait_timer);
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
                indirect_read_bytes += read_size;
                if (st.ok()) {
                    source_read_breakdown.local_bytes += read_size;
                }
            }
            if (block_state == FileBlock::State::DOWNLOADED && st.is<ErrorCode::NOT_FOUND>()) {
                need_self_heal = true;
                g_read_cache_self_heal_on_not_found << 1;
                LOG_EVERY_N(WARNING, 100)
                        << "Cache block file is missing, will self-heal by clearing cache hash. "
                        << "path=" << path().native() << ", hash=" << _cache_hash.to_string()
                        << ", offset=" << left << ", err=" << st.msg();
            }
        }
        if (!st || block_state != FileBlock::State::DOWNLOADED) {
            if (is_dryrun) [[unlikely]] {
                *bytes_read += read_size;
                current_offset = right + 1;
                continue;
            }
            LOG(WARNING) << "Read data failed from file cache downloaded by others. err="
                         << st.msg() << ", block state=" << block_state;
            size_t remote_bytes_read {0};
            stats.hit_cache = false;
            stats.from_peer_cache = false;
            s3_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(
                    current_offset, Slice(result.data + (current_offset - offset), read_size),
                    &remote_bytes_read, io_ctx));
            indirect_read_bytes += read_size;
            source_read_breakdown.remote_bytes += remote_bytes_read;
            DCHECK(remote_bytes_read == read_size);
        }

        *bytes_read += read_size;
        current_offset = right + 1;
    }
    if (need_self_heal && _cache != nullptr) {
        _cache->remove_if_cached_async(_cache_hash);
    }
    return Status::OK();
}

Status CachedRemoteFileReader::_read_from_indirect_cache(size_t offset, Slice result,
                                                         size_t bytes_req, size_t already_read,
                                                         bool is_dryrun, size_t* bytes_read,
                                                         ReadStatistics& stats,
                                                         SourceReadBreakdown& source_read_breakdown,
                                                         const IOContext* io_ctx) {
    g_read_cache_indirect_num << 1;
    size_t indirect_read_bytes = 0;
    auto [align_left, align_size] =
            s_align_size(offset + already_read, bytes_req - already_read, size());
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    MonotonicStopWatch sw;
    sw.start();
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->increment();
    FileBlocksHolder holder =
            _cache->get_or_set(_cache_hash, align_left, align_size, cache_context);
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->decrement();
    stats.cache_get_or_set_timer += sw.elapsed_time();

    auto empty_blocks = _collect_remote_read_blocks(holder, stats);
    size_t empty_start = 0;
    size_t empty_end = 0;
    PeerFetchedBlockSet peer_fetched_blocks;
    RETURN_IF_ERROR(_read_remote_blocks_into_cache(empty_blocks, offset, bytes_req, already_read,
                                                   result, is_dryrun, stats, source_read_breakdown,
                                                   io_ctx, indirect_read_bytes, empty_start,
                                                   empty_end, peer_fetched_blocks));
    *bytes_read = already_read;
    RETURN_IF_ERROR(_read_remaining_blocks_from_cache(holder, offset, bytes_req, result, is_dryrun,
                                                      empty_start, empty_end, peer_fetched_blocks,
                                                      stats, source_read_breakdown,
                                                      indirect_read_bytes, bytes_read, io_ctx));
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << *bytes_read;
    DCHECK(*bytes_read == bytes_req);
    return Status::OK();
}

Status CachedRemoteFileReader::_read_remote_only_on_cache_miss(
        size_t offset, Slice result, size_t bytes_req, bool is_dryrun, size_t* bytes_read,
        ReadStatistics& stats, SourceReadBreakdown& source_read_breakdown,
        const IOContext* io_ctx) {
    auto read_remote = [&]() -> Status {
        stats.hit_cache = false;
        stats.from_peer_cache = false;
        stats.skip_cache = true;
        s3_read_counter << 1;
        if (is_dryrun) [[unlikely]] {
            *bytes_read = bytes_req;
            g_read_cache_indirect_bytes << 0;
            g_read_cache_indirect_total_bytes << bytes_req;
            return Status::OK();
        }

        size_t remote_bytes_read = bytes_req;
        SCOPED_RAW_TIMER(&stats.remote_read_timer);
        RETURN_IF_ERROR(_remote_file_reader->read_at(offset, Slice(result.data, bytes_req),
                                                     &remote_bytes_read, io_ctx));
        *bytes_read = remote_bytes_read;
        DCHECK_EQ(*bytes_read, bytes_req);
        source_read_breakdown.remote_bytes += remote_bytes_read;
        g_read_cache_indirect_bytes << remote_bytes_read;
        g_read_cache_indirect_total_bytes << remote_bytes_read;
        return Status::OK();
    };

    g_read_cache_indirect_num << 1;
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    FileBlocks file_blocks;
    bool fully_covered = false;
    {
        SCOPED_RAW_TIMER(&stats.get_timer);
        RETURN_IF_ERROR(_cache->get_downloaded_blocks_if_fully_covered(
                _cache_hash, offset, bytes_req, cache_context, &file_blocks, &fully_covered));
    }
    if (!fully_covered) {
        return read_remote();
    }

    size_t local_read_bytes = 0;
    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    for (auto& block : file_blocks) {
        if (current_offset > end_offset) {
            break;
        }
        const auto& block_range = block->range();
        if (block_range.right < current_offset) {
            continue;
        }

        size_t read_left = std::max(current_offset, block_range.left);
        size_t read_right = std::min(end_offset, block_range.right);
        size_t read_size = read_right - read_left + 1;
        if (is_dryrun) [[unlikely]] {
            g_skip_local_cache_io_sum_bytes << read_size;
        } else {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            Status st = block->read(Slice(result.data + (read_left - offset), read_size),
                                    read_left - block_range.left);
            if (!st.ok()) {
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    _cache->remove_if_cached_async(_cache_hash);
                }
                LOG_EVERY_N(WARNING, 100)
                        << "Read data failed from file cache in remote-only-on-miss path. "
                        << "Fallback to remote. err=" << st.msg()
                        << ", block state=" << block->state();
                return read_remote();
            }
            source_read_breakdown.local_bytes += read_size;
            local_read_bytes += read_size;
        }
        current_offset = read_right + 1;
    }

    *bytes_read = bytes_req;
    stats.hit_cache = true;
    g_read_cache_indirect_bytes << local_read_bytes;
    g_read_cache_indirect_total_bytes << bytes_req;
    return Status::OK();
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().cached_remote_reader_read_at);
    IOContext default_io_ctx;
    if (io_ctx == nullptr) {
        io_ctx = &default_io_ctx;
    }
    DCHECK(io_ctx);
    DCHECK(!closed());

    const bool is_dryrun = io_ctx->is_dryrun;
    if (offset > size()) {
        return Status::InvalidArgument(
                fmt::format("offset exceeds file size(offset: {}, file size: {}, path: {})", offset,
                            size(), path().native()));
    }

    size_t bytes_req = std::min(result.size, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    ReadStatistics stats;
    SourceReadBreakdown source_read_breakdown;
    Status read_st = Status::OK();
    MonotonicStopWatch read_at_sw;
    read_at_sw.start();
    stats.bytes_read += bytes_req;
    Defer defer {[&]() {
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
        if (read_st.ok() && !is_dryrun) {
            // Only successful reads contribute to query profile and file-cache metrics.
            const auto file_cache_read_type =
                    io_ctx->is_inverted_index
                            ? FileCacheReadType::INVERTED_INDEX
                            : (io_ctx->is_index_data ? FileCacheReadType::SEGMENT_FOOTER_INDEX
                                                     : FileCacheReadType::DATA);
            if (io_ctx->file_cache_stats) {
                _update_stats(stats, source_read_breakdown, io_ctx->file_cache_stats,
                              file_cache_read_type);
                auto* limiter = io_ctx->remote_scan_cache_write_limiter;
                if (limiter != nullptr) {
                    io_ctx->file_cache_stats->remote_only_on_miss_triggered =
                            io_ctx->file_cache_stats->remote_only_on_miss_triggered ||
                            limiter->remote_only_on_miss();
                    io_ctx->file_cache_stats->remote_only_on_miss_threshold_bytes =
                            limiter->threshold_bytes();
                }
            }
            if (!io_ctx->is_warmup) {
                FileCacheStatistics fcache_stats_increment;
                _update_stats(stats, source_read_breakdown, &fcache_stats_increment,
                              file_cache_read_type);
                io::FileCacheMetrics::instance().update(&fcache_stats_increment);
            }
        }
    }};

    if (use_remote_only_on_cache_miss(io_ctx)) {
        read_st = _read_remote_only_on_cache_miss(offset, result, bytes_req, is_dryrun, bytes_read,
                                                  stats, source_read_breakdown, io_ctx);
        return read_st;
    }

    size_t already_read = 0;
    if (_try_read_from_cached_files_directly(offset, result, bytes_req, is_dryrun, stats,
                                             source_read_breakdown, already_read, bytes_read)) {
        return Status::OK();
    }

    read_st = _read_from_indirect_cache(offset, result, bytes_req, already_read, is_dryrun,
                                        bytes_read, stats, source_read_breakdown, io_ctx);
    return read_st;
}

void CachedRemoteFileReader::prefetch_range(size_t offset, size_t size, const IOContext* io_ctx) {
    if (offset >= this->size() || size == 0) {
        return;
    }

    size = std::min(size, this->size() - offset);

    ThreadPool* pool = ExecEnv::GetInstance()->segment_prefetch_thread_pool();
    if (pool == nullptr) {
        return;
    }

    IOContext dryrun_ctx;
    if (io_ctx != nullptr) {
        dryrun_ctx = *io_ctx;
    }
    dryrun_ctx.is_dryrun = true;
    dryrun_ctx.query_id = nullptr;
    dryrun_ctx.file_cache_stats = nullptr;
    dryrun_ctx.file_reader_stats = nullptr;

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log)
            << fmt::format("[verbose] Submitting prefetch task for offset={} size={}, file={}",
                           offset, size, path().filename().native());
    std::weak_ptr<CachedRemoteFileReader> weak_this = shared_from_this();
    auto st = pool->submit_func([weak_this, offset, size, dryrun_ctx]() {
        auto self = weak_this.lock();
        if (self == nullptr) {
            return;
        }
        size_t bytes_read = 0;
        Slice dummy_buffer((char*)nullptr, size);
        (void)self->read_at_impl(offset, dummy_buffer, &bytes_read, &dryrun_ctx);
        LOG_IF(INFO, config::enable_segment_prefetch_verbose_log)
                << fmt::format("[verbose] Prefetch task completed for offset={} size={}, file={}",
                               offset, size, self->path().filename().native());
    });

    if (!st.ok()) {
        VLOG_DEBUG << "Failed to submit prefetch task for offset=" << offset << " size=" << size
                   << " error=" << st.to_string();
    }
}

void CachedRemoteFileReader::_update_stats(const ReadStatistics& read_stats,
                                           const SourceReadBreakdown& source_read_breakdown,
                                           FileCacheStatistics* statis,
                                           FileCacheReadType read_type) const {
    if (statis == nullptr) {
        return;
    }
    const bool has_source_bytes = source_read_breakdown.local_bytes != 0 ||
                                  source_read_breakdown.remote_bytes != 0 ||
                                  source_read_breakdown.peer_bytes != 0;
    if (has_source_bytes) {
        if (source_read_breakdown.local_bytes != 0) {
            statis->num_local_io_total++;
            statis->bytes_read_from_local += source_read_breakdown.local_bytes;
        }
        if (source_read_breakdown.peer_bytes != 0 || read_stats.from_peer_cache) {
            // Count peer IO whenever peer was used, even if its fetched blocks were entirely
            // outside the copy range (e.g., backward-aligned prefetch block before
            // offset+already_read).  In that case peer_bytes==0 but the peer RPC did happen
            // and wrote data into the local file cache.
            statis->num_peer_io_total++;
            statis->bytes_read_from_peer += source_read_breakdown.peer_bytes;
            statis->peer_io_timer += read_stats.peer_read_timer;
        }
        if (source_read_breakdown.remote_bytes != 0) {
            statis->num_remote_io_total++;
            statis->bytes_read_from_remote += source_read_breakdown.remote_bytes;
            statis->remote_io_timer += read_stats.remote_read_timer;
        }
    } else if (read_stats.hit_cache) {
        statis->num_local_io_total++;
        statis->bytes_read_from_local += read_stats.bytes_read;
    } else if (read_stats.from_peer_cache) {
        statis->num_peer_io_total++;
        statis->bytes_read_from_peer += read_stats.bytes_read;
        statis->peer_io_timer += read_stats.peer_read_timer;
    } else {
        statis->num_remote_io_total++;
        statis->bytes_read_from_remote += read_stats.bytes_read;
        statis->remote_io_timer += read_stats.remote_read_timer;
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

    auto update_index_stats = [&](int64_t& num_local_io_total, int64_t& num_remote_io_total,
                                  int64_t& num_peer_io_total, int64_t& bytes_read_from_local,
                                  int64_t& bytes_read_from_remote, int64_t& bytes_read_from_peer,
                                  int64_t& local_io_timer, int64_t& remote_io_timer,
                                  int64_t& peer_io_timer) {
        if (has_source_bytes) {
            if (source_read_breakdown.local_bytes != 0) {
                num_local_io_total++;
                bytes_read_from_local += source_read_breakdown.local_bytes;
            }
            if (source_read_breakdown.peer_bytes != 0 || read_stats.from_peer_cache) {
                num_peer_io_total++;
                bytes_read_from_peer += source_read_breakdown.peer_bytes;
                peer_io_timer += read_stats.peer_read_timer;
            }
            if (source_read_breakdown.remote_bytes != 0) {
                num_remote_io_total++;
                bytes_read_from_remote += source_read_breakdown.remote_bytes;
                remote_io_timer += read_stats.remote_read_timer;
            }
        } else if (read_stats.hit_cache) {
            num_local_io_total++;
            bytes_read_from_local += read_stats.bytes_read;
        } else if (read_stats.from_peer_cache) {
            num_peer_io_total++;
            bytes_read_from_peer += read_stats.bytes_read;
            peer_io_timer += read_stats.peer_read_timer;
        } else {
            num_remote_io_total++;
            bytes_read_from_remote += read_stats.bytes_read;
            remote_io_timer += read_stats.remote_read_timer;
        }
        local_io_timer += read_stats.local_read_timer;
    };

    switch (read_type) {
    case FileCacheReadType::DATA:
        break;
    case FileCacheReadType::INVERTED_INDEX:
        update_index_stats(
                statis->inverted_index_num_local_io_total,
                statis->inverted_index_num_remote_io_total,
                statis->inverted_index_num_peer_io_total,
                statis->inverted_index_bytes_read_from_local,
                statis->inverted_index_bytes_read_from_remote,
                statis->inverted_index_bytes_read_from_peer, statis->inverted_index_local_io_timer,
                statis->inverted_index_remote_io_timer, statis->inverted_index_peer_io_timer);
        break;
    case FileCacheReadType::SEGMENT_FOOTER_INDEX:
        update_index_stats(statis->segment_footer_index_num_local_io_total,
                           statis->segment_footer_index_num_remote_io_total,
                           statis->segment_footer_index_num_peer_io_total,
                           statis->segment_footer_index_bytes_read_from_local,
                           statis->segment_footer_index_bytes_read_from_remote,
                           statis->segment_footer_index_bytes_read_from_peer,
                           statis->segment_footer_index_local_io_timer,
                           statis->segment_footer_index_remote_io_timer,
                           statis->segment_footer_index_peer_io_timer);
        break;
    }

    g_skip_cache_sum << read_stats.skip_cache;
}

} // namespace doris::io
