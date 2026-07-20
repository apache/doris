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

#include "cloud/cloud_internal_service.h"

#include <brpc/controller.h>
#include <bthread/countdown_event.h>
#include <butil/iobuf.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/cloud_warmup_metrics.h"
#include "cloud/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/path.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "storage/storage_policy.h"
#include "util/async_io.h"
#include "util/bvar_windowed_adder.h"
#include "util/debug_points.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

bvar::Adder<uint64_t> g_file_cache_get_by_peer_num("file_cache_get_by_peer_num");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_blocks_num("file_cache_get_by_peer_blocks_num");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_success_num("file_cache_get_by_peer_success_num");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_failed_num("file_cache_get_by_peer_failed_num");
bvar::LatencyRecorder g_file_cache_get_by_peer_server_latency(
        "file_cache_get_by_peer_server_latency");
bvar::LatencyRecorder g_file_cache_get_by_peer_read_cache_file_latency(
        "file_cache_get_by_peer_read_cache_file_latency");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_offer_failed_num(
        "file_cache_get_by_peer_offer_failed_num");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_queue_timeout_num(
        "file_cache_get_by_peer_queue_timeout_num");
bvar::LatencyRecorder g_file_cache_get_by_peer_queue_wait_latency(
        "file_cache_get_by_peer_queue_wait_latency");
bvar::LatencyRecorder g_file_cache_get_by_peer_handle_cache_block_req_latency(
        "file_cache_get_by_peer_handle_cache_block_req_latency");
bvar::LatencyRecorder g_file_cache_get_by_peer_get_cache_latency(
        "file_cache_get_by_peer_get_cache_latency");
bvar::LatencyRecorder g_file_cache_get_by_peer_get_or_set_latency(
        "file_cache_get_by_peer_get_or_set_latency");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_get_or_set_calls(
        "file_cache_get_by_peer_get_or_set_calls");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_get_or_set_blocks_total(
        "file_cache_get_by_peer_get_or_set_blocks_total");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_request_blocks_total(
        "file_cache_get_by_peer_request_blocks_total");
bvar::LatencyRecorder g_file_cache_get_by_peer_request_blocks_per_rpc(
        "file_cache_get_by_peer_request_blocks_per_rpc");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_response_blocks_total(
        "file_cache_get_by_peer_response_blocks_total");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_response_bytes_total(
        "file_cache_get_by_peer_response_bytes_total");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_not_downloaded_block_num(
        "file_cache_get_by_peer_not_downloaded_block_num");
bvar::LatencyRecorder g_file_cache_get_by_peer_read_file_block_total_latency(
        "file_cache_get_by_peer_read_file_block_total_latency");
bvar::LatencyRecorder g_file_cache_get_by_peer_set_response_data_latency(
        "file_cache_get_by_peer_set_response_data_latency");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_attachment_response_num(
        "file_cache_get_by_peer_attachment_response_num");
bvar::Adder<uint64_t> g_file_cache_get_by_peer_pb_response_num(
        "file_cache_get_by_peer_pb_response_num");

bvar::Adder<int64_t> g_peer_server_fill_requested("peer_server_fill_requested");
bvar::Adder<int64_t> g_peer_server_fill_success("peer_server_fill_success");
bvar::Adder<int64_t> g_peer_server_fill_timeout("peer_server_fill_timeout");
bvar::Adder<int64_t> g_peer_server_fill_rejected("peer_server_fill_rejected");
bvar::LatencyRecorder g_peer_server_fill_latency("peer_server_fill_latency");
bvar::LatencyRecorder g_cloud_internal_service_get_file_cache_meta_by_tablet_id_latency(
        "cloud_internal_service_get_file_cache_meta_by_tablet_id_latency");

// Concurrency guard for server-side S3 pull-through fills.
static std::atomic<int32_t> g_active_server_fills {0};
bvar::PassiveStatus<int32_t> g_peer_active_fills(
        "peer_active_fills",
        [](void*) { return g_active_server_fills.load(std::memory_order_relaxed); }, nullptr);

CloudInternalServiceImpl::CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

CloudInternalServiceImpl::~CloudInternalServiceImpl() = default;

void CloudInternalServiceImpl::alter_vault_sync(google::protobuf::RpcController* controller,
                                                const doris::PAlterVaultSyncRequest* request,
                                                PAlterVaultSyncResponse* response,
                                                google::protobuf::Closure* done) {
    LOG(INFO) << "alter be to sync vault info from Meta Service";
    // If the vaults containing hdfs vault then it would try to create hdfs connection using jni
    // which would acuiqre one thread local jniEnv. But bthread context can't guarantee that the brpc
    // worker thread wouldn't do bthread switch between worker threads.
    bool ret = _heavy_work_pool.try_offer([this, done]() {
        brpc::ClosureGuard closure_guard(done);
        _engine.sync_storage_vault();
    });
    if (!ret) {
        brpc::ClosureGuard closure_guard(done);
        LOG(WARNING) << "fail to offer alter_vault_sync request to the work pool, pool="
                     << _heavy_work_pool.get_info();
    }
}

FileCacheType cache_type_to_pb(io::FileCacheType type) {
    switch (type) {
    case io::FileCacheType::TTL:
        return FileCacheType::TTL;
    case io::FileCacheType::INDEX:
        return FileCacheType::INDEX;
    case io::FileCacheType::NORMAL:
        return FileCacheType::NORMAL;
    default:
        DCHECK(false);
    }
    return FileCacheType::NORMAL;
}

static int64_t current_unix_time_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

static std::optional<int64_t> warm_up_rowset_cross_host_latency_us(int64_t start_unix_ts_us,
                                                                   int64_t end_unix_ts_us) {
    // The start timestamp is generated by the caller BE. Mixed-version callers may omit it, and
    // system clocks across BEs are not guaranteed to be ordered.
    if (start_unix_ts_us <= 0 || end_unix_ts_us < start_unix_ts_us) {
        return std::nullopt;
    }
    return end_unix_ts_us - start_unix_ts_us;
}

static void add_file_cache_block_meta_to_response(
        PGetFileCacheMetaResponse* resp, int64_t tablet_id, const std::string& rowset_id,
        int32_t segment_id, const std::string& file_name,
        const std::tuple<int64_t, int64_t, io::FileCacheType, int64_t>& tuple,
        const RowsetSharedPtr& rowset, bool is_index) {
    FileCacheBlockMeta* meta = resp->add_file_cache_block_metas();
    meta->set_tablet_id(tablet_id);
    meta->set_rowset_id(rowset_id);
    meta->set_segment_id(segment_id);
    meta->set_file_name(file_name);

    if (!is_index) {
        // .dat
        meta->set_file_size(rowset->rowset_meta()->segment_file_size(segment_id));
        meta->set_file_type(doris::FileType::SEGMENT_FILE);
    } else {
        // .idx
        const auto& idx_file_info = rowset->rowset_meta()->inverted_index_file_info(segment_id);
        meta->set_file_size(idx_file_info.has_index_size() ? idx_file_info.index_size() : -1);
        meta->set_file_type(doris::FileType::INVERTED_INDEX_FILE);
    }

    meta->set_offset(std::get<0>(tuple));
    meta->set_size(std::get<1>(tuple));
    meta->set_cache_type(cache_type_to_pb(std::get<2>(tuple)));
    meta->set_expiration_time(std::get<3>(tuple));
}

static void process_segment_file_cache_meta(PGetFileCacheMetaResponse* resp,
                                            const RowsetSharedPtr& rowset, int64_t tablet_id,
                                            const std::string& rowset_id, int32_t segment_id,
                                            bool is_index) {
    const char* extension = is_index ? ".idx" : ".dat";
    std::string file_name = fmt::format("{}_{}{}", rowset_id, segment_id, extension);
    auto cache_key = io::BlockFileCache::hash(file_name);
    auto* cache = io::FileCacheFactory::instance()->get_by_path(cache_key);
    if (!cache) return;
    auto segments_meta = cache->get_hot_blocks_meta(cache_key);
    for (const auto& tuple : segments_meta) {
        add_file_cache_block_meta_to_response(resp, tablet_id, rowset_id, segment_id, file_name,
                                              tuple, rowset, is_index);
    }
}

void CloudInternalServiceImpl::get_file_cache_meta_by_tablet_id(
        google::protobuf::RpcController* controller [[maybe_unused]],
        const PGetFileCacheMetaRequest* request, PGetFileCacheMetaResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    if (!config::enable_file_cache) {
        LOG_WARNING("try to access tablet file cache meta, but file cache not enabled");
        return;
    }
    auto begin_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    std::ostringstream tablet_ids_stream;
    int count = 0;
    for (const auto& tablet_id : request->tablet_ids()) {
        tablet_ids_stream << tablet_id << ", ";
        count++;
        if (count >= 10) {
            break;
        }
    }
    LOG(INFO) << "warm up get meta from this be, tablets num=" << request->tablet_ids().size()
              << ", first 10 tablet_ids=[ " << tablet_ids_stream.str() << " ]";
    for (const auto& tablet_id : request->tablet_ids()) {
        auto res = _engine.tablet_mgr().get_tablet(tablet_id);
        if (!res.has_value()) {
            LOG(ERROR) << "failed to get tablet: " << tablet_id
                       << " err msg: " << res.error().msg();
            continue;
        }
        CloudTabletSPtr tablet = std::move(res.value());
        auto st = tablet->sync_rowsets();
        if (!st) {
            // just log failed, try it best
            LOG(WARNING) << "failed to sync rowsets: " << tablet_id
                         << " err msg: " << st.to_string();
        }
        auto rowsets = tablet->get_snapshot_rowset();

        for (const RowsetSharedPtr& rowset : rowsets) {
            std::string rowset_id = rowset->rowset_id().to_string();
            for (int32_t segment_id = 0; segment_id < rowset->num_segments(); ++segment_id) {
                process_segment_file_cache_meta(response, rowset, tablet_id, rowset_id, segment_id,
                                                false);
                process_segment_file_cache_meta(response, rowset, tablet_id, rowset_id, segment_id,
                                                true);
            }
        }
    }
    auto end_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count();
    g_cloud_internal_service_get_file_cache_meta_by_tablet_id_latency << (end_ts - begin_ts);
    LOG(INFO) << "get file cache meta by tablet ids = [ " << tablet_ids_stream.str() << " ] took "
              << end_ts - begin_ts << " us";
    VLOG_DEBUG << "get file cache meta by tablet id request=" << request->DebugString()
               << ", response=" << response->DebugString();
}

namespace {
// Helper functions for fetch_peer_data
inline int64_t elapsed_us(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                 start)
            .count();
}

int64_t get_rowset_meta_tablet_id_from_request(const PFetchPeerDataRequest* request) {
    return request->has_rowset_meta() && request->rowset_meta().has_tablet_id()
                   ? request->rowset_meta().tablet_id()
                   : -1;
}

std::string get_rowset_meta_resource_id_from_request(const PFetchPeerDataRequest* request) {
    if (request->has_rowset_meta() && request->rowset_meta().has_resource_id()) {
        return request->rowset_meta().resource_id();
    }
    return "";
}

std::string get_peer_cache_filename(std::string_view path) {
    return io::Path(std::string(path)).filename().native();
}

std::string format_peer_request_context(const PFetchPeerDataRequest* request,
                                        const io::UInt128Wrapper& hash, size_t file_size) {
    const std::string file_size_str =
            request->has_file_size() ? std::to_string(request->file_size()) : "unknown";
    return fmt::format(
            "type={}, path={}, cache_hash={}, request_fill={}, fill_tablet_id={}, "
            "fill_remote_path={}, fill_resource_id={}, file_size={}, resolved_file_size={}, "
            "cache_req_count={}, support_attachment={}",
            request->type(), request->path(), hash.to_string(),
            request->has_request_cache_fill() && request->request_cache_fill(),
            get_rowset_meta_tablet_id_from_request(request), request->path(),
            get_rowset_meta_resource_id_from_request(request), file_size_str,
            file_size == std::numeric_limits<size_t>::max() ? std::string("unknown")
                                                            : std::to_string(file_size),
            request->cache_req_size(),
            request->has_support_attachment() && request->support_attachment());
}

std::string format_peer_cache_block_context(const PFetchPeerDataRequest* request,
                                            const CacheBlockReqest& cb_req,
                                            const io::FileBlockSPtr& fb,
                                            const io::UInt128Wrapper& hash, size_t file_size,
                                            bool do_fill) {
    return fmt::format("{}, req_block=[offset={}, size={}], do_fill={}, block={}, cache_file={}",
                       format_peer_request_context(request, hash, file_size), cb_req.block_offset(),
                       cb_req.block_size(), do_fill, fb->get_info_for_log(), fb->get_cache_file());
}

std::string format_peer_fill_context(const io::FileBlockSPtr& fb, int64_t fill_tablet_id,
                                     const std::string& filename, const std::string& resource_id,
                                     const std::string& remote_path, int64_t file_size,
                                     int64_t offset, int64_t size, int32_t timeout_ms) {
    return fmt::format(
            "tablet_id={}, filename={}, resource_id={}, remote_path={}, file_size={}, "
            "request_range=[offset={}, size={}], timeout_ms={}, block={}, cache_file={}",
            fill_tablet_id, filename, resource_id.empty() ? "<unknown>" : resource_id,
            remote_path.empty() ? "<unknown>" : remote_path, file_size, offset, size, timeout_ms,
            fb->get_info_for_log(), fb->get_cache_file());
}

bool wait_for_file_block_state(const io::FileBlockSPtr& fb, int32_t timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        const auto state = fb->state();
        if (state == io::FileBlock::State::DOWNLOADED ||
            state == io::FileBlock::State::SKIP_CACHE || state == io::FileBlock::State::EMPTY) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        fb->wait();
    }
}

Status handle_peer_file_range_request(const std::string& path, PFetchPeerDataResponse* response) {
    // Legacy path: PEER_FILE_RANGE still returns payload via protobuf bytes.
    // Keep this for compatibility until range path is migrated to attachment mode.
    // Read specific range [file_offset, file_offset+file_size) across cached blocks
    auto datas =
            io::FileCacheFactory::instance()->get_cache_data_by_path(get_peer_cache_filename(path));
    for (auto& cb : datas) {
        *(response->add_datas()) = std::move(cb);
    }
    return Status::OK();
}

void set_error_response(PFetchPeerDataResponse* response, const std::string& error_msg) {
    response->mutable_status()->add_error_msgs(error_msg);
    response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
}

void set_too_many_tasks_response(PFetchPeerDataResponse* response, const std::string& error_msg) {
    response->mutable_status()->add_error_msgs(error_msg);
    response->mutable_status()->set_status_code(TStatusCode::TOO_MANY_TASKS);
}

bool try_reject_if_queue_timed_out(std::chrono::steady_clock::time_point enqueue_ts,
                                   PFetchPeerDataResponse* response) {
    auto wait_us = elapsed_us(enqueue_ts);
    g_file_cache_get_by_peer_queue_wait_latency << wait_us;
    auto wait_ms = wait_us / 1000;
    if (wait_ms <= config::peer_fetch_queue_timeout_ms) {
        return false;
    }

    const std::string msg = fmt::format("fetch peer data queue timeout, wait_ms={}, timeout_ms={}",
                                        wait_ms, config::peer_fetch_queue_timeout_ms);
    g_file_cache_get_by_peer_queue_timeout_num << 1;
    set_too_many_tasks_response(response, msg);
    return true;
}

Status read_file_block(const std::shared_ptr<io::FileBlock>& file_block, size_t file_size,
                       doris::CacheBlockPB* output, butil::IOBuf* response_attachment) {
    auto total_start = std::chrono::steady_clock::now();
    int64_t set_data_us = 0;
    Defer report {[&]() {
        g_file_cache_get_by_peer_read_file_block_total_latency << elapsed_us(total_start);
        if (set_data_us > 0) {
            g_file_cache_get_by_peer_set_response_data_latency << set_data_us;
        }
    }};
    // ATTN: calculate the rightmost boundary value of the block, due to inaccurate current block meta information.
    // see CachedRemoteFileReader::read_at_impl for more details.
    // Ensure file_size >= file_block->offset() to avoid underflow
    if (file_size < file_block->offset()) {
        LOG(WARNING) << "file_size (" << file_size << ") < file_block->offset("
                     << file_block->offset() << ")";
        return Status::InternalError<false>("file_size less than block offset");
    }
    size_t read_size = std::min(static_cast<size_t>(file_size - file_block->offset()),
                                file_block->range().size());
    output->set_block_offset(static_cast<int64_t>(file_block->offset()));
    output->set_block_size(static_cast<int64_t>(read_size));
    if (read_size == 0) {
        return Status::OK();
    }

    Status read_st = Status::OK();
    // Attachment payload mode: protobuf carries metadata only, payload goes to attachment.
    // This allows FS cache to use a file-descriptor->IOBuf path directly.
    if (response_attachment != nullptr) {
        size_t bytes_read = 0;
        auto begin_read_file_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count();
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->s3_file_buffer_tracker());
        read_st = file_block->read_to_iobuf(response_attachment, /*read_offset=*/0, read_size,
                                            &bytes_read);
        auto end_read_file_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        g_file_cache_get_by_peer_read_cache_file_latency << (end_read_file_ts - begin_read_file_ts);

        if (read_st.ok()) {
            if (bytes_read != read_size) {
                return Status::InternalError<false>(
                        "peer cache read short data, expected={}, actual={}", read_size,
                        bytes_read);
            }
            g_file_cache_get_by_peer_response_bytes_total << bytes_read;
            return Status::OK();
        }
    } else {
        std::string data;
        data.resize(read_size);
        auto begin_read_file_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count();
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->s3_file_buffer_tracker());
        Slice slice(data.data(), data.size());
        read_st = file_block->read(slice, /*read_offset=*/0);
        auto end_read_file_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        g_file_cache_get_by_peer_read_cache_file_latency << (end_read_file_ts - begin_read_file_ts);

        if (read_st.ok()) {
            auto set_data_start = std::chrono::steady_clock::now();
            output->set_data(std::move(data));
            set_data_us = elapsed_us(set_data_start);
            g_file_cache_get_by_peer_response_bytes_total << read_size;
            return Status::OK();
        }
    }

    g_file_cache_get_by_peer_failed_num << 1;
    LOG(WARNING) << "read cache block failed, file_size=" << file_size
                 << ", block=" << file_block->get_info_for_log()
                 << ", cache_file=" << file_block->get_cache_file() << ", err=" << read_st;
    return read_st;
}

// Trigger S3 -> local cache fill for the given file block.
// Returns OK when the block is DOWNLOADED after the fill.
// Returns TOO_MANY_TASKS when the fill slot is exhausted (server healthy but overloaded):
//   client should not rotate or evict, just fall back to S3 and retry same candidate later.
// Returns NOT_FOUND for soft misses (tablet not found, fill incomplete, timeout):
//   client should rotate the candidate to try a different CG next time.
// The peer uses request.path as the full remote path. tablet_id/filename are kept for logging.
Status trigger_peer_server_fill(io::FileBlockSPtr& fb, int64_t fill_tablet_id,
                                const std::string& filename, const std::string& resource_id,
                                const std::string& remote_path, int64_t file_size, int64_t offset,
                                int64_t size, int32_t timeout_ms, const std::string& table_name,
                                const std::string& partition_name) {
    g_peer_server_fill_requested << 1;

    // Concurrency guard: atomically reserve a fill slot.
    // Excess requests are rejected so the client falls back to its own S3 read.
    // Return NOT_FOUND so the client rotates the candidate instead of evicting it.
    if (g_active_server_fills.fetch_add(1, std::memory_order_relaxed) >=
        config::max_concurrent_peer_server_fills) {
        g_active_server_fills.fetch_sub(1, std::memory_order_relaxed);
        g_peer_server_fill_rejected << 1;
        VLOG_DEBUG << "trigger_peer_server_fill: rejected (concurrency limit "
                   << config::max_concurrent_peer_server_fills << "), tablet_id=" << fill_tablet_id;
        // TOO_MANY_TASKS: server is healthy but overloaded. Client must not rotate or evict;
        // just fall back to S3 for this request and retry the same candidate next time.
        return Status::Error<ErrorCode::TOO_MANY_TASKS, false>("fill slot exhausted");
    }
    // RAII decrement: runs on every return path below.
    Defer fill_guard {[]() { g_active_server_fills.fetch_sub(1, std::memory_order_relaxed); }};

    if (remote_path.empty() || resource_id.empty()) {
        const std::string ctx =
                format_peer_fill_context(fb, fill_tablet_id, filename, resource_id, remote_path,
                                         file_size, offset, size, timeout_ms);
        LOG(WARNING) << "trigger_peer_server_fill: missing remote_path or resource_id, " << ctx;
        g_peer_server_fill_rejected << 1;
        return Status::NotFound<false>("fill: missing remote_path or resource_id, {}", ctx);
    }
    auto storage_resource = doris::get_storage_resource(resource_id);
    if (!storage_resource.has_value()) {
        const std::string ctx =
                format_peer_fill_context(fb, fill_tablet_id, filename, resource_id, remote_path,
                                         file_size, offset, size, timeout_ms);
        LOG(WARNING) << "trigger_peer_server_fill: storage resource not found, " << ctx;
        g_peer_server_fill_rejected << 1;
        return Status::NotFound<false>("fill: storage resource not found, {}", ctx);
    }
    auto fs = storage_resource->first.fs;

    const auto initial_state = fb->state();
    if (initial_state == io::FileBlock::State::DOWNLOADING) {
        // Another thread already owns the block downloader. Wait up to the request timeout instead
        // of the shorter per-wait timeout in FileBlock::wait().
        [[maybe_unused]] const bool completed = wait_for_file_block_state(fb, timeout_ms);
        const std::string ctx =
                format_peer_fill_context(fb, fill_tablet_id, filename, resource_id, remote_path,
                                         file_size, offset, size, timeout_ms);
        return fb->state() == io::FileBlock::State::DOWNLOADED
                       ? Status::OK()
                       : Status::NotFound<false>("fill: concurrent download incomplete, {}", ctx);
    }
    if (initial_state != io::FileBlock::State::EMPTY) {
        const std::string ctx =
                format_peer_fill_context(fb, fill_tablet_id, filename, resource_id, remote_path,
                                         file_size, offset, size, timeout_ms);
        return initial_state == io::FileBlock::State::DOWNLOADED
                       ? Status::OK()
                       : Status::NotFound<false>("fill: unexpected initial block state, {}", ctx);
    }

    auto fill_start = std::chrono::steady_clock::now();
    auto fill_done = std::make_shared<bthread::CountdownEvent>(1);
    auto fill_status = std::make_shared<Status>(Status::OK());
    io::DownloadFileMeta download_meta {
            .path = remote_path,
            .file_size = file_size,
            .offset = offset,
            .download_size = size,
            .file_system = fs,
            .ctx = {.is_dryrun = config::enable_reader_dryrun_when_download_file_cache,
                    // Pull-through fill must go straight to remote storage. If this download
                    // re-enters peer race, the original block can remain DOWNLOADING for the
                    // duration of nested peer retries and timeouts.
                    .is_warmup = false,
                    .bypass_peer_read = true,
                    .table_name = table_name,
                    .partition_name = partition_name},
            .download_done =
                    [fill_done, fill_status](Status st) {
                        *fill_status = std::move(st);
                        fill_done->signal();
                    },
            .tablet_id = fill_tablet_id,
    };

    io::DownloadTask task(std::move(download_meta));
    ExecEnv::GetInstance()
            ->storage_engine()
            .to_cloud()
            .file_cache_block_downloader()
            .submit_download_task(std::move(task));

    const timespec due_time = butil::milliseconds_from_now(timeout_ms);
    const bool timed_out = fill_done->timed_wait(due_time) != 0;

    int64_t fill_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - fill_start)
                              .count();
    g_peer_server_fill_latency << fill_ms * 1000; // LatencyRecorder takes microseconds

    if (!timed_out && fill_status->ok() && fb->state() == io::FileBlock::State::DOWNLOADING) {
        const int32_t settle_timeout_ms =
                std::max<int32_t>(1, timeout_ms - static_cast<int32_t>(fill_ms));
        [[maybe_unused]] const bool settled = wait_for_file_block_state(fb, settle_timeout_ms);
    }

    auto final_state = fb->state();
    if (final_state == io::FileBlock::State::DOWNLOADED) {
        g_peer_server_fill_success << 1;
        return Status::OK();
    }
    if (timed_out) {
        LOG(WARNING) << "trigger_peer_server_fill: fill timeout, elapsed_ms=" << fill_ms << ", "
                     << format_peer_fill_context(fb, fill_tablet_id, filename, resource_id,
                                                 remote_path, file_size, offset, size, timeout_ms);
        g_peer_server_fill_timeout << 1;
    } else if (!fill_status->ok()) {
        LOG(WARNING) << "trigger_peer_server_fill: fill failed, elapsed_ms=" << fill_ms
                     << ", status=" << fill_status->to_string() << ", "
                     << format_peer_fill_context(fb, fill_tablet_id, filename, resource_id,
                                                 remote_path, file_size, offset, size, timeout_ms);
    }
    // Any non-DOWNLOADED outcome is a soft failure: the server is otherwise healthy so the
    // client should rotate the candidate rather than evict it.
    return Status::NotFound<false>(
            "fill: block not downloaded, {}",
            format_peer_fill_context(fb, fill_tablet_id, filename, resource_id, remote_path,
                                     file_size, offset, size, timeout_ms));
}

Status handle_peer_file_cache_block_request(const PFetchPeerDataRequest* request,
                                            PFetchPeerDataResponse* response,
                                            brpc::Controller* cntl) {
    auto handle_start = std::chrono::steady_clock::now();
    const uint64_t request_blocks = request->cache_req_size();
    uint64_t response_blocks = 0;
    uint64_t get_or_set_calls = 0;
    uint64_t get_or_set_blocks = 0;
    int64_t get_cache_us = 0;
    Defer report {[&]() {
        g_file_cache_get_by_peer_handle_cache_block_req_latency << elapsed_us(handle_start);
        g_file_cache_get_by_peer_get_cache_latency << get_cache_us;
        g_file_cache_get_by_peer_get_or_set_calls << get_or_set_calls;
        g_file_cache_get_by_peer_get_or_set_blocks_total << get_or_set_blocks;
        g_file_cache_get_by_peer_request_blocks_total << request_blocks;
        g_file_cache_get_by_peer_request_blocks_per_rpc << request_blocks;
        g_file_cache_get_by_peer_response_blocks_total << response_blocks;
    }};
    const auto& path = request->path();
    const auto cache_key_path = get_peer_cache_filename(path);
    auto hash = io::BlockFileCache::hash(cache_key_path);
    auto get_cache_start = std::chrono::steady_clock::now();
    auto* cache = io::FileCacheFactory::instance()->get_by_path(hash);
    get_cache_us = elapsed_us(get_cache_start);
    if (cache == nullptr) {
        g_file_cache_get_by_peer_failed_num << 1;
        set_error_response(response, "can't get file cache instance");
        return Status::InternalError<false>("can't get file cache instance");
    }

    io::CacheContext ctx {};
    io::ReadStatistics local_stats;
    ctx.stats = &local_stats;
    ctx.table_name = request->has_table_name() ? request->table_name() : "";
    ctx.partition_name = request->has_partition_name() ? request->partition_name() : "";
    const size_t file_size =
            request->has_file_size()
                    ? static_cast<size_t>(std::max<int64_t>(0, request->file_size()))
                    : std::numeric_limits<size_t>::max();
    // Enable attachment mode only when client advertises support.
    // This keeps mixed-version rolling upgrades safe.
    const bool use_attachment =
            cntl != nullptr && request->has_support_attachment() && request->support_attachment();
    response->set_data_in_attachment(use_attachment);
    if (use_attachment) {
        g_file_cache_get_by_peer_attachment_response_num << 1;
    } else {
        g_file_cache_get_by_peer_pb_response_num << 1;
    }

    const bool do_fill = request->has_request_cache_fill() && request->request_cache_fill() &&
                         config::enable_peer_server_cache_fill;

    for (const auto& cb_req : request->cache_req()) {
        size_t offset = static_cast<size_t>(std::max<int64_t>(0, cb_req.block_offset()));
        size_t size = static_cast<size_t>(std::max<int64_t>(0, cb_req.block_size()));
        if (offset >= file_size) {
            continue;
        }
        // Clip tail requests before get_or_set so peer reads do not synthesize EMPTY blocks past
        // EOF and then fail the whole RPC.
        size = std::min(size, file_size - offset);
        if (size == 0) {
            continue;
        }
        DBUG_EXECUTE_IF(
                "CloudInternalServiceImpl::handle_peer_file_cache_block_request_hold_before_get_or_"
                "set",
                {
                    int sleep_ms = dp->param<int>("sleep_ms", 300);
                    bthread_usleep(sleep_ms * 1000);
                });
        auto get_or_set_start = std::chrono::steady_clock::now();
        auto holder = cache->get_or_set(hash, offset, size, ctx);
        g_file_cache_get_by_peer_get_or_set_latency << elapsed_us(get_or_set_start);
        ++get_or_set_calls;
        get_or_set_blocks += holder.file_blocks.size();

        for (auto& fb : holder.file_blocks) {
            auto fb_state = fb->state();
            if (fb_state == io::FileBlock::State::DOWNLOADING) {
                if (do_fill) {
                    // Only peer fill requests should wait longer here. Plain peer-cache reads keep
                    // the short wait semantics so they can fail fast and let the client race S3.
                    [[maybe_unused]] const bool completed = wait_for_file_block_state(
                            fb, config::peer_server_cache_fill_timeout_ms);
                    fb_state = fb->state();
                } else {
                    // Wait for in-progress download to complete using the normal short timeout.
                    fb_state = fb->wait();
                }
            }
            if (fb_state == io::FileBlock::State::EMPTY) {
                if (!do_fill) {
                    const std::string msg =
                            fmt::format("cache block not downloaded, {}",
                                        format_peer_cache_block_context(request, cb_req, fb, hash,
                                                                        file_size, do_fill));
                    g_file_cache_get_by_peer_failed_num << 1;
                    g_file_cache_get_by_peer_not_downloaded_block_num << 1;
                    LOG(WARNING) << msg;
                    // Use NOT_FOUND so the client can distinguish "block not cached"
                    // from an actual RPC/server error.  On NOT_FOUND the client rotates
                    // the candidate to the end of its list (trying another CG next time)
                    // rather than incrementing the RPC-failure eviction counter.
                    response->mutable_status()->add_error_msgs(msg);
                    response->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
                    return Status::NotFound<false>(msg);
                }
                // Server-side fill: request.path already carries the full remote path.
                auto fill_st = trigger_peer_server_fill(
                        fb, get_rowset_meta_tablet_id_from_request(request), cache_key_path,
                        get_rowset_meta_resource_id_from_request(request), path,
                        request->has_file_size() ? request->file_size() : -1,
                        static_cast<int64_t>(fb->range().left),
                        static_cast<int64_t>(fb->range().size()),
                        config::peer_server_cache_fill_timeout_ms, ctx.table_name,
                        ctx.partition_name);
                if (!fill_st.ok()) {
                    g_file_cache_get_by_peer_failed_num << 1;
                    g_file_cache_get_by_peer_not_downloaded_block_num << 1;
                    if (fill_st.is<ErrorCode::TOO_MANY_TASKS>()) {
                        // Server slot exhausted: healthy but overloaded. Client must not rotate
                        // or evict — just fall back to S3 and retry same candidate next time.
                        response->mutable_status()->add_error_msgs(std::string(fill_st.msg()));
                        response->mutable_status()->set_status_code(TStatusCode::TOO_MANY_TASKS);
                    } else if (fill_st.is<ErrorCode::NOT_FOUND>()) {
                        // Soft miss (fill incomplete, timeout, unexpected state) — client rotates,
                        // not evicts.
                        response->mutable_status()->add_error_msgs(std::string(fill_st.msg()));
                        response->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
                    } else {
                        LOG(WARNING) << "cache block fill failed, status=" << fill_st << ", "
                                     << format_peer_cache_block_context(request, cb_req, fb, hash,
                                                                        file_size, do_fill);
                        set_error_response(response, "cache block not ready");
                    }
                    return fill_st;
                }
                fb_state = io::FileBlock::State::DOWNLOADED;
            }
            if (fb_state != io::FileBlock::State::DOWNLOADED) {
                // A concurrent download was in progress (DOWNLOADING at request time) but its
                // wait() returned a non-DOWNLOADED state (e.g., timed-out while still
                // DOWNLOADING, or some other non-EMPTY intermediate state).  The server is
                // healthy; the block just isn't available yet.  Return NOT_FOUND so the client
                // rotates the candidate instead of evicting it.
                const std::string msg =
                        fmt::format("cache block not ready after wait, {}",
                                    format_peer_cache_block_context(request, cb_req, fb, hash,
                                                                    file_size, do_fill));
                g_file_cache_get_by_peer_failed_num << 1;
                g_file_cache_get_by_peer_not_downloaded_block_num << 1;
                LOG(WARNING) << msg;
                response->mutable_status()->add_error_msgs(msg);
                response->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
                return Status::NotFound<false>(msg);
            }

            g_file_cache_get_by_peer_blocks_num << 1;
            doris::CacheBlockPB* out = response->add_datas();
            // In attachment mode, metadata order must match attachment append order because the
            // client consumes attachment payload sequentially using resp.datas() order.
            Status read_status = read_file_block(
                    fb, file_size, out, use_attachment ? &cntl->response_attachment() : nullptr);
            if (!read_status.ok()) {
                set_error_response(response, "read cache file error");
                return read_status;
            }
            ++response_blocks;
        }
    }

    return Status::OK();
}
} // namespace

#ifdef BE_TEST
Status test_handle_peer_file_cache_block_request(const PFetchPeerDataRequest* request,
                                                 PFetchPeerDataResponse* response,
                                                 brpc::Controller* cntl) {
    return handle_peer_file_cache_block_request(request, response, cntl);
}

bool test_try_reject_if_queue_timed_out(std::chrono::steady_clock::time_point enqueue_ts,
                                        PFetchPeerDataResponse* response) {
    return try_reject_if_queue_timed_out(enqueue_ts, response);
}
#endif

void CloudInternalServiceImpl::fetch_peer_data(google::protobuf::RpcController* controller,
                                               const PFetchPeerDataRequest* request,
                                               PFetchPeerDataResponse* response,
                                               google::protobuf::Closure* done) {
    auto enqueue_ts = std::chrono::steady_clock::now();
    // Lifetime: cntl is owned by brpc framework and valid until done->Run() is called.
    // The ClosureGuard inside the lambda ensures done->Run() happens after all cntl usage,
    // so capturing the raw pointer by value is safe.
    auto* cntl = static_cast<brpc::Controller*>(controller);
    bool ret = _peer_fetch_pool.try_offer([request, response, done, enqueue_ts, cntl]() {
        brpc::ClosureGuard closure_guard(done);
        g_file_cache_get_by_peer_num << 1;
        if (try_reject_if_queue_timed_out(enqueue_ts, response)) {
            return;
        }

        if (!config::enable_file_cache) {
            LOG_WARNING("try to access file cache data, but file cache not enabled");
            return;
        }

        auto begin_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now().time_since_epoch())
                                .count();

        const auto type = request->type();
        const auto& path = request->path();
        response->mutable_status()->set_status_code(TStatusCode::OK);

        Status status = Status::OK();
        if (type == PFetchPeerDataRequest_Type_PEER_FILE_RANGE) {
            status = handle_peer_file_range_request(path, response);
        } else if (type == PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK) {
            status = handle_peer_file_cache_block_request(request, response, cntl);
        }

        if (!status.ok()) {
            const std::string msg =
                    "fetch peer data failed: " + status.to_string() + ", " +
                    format_peer_request_context(
                            request, io::BlockFileCache::hash(get_peer_cache_filename(path)),
                            request->has_file_size() ? static_cast<size_t>(std::max<int64_t>(
                                                               0, request->file_size()))
                                                     : std::numeric_limits<size_t>::max());
            if (status.is<ErrorCode::NOT_FOUND>() || status.is<ErrorCode::TOO_MANY_TASKS>()) {
                VLOG_DEBUG << msg;
            } else {
                LOG(WARNING) << msg;
            }
            auto* resp_status = response->mutable_status();
            if (resp_status->status_code() == TStatusCode::OK) {
                set_error_response(response, status.to_string());
            } else if (resp_status->error_msgs().empty()) {
                resp_status->add_error_msgs(status.to_string());
            }
        }

        DBUG_EXECUTE_IF("CloudInternalServiceImpl::fetch_peer_data_slower", {
            int st_us = dp->param<int>("sleep", 1000);
            LOG_WARNING("CloudInternalServiceImpl::fetch_peer_data_slower").tag("sleep", st_us);
            bthread_usleep(st_us);
        });

        auto end_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now().time_since_epoch())
                              .count();
        // Latency covers every completed callback (including failures) so the
        // server-side fail-fast paths still show up in the latency histogram.
        // success_num must only count actual OK results, otherwise dedup
        // TOO_MANY_TASKS / NOT_FOUND / handler errors all fall through here
        // and the success rate is meaningless. Use file_cache_get_by_peer_num
        // for the total completed-callback count.
        g_file_cache_get_by_peer_server_latency << (end_ts - begin_ts);
        if (status.ok()) {
            g_file_cache_get_by_peer_success_num << 1;
        }

        VLOG_DEBUG << "fetch cache request=" << request->DebugString()
                   << ", response=" << response->DebugString();
    });

    if (!ret) {
        g_file_cache_get_by_peer_offer_failed_num << 1;
        brpc::ClosureGuard closure_guard(done);
        const std::string msg = fmt::format(
                "fail to offer fetch peer data request to the peer fetch work pool, pool={}",
                _peer_fetch_pool.get_info());
        set_too_many_tasks_response(response, msg);
        LOG(WARNING) << msg;
    }
}

bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_segment_num(
        "file_cache_event_driven_warm_up_submitted_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_segment_num(
        "file_cache_event_driven_warm_up_finished_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_segment_num(
        "file_cache_event_driven_warm_up_failed_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_segment_size(
        "file_cache_event_driven_warm_up_submitted_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_segment_size(
        "file_cache_event_driven_warm_up_finished_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_segment_size(
        "file_cache_event_driven_warm_up_failed_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_index_num(
        "file_cache_event_driven_warm_up_submitted_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_index_num(
        "file_cache_event_driven_warm_up_finished_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_index_num(
        "file_cache_event_driven_warm_up_failed_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_index_size(
        "file_cache_event_driven_warm_up_submitted_index_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_index_size(
        "file_cache_event_driven_warm_up_finished_index_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_index_size(
        "file_cache_event_driven_warm_up_failed_index_size");
bvar::Status<int64_t> g_file_cache_warm_up_rowset_last_handle_unix_ts(
        "file_cache_warm_up_rowset_last_handle_unix_ts", 0);
bvar::Status<int64_t> g_file_cache_warm_up_rowset_last_finish_unix_ts(
        "file_cache_warm_up_rowset_last_finish_unix_ts", 0);
bvar::LatencyRecorder g_file_cache_warm_up_rowset_latency("file_cache_warm_up_rowset_latency");
bvar::LatencyRecorder g_file_cache_warm_up_rowset_request_to_handle_latency(
        "file_cache_warm_up_rowset_request_to_handle_latency");
bvar::LatencyRecorder g_file_cache_warm_up_rowset_handle_to_finish_latency(
        "file_cache_warm_up_rowset_handle_to_finish_latency");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_slow_count(
        "file_cache_warm_up_rowset_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_request_to_handle_slow_count(
        "file_cache_warm_up_rowset_request_to_handle_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_handle_to_finish_slow_count(
        "file_cache_warm_up_rowset_handle_to_finish_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_wait_for_compaction_num(
        "file_cache_warm_up_rowset_wait_for_compaction_num");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_wait_for_compaction_timeout_num(
        "file_cache_warm_up_rowset_wait_for_compaction_timeout_num");

// Per-job windowed metrics for target BE
// bvar::Window enforces MAX_SECONDS_LIMIT = 3600, so the longest window is 1h.
static constexpr int WINDOW_5M = 300;
static constexpr int WINDOW_30M = 1800;
static constexpr int WINDOW_1H = 3600;

MBvarWindowedAdder g_warmup_ed_finish_segment_num("warmup_ed_finish_segment_num", {"job_id"},
                                                  {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_finish_segment_size("warmup_ed_finish_segment_size", {"job_id"},
                                                   {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_finish_index_num("warmup_ed_finish_index_num", {"job_id"},
                                                {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_finish_index_size("warmup_ed_finish_index_size", {"job_id"},
                                                 {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_fail_segment_num("warmup_ed_fail_segment_num", {"job_id"},
                                                {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_fail_segment_size("warmup_ed_fail_segment_size", {"job_id"},
                                                 {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_fail_index_num("warmup_ed_fail_index_num", {"job_id"},
                                              {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_fail_index_size("warmup_ed_fail_index_size", {"job_id"},
                                               {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_finish_ts({"job_id"});

void update_warmup_ed_last_finish_ts(const std::string& job_id_str) {
    auto* finish_ts = g_warmup_ed_last_finish_ts.get_stats(std::list<std::string> {job_id_str});
    if (finish_ts) {
        finish_ts->set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
    }
}

void record_warmup_ed_finish_segment(const std::string& job_id_str, int64_t segment_size) {
    g_warmup_ed_finish_segment_num.put({job_id_str}, 1);
    g_warmup_ed_finish_segment_size.put({job_id_str}, segment_size);
    update_warmup_ed_last_finish_ts(job_id_str);
}

void record_warmup_ed_finish_index(const std::string& job_id_str, int64_t idx_size) {
    g_warmup_ed_finish_index_num.put({job_id_str}, 1);
    g_warmup_ed_finish_index_size.put({job_id_str}, idx_size);
    update_warmup_ed_last_finish_ts(job_id_str);
}

void record_warmup_ed_fail_segment(const std::string& job_id_str, int64_t segment_size) {
    g_warmup_ed_fail_segment_num.put({job_id_str}, 1);
    g_warmup_ed_fail_segment_size.put({job_id_str}, segment_size);
}

void record_warmup_ed_fail_index(const std::string& job_id_str, int64_t idx_size) {
    g_warmup_ed_fail_index_num.put({job_id_str}, 1);
    g_warmup_ed_fail_index_size.put({job_id_str}, idx_size);
}

void record_warmup_ed_skipped_rowset_as_finished(RowsetMeta& rs_meta,
                                                 const std::string& job_id_str) {
    auto schema_ptr = rs_meta.tablet_schema();
    bool has_inverted_index = schema_ptr->has_inverted_index() || schema_ptr->has_ann_index();
    auto idx_version = schema_ptr->get_inverted_index_storage_format();
    for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
        record_warmup_ed_finish_segment(job_id_str, rs_meta.segment_file_size(segment_id));

        if (!has_inverted_index) {
            continue;
        }
        auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
        if (idx_version == InvertedIndexStorageFormatPB::V1) {
            std::unordered_map<int64_t, int64_t> index_size_map;
            for (const auto& info : inverted_index_info.index_info()) {
                if (info.index_file_size() != -1) {
                    index_size_map[info.index_id()] = info.index_file_size();
                } else {
                    VLOG_DEBUG << "Invalid index_file_size for segment_id " << segment_id
                               << ", index_id " << info.index_id();
                }
            }
            for (const auto& index : schema_ptr->inverted_indexes()) {
                record_warmup_ed_finish_index(job_id_str, index_size_map[index->index_id()]);
            }
        } else { // InvertedIndexStorageFormatPB::V2
            int64_t idx_size = 0;
            if (inverted_index_info.has_index_size()) {
                idx_size = inverted_index_info.index_size();
            } else {
                VLOG_DEBUG << "index_size is not set for segment " << segment_id;
            }
            record_warmup_ed_finish_index(job_id_str, idx_size);
        }
    }
}

void handle_segment_download_done(Status st, int64_t tablet_id, const RowsetId& rowset_id,
                                  int64_t segment_id, std::shared_ptr<CloudTablet> tablet,
                                  std::shared_ptr<bthread::CountdownEvent> wait, Version version,
                                  int64_t segment_size, int64_t request_ts, int64_t handle_ts,
                                  std::string job_id_str, int64_t upstream_trigger_ts_ms) {
    DBUG_EXECUTE_IF("CloudInternalServiceImpl::warm_up_rowset.download_segment", {
        auto sleep_time = dp->param<int>("sleep", 3);
        LOG_INFO("[verbose] block download for rowset={}, version={}, sleep={}",
                 rowset_id.to_string(), version.to_string(), sleep_time);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
    });
    DBUG_EXECUTE_IF(
            "CloudInternalServiceImpl::warm_up_rowset.download_segment.inject_"
            "error",
            {
                st = Status::InternalError("injected error");
                LOG_INFO("[verbose] inject error, tablet={}, rowset={}, st={}", tablet_id,
                         rowset_id.to_string(), st.to_string());
            });
    if (st.ok()) {
        g_file_cache_event_driven_warm_up_finished_segment_num << 1;
        g_file_cache_event_driven_warm_up_finished_segment_size << segment_size;
        record_warmup_ed_finish_segment(job_id_str, segment_size);
        int64_t now_ts = current_unix_time_us();
        g_file_cache_warm_up_rowset_last_finish_unix_ts.set_value(now_ts);
        auto rowset_latency_us = warm_up_rowset_cross_host_latency_us(request_ts, now_ts);
        if (rowset_latency_us.has_value()) {
            g_file_cache_warm_up_rowset_latency << *rowset_latency_us;
        }
        g_file_cache_warm_up_rowset_handle_to_finish_latency << (now_ts - handle_ts);
        if (rowset_latency_us.has_value() &&
            *rowset_latency_us > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_slow_count << 1;
            LOG(INFO) << "warm up rowset took " << *rowset_latency_us
                      << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id.to_string()
                      << ", segment_id: " << segment_id;
        }
        if (now_ts - handle_ts > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_handle_to_finish_slow_count << 1;
            LOG(INFO) << "warm up rowset (handle to finish) took " << now_ts - handle_ts
                      << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id.to_string()
                      << ", segment_id: " << segment_id;
        }
    } else {
        g_file_cache_event_driven_warm_up_failed_segment_num << 1;
        g_file_cache_event_driven_warm_up_failed_segment_size << segment_size;
        record_warmup_ed_fail_segment(job_id_str, segment_size);
        LOG(WARNING) << "download segment failed, tablet_id: " << tablet_id
                     << " rowset_id: " << rowset_id.to_string() << ", error: " << st;
    }
    if (tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN, rowset_id, st, 1,
                                               0)
                .trigger_source == WarmUpTriggerSource::EVENT_DRIVEN) {
        VLOG_DEBUG << "warmup rowset " << version.to_string() << "(" << rowset_id.to_string()
                   << ") completed";
    }
    g_warmup_ed_downstream_progress_tracker.record_task_done(job_id_str, upstream_trigger_ts_ms);
    if (wait) {
        wait->signal();
    }
}

void handle_inverted_index_download_done(Status st, int64_t tablet_id, const RowsetId& rowset_id,
                                         int64_t segment_id, std::string index_path,
                                         std::shared_ptr<CloudTablet> tablet,
                                         std::shared_ptr<bthread::CountdownEvent> wait,
                                         Version version, uint64_t idx_size, int64_t request_ts,
                                         int64_t handle_ts, std::string job_id_str,
                                         int64_t upstream_trigger_ts_ms) {
    DBUG_EXECUTE_IF("CloudInternalServiceImpl::warm_up_rowset.download_inverted_idx", {
        auto sleep_time = dp->param<int>("sleep", 3);
        LOG_INFO(
                "[verbose] block download for rowset={}, inverted index "
                "file={}, sleep={}",
                rowset_id.to_string(), index_path, sleep_time);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
    });
    if (st.ok()) {
        g_file_cache_event_driven_warm_up_finished_index_num << 1;
        g_file_cache_event_driven_warm_up_finished_index_size << idx_size;
        record_warmup_ed_finish_index(job_id_str, static_cast<int64_t>(idx_size));
        int64_t now_ts = current_unix_time_us();
        g_file_cache_warm_up_rowset_last_finish_unix_ts.set_value(now_ts);
        auto rowset_latency_us = warm_up_rowset_cross_host_latency_us(request_ts, now_ts);
        if (rowset_latency_us.has_value()) {
            g_file_cache_warm_up_rowset_latency << *rowset_latency_us;
        }
        g_file_cache_warm_up_rowset_handle_to_finish_latency << (now_ts - handle_ts);
        if (rowset_latency_us.has_value() &&
            *rowset_latency_us > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_slow_count << 1;
            LOG(INFO) << "warm up rowset took " << *rowset_latency_us
                      << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id.to_string()
                      << ", segment_id: " << segment_id;
        }
        if (now_ts - handle_ts > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_handle_to_finish_slow_count << 1;
            LOG(INFO) << "warm up rowset (handle to finish) took " << now_ts - handle_ts
                      << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id.to_string()
                      << ", segment_id: " << segment_id;
        }
    } else {
        g_file_cache_event_driven_warm_up_failed_index_num << 1;
        g_file_cache_event_driven_warm_up_failed_index_size << idx_size;
        record_warmup_ed_fail_index(job_id_str, static_cast<int64_t>(idx_size));
        LOG(WARNING) << "download inverted index failed, tablet_id: " << tablet_id
                     << " rowset_id: " << rowset_id << ", error: " << st;
    }
    if (tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN, rowset_id, st, 0,
                                               1)
                .trigger_source == WarmUpTriggerSource::EVENT_DRIVEN) {
        VLOG_DEBUG << "warmup rowset " << version.to_string() << "(" << rowset_id.to_string()
                   << ") completed";
    }
    g_warmup_ed_downstream_progress_tracker.record_task_done(job_id_str, upstream_trigger_ts_ms);
    if (wait) {
        wait->signal();
    }
}

void CloudInternalServiceImpl::warm_up_rowset(google::protobuf::RpcController* controller
                                              [[maybe_unused]],
                                              const PWarmUpRowsetRequest* request,
                                              PWarmUpRowsetResponse* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    std::shared_ptr<bthread::CountdownEvent> wait = nullptr;
    timespec due_time;
    if (request->has_sync_wait_timeout_ms() && request->sync_wait_timeout_ms() > 0) {
        g_file_cache_warm_up_rowset_wait_for_compaction_num << 1;
        wait = std::make_shared<bthread::CountdownEvent>(0);
        VLOG_DEBUG << "sync_wait_timeout: " << request->sync_wait_timeout_ms() << " ms";
        due_time = butil::milliseconds_from_now(request->sync_wait_timeout_ms());
    }

    // Extract job_id from request (0 if not set, for backward compatibility)
    std::string job_id_str = std::to_string(request->has_job_id() ? request->job_id() : 0);
    int64_t upstream_trigger_ts_ms =
            request->has_upstream_trigger_ts_ms() ? request->upstream_trigger_ts_ms() : 0;

    for (auto& rs_meta_pb : request->rowset_metas()) {
        RowsetMeta rs_meta;
        rs_meta.init_from_pb(rs_meta_pb);
        auto storage_resource = rs_meta.remote_storage_resource();
        if (!storage_resource) {
            LOG(WARNING) << storage_resource.error();
            continue;
        }
        int64_t tablet_id = rs_meta.tablet_id();
        auto rowset_id = rs_meta.rowset_id();
        bool local_only = !(request->has_skip_existence_check() && request->skip_existence_check());
        auto res = _engine.tablet_mgr().get_tablet(tablet_id, /* warmup_data = */ false,
                                                   /* sync_delete_bitmap = */ true,
                                                   /* sync_stats = */ nullptr,
                                                   /* local_only = */ local_only);
        if (!res.has_value()) {
            LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(res.error());
            if (res.error().msg().find("local_only=true") != std::string::npos ||
                res.error().msg().find("force_use_only_cached=true") != std::string::npos) {
                res.error().set_code(ErrorCode::TABLE_NOT_FOUND);
            }
            res.error().to_protobuf(response->mutable_status());
            continue;
        }
        auto tablet = res.value();
        auto tablet_meta = tablet->tablet_meta();

        int64_t handle_ts = current_unix_time_us();
        g_file_cache_warm_up_rowset_last_handle_unix_ts.set_value(handle_ts);
        int64_t request_ts = request->has_unix_ts_us() ? request->unix_ts_us() : 0;
        auto request_to_handle_latency_us =
                warm_up_rowset_cross_host_latency_us(request_ts, handle_ts);
        if (request_to_handle_latency_us.has_value()) {
            g_file_cache_warm_up_rowset_request_to_handle_latency << *request_to_handle_latency_us;
        }
        if (request_to_handle_latency_us.has_value() &&
            *request_to_handle_latency_us > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_request_to_handle_slow_count << 1;
            LOG(INFO) << "warm up rowset (request to handle) took " << *request_to_handle_latency_us
                      << " us, tablet_id: " << rs_meta.tablet_id()
                      << ", rowset_id: " << rowset_id.to_string();
        }
        int64_t expiration_time = tablet_meta->ttl_seconds();

        if (!tablet->add_rowset_warmup_state(rs_meta, WarmUpTriggerSource::EVENT_DRIVEN)) {
            LOG(INFO) << "found duplicate warmup task for rowset " << rowset_id.to_string()
                      << ", skip it";
            g_warmup_ed_downstream_progress_tracker.record_task_done(job_id_str,
                                                                     upstream_trigger_ts_ms);
            record_warmup_ed_skipped_rowset_as_finished(rs_meta, job_id_str);
            continue;
        }
        if (rs_meta.num_segments() == 0) {
            g_warmup_ed_downstream_progress_tracker.record_task_done(job_id_str,
                                                                     upstream_trigger_ts_ms);
        }

        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            if (!config::file_cache_enable_only_warm_up_idx) {
                auto segment_size = rs_meta.segment_file_size(segment_id);

                // Use rs_meta.fs() instead of storage_resource.value()->fs to support packed files.
                // PackedFileSystem wrapper in rs_meta.fs() handles the index_map lookup and
                // reads from the correct packed file.
                io::DownloadFileMeta download_meta {
                        .path = storage_resource.value()->remote_segment_path(rs_meta, segment_id),
                        .file_size = segment_size,
                        .offset = 0,
                        .download_size = segment_size,
                        .file_system = rs_meta.fs(),
                        .ctx = {.is_index_data = false,
                                .expiration_time = expiration_time,
                                .is_dryrun = config::enable_reader_dryrun_when_download_file_cache,
                                .is_warmup = true,
                                .table_name = "",
                                .partition_name = ""},
                        .download_done =
                                [=, version = rs_meta.version()](Status st) {
                                    handle_segment_download_done(
                                            st, tablet_id, rowset_id, segment_id, tablet, wait,
                                            version, segment_size, request_ts, handle_ts,
                                            job_id_str, upstream_trigger_ts_ms);
                                },
                        .tablet_id = tablet_id};

                g_file_cache_event_driven_warm_up_submitted_segment_num << 1;
                g_file_cache_event_driven_warm_up_submitted_segment_size << segment_size;
                if (wait) {
                    wait->add_count();
                }
                g_warmup_ed_downstream_progress_tracker.record_task_submit(job_id_str,
                                                                           upstream_trigger_ts_ms);

                _engine.file_cache_block_downloader().submit_download_task(download_meta);
            }

            // Use rs_meta.fs() to support packed files for inverted index download.
            auto download_inverted_index = [&, tablet, job_id_str](std::string index_path,
                                                                   uint64_t idx_size) {
                io::DownloadFileMeta download_meta {
                        .path = io::Path(index_path),
                        .file_size = static_cast<int64_t>(idx_size),
                        .file_system = rs_meta.fs(),
                        .ctx = {.is_index_data = false, // DORIS-20877
                                .expiration_time = expiration_time,
                                .is_dryrun = config::enable_reader_dryrun_when_download_file_cache,
                                .is_warmup = true,
                                .table_name = "",
                                .partition_name = ""},
                        .download_done =
                                [=, version = rs_meta.version()](Status st) {
                                    handle_inverted_index_download_done(
                                            st, tablet_id, rowset_id, segment_id, index_path,
                                            tablet, wait, version, idx_size, request_ts, handle_ts,
                                            job_id_str, upstream_trigger_ts_ms);
                                },
                        .tablet_id = tablet_id,
                };
                g_file_cache_event_driven_warm_up_submitted_index_num << 1;
                g_file_cache_event_driven_warm_up_submitted_index_size << idx_size;
                tablet->update_rowset_warmup_state_inverted_idx_num(
                        WarmUpTriggerSource::EVENT_DRIVEN, rowset_id, 1);
                if (wait) {
                    wait->add_count();
                }
                g_warmup_ed_downstream_progress_tracker.record_task_submit(job_id_str,
                                                                           upstream_trigger_ts_ms);
                _engine.file_cache_block_downloader().submit_download_task(download_meta);
            };

            // inverted index
            auto schema_ptr = rs_meta.tablet_schema();
            auto idx_version = schema_ptr->get_inverted_index_storage_format();

            if (schema_ptr->has_inverted_index() || schema_ptr->has_ann_index()) {
                if (idx_version == InvertedIndexStorageFormatPB::V1) {
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    std::unordered_map<int64_t, int64_t> index_size_map;
                    for (const auto& info : inverted_index_info.index_info()) {
                        if (info.index_file_size() != -1) {
                            index_size_map[info.index_id()] = info.index_file_size();
                        } else {
                            VLOG_DEBUG << "Invalid index_file_size for segment_id " << segment_id
                                       << ", index_id " << info.index_id();
                        }
                    }
                    for (const auto& index : schema_ptr->inverted_indexes()) {
                        auto idx_path = storage_resource.value()->remote_idx_v1_path(
                                rs_meta, segment_id, index->index_id(), index->get_index_suffix());
                        download_inverted_index(idx_path, index_size_map[index->index_id()]);
                    }
                } else { // InvertedIndexStorageFormatPB::V2
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    int64_t idx_size = 0;
                    if (inverted_index_info.has_index_size()) {
                        idx_size = inverted_index_info.index_size();
                    } else {
                        VLOG_DEBUG << "index_size is not set for segment " << segment_id;
                    }
                    auto idx_path =
                            storage_resource.value()->remote_idx_v2_path(rs_meta, segment_id);
                    download_inverted_index(idx_path, idx_size);
                }
            }
        }
    }
    if (wait && wait->timed_wait(due_time)) {
        g_file_cache_warm_up_rowset_wait_for_compaction_timeout_num << 1;
        LOG_WARNING("the time spent warming up {} rowsets exceeded {} ms",
                    request->rowset_metas().size(), request->sync_wait_timeout_ms());
    }
}

bvar::Adder<uint64_t> g_file_cache_recycle_cache_finished_segment_num(
        "file_cache_recycle_cache_finished_segment_num");
bvar::Adder<uint64_t> g_file_cache_recycle_cache_finished_index_num(
        "file_cache_recycle_cache_finished_index_num");

void CloudInternalServiceImpl::recycle_cache(google::protobuf::RpcController* controller
                                             [[maybe_unused]],
                                             const PRecycleCacheRequest* request,
                                             PRecycleCacheResponse* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);

    if (!config::enable_file_cache) {
        return;
    }
    for (const auto& meta : request->cache_metas()) {
        for (int64_t segment_id = 0; segment_id < meta.num_segments(); segment_id++) {
            auto file_key = Segment::file_cache_key(meta.rowset_id(), segment_id);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
            g_file_cache_recycle_cache_finished_segment_num << 1;
        }

        // inverted index
        for (const auto& file_name : meta.index_file_names()) {
            auto file_key = io::BlockFileCache::hash(file_name);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
            g_file_cache_recycle_cache_finished_index_num << 1;
        }
    }
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
