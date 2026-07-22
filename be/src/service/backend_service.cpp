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

#include "service/backend_service.h"

#include <absl/strings/str_split.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>
#include <gen_cpp/BackendService.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Planner_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <sys/types.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <time.h>

#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/strings/substitute.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "exprs/function/dictionary_factory.h"
#include "format/arrow/arrow_row_batch.h"
#include "io/fs/connectivity/storage_connectivity_tester.h"
#include "io/fs/local_file_system.h"
#include "load/routine_load/routine_load_task_executor.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_recorder.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_profile.h"
#include "service/backend_options.h"
#include "service/backend_service_ingest_helper.h"
#include "service/http/http_client.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/pending_rowset_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/snapshot/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/txn/txn_manager.h"
#include "udf/python/python_env.h"
#include "util/client_cache.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace apache {
namespace thrift {
class TException;
class TMultiplexedProcessor;
class TProcessor;
namespace transport {
class TTransportException;
} // namespace transport
} // namespace thrift
} // namespace apache

namespace doris {

IngestCommitResult::IngestCommitResult(Code c) : code(c) {}
IngestCommitResult::IngestCommitResult(Code c, Status s) : code(c), status(std::move(s)) {}
bool IngestCommitResult::operator==(Code c) const {
    return code == c;
}

IngestCommitResult commit_ingested_rowset(
        StorageEngine& engine, const TabletSharedPtr& local_tablet, int64_t txn_id,
        int64_t partition_id, const RowsetMetaSharedPtr& rowset_meta,
        PendingRowsetGuard pending_rs_guard, MonotonicStopWatch& watch,
        std::unordered_map<std::string_view, uint64_t>& elapsed_time_map) {
    // Step 7.1: create rowset
    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(local_tablet->tablet_schema(),
                                               local_tablet->tablet_path(), rowset_meta, &rowset);
    if (!status) {
        LOG(WARNING) << "failed to create rowset from rowset meta for remote tablet"
                     << ". rowset_id: " << rowset_meta->rowset_id()
                     << ", rowset_type: " << rowset_meta->rowset_type()
                     << ", tablet_id=" << rowset_meta->tablet_id() << ", txn_id=" << txn_id
                     << ", status=" << status.to_string();
        return {IngestCommitResult::kError, std::move(status)};
    }

    // Step 7.2 calculate delete bitmap before commit
    auto calc_delete_bitmap_token = engine.calc_delete_bitmap_executor()->create_token();
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(rowset_meta->tablet_id());
    RowsetIdUnorderedSet pre_rowset_ids;
    if (local_tablet->enable_unique_key_merge_on_write()) {
        auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        status = beta_rowset->load_segments(&segments);
        if (!status) {
            LOG(WARNING) << "failed to load segments from rowset"
                         << ". rowset_id: " << beta_rowset->rowset_id() << ", txn_id=" << txn_id
                         << ", status=" << status.to_string();
            return {IngestCommitResult::kError, std::move(status)};
        }
        elapsed_time_map.emplace("load_segments", watch.elapsed_time_microseconds());
        if (segments.size() > 1) {
            // calculate delete bitmap between segments
            status = local_tablet->calc_delete_bitmap_between_segments(
                    rowset->tablet_schema(), rowset->rowset_id(), segments, delete_bitmap);
            if (!status) {
                LOG(WARNING) << "failed to calculate delete bitmap"
                             << ". tablet_id: " << local_tablet->tablet_id()
                             << ". rowset_id: " << rowset->rowset_id() << ", txn_id=" << txn_id
                             << ", status=" << status.to_string();
                return {IngestCommitResult::kError, std::move(status)};
            }
            elapsed_time_map.emplace("calc_delete_bitmap", watch.elapsed_time_microseconds());
        }

        static_cast<void>(BaseTablet::commit_phase_update_delete_bitmap(
                local_tablet, rowset, pre_rowset_ids, delete_bitmap, segments, txn_id,
                calc_delete_bitmap_token.get(), nullptr));
        elapsed_time_map.emplace("commit_phase_update_delete_bitmap",
                                 watch.elapsed_time_microseconds());
        static_cast<void>(calc_delete_bitmap_token->wait());
        elapsed_time_map.emplace("wait_delete_bitmap", watch.elapsed_time_microseconds());
    }

    // Step 7.3: commit txn
    Status commit_txn_status = engine.txn_manager()->commit_txn(
            local_tablet->data_dir()->get_meta(), rowset_meta->partition_id(),
            rowset_meta->txn_id(), rowset_meta->tablet_id(), local_tablet->tablet_uid(),
            rowset_meta->load_id(), rowset, std::move(pending_rs_guard), false);
    elapsed_time_map.emplace("commit_txn", watch.elapsed_time_microseconds());

    if (!commit_txn_status) {
        if (commit_txn_status.is<ErrorCode::PUSH_TRANSACTION_ALREADY_EXIST>()) {
            LOG(INFO) << "find transaction already exist when commit ingested rowset, skip commit."
                      << " rowset_id: " << rowset_meta->rowset_id().to_string()
                      << ", tablet_id=" << rowset_meta->tablet_id()
                      << ", txn_id=" << rowset_meta->txn_id();
            return IngestCommitResult::kAlreadyExist;
        }
        auto err_msg = fmt::format(
                "failed to commit txn for remote tablet. rowset_id: {}, tablet_id={}, "
                "txn_id={}, status={}",
                rowset_meta->rowset_id().to_string(), rowset_meta->tablet_id(),
                rowset_meta->txn_id(), commit_txn_status.to_string());
        LOG(WARNING) << err_msg;
        return {IngestCommitResult::kError, std::move(commit_txn_status)};
    }

    if (local_tablet->enable_unique_key_merge_on_write()) {
        engine.txn_manager()->set_txn_related_delete_bitmap(
                partition_id, txn_id, rowset_meta->tablet_id(), local_tablet->tablet_uid(), true,
                delete_bitmap, pre_rowset_ids, nullptr);
        elapsed_time_map.emplace("set_txn_related_delete_bitmap",
                                 watch.elapsed_time_microseconds());
    }

    return IngestCommitResult::kCommitted;
}

// Delete files downloaded during ingest. Returns the deletion status so callers can
// update metrics or decide whether additional action is needed. Does not change the
// caller's transaction result; failures are logged so orphan-file issues remain visible.
Status _delete_downloaded_files(const std::vector<std::string>& files, std::string_view reason,
                                int64_t txn_id) {
    if (files.empty()) {
        return Status::OK();
    }
    std::vector<io::Path> paths;
    paths.reserve(files.size());
    for (const auto& file : files) {
        paths.emplace_back(file);
    }
    auto st = io::global_local_filesystem()->batch_delete(paths);
    if (!st.ok()) {
        LOG(WARNING) << "failed to delete " << files.size() << " downloaded files (" << reason
                     << "), txn_id=" << txn_id << ", status=" << st.to_string();
    } else {
        LOG(INFO) << "done delete " << files.size() << " downloaded files (" << reason
                  << "), txn_id=" << txn_id;
    }
    return st;
}

namespace {

bvar::LatencyRecorder g_ingest_binlog_latency("doris_backend_service", "ingest_binlog");

struct IngestBinlogArg {
    int64_t txn_id;
    int64_t partition_id;
    int64_t local_tablet_id;
    TabletSharedPtr local_tablet;
    TIngestBinlogRequest request;
    TStatus* tstatus;
    std::vector<int64_t>* success_replica_backend_ids = nullptr;
    std::vector<int64_t>* failed_replica_backend_ids = nullptr;
    ThreadPool* follower_distribute_pool = nullptr;
};

Status _exec_http_req(std::optional<HttpClient>& client, int retry_times, int sleep_time,
                      const std::function<Status(HttpClient*)>& callback) {
    if (client.has_value()) {
        return client->execute(retry_times, sleep_time, callback);
    } else {
        return HttpClient::execute_with_retry(retry_times, sleep_time, callback);
    }
}

Status _download_binlog_segment_file(HttpClient* client, const std::string& get_segment_file_url,
                                     const std::string& segment_path, uint64_t segment_file_size,
                                     uint64_t estimate_timeout,
                                     std::vector<std::string>& download_success_files,
                                     std::string* file_md5 = nullptr) {
    RETURN_IF_ERROR(client->init(get_segment_file_url));
    client->set_timeout_ms(estimate_timeout * 1000);
    RETURN_IF_ERROR(client->download(segment_path));
    download_success_files.push_back(segment_path);

    std::string remote_file_md5;
    RETURN_IF_ERROR(client->get_content_md5(&remote_file_md5));
    LOG(INFO) << "download segment file to " << segment_path << ", remote md5: " << remote_file_md5
              << ", remote size: " << segment_file_size;

    std::error_code ec;
    // Check file length
    uint64_t local_file_size = std::filesystem::file_size(segment_path, ec);
    if (ec) {
        LOG(WARNING) << "download file error" << ec.message();
        return Status::IOError("can't retrive file_size of {}, due to {}", segment_path,
                               ec.message());
    }

    if (local_file_size != segment_file_size) {
        LOG(WARNING) << "download file length error"
                     << ", get_segment_file_url=" << get_segment_file_url
                     << ", file_size=" << segment_file_size
                     << ", local_file_size=" << local_file_size;
        return Status::RuntimeError("downloaded file size is not equal, local={}, remote={}",
                                    local_file_size, segment_file_size);
    }

    if (!remote_file_md5.empty()) { // keep compatibility
        std::string local_file_md5;
        RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(segment_path, &local_file_md5));
        if (local_file_md5 != remote_file_md5) {
            LOG(WARNING) << "download file md5 error"
                         << ", get_segment_file_url=" << get_segment_file_url
                         << ", remote_file_md5=" << remote_file_md5
                         << ", local_file_md5=" << local_file_md5;
            return Status::RuntimeError("download file md5 is not equal, local={}, remote={}",
                                        local_file_md5, remote_file_md5);
        }
    }

    if (file_md5 != nullptr) {
        if (remote_file_md5.empty()) {
            RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(segment_path, file_md5));
        } else {
            *file_md5 = remote_file_md5;
        }
    }

    return io::global_local_filesystem()->permission(segment_path,
                                                     io::LocalFileSystem::PERMS_OWNER_RW);
}

Status _download_binlog_index_file(HttpClient* client,
                                   const std::string& get_segment_index_file_url,
                                   const std::string& local_segment_index_path,
                                   uint64_t segment_index_file_size, uint64_t estimate_timeout,
                                   std::vector<std::string>& download_success_files,
                                   std::string* file_md5 = nullptr) {
    RETURN_IF_ERROR(client->init(get_segment_index_file_url));
    client->set_timeout_ms(estimate_timeout * 1000);
    RETURN_IF_ERROR(client->download(local_segment_index_path));
    download_success_files.push_back(local_segment_index_path);

    std::string remote_file_md5;
    RETURN_IF_ERROR(client->get_content_md5(&remote_file_md5));

    LOG(INFO) << "download segment index file to " << local_segment_index_path
              << ", remote md5: " << remote_file_md5
              << ", remote size: " << segment_index_file_size;

    std::error_code ec;
    // Check file length
    uint64_t local_index_file_size = std::filesystem::file_size(local_segment_index_path, ec);
    if (ec) {
        LOG(WARNING) << "download index file error" << ec.message();
        return Status::IOError("can't retrive file_size of {}, due to {}", local_segment_index_path,
                               ec.message());
    }
    if (local_index_file_size != segment_index_file_size) {
        LOG(WARNING) << "download index file length error"
                     << ", get_segment_index_file_url=" << get_segment_index_file_url
                     << ", index_file_size=" << segment_index_file_size
                     << ", local_index_file_size=" << local_index_file_size;
        return Status::RuntimeError("downloaded index file size is not equal, local={}, remote={}",
                                    local_index_file_size, segment_index_file_size);
    }

    if (!remote_file_md5.empty()) { // keep compatibility
        std::string local_file_md5;
        RETURN_IF_ERROR(
                io::global_local_filesystem()->md5sum(local_segment_index_path, &local_file_md5));
        if (local_file_md5 != remote_file_md5) {
            LOG(WARNING) << "download file md5 error"
                         << ", get_segment_index_file_url=" << get_segment_index_file_url
                         << ", remote_file_md5=" << remote_file_md5
                         << ", local_file_md5=" << local_file_md5;
            return Status::RuntimeError("download file md5 is not equal, local={}, remote={}",
                                        local_file_md5, remote_file_md5);
        }
    }

    if (file_md5 != nullptr) {
        if (remote_file_md5.empty()) {
            RETURN_IF_ERROR(
                    io::global_local_filesystem()->md5sum(local_segment_index_path, file_md5));
        } else {
            *file_md5 = remote_file_md5;
        }
    }

    return io::global_local_filesystem()->permission(local_segment_index_path,
                                                     io::LocalFileSystem::PERMS_OWNER_RW);
}

Status _download_file_from_peer(const std::string& peer_host, const std::string& peer_http_port,
                                const std::string& peer_token, const std::string& remote_path,
                                const std::string& local_path, uint64_t file_size,
                                const std::string& expected_md5, uint64_t estimate_timeout,
                                std::vector<std::string>& download_success_files) {
    auto remote_file_url =
            fmt::format("http://{}:{}/api/_tablet/_download?token={}&file={}&channel=ingest_binlog",
                        peer_host, peer_http_port, peer_token, remote_path);
    auto download_cb = [&remote_file_url, &local_path, &peer_host, &remote_path, file_size,
                        estimate_timeout, &expected_md5,
                        &download_success_files](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_file_url));
        client->set_timeout_ms(estimate_timeout * 1000);
        RETURN_IF_ERROR(client->download(local_path));
        download_success_files.push_back(local_path);

        LOG(INFO) << "download file from peer host=" << peer_host << " path=" << remote_path
                  << " to " << local_path << ", expected md5: " << expected_md5
                  << ", size: " << file_size;

        std::error_code ec;
        uint64_t local_file_size = std::filesystem::file_size(local_path, ec);
        if (ec) {
            LOG(WARNING) << "download file from peer error " << ec.message();
            return Status::IOError("can't retrieve file_size of {}, due to {}", local_path,
                                   ec.message());
        }
        if (local_file_size != file_size) {
            LOG(WARNING) << "download file from peer length error"
                         << ", peer_host=" << peer_host << ", remote_path=" << remote_path
                         << ", file_size=" << file_size << ", local_file_size=" << local_file_size;
            return Status::RuntimeError("downloaded file size is not equal, local={}, remote={}",
                                        local_file_size, file_size);
        }

        if (!expected_md5.empty()) {
            std::string local_file_md5;
            RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(local_path, &local_file_md5));
            if (local_file_md5 != expected_md5) {
                LOG(WARNING) << "download file from peer md5 error"
                             << ", peer_host=" << peer_host << ", remote_path=" << remote_path
                             << ", expected_md5=" << expected_md5
                             << ", local_file_md5=" << local_file_md5;
                return Status::RuntimeError("downloaded file md5 is not equal, local={}, remote={}",
                                            local_file_md5, expected_md5);
            }
        }

        return io::global_local_filesystem()->permission(local_path,
                                                         io::LocalFileSystem::PERMS_OWNER_RW);
    };
    return HttpClient::execute_with_retry(3, 1, download_cb);
}

void _ingest_binlog_from_peer_impl(StorageEngine& engine, const TIngestBinlogRequest& request,
                                   const TabletSharedPtr& local_tablet, int64_t txn_id,
                                   int64_t partition_id, TStatus& tstatus) {
    auto set_tstatus = [&tstatus](TStatusCode::type code, std::string error_msg) {
        tstatus.__set_status_code(code);
        tstatus.__isset.error_msgs = true;
        tstatus.error_msgs.push_back(std::move(error_msg));
    };

    std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, fmt::format("IngestBinlogFromPeer#TxnId={}", txn_id));
    SCOPED_ATTACH_TASK(mem_tracker);

    auto estimate_download_timeout = [](int64_t file_size) {
        uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }
        return estimate_timeout;
    };

    MonotonicStopWatch watch(true);
    std::unordered_map<std::string_view, uint64_t> elapsed_time_map;
    std::vector<std::string> download_success_files;
    bool commit_already_exist = false;
    Defer defer {[&engine, &tstatus, txn_id, partition_id, &local_tablet, &download_success_files,
                  &commit_already_exist]() {
        if (tstatus.status_code != TStatusCode::OK) {
            engine.txn_manager()->abort_txn(partition_id, txn_id, local_tablet->tablet_id(),
                                            local_tablet->tablet_uid());
            LOG(WARNING) << "will delete downloaded peer files due to error " << tstatus;
            static_cast<void>(
                    _delete_downloaded_files(download_success_files, "peer error cleanup", txn_id));
            return;
        }

        // Follower path has no distribution step. If the transaction was already committed,
        // the rowset files downloaded in this round are redundant and can be deleted immediately.
        if (commit_already_exist && !download_success_files.empty()) {
            LOG(INFO) << "will delete redundant peer files for already-committed txn " << txn_id
                      << ", count=" << download_success_files.size();
            auto cleanup_st = _delete_downloaded_files(download_success_files,
                                                       "redundant peer cleanup", txn_id);
            if (cleanup_st.ok()) {
                DorisMetrics::instance()
                        ->binlog_ingest_redundant_rowset_cleanup_success_total->increment(1);
            } else {
                DorisMetrics::instance()
                        ->binlog_ingest_redundant_rowset_cleanup_failed_total->increment(1);
            }
        }
    }};

    // Check required fields
    if (!request.__isset.rowset_meta || request.rowset_meta.empty()) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "rowset_meta is empty for fetch_from_peer");
        return;
    }
    if (!request.__isset.files) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "files is not set for fetch_from_peer");
        return;
    }
    if (!request.__isset.peer_host || request.peer_host.empty()) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "peer_host is empty for fetch_from_peer");
        return;
    }
    if (!request.__isset.peer_http_port || request.peer_http_port.empty()) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "peer_http_port is empty for fetch_from_peer");
        return;
    }
    if (!request.__isset.peer_token || request.peer_token.empty()) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "peer_token is empty for fetch_from_peer");
        return;
    }

    // Parse rowset meta from leader
    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(request.rowset_meta)) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "failed to parse rowset_meta from peer");
        return;
    }

    // Generate local rowset id and localize tablet uid
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
        set_tstatus(TStatusCode::ANALYSIS_ERROR, "failed to init rowset meta from peer");
        return;
    }
    RowsetId new_rowset_id = engine.next_rowset_id();
    auto pending_rs_guard = engine.pending_local_rowsets().add(new_rowset_id);
    rowset_meta->set_rowset_id(new_rowset_id);
    rowset_meta->set_tablet_uid(local_tablet->tablet_uid());
    rowset_meta->set_tablet_schema_hash(local_tablet->tablet_meta()->schema_hash());

    // Empty rowset (no segments/data files) is valid: skip download and commit directly.
    if (request.files.empty()) {
        if (rowset_meta->num_segments() != 0) {
            set_tstatus(TStatusCode::ANALYSIS_ERROR,
                        "files is empty for fetch_from_peer but rowset has segments");
            return;
        }
        auto commit_result =
                commit_ingested_rowset(engine, local_tablet, txn_id, partition_id, rowset_meta,
                                       std::move(pending_rs_guard), watch, elapsed_time_map);
        if (commit_result == IngestCommitResult::kError) {
            set_tstatus(TStatusCode::RUNTIME_ERROR,
                        fmt::format("failed to commit empty rowset from peer, status={}",
                                    commit_result.status.to_string()));
            return;
        }
        if (commit_result == IngestCommitResult::kAlreadyExist) {
            commit_already_exist = true;
            LOG(INFO) << "ingest binlog from peer empty rowset already committed, txn_id="
                      << txn_id;
        }
        tstatus.__set_status_code(TStatusCode::OK);
        return;
    }

    // Check capacity
    uint64_t total_size = 0;
    for (const auto& file : request.files) {
        if (!file.__isset.size) {
            set_tstatus(TStatusCode::ANALYSIS_ERROR,
                        fmt::format("file size is missing for {}", file.remote_path));
            return;
        }
        total_size += file.size;
    }
    if (!local_tablet->can_add_binlog(total_size)) {
        set_tstatus(TStatusCode::INTERNAL_ERROR,
                    fmt::format("failed to add binlog from peer, no enough space, total_size={}",
                                total_size));
        return;
    }

    // Download files from peer
    for (const auto& file : request.files) {
        if (!file.__isset.remote_path || file.remote_path.empty()) {
            set_tstatus(TStatusCode::ANALYSIS_ERROR, "remote_path is empty in peer file info");
            return;
        }
        if (!file.__isset.segment_index) {
            set_tstatus(TStatusCode::ANALYSIS_ERROR,
                        fmt::format("segment_index is missing for {}", file.remote_path));
            return;
        }

        std::string local_path;
        if (file.__isset.is_index_file && file.is_index_file) {
            auto segment_path =
                    local_segment_path(local_tablet->tablet_path(),
                                       rowset_meta->rowset_id().to_string(), file.segment_index);
            if (file.__isset.index_id && file.index_id != -1) {
                // V1 format
                std::string suffix_path = file.__isset.suffix_path ? file.suffix_path : "";
                local_path = InvertedIndexDescriptor::get_index_file_path_v1(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path),
                        file.index_id, suffix_path);
            } else {
                // V2 format
                local_path = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path));
            }
        } else {
            local_path =
                    local_segment_path(local_tablet->tablet_path(),
                                       rowset_meta->rowset_id().to_string(), file.segment_index);
        }

        uint64_t estimate_timeout = estimate_download_timeout(file.size);
        std::string expected_md5 = file.__isset.md5 ? file.md5 : "";
        auto status = _download_file_from_peer(
                request.peer_host, request.peer_http_port, request.peer_token, file.remote_path,
                local_path, file.size, expected_md5, estimate_timeout, download_success_files);
        if (!status.ok()) {
            set_tstatus(TStatusCode::RUNTIME_ERROR, status.to_string());
            return;
        }
    }
    elapsed_time_map.emplace("download_files_from_peer", watch.elapsed_time_microseconds());

    // Commit rowset
    auto commit_result =
            commit_ingested_rowset(engine, local_tablet, txn_id, partition_id, rowset_meta,
                                   std::move(pending_rs_guard), watch, elapsed_time_map);
    if (commit_result == IngestCommitResult::kError) {
        set_tstatus(TStatusCode::RUNTIME_ERROR,
                    fmt::format("failed to commit ingested rowset from peer, status={}",
                                commit_result.status.to_string()));
        return;
    }
    if (commit_result == IngestCommitResult::kAlreadyExist) {
        commit_already_exist = true;
        LOG(INFO) << "ingest binlog from peer txn already committed, will clean up redundant "
                     "files, txn_id="
                  << txn_id << ", file_count=" << download_success_files.size();
    }

    tstatus.__set_status_code(TStatusCode::OK);
}

Status _distribute_ingested_rowset_to_followers(
        StorageEngine& engine, const TIngestBinlogRequest& request,
        const RowsetMetaSharedPtr& rowset_meta,
        const std::vector<TIngestedFileInfo>& ingested_files,
        std::vector<int64_t>& success_backend_ids, std::vector<int64_t>& failed_backend_ids,
        ThreadPool* distribute_pool, const std::shared_ptr<MemTrackerLimiter>& parent_mem_tracker) {
    if (!request.__isset.follower_replicas || request.follower_replicas.empty()) {
        return Status::OK();
    }

    std::string rowset_meta_str;
    if (!rowset_meta->serialize(&rowset_meta_str)) {
        return Status::InternalError("failed to serialize rowset meta for followers");
    }

    std::string peer_host = BackendOptions::get_localhost();
    std::string peer_http_port = std::to_string(config::webserver_port);
    std::string peer_token = ExecEnv::GetInstance()->token();

    uint64_t total_file_size = 0;
    for (const auto& file : ingested_files) {
        if (file.__isset.size) {
            total_file_size += file.size;
        }
    }
    uint64_t estimate_timeout_s = total_file_size / config::download_low_speed_limit_kbps / 1024;
    if (estimate_timeout_s < config::download_low_speed_time) {
        estimate_timeout_s = config::download_low_speed_time;
    }
    estimate_timeout_s = estimate_timeout_s * 3 / 2; // 1.5x margin
    int timeout_ms = static_cast<int>(std::min(estimate_timeout_s, static_cast<uint64_t>(7200)) *
                                      1000); // cap 2h

    // Validate all follower infos before launching any RPC. Invalid ones are recorded as failed
    // so that the caller can decide to fallback instead of aborting the already-committed leader.
    struct FollowerTask {
        int64_t backend_id;
        std::string host;
        int32_t be_port;
    };
    std::vector<FollowerTask> valid_followers;
    valid_followers.reserve(request.follower_replicas.size());
    for (const auto& follower : request.follower_replicas) {
        if (!follower.__isset.backend_id || !follower.__isset.host || !follower.__isset.be_port) {
            int64_t bad_id = follower.__isset.backend_id ? follower.backend_id : -1;
            LOG(WARNING) << "invalid follower replica info, backend_id=" << bad_id;
            failed_backend_ids.push_back(bad_id);
            continue;
        }
        valid_followers.push_back({follower.backend_id, follower.host, follower.be_port});
    }

    std::vector<std::future<std::pair<int64_t, Status>>> futures;
    futures.reserve(valid_followers.size());

    for (const auto& task : valid_followers) {
        int64_t backend_id = task.backend_id;
        std::string host = task.host;
        int32_t be_port = task.be_port;

        auto promise_ptr = std::make_shared<std::promise<std::pair<int64_t, Status>>>();
        futures.push_back(promise_ptr->get_future());

        auto worker = [promise_ptr, backend_id, host, be_port, timeout_ms, &request,
                       &rowset_meta_str, &ingested_files, &peer_host, &peer_http_port, &peer_token,
                       parent_mem_tracker]() {
            SCOPED_ATTACH_TASK(parent_mem_tracker);
            try {
                TIngestBinlogResult follower_result;
                TIngestBinlogRequest follower_request;
                follower_request.__set_txn_id(request.txn_id);
                follower_request.__set_partition_id(request.partition_id);
                follower_request.__set_local_tablet_id(request.local_tablet_id);
                follower_request.__set_load_id(request.load_id);
                follower_request.__set_fetch_from_peer(true);
                follower_request.__set_peer_host(peer_host);
                follower_request.__set_peer_http_port(peer_http_port);
                follower_request.__set_peer_token(peer_token);
                follower_request.__set_rowset_meta(rowset_meta_str);
                follower_request.__set_files(ingested_files);

                DBUG_EXECUTE_IF("ingest_binlog.follower.force_fail", {
                    auto target_backend_id =
                            DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                                    "ingest_binlog.follower.force_fail", "backend_id", -1);
                    if (target_backend_id == -1 || target_backend_id == backend_id) {
                        LOG(WARNING) << "debug point force follower ingest_binlog fail, "
                                     << "backend_id=" << backend_id;
                        promise_ptr->set_value(std::make_pair(
                                backend_id,
                                Status::InternalError("debug point force follower fail")));
                        return;
                    }
                });

                Status status = ThriftRpcHelper::rpc<BackendServiceClient>(
                        host, be_port,
                        [&follower_request,
                         &follower_result](ClientConnection<BackendServiceClient>& client) {
                            client->ingest_binlog(follower_result, follower_request);
                        },
                        timeout_ms);
                if (!status.ok()) {
                    LOG(WARNING) << "failed to send ingest_binlog to follower " << host << ":"
                                 << be_port << ", backend_id=" << backend_id
                                 << ", status=" << status.to_string();
                    promise_ptr->set_value(std::make_pair(backend_id, status));
                    return;
                }
                if (follower_result.status.status_code != TStatusCode::OK) {
                    status = Status::create(follower_result.status);
                    LOG(WARNING) << "follower ingest_binlog failed, backend_id=" << backend_id
                                 << ", status=" << status.to_string();
                    promise_ptr->set_value(std::make_pair(backend_id, status));
                    return;
                }
                promise_ptr->set_value(std::make_pair(backend_id, Status::OK()));
            } catch (const std::exception& e) {
                LOG(WARNING) << "follower ingest_binlog task threw exception, backend_id="
                             << backend_id << ", exception=" << e.what();
                promise_ptr->set_value(std::make_pair(backend_id, Status::InternalError(e.what())));
            }
        };

        if (distribute_pool != nullptr) {
            Status st = distribute_pool->submit_func(worker);
            if (st.ok()) {
                continue;
            }
            // The pool queue is full. Fall back to inline execution in the thrift
            // handler thread instead of failing the follower: this transfers
            // backpressure to the caller (CCR acquires a per-backend concurrency
            // window for every ingest) and avoids spurious whole-txn retries that
            // would waste the cross-cluster download this feature saves.
            LOG(WARNING) << "ingest binlog follower distribute pool is full, run follower "
                            "distribution inline, backend_id="
                         << backend_id << ", status=" << st.to_string();
        }
        worker();
    }

    for (auto& future : futures) {
        auto [backend_id, status] = future.get();
        if (status.ok()) {
            success_backend_ids.push_back(backend_id);
        } else {
            failed_backend_ids.push_back(backend_id);
        }
    }

    if (!failed_backend_ids.empty()) {
        return Status::RuntimeError("{} follower(s) failed to ingest from peer",
                                    failed_backend_ids.size());
    }
    return Status::OK();
}

void _ingest_binlog(StorageEngine& engine, IngestBinlogArg* arg) {
    std::optional<HttpClient> client;
    if (config::enable_ingest_binlog_with_persistent_connection) {
        // Save the http client instance for persistent connection
        client = std::make_optional<HttpClient>();
    }

    auto txn_id = arg->txn_id;
    auto partition_id = arg->partition_id;
    auto local_tablet_id = arg->local_tablet_id;
    const auto& local_tablet = arg->local_tablet;
    const auto& local_tablet_uid = local_tablet->tablet_uid();

    std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, fmt::format("IngestBinlog#TxnId={}", txn_id));
    SCOPED_ATTACH_TASK(mem_tracker);

    auto& request = arg->request;

    MonotonicStopWatch watch(true);
    int64_t total_download_bytes = 0;
    int64_t total_download_files = 0;
    TStatus tstatus;
    std::vector<std::string> download_success_files;
    std::unordered_map<std::string_view, uint64_t> elapsed_time_map;
    bool is_single_replica_download =
            request.__isset.single_replica_download && request.single_replica_download;
    std::vector<TIngestedFileInfo> ingested_files;
    bool committed = false;
    bool commit_already_exist = false;
    bool distribution_done = false;
    std::vector<std::string> redundant_files_to_delete;
    Defer defer {[=, &engine, &tstatus, ingest_binlog_tstatus = arg->tstatus, &watch,
                  &total_download_bytes, &total_download_files, &elapsed_time_map,
                  &download_success_files, &committed, &commit_already_exist, &distribution_done,
                  &redundant_files_to_delete]() {
        g_ingest_binlog_latency << watch.elapsed_time_microseconds();
        auto elapsed_time_ms = watch.elapsed_time_milliseconds();
        double copy_rate = 0.0;
        if (elapsed_time_ms > 0) {
            copy_rate = (double)total_download_bytes / ((double)elapsed_time_ms) / 1000;
        }
        LOG(INFO) << "ingest binlog elapsed " << elapsed_time_ms << " ms, download "
                  << total_download_files << " files, total " << total_download_bytes
                  << " bytes, avg rate " << copy_rate
                  << " MB/s. result: " << apache::thrift::ThriftDebugString(tstatus);
        if (config::ingest_binlog_elapsed_threshold_ms >= 0 &&
            elapsed_time_ms > config::ingest_binlog_elapsed_threshold_ms) {
            auto elapsed_details_view =
                    elapsed_time_map | std::views::transform([](const auto& pair) {
                        return fmt::format("{}:{}", pair.first, pair.second);
                    });
            std::string elapsed_details = fmt::format("{}", fmt::join(elapsed_details_view, ", "));
            LOG(WARNING) << "ingest binlog elapsed " << elapsed_time_ms << " ms, "
                         << elapsed_details;
        }
        if (tstatus.status_code != TStatusCode::OK && !committed) {
            // abort txn
            engine.txn_manager()->abort_txn(partition_id, txn_id, local_tablet_id,
                                            local_tablet_uid);
            // delete all successfully downloaded files
            LOG(WARNING) << "will delete downloaded success files due to error " << tstatus;
            static_cast<void>(_delete_downloaded_files(download_success_files,
                                                       "leader error cleanup", txn_id));
        }

        // When the transaction was already committed by a previous attempt, the rowset files
        // downloaded in this round (R2) are redundant after follower distribution completes.
        // Delete them to avoid orphan files, but only after distribution is done because
        // followers may still be fetching these files via HTTP.
        if (commit_already_exist && distribution_done && !redundant_files_to_delete.empty()) {
            LOG(INFO) << "will delete redundant rowset files downloaded for already-committed txn "
                      << txn_id << ", count=" << redundant_files_to_delete.size();
            auto cleanup_st = _delete_downloaded_files(redundant_files_to_delete,
                                                       "leader redundant cleanup", txn_id);
            if (cleanup_st.ok()) {
                DorisMetrics::instance()
                        ->binlog_ingest_redundant_rowset_cleanup_success_total->increment(1);
            } else {
                DorisMetrics::instance()
                        ->binlog_ingest_redundant_rowset_cleanup_failed_total->increment(1);
            }
        }

        if (ingest_binlog_tstatus) {
            *ingest_binlog_tstatus = std::move(tstatus);
        }
    }};

    auto estimate_download_timeout = [](int64_t file_size) {
        uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }
        return estimate_timeout;
    };

    // Step 3: get binlog info
    auto binlog_api_url = fmt::format("http://{}:{}/api/_binlog/_download", request.remote_host,
                                      request.remote_port);
    constexpr int max_retry = 3;

    auto get_binlog_info_url =
            fmt::format("{}?method={}&tablet_id={}&binlog_version={}", binlog_api_url,
                        "get_binlog_info", request.remote_tablet_id, request.binlog_version);
    std::string binlog_info;
    auto get_binlog_info_cb = [&get_binlog_info_url, &binlog_info](HttpClient* client) {
        RETURN_IF_ERROR(client->init(get_binlog_info_url));
        client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
        return client->execute(&binlog_info);
    };
    auto status = _exec_http_req(client, max_retry, 1, get_binlog_info_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get binlog info from " << get_binlog_info_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }
    elapsed_time_map.emplace("get_binlog_info", watch.elapsed_time_microseconds());

    std::vector<std::string> binlog_info_parts = absl::StrSplit(binlog_info, ":");
    if (binlog_info_parts.size() != 2) {
        status = Status::RuntimeError("failed to parse binlog info into 2 parts: {}", binlog_info);
        LOG(WARNING) << "failed to get binlog info from " << get_binlog_info_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }
    std::string remote_rowset_id = std::move(binlog_info_parts[0]);
    int64_t num_segments = -1;
    try {
        num_segments = std::stoll(binlog_info_parts[1]);
    } catch (std::exception& e) {
        status = Status::RuntimeError("failed to parse num segments from binlog info {}: {}",
                                      binlog_info, e.what());
        LOG(WARNING) << "failed to get binlog info from " << get_binlog_info_url
                     << ", status=" << status;
        status.to_thrift(&tstatus);
        return;
    }

    // Step 4: get rowset meta
    auto get_rowset_meta_url = fmt::format(
            "{}?method={}&tablet_id={}&rowset_id={}&binlog_version={}", binlog_api_url,
            "get_rowset_meta", request.remote_tablet_id, remote_rowset_id, request.binlog_version);
    std::string rowset_meta_str;
    auto get_rowset_meta_cb = [&get_rowset_meta_url, &rowset_meta_str](HttpClient* client) {
        RETURN_IF_ERROR(client->init(get_rowset_meta_url));
        client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
        return client->execute(&rowset_meta_str);
    };
    status = _exec_http_req(client, max_retry, 1, get_rowset_meta_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get rowset meta from " << get_rowset_meta_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }
    elapsed_time_map.emplace("get_rowset_meta", watch.elapsed_time_microseconds());

    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(rowset_meta_str)) {
        LOG(WARNING) << "failed to parse rowset meta from " << get_rowset_meta_url;
        status = Status::InternalError("failed to parse rowset meta");
        status.to_thrift(&tstatus);
        return;
    }
    // save source rowset id and tablet id
    rowset_meta_pb.set_source_rowset_id(remote_rowset_id);
    rowset_meta_pb.set_source_tablet_id(request.remote_tablet_id);
    // rewrite rowset meta
    rowset_meta_pb.set_tablet_id(local_tablet_id);
    rowset_meta_pb.set_partition_id(partition_id);
    rowset_meta_pb.set_tablet_schema_hash(local_tablet->tablet_meta()->schema_hash());
    rowset_meta_pb.set_txn_id(txn_id);
    rowset_meta_pb.set_rowset_state(RowsetStatePB::COMMITTED);
    // Unify load id: both prepare_txn and commit_txn use the load id from the ingest request,
    // so retries of the same transaction hit the idempotent short-circuit instead of replacing
    // the already-committed rowset.
    rowset_meta_pb.mutable_load_id()->set_hi(request.load_id.hi);
    rowset_meta_pb.mutable_load_id()->set_lo(request.load_id.lo);
    auto rowset_meta = std::make_shared<RowsetMeta>();
    if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
        LOG(WARNING) << "failed to init rowset meta from " << get_rowset_meta_url;
        status = Status::InternalError("failed to init rowset meta");
        status.to_thrift(&tstatus);
        return;
    }
    RowsetId new_rowset_id = engine.next_rowset_id();
    auto pending_rs_guard = engine.pending_local_rowsets().add(new_rowset_id);
    rowset_meta->set_rowset_id(new_rowset_id);
    rowset_meta->set_tablet_uid(local_tablet->tablet_uid());

    // Step 5: get all segment files
    // Step 5.1: get all segment files size
    std::vector<std::string> segment_file_urls;
    segment_file_urls.reserve(num_segments);
    std::vector<uint64_t> segment_file_sizes;
    segment_file_sizes.reserve(num_segments);
    for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
        auto get_segment_file_size_url = fmt::format(
                "{}?method={}&tablet_id={}&rowset_id={}&segment_index={}", binlog_api_url,
                "get_segment_file", request.remote_tablet_id, remote_rowset_id, segment_index);
        uint64_t segment_file_size;
        auto get_segment_file_size_cb = [&get_segment_file_size_url,
                                         &segment_file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(get_segment_file_size_url));
            client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
            RETURN_IF_ERROR(client->head());
            return client->get_content_length(&segment_file_size);
        };

        status = _exec_http_req(client, max_retry, 1, get_segment_file_size_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file size from " << get_segment_file_size_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }

        segment_file_sizes.push_back(segment_file_size);
        segment_file_urls.push_back(std::move(get_segment_file_size_url));
    }
    elapsed_time_map.emplace("get_segment_file_size", watch.elapsed_time_microseconds());

    // Step 5.2: check data capacity
    uint64_t total_size = std::accumulate(segment_file_sizes.begin(), segment_file_sizes.end(),
                                          0ULL); // NOLINT(bugprone-fold-init-type)
    if (!local_tablet->can_add_binlog(total_size)) {
        LOG(WARNING) << "failed to add binlog, no enough space, total_size=" << total_size
                     << ", tablet=" << local_tablet->tablet_id();
        status = Status::InternalError("no enough space");
        status.to_thrift(&tstatus);
        return;
    }
    total_download_bytes = total_size;
    total_download_files = num_segments;

    // Step 5.3: get all segment files
    for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
        auto segment_file_size = segment_file_sizes[segment_index];
        auto get_segment_file_url = segment_file_urls[segment_index];
        if (config::enable_download_md5sum_check) {
            get_segment_file_url = fmt::format("{}&acquire_md5=true", get_segment_file_url);
        }

        auto segment_path = local_segment_path(local_tablet->tablet_path(),
                                               rowset_meta->rowset_id().to_string(), segment_index);
        LOG(INFO) << "download segment file from " << get_segment_file_url << " to "
                  << segment_path;
        uint64_t estimate_timeout = estimate_download_timeout(segment_file_size);
        std::string segment_file_md5;
        std::string* segment_file_md5_ptr =
                is_single_replica_download ? &segment_file_md5 : nullptr;
        auto get_segment_file_cb = [&get_segment_file_url, &segment_path, segment_file_size,
                                    estimate_timeout, &download_success_files,
                                    segment_file_md5_ptr](HttpClient* client) {
            return _download_binlog_segment_file(client, get_segment_file_url, segment_path,
                                                 segment_file_size, estimate_timeout,
                                                 download_success_files, segment_file_md5_ptr);
        };

        status = _exec_http_req(client, max_retry, 1, get_segment_file_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file from " << get_segment_file_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }

        if (is_single_replica_download) {
            TIngestedFileInfo file_info;
            file_info.__set_remote_path(segment_path);
            file_info.__set_size(segment_file_size);
            file_info.__set_segment_index(static_cast<int32_t>(segment_index));
            file_info.__set_index_id(-1);
            file_info.__set_is_index_file(false);
            if (!segment_file_md5.empty()) {
                file_info.__set_md5(segment_file_md5);
            }
            ingested_files.push_back(std::move(file_info));
        }
    }
    elapsed_time_map.emplace("get_segment_files", watch.elapsed_time_microseconds());

    // Step 6: get all segment index files
    // Step 6.1: get all segment index files size
    std::vector<std::string> segment_index_file_urls;
    std::vector<uint64_t> segment_index_file_sizes;
    std::vector<std::string> segment_index_file_names;
    std::vector<int32_t> segment_index_file_segment_indices;
    std::vector<int64_t> segment_index_file_index_ids;
    std::vector<std::string> segment_index_file_suffix_paths;
    auto tablet_schema = rowset_meta->tablet_schema();
    if (tablet_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        for (const auto& index : tablet_schema->inverted_indexes()) {
            auto index_id = index->index_id();
            for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
                auto get_segment_index_file_size_url = fmt::format(
                        "{}?method={}&tablet_id={}&rowset_id={}&segment_index={}&segment_index_id={"
                        "}",
                        binlog_api_url, "get_segment_index_file", request.remote_tablet_id,
                        remote_rowset_id, segment_index, index_id);
                uint64_t segment_index_file_size;
                auto get_segment_index_file_size_cb =
                        [&get_segment_index_file_size_url,
                         &segment_index_file_size](HttpClient* client) {
                            RETURN_IF_ERROR(client->init(get_segment_index_file_size_url));
                            client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
                            RETURN_IF_ERROR(client->head());
                            return client->get_content_length(&segment_index_file_size);
                        };

                auto segment_path =
                        local_segment_path(local_tablet->tablet_path(),
                                           rowset_meta->rowset_id().to_string(), segment_index);
                segment_index_file_names.push_back(InvertedIndexDescriptor::get_index_file_path_v1(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path), index_id,
                        index->get_index_suffix()));
                segment_index_file_segment_indices.push_back(static_cast<int32_t>(segment_index));
                segment_index_file_index_ids.push_back(index_id);
                segment_index_file_suffix_paths.push_back(index->get_index_suffix());

                status = _exec_http_req(client, max_retry, 1, get_segment_index_file_size_cb);
                if (!status.ok()) {
                    LOG(WARNING) << "failed to get segment file size from "
                                 << get_segment_index_file_size_url
                                 << ", status=" << status.to_string();
                    status.to_thrift(&tstatus);
                    return;
                }

                segment_index_file_sizes.push_back(segment_index_file_size);
                segment_index_file_urls.push_back(std::move(get_segment_index_file_size_url));
            }
        }
    } else {
        for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
            if (tablet_schema->has_inverted_index() || tablet_schema->has_ann_index()) {
                auto get_segment_index_file_size_url = fmt::format(
                        "{}?method={}&tablet_id={}&rowset_id={}&segment_index={}&segment_index_id={"
                        "}",
                        binlog_api_url, "get_segment_index_file", request.remote_tablet_id,
                        remote_rowset_id, segment_index, -1);
                uint64_t segment_index_file_size;
                auto get_segment_index_file_size_cb =
                        [&get_segment_index_file_size_url,
                         &segment_index_file_size](HttpClient* client) {
                            RETURN_IF_ERROR(client->init(get_segment_index_file_size_url));
                            client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
                            RETURN_IF_ERROR(client->head());
                            return client->get_content_length(&segment_index_file_size);
                        };
                auto segment_path =
                        local_segment_path(local_tablet->tablet_path(),
                                           rowset_meta->rowset_id().to_string(), segment_index);
                segment_index_file_names.push_back(InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)));
                segment_index_file_segment_indices.push_back(static_cast<int32_t>(segment_index));
                segment_index_file_index_ids.push_back(-1);
                segment_index_file_suffix_paths.emplace_back();

                status = _exec_http_req(client, max_retry, 1, get_segment_index_file_size_cb);
                if (!status.ok()) {
                    LOG(WARNING) << "failed to get segment file size from "
                                 << get_segment_index_file_size_url
                                 << ", status=" << status.to_string();
                    status.to_thrift(&tstatus);
                    return;
                }

                segment_index_file_sizes.push_back(segment_index_file_size);
                segment_index_file_urls.push_back(std::move(get_segment_index_file_size_url));
            }
        }
    }
    elapsed_time_map.emplace("get_segment_index_file_size", watch.elapsed_time_microseconds());

    // Step 6.2: check data capacity
    uint64_t total_index_size =
            std::accumulate(segment_index_file_sizes.begin(), segment_index_file_sizes.end(),
                            0ULL); // NOLINT(bugprone-fold-init-type)
    if (!local_tablet->can_add_binlog(total_index_size)) {
        LOG(WARNING) << "failed to add binlog, no enough space, total_index_size="
                     << total_index_size << ", tablet=" << local_tablet->tablet_id();
        status = Status::InternalError("no enough space");
        status.to_thrift(&tstatus);
        return;
    }
    total_download_bytes += total_index_size;
    total_download_files += segment_index_file_urls.size();

    // Step 6.3: get all segment index files
    DCHECK(segment_index_file_sizes.size() == segment_index_file_names.size());
    DCHECK(segment_index_file_names.size() == segment_index_file_urls.size());
    DCHECK(segment_index_file_names.size() == segment_index_file_segment_indices.size());
    DCHECK(segment_index_file_names.size() == segment_index_file_index_ids.size());
    DCHECK(segment_index_file_names.size() == segment_index_file_suffix_paths.size());
    for (int64_t i = 0; i < segment_index_file_urls.size(); ++i) {
        auto segment_index_file_size = segment_index_file_sizes[i];
        auto get_segment_index_file_url = segment_index_file_urls[i];
        if (config::enable_download_md5sum_check) {
            get_segment_index_file_url =
                    fmt::format("{}&acquire_md5=true", get_segment_index_file_url);
        }

        uint64_t estimate_timeout = estimate_download_timeout(segment_index_file_size);
        auto local_segment_index_path = segment_index_file_names[i];
        LOG(INFO) << fmt::format("download segment index file from {} to {}",
                                 get_segment_index_file_url, local_segment_index_path);
        std::string index_file_md5;
        std::string* index_file_md5_ptr = is_single_replica_download ? &index_file_md5 : nullptr;
        auto get_segment_index_file_cb = [&get_segment_index_file_url, &local_segment_index_path,
                                          segment_index_file_size, estimate_timeout,
                                          &download_success_files,
                                          index_file_md5_ptr](HttpClient* client) {
            return _download_binlog_index_file(client, get_segment_index_file_url,
                                               local_segment_index_path, segment_index_file_size,
                                               estimate_timeout, download_success_files,
                                               index_file_md5_ptr);
        };

        status = _exec_http_req(client, max_retry, 1, get_segment_index_file_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment index file from " << get_segment_index_file_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }

        if (is_single_replica_download) {
            TIngestedFileInfo file_info;
            file_info.__set_remote_path(local_segment_index_path);
            file_info.__set_size(segment_index_file_size);
            file_info.__set_segment_index(segment_index_file_segment_indices[i]);
            file_info.__set_index_id(segment_index_file_index_ids[i]);
            file_info.__set_suffix_path(segment_index_file_suffix_paths[i]);
            file_info.__set_is_index_file(true);
            if (!index_file_md5.empty()) {
                file_info.__set_md5(index_file_md5);
            }
            ingested_files.push_back(std::move(file_info));
        }
    }
    elapsed_time_map.emplace("get_segment_index_files", watch.elapsed_time_microseconds());

    // Step 7: create rowset && calculate delete bitmap && commit
    auto commit_result =
            commit_ingested_rowset(engine, local_tablet, txn_id, partition_id, rowset_meta,
                                   std::move(pending_rs_guard), watch, elapsed_time_map);
    if (commit_result == IngestCommitResult::kError) {
        status = Status::RuntimeError("failed to commit ingested rowset, status={}",
                                      commit_result.status.to_string());
        status.to_thrift(&tstatus);
        return;
    }
    if (commit_result == IngestCommitResult::kAlreadyExist) {
        commit_already_exist = true;
        // The current round files (R2) are redundant on the leader because a previous attempt
        // already committed R1. We still need them for follower distribution below; schedule
        // cleanup after distribution completes.
        redundant_files_to_delete.assign(download_success_files.begin(),
                                         download_success_files.end());
        LOG(INFO) << "ingest binlog txn already committed, will distribute current files to "
                     "followers and then clean up redundant files, txn_id="
                  << txn_id << ", file_count=" << redundant_files_to_delete.size();
    } else {
        committed = true;
    }

    // Step 8: distribute to followers if single replica download
    if (is_single_replica_download) {
        DCHECK(arg->success_replica_backend_ids != nullptr);
        DCHECK(arg->failed_replica_backend_ids != nullptr);
        status = _distribute_ingested_rowset_to_followers(
                engine, request, rowset_meta, ingested_files, *arg->success_replica_backend_ids,
                *arg->failed_replica_backend_ids, arg->follower_distribute_pool, mem_tracker);
        if (!status) {
            LOG(WARNING) << "distribute ingested rowset to followers partially failed, success="
                         << arg->success_replica_backend_ids->size()
                         << ", failed=" << arg->failed_replica_backend_ids->size()
                         << ", status=" << status.to_string();
            // Do NOT set tstatus to error and do NOT delete files. The rowset is already
            // committed on the leader; downstream syncer will retry/fallback based on the
            // success/failed replica backend id lists.
        }
    }
    distribution_done = true;

    tstatus.__set_status_code(TStatusCode::OK);
}
} // namespace

BaseBackendService::BaseBackendService(ExecEnv* exec_env)
        : _exec_env(exec_env), _agent_server(new AgentServer(exec_env, exec_env->cluster_info())) {}

BaseBackendService::~BaseBackendService() = default;

BackendService::BackendService(StorageEngine& engine, ExecEnv* exec_env)
        : BaseBackendService(exec_env), _engine(engine) {}

BackendService::~BackendService() = default;

Status BackendService::start_thrift_dependencies() {
    _agent_server->start_workers(_engine, _exec_env);

    auto thread_num = config::ingest_binlog_work_pool_size;
    if (thread_num < 0) {
        LOG(INFO) << fmt::format("ingest binlog work pool size is {}, so we will in sync mode",
                                 thread_num);
    } else {
        if (thread_num == 0) {
            thread_num = std::thread::hardware_concurrency();
        }
        RETURN_IF_ERROR(doris::ThreadPoolBuilder("IngestBinlog")
                                .set_min_threads(thread_num)
                                .set_max_threads(thread_num * 2)
                                .build(&_ingest_binlog_workers));
        LOG(INFO) << fmt::format("ingest binlog work pool size is {}, in async mode", thread_num);
    }

    // Always create the follower distribution pool for single-replica ingest binlog,
    // regardless of whether the legacy async ingest pool is enabled. This turns follower
    // fan-out from serial RPC execution into parallel execution bounded by the pool size.
    // When the pool queue is full, the follower task falls back to inline execution
    // instead of being rejected, so a busy pool never fails an ingest by itself.
    auto distribute_thread_num = config::ingest_binlog_distribute_work_pool_size;
    if (distribute_thread_num < 0) {
        return Status::InvalidArgument(
                "ingest_binlog_distribute_work_pool_size must be non-negative, got {}",
                distribute_thread_num);
    }
    if (distribute_thread_num == 0) {
        auto hc = static_cast<int>(std::thread::hardware_concurrency());
        distribute_thread_num = hc > 0 ? hc : 1;
    }
    RETURN_IF_ERROR(doris::ThreadPoolBuilder("IngestBinlogDistribute")
                            .set_min_threads(0)
                            .set_max_threads(distribute_thread_num)
                            .set_max_queue_size(distribute_thread_num * 4)
                            .build(&_ingest_binlog_distribute_workers));
    LOG(INFO) << fmt::format("ingest binlog distribute work pool size is {}",
                             distribute_thread_num);
    return Status::OK();
}

void BackendService::get_tablet_stat(TTabletStatResult& result) {
    _engine.tablet_manager()->get_tablet_stat(&result);
}

int64_t BackendService::get_trash_used_capacity() {
    int64_t result = 0;

    std::vector<DataDirInfo> data_dir_infos;
    static_cast<void>(_engine.get_all_data_dir_info(&data_dir_infos, false /*do not update */));

    // uses excute sql `show trash`, then update backend trash capacity too.
    _engine.notify_listener("REPORT_DISK_STATE");

    for (const auto& root_path_info : data_dir_infos) {
        result += root_path_info.trash_used_capacity;
    }

    return result;
}

void BackendService::get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) {
    std::vector<DataDirInfo> data_dir_infos;
    static_cast<void>(_engine.get_all_data_dir_info(&data_dir_infos, false /*do not update */));

    // uses excute sql `show trash on <be>`, then update backend trash capacity too.
    _engine.notify_listener("REPORT_DISK_STATE");

    for (const auto& root_path_info : data_dir_infos) {
        TDiskTrashInfo diskTrashInfo;
        diskTrashInfo.__set_root_path(root_path_info.path);
        diskTrashInfo.__set_state(root_path_info.is_used ? "ONLINE" : "OFFLINE");
        diskTrashInfo.__set_trash_used_capacity(root_path_info.trash_used_capacity);
        diskTrashInfos.push_back(diskTrashInfo);
    }
}

void BaseBackendService::submit_routine_load_task(TStatus& t_status,
                                                  const std::vector<TRoutineLoadTask>& tasks) {
    for (auto& task : tasks) {
        Status st = _exec_env->routine_load_task_executor()->submit_task(task);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit routine load task. job id: " << task.job_id
                         << " task id: " << task.id;
            return st.to_thrift(&t_status);
        }
    }

    return Status::OK().to_thrift(&t_status);
}

/*
 * 1. validate user privilege (todo)
 * 2. FragmentMgr#exec_plan_fragment
 */
void BaseBackendService::open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) {
    TStatus t_status;
    TUniqueId fragment_instance_id = generate_uuid();
    // A query_id is randomly generated to replace t_query_plan_info.query_id.
    // external query does not need to report anything to FE, so the query_id can be changed.
    // Otherwise, multiple independent concurrent open tablet scanners have the same query_id.
    // when one of the scanners ends, the other scanners will be canceled through FragmentMgr.cancel(query_id).
    TUniqueId query_id = generate_uuid();
    std::shared_ptr<ScanContext> p_context;
    static_cast<void>(_exec_env->external_scan_context_mgr()->create_scan_context(&p_context));
    p_context->fragment_instance_id = fragment_instance_id;
    p_context->offset = 0;
    p_context->last_access_time = time(nullptr);
    if (params.__isset.keep_alive_min) {
        p_context->keep_alive_min = params.keep_alive_min;
    } else {
        p_context->keep_alive_min = 5;
    }

    Status exec_st;
    TQueryPlanInfo t_query_plan_info;
    {
        const std::string& opaqued_query_plan = params.opaqued_query_plan;
        std::string query_plan_info;
        // base64 decode query plan
        if (!base64_decode(opaqued_query_plan, &query_plan_info)) {
            LOG(WARNING) << "open context error: base64_decode decode opaqued_query_plan failure";
            std::stringstream msg;
            msg << "query_plan_info: " << query_plan_info
                << " validate error, should not be modified after returned Doris FE processed";
            exec_st = Status::InvalidArgument(msg.str());
        }

        const uint8_t* buf = (const uint8_t*)query_plan_info.data();
        uint32_t len = (uint32_t)query_plan_info.size();
        // deserialize TQueryPlanInfo
        auto st = deserialize_thrift_msg(buf, &len, false, &t_query_plan_info);
        if (!st.ok()) {
            LOG(WARNING) << "open context error: deserialize TQueryPlanInfo failure";
            std::stringstream msg;
            msg << "query_plan_info: " << query_plan_info
                << " deserialize error, should not be modified after returned Doris FE processed";
            exec_st = Status::InvalidArgument(msg.str());
        }
        p_context->query_id = query_id;
    }
    std::vector<TScanColumnDesc> selected_columns;
    if (exec_st.ok()) {
        // start the scan procedure
        LOG(INFO) << fmt::format(
                "exec external scanner, old_query_id = {}, new_query_id = {}, fragment_instance_id "
                "= {}",
                print_id(t_query_plan_info.query_id), print_id(query_id),
                print_id(fragment_instance_id));
        exec_st = _exec_env->fragment_mgr()->exec_external_plan_fragment(
                params, t_query_plan_info, query_id, fragment_instance_id, &selected_columns);
    }
    exec_st.to_thrift(&t_status);
    //return status
    // t_status.status_code = TStatusCode::OK;
    result_.status = t_status;
    result_.__set_context_id(p_context->context_id);
    result_.__set_selected_columns(selected_columns);
}

// fetch result from polling the queue, should always maintain the context offset, otherwise inconsistent result
void BaseBackendService::get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) {
    std::string context_id = params.context_id;
    u_int64_t offset = params.offset;
    TStatus t_status;
    std::shared_ptr<ScanContext> context;
    Status st = _exec_env->external_scan_context_mgr()->get_scan_context(context_id, &context);
    if (!st.ok()) {
        st.to_thrift(&t_status);
        result_.status = t_status;
        return;
    }
    if (offset != context->offset) {
        LOG(ERROR) << "getNext error: context offset [" << context->offset << " ]"
                   << " ,client offset [ " << offset << " ]";
        // invalid offset
        t_status.status_code = TStatusCode::NOT_FOUND;
        t_status.error_msgs.push_back(
                absl::Substitute("context_id=$0, send_offset=$1, context_offset=$2", context_id,
                                 offset, context->offset));
        result_.status = t_status;
    } else {
        // during accessing, should disabled last_access_time
        context->last_access_time = -1;
        TUniqueId fragment_instance_id = context->fragment_instance_id;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        bool eos;

        st = _exec_env->result_queue_mgr()->fetch_result(fragment_instance_id, &record_batch, &eos);
        if (st.ok()) {
            result_.__set_eos(eos);
            if (!eos) {
                std::string record_batch_str;
                st = serialize_record_batch(*record_batch, &record_batch_str);
                st.to_thrift(&t_status);
                if (st.ok()) {
                    // avoid copy large string
                    result_.rows = std::move(record_batch_str);
                    // set __isset
                    result_.__isset.rows = true;
                    context->offset += record_batch->num_rows();
                }
            }
        } else {
            LOG(WARNING) << "fragment_instance_id [" << print_id(fragment_instance_id)
                         << "] fetch result status [" << st.to_string() + "]";
            st.to_thrift(&t_status);
            result_.status = t_status;
        }
    }
    context->last_access_time = time(nullptr);
}

void BaseBackendService::close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) {
    std::string context_id = params.context_id;
    TStatus t_status;
    Status st = _exec_env->external_scan_context_mgr()->clear_scan_context(context_id);
    st.to_thrift(&t_status);
    result_.status = t_status;
}

void BackendService::get_stream_load_record(TStreamLoadRecordResult& result,
                                            int64_t last_stream_record_time) {
    BaseBackendService::get_stream_load_record(result, last_stream_record_time,
                                               _engine.get_stream_load_recorder());
}

void BackendService::check_storage_format(TCheckStorageFormatResult& result) {
    _engine.tablet_manager()->get_all_tablets_storage_format(&result);
}

void BackendService::make_snapshot(TAgentResult& return_value,
                                   const TSnapshotRequest& snapshot_request) {
    std::string snapshot_path;
    bool allow_incremental_clone = false;
    Status status = _engine.snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                          &allow_incremental_clone);
    if (!status) {
        LOG_WARNING("failed to make snapshot")
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("schema_hash", snapshot_request.schema_hash)
                .error(status);
    } else {
        LOG_INFO("successfully make snapshot")
                .tag("tablet_id", snapshot_request.tablet_id)
                .tag("schema_hash", snapshot_request.schema_hash)
                .tag("snapshot_path", snapshot_path);
        return_value.__set_snapshot_path(snapshot_path);
        return_value.__set_allow_incremental_clone(allow_incremental_clone);
    }

    status.to_thrift(&return_value.status);
    return_value.__set_snapshot_version(snapshot_request.preferred_snapshot_version);
}

void BackendService::release_snapshot(TAgentResult& return_value,
                                      const std::string& snapshot_path) {
    Status status = _engine.snapshot_mgr()->release_snapshot(snapshot_path);
    if (!status) {
        LOG_WARNING("failed to release snapshot").tag("snapshot_path", snapshot_path).error(status);
    } else {
        LOG_INFO("successfully release snapshot").tag("snapshot_path", snapshot_path);
    }
    status.to_thrift(&return_value.status);
}

void BackendService::ingest_binlog(TIngestBinlogResult& result,
                                   const TIngestBinlogRequest& request) {
    LOG(INFO) << "ingest binlog. txn_id=" << (request.__isset.txn_id ? request.txn_id : -1)
              << ", tablet_id=" << (request.__isset.local_tablet_id ? request.local_tablet_id : -1)
              << ", load_id=" << (request.__isset.load_id ? print_id(request.load_id) : "not_set")
              << ", fetch_from_peer="
              << (request.__isset.fetch_from_peer && request.fetch_from_peer)
              << ", single_replica_download="
              << (request.__isset.single_replica_download && request.single_replica_download);

    TStatus tstatus;
    Defer defer {[&result, &tstatus]() {
        result.__set_status(tstatus);
        LOG(INFO) << "ingest binlog. result: " << apache::thrift::ThriftDebugString(result);
    }};

    auto set_tstatus = [&tstatus](TStatusCode::type code, std::string error_msg) {
        tstatus.__set_status_code(code);
        tstatus.__isset.error_msgs = true;
        tstatus.error_msgs.push_back(std::move(error_msg));
    };

    if (!config::enable_feature_binlog) {
        set_tstatus(TStatusCode::RUNTIME_ERROR, "enable feature binlog is false");
        return;
    }

    bool is_fetch_from_peer = request.__isset.fetch_from_peer && request.fetch_from_peer;

    /// Check common args: txn_id, partition_id, local_tablet_id, load_id
    if (!request.__isset.txn_id) {
        auto error_msg = "txn_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.partition_id) {
        auto error_msg = "partition_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.local_tablet_id) {
        auto error_msg = "local_tablet_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.load_id) {
        auto error_msg = "load_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }

    // For leader/old path, remote info is required
    if (!is_fetch_from_peer) {
        if (!request.__isset.remote_tablet_id) {
            auto error_msg = "remote_tablet_id is empty";
            LOG(WARNING) << error_msg;
            set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
            return;
        }
        if (!request.__isset.binlog_version) {
            auto error_msg = "binlog_version is empty";
            LOG(WARNING) << error_msg;
            set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
            return;
        }
        if (!request.__isset.remote_host) {
            auto error_msg = "remote_host is empty";
            LOG(WARNING) << error_msg;
            set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
            return;
        }
        if (!request.__isset.remote_port) {
            auto error_msg = "remote_port is empty";
            LOG(WARNING) << error_msg;
            set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
            return;
        }
    }

    auto txn_id = request.txn_id;
    // Step 1: get local tablet
    auto const& local_tablet_id = request.local_tablet_id;
    auto local_tablet = _engine.tablet_manager()->get_tablet(local_tablet_id);
    if (local_tablet == nullptr) {
        auto error_msg = fmt::format("tablet {} not found", local_tablet_id);
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::TABLET_MISSING, std::move(error_msg));
        return;
    }

    // Step 2: check txn, create txn, prepare_txn will check it
    auto partition_id = request.partition_id;
    auto& load_id = request.load_id;
    auto is_ingrest = true;
    PUniqueId p_load_id;
    p_load_id.set_hi(load_id.hi);
    p_load_id.set_lo(load_id.lo);

    {
        // TODO: Before push_lock is not held, but I think it should hold.
        auto status = local_tablet->prepare_txn(partition_id, txn_id, p_load_id, is_ingrest);
        if (!status.ok()) {
            LOG(WARNING) << "prepare txn failed. txn_id=" << txn_id
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
    }

    // Dispatch by mode
    if (is_fetch_from_peer) {
        // Follower mode: always synchronous
        _ingest_binlog_from_peer_impl(_engine, request, local_tablet, txn_id, partition_id,
                                      tstatus);
        return;
    }

    bool is_single_replica_download =
            request.__isset.single_replica_download && request.single_replica_download;
    bool is_async = (_ingest_binlog_workers != nullptr);

    if (is_single_replica_download) {
        if (is_async) {
            set_tstatus(TStatusCode::RUNTIME_ERROR,
                        "single_replica_download is not supported in async ingest mode");
            return;
        }
        // Leader mode: synchronous, collect follower results
        std::vector<int64_t> success_backend_ids;
        std::vector<int64_t> failed_backend_ids;
        IngestBinlogArg ingest_binlog_arg = {
                .txn_id = txn_id,
                .partition_id = partition_id,
                .local_tablet_id = local_tablet_id,
                .local_tablet = local_tablet,
                .request = request,
                .tstatus = &tstatus,
                .success_replica_backend_ids = &success_backend_ids,
                .failed_replica_backend_ids = &failed_backend_ids,
                .follower_distribute_pool = _ingest_binlog_distribute_workers.get(),
        };
        _ingest_binlog(_engine, &ingest_binlog_arg);
        // Always return the per-replica result lists so that syncer can decide whether
        // to retry / fallback, even when some followers failed after leader commit.
        result.__set_success_replica_backend_ids(success_backend_ids);
        result.__set_failed_replica_backend_ids(failed_backend_ids);
        return;
    }

    // Old path
    result.__set_is_async(is_async);

    auto ingest_binlog_func = [=, this, tstatus = &tstatus]() {
        IngestBinlogArg ingest_binlog_arg = {
                .txn_id = txn_id,
                .partition_id = partition_id,
                .local_tablet_id = local_tablet_id,
                .local_tablet = local_tablet,

                .request = request,
                .tstatus = is_async ? nullptr : tstatus,
        };

        _ingest_binlog(_engine, &ingest_binlog_arg);
    };

    if (is_async) {
        auto status = _ingest_binlog_workers->submit_func(std::move(ingest_binlog_func));
        if (!status.ok()) {
            status.to_thrift(&tstatus);
            return;
        }
    } else {
        ingest_binlog_func();
    }
}

void BackendService::query_ingest_binlog(TQueryIngestBinlogResult& result,
                                         const TQueryIngestBinlogRequest& request) {
    LOG(INFO) << "query ingest binlog. request: " << apache::thrift::ThriftDebugString(request);

    auto set_result = [&](TIngestBinlogStatus::type status, std::string error_msg) {
        result.__set_status(status);
        result.__set_err_msg(std::move(error_msg));
    };

    /// Check args: txn_id, partition_id, tablet_id, load_id
    if (!request.__isset.txn_id) {
        auto error_msg = "txn_id is empty";
        LOG(WARNING) << error_msg;
        set_result(TIngestBinlogStatus::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.partition_id) {
        auto error_msg = "partition_id is empty";
        LOG(WARNING) << error_msg;
        set_result(TIngestBinlogStatus::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.tablet_id) {
        auto error_msg = "tablet_id is empty";
        LOG(WARNING) << error_msg;
        set_result(TIngestBinlogStatus::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.load_id) {
        auto error_msg = "load_id is empty";
        LOG(WARNING) << error_msg;
        set_result(TIngestBinlogStatus::ANALYSIS_ERROR, error_msg);
        return;
    }

    auto partition_id = request.partition_id;
    auto txn_id = request.txn_id;
    auto tablet_id = request.tablet_id;

    // Step 1: get local tablet
    auto local_tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (local_tablet == nullptr) {
        auto error_msg = fmt::format("tablet {} not found", tablet_id);
        LOG(WARNING) << error_msg;
        set_result(TIngestBinlogStatus::NOT_FOUND, std::move(error_msg));
        return;
    }

    // Step 2: get txn state
    auto tablet_uid = local_tablet->tablet_uid();
    auto txn_state =
            _engine.txn_manager()->get_txn_state(partition_id, txn_id, tablet_id, tablet_uid);
    switch (txn_state) {
    case TxnState::NOT_FOUND:
        result.__set_status(TIngestBinlogStatus::NOT_FOUND);
        break;
    case TxnState::PREPARED:
        result.__set_status(TIngestBinlogStatus::DOING);
        break;
    case TxnState::COMMITTED:
        result.__set_status(TIngestBinlogStatus::OK);
        break;
    case TxnState::ROLLEDBACK:
        result.__set_status(TIngestBinlogStatus::FAILED);
        break;
    case TxnState::ABORTED:
        result.__set_status(TIngestBinlogStatus::FAILED);
        break;
    case TxnState::DELETED:
        result.__set_status(TIngestBinlogStatus::FAILED);
        break;
    }
}

void BaseBackendService::get_tablet_stat(TTabletStatResult& result) {
    LOG(ERROR) << "get_tablet_stat is not implemented";
}

int64_t BaseBackendService::get_trash_used_capacity() {
    LOG(ERROR) << "get_trash_used_capacity is not implemented";
    return 0;
}

void BaseBackendService::get_stream_load_record(TStreamLoadRecordResult& result,
                                                int64_t last_stream_record_time) {
    LOG(ERROR) << "get_stream_load_record is not implemented";
}

void BaseBackendService::get_stream_load_record(
        TStreamLoadRecordResult& result, int64_t last_stream_record_time,
        std::shared_ptr<StreamLoadRecorder> stream_load_recorder) {
    if (stream_load_recorder != nullptr) {
        std::map<std::string, std::string> records;
        auto st = stream_load_recorder->get_batch(std::to_string(last_stream_record_time),
                                                  config::stream_load_record_batch_size, &records);
        if (st.ok()) {
            LOG(INFO) << "get_batch stream_load_record rocksdb successfully. records size: "
                      << records.size()
                      << ", last_stream_load_timestamp: " << last_stream_record_time;
            std::map<std::string, TStreamLoadRecord> stream_load_record_batch;
            auto it = records.begin();
            for (; it != records.end(); ++it) {
                TStreamLoadRecord stream_load_item;
                StreamLoadContext::parse_stream_load_record(it->second, stream_load_item);
                stream_load_record_batch.emplace(it->first.c_str(), stream_load_item);
            }
            result.__set_stream_load_record(stream_load_record_batch);
        }
    } else {
        LOG(WARNING) << "stream_load_recorder is null.";
    }
}

void BaseBackendService::get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) {
    LOG(ERROR) << "get_disk_trash_used_capacity is not implemented";
}

void BaseBackendService::make_snapshot(TAgentResult& return_value,
                                       const TSnapshotRequest& snapshot_request) {
    LOG(ERROR) << "make_snapshot is not implemented";
    return_value.__set_status(Status::NotSupported("make_snapshot is not implemented").to_thrift());
}

void BaseBackendService::release_snapshot(TAgentResult& return_value,
                                          const std::string& snapshot_path) {
    LOG(ERROR) << "release_snapshot is not implemented";
    return_value.__set_status(
            Status::NotSupported("release_snapshot is not implemented").to_thrift());
}

void BaseBackendService::check_storage_format(TCheckStorageFormatResult& result) {
    LOG(ERROR) << "check_storage_format is not implemented";
}

void BaseBackendService::ingest_binlog(TIngestBinlogResult& result,
                                       const TIngestBinlogRequest& request) {
    LOG(ERROR) << "ingest_binlog is not implemented";
    result.__set_status(Status::NotSupported("ingest_binlog is not implemented").to_thrift());
}

void BaseBackendService::query_ingest_binlog(TQueryIngestBinlogResult& result,
                                             const TQueryIngestBinlogRequest& request) {
    LOG(ERROR) << "query_ingest_binlog is not implemented";
    result.__set_status(TIngestBinlogStatus::UNKNOWN);
    result.__set_err_msg("query_ingest_binlog is not implemented");
}

void BaseBackendService::warm_up_cache_async(TWarmUpCacheAsyncResponse& response,
                                             const TWarmUpCacheAsyncRequest& request) {
    LOG(ERROR) << "warm_up_cache_async is not implemented";
    response.__set_status(
            Status::NotSupported("warm_up_cache_async is not implemented").to_thrift());
}

void BaseBackendService::check_warm_up_cache_async(TCheckWarmUpCacheAsyncResponse& response,
                                                   const TCheckWarmUpCacheAsyncRequest& request) {
    LOG(ERROR) << "check_warm_up_cache_async is not implemented";
    response.__set_status(
            Status::NotSupported("check_warm_up_cache_async is not implemented").to_thrift());
}

void BaseBackendService::sync_load_for_tablets(TSyncLoadForTabletsResponse& response,
                                               const TSyncLoadForTabletsRequest& request) {
    LOG(ERROR) << "sync_load_for_tablets is not implemented";
}

void BaseBackendService::get_top_n_hot_partitions(TGetTopNHotPartitionsResponse& response,
                                                  const TGetTopNHotPartitionsRequest& request) {
    LOG(ERROR) << "get_top_n_hot_partitions is not implemented";
}

void BaseBackendService::warm_up_tablets(TWarmUpTabletsResponse& response,
                                         const TWarmUpTabletsRequest& request) {
    LOG(ERROR) << "warm_up_tablets is not implemented";
    response.__set_status(Status::NotSupported("warm_up_tablets is not implemented").to_thrift());
}

void BaseBackendService::get_realtime_exec_status(TGetRealtimeExecStatusResponse& response,
                                                  const TGetRealtimeExecStatusRequest& request) {
    if (!request.__isset.id) {
        LOG_WARNING("Invalidate argument, id is empty");
        response.__set_status(Status::InvalidArgument("id is empty").to_thrift());
        return;
    }

    RuntimeProfile::Counter get_realtime_timer {TUnit::TIME_NS};

    Defer _print_log([&]() {
        LOG_INFO("Getting realtime exec status of query {} , cost time {}", print_id(request.id),
                 PrettyPrinter::print(get_realtime_timer.value(), get_realtime_timer.type()));
    });

    SCOPED_TIMER(&get_realtime_timer);

    std::unique_ptr<TReportExecStatusParams> report_exec_status_params =
            std::make_unique<TReportExecStatusParams>();
    std::unique_ptr<TQueryStatistics> query_stats = std::make_unique<TQueryStatistics>();

    std::string req_type = request.__isset.req_type ? request.req_type : "profile";
    Status st;
    if (req_type == "stats") {
        st = ExecEnv::GetInstance()->fragment_mgr()->get_query_statistics(request.id,
                                                                          query_stats.get());
        if (st.ok()) {
            response.__set_query_stats(*query_stats);
        }
    } else {
        // default is "profile"
        st = ExecEnv::GetInstance()->fragment_mgr()->get_realtime_exec_status(
                request.id, report_exec_status_params.get());
        if (st.ok()) {
            response.__set_report_exec_status_params(*report_exec_status_params);
        }
    }

    report_exec_status_params->__set_query_id(TUniqueId());
    report_exec_status_params->__set_done(false);
    response.__set_status(st.to_thrift());
}

void BaseBackendService::get_dictionary_status(TDictionaryStatusList& result,
                                               const std::vector<int64_t>& dictionary_ids) {
    std::vector<TDictionaryStatus> dictionary_status;
    ExecEnv::GetInstance()->dict_factory()->get_dictionary_status(dictionary_status,
                                                                  dictionary_ids);
    result.__set_dictionary_status_list(dictionary_status);
    LOG(INFO) << "query for dictionary status, return " << result.dictionary_status_list.size()
              << " rows";
}

void BaseBackendService::test_storage_connectivity(TTestStorageConnectivityResponse& response,
                                                   const TTestStorageConnectivityRequest& request) {
    Status status = io::StorageConnectivityTester::test(request.type, request.properties);
    response.__set_status(status.to_thrift());
}

void BaseBackendService::get_python_envs(std::vector<TPythonEnvInfo>& result) {
    result = PythonVersionManager::instance().env_infos_to_thrift();
}

void BaseBackendService::get_python_packages(std::vector<TPythonPackageInfo>& result,
                                             const std::string& python_version) {
    PythonVersion version;
    auto& manager = PythonVersionManager::instance();
    THROW_IF_ERROR(manager.get_version(python_version, &version));

    std::vector<std::pair<std::string, std::string>> packages;
    THROW_IF_ERROR(list_installed_packages(version, &packages));
    result = manager.package_infos_to_thrift(packages);
}

// Exposed wrapper for unit tests. The implementation lives in the unnamed namespace
// and is not directly visible to other translation units.
void _ingest_binlog_from_peer(StorageEngine& engine, const TIngestBinlogRequest& request,
                              const TabletSharedPtr& local_tablet, int64_t txn_id,
                              int64_t partition_id, TStatus& tstatus) {
    _ingest_binlog_from_peer_impl(engine, request, local_tablet, txn_id, partition_id, tstatus);
}

} // namespace doris
