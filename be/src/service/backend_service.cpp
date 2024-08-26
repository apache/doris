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

#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/arrow/row_batch.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"
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

namespace {
constexpr uint64_t kMaxTimeoutMs = 3000; // 3s
struct IngestBinlogArg {
    int64_t txn_id;
    int64_t partition_id;
    int64_t local_tablet_id;
    TabletSharedPtr local_tablet;
    TIngestBinlogRequest request;
    TStatus* tstatus;
};

void _ingest_binlog(StorageEngine& engine, IngestBinlogArg* arg) {
    auto txn_id = arg->txn_id;
    auto partition_id = arg->partition_id;
    auto local_tablet_id = arg->local_tablet_id;
    const auto& local_tablet = arg->local_tablet;
    const auto& local_tablet_uid = local_tablet->tablet_uid();

    std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, fmt::format("IngestBinlog#TxnId={}", txn_id));
    SCOPED_ATTACH_TASK(mem_tracker);

    auto& request = arg->request;

    TStatus tstatus;
    std::vector<std::string> download_success_files;
    Defer defer {[=, &engine, &tstatus, ingest_binlog_tstatus = arg->tstatus]() {
        LOG(INFO) << "ingest binlog. result: " << apache::thrift::ThriftDebugString(tstatus);
        if (tstatus.status_code != TStatusCode::OK) {
            // abort txn
            engine.txn_manager()->abort_txn(partition_id, txn_id, local_tablet_id,
                                            local_tablet_uid);
            // delete all successfully downloaded files
            LOG(WARNING) << "will delete downloaded success files due to error " << tstatus;
            std::vector<io::Path> paths;
            for (const auto& file : download_success_files) {
                paths.emplace_back(file);
                LOG(WARNING) << "will delete downloaded success file " << file << " due to error";
            }
            static_cast<void>(io::global_local_filesystem()->batch_delete(paths));
            LOG(WARNING) << "done delete downloaded success files due to error " << tstatus;
        }

        if (ingest_binlog_tstatus) {
            *ingest_binlog_tstatus = std::move(tstatus);
        }
    }};

    auto set_tstatus = [&tstatus](TStatusCode::type code, std::string error_msg) {
        tstatus.__set_status_code(code);
        tstatus.__isset.error_msgs = true;
        tstatus.error_msgs.push_back(std::move(error_msg));
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
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&binlog_info);
    };
    auto status = HttpClient::execute_with_retry(max_retry, 1, get_binlog_info_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get binlog info from " << get_binlog_info_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    std::vector<std::string> binlog_info_parts = strings::Split(binlog_info, ":");
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
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&rowset_meta_str);
    };
    status = HttpClient::execute_with_retry(max_retry, 1, get_rowset_meta_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get rowset meta from " << get_rowset_meta_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }
    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(rowset_meta_str)) {
        LOG(WARNING) << "failed to parse rowset meta from " << get_rowset_meta_url;
        status = Status::InternalError("failed to parse rowset meta");
        status.to_thrift(&tstatus);
        return;
    }
    // rewrite rowset meta
    rowset_meta_pb.set_tablet_id(local_tablet_id);
    rowset_meta_pb.set_partition_id(partition_id);
    rowset_meta_pb.set_tablet_schema_hash(local_tablet->tablet_meta()->schema_hash());
    rowset_meta_pb.set_txn_id(txn_id);
    rowset_meta_pb.set_rowset_state(RowsetStatePB::COMMITTED);
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
            client->set_timeout_ms(kMaxTimeoutMs);
            RETURN_IF_ERROR(client->head());
            return client->get_content_length(&segment_file_size);
        };

        status = HttpClient::execute_with_retry(max_retry, 1, get_segment_file_size_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file size from " << get_segment_file_size_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }

        segment_file_sizes.push_back(segment_file_size);
        segment_file_urls.push_back(std::move(get_segment_file_size_url));
    }

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

    // Step 5.3: get all segment files
    for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
        auto segment_file_size = segment_file_sizes[segment_index];
        auto get_segment_file_url =
                fmt::format("{}&acquire_md5=true", segment_file_urls[segment_index]);

        uint64_t estimate_timeout =
                segment_file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        auto segment_path = local_segment_path(local_tablet->tablet_path(),
                                               rowset_meta->rowset_id().to_string(), segment_index);
        LOG(INFO) << "download segment file from " << get_segment_file_url << " to "
                  << segment_path;
        auto get_segment_file_cb = [&get_segment_file_url, &segment_path, segment_file_size,
                                    estimate_timeout, &download_success_files](HttpClient* client) {
            RETURN_IF_ERROR(client->init(get_segment_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(segment_path));
            download_success_files.push_back(segment_path);

            std::string remote_file_md5;
            RETURN_IF_ERROR(client->get_content_md5(&remote_file_md5));
            LOG(INFO) << "download segment file to " << segment_path
                      << ", remote md5: " << remote_file_md5
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
                return Status::RuntimeError(
                        "downloaded file size is not equal, local={}, remote={}", local_file_size,
                        segment_file_size);
            }

            if (!remote_file_md5.empty()) { // keep compatibility
                std::string local_file_md5;
                RETURN_IF_ERROR(
                        io::global_local_filesystem()->md5sum(segment_path, &local_file_md5));
                if (local_file_md5 != remote_file_md5) {
                    LOG(WARNING) << "download file md5 error"
                                 << ", get_segment_file_url=" << get_segment_file_url
                                 << ", remote_file_md5=" << remote_file_md5
                                 << ", local_file_md5=" << local_file_md5;
                    return Status::RuntimeError(
                            "download file md5 is not equal, local={}, remote={}", local_file_md5,
                            remote_file_md5);
                }
            }

            return io::global_local_filesystem()->permission(segment_path,
                                                             io::LocalFileSystem::PERMS_OWNER_RW);
        };

        auto status = HttpClient::execute_with_retry(max_retry, 1, get_segment_file_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file from " << get_segment_file_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
    }

    // Step 6: get all segment index files
    // Step 6.1: get all segment index files size
    std::vector<std::string> segment_index_file_urls;
    std::vector<uint64_t> segment_index_file_sizes;
    std::vector<std::string> segment_index_file_names;
    auto tablet_schema = rowset_meta->tablet_schema();
    if (tablet_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        for (const auto& index : tablet_schema->indexes()) {
            if (index.index_type() != IndexType::INVERTED) {
                continue;
            }
            auto index_id = index.index_id();
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
                            client->set_timeout_ms(kMaxTimeoutMs);
                            RETURN_IF_ERROR(client->head());
                            return client->get_content_length(&segment_index_file_size);
                        };

                auto segment_path =
                        local_segment_path(local_tablet->tablet_path(),
                                           rowset_meta->rowset_id().to_string(), segment_index);
                segment_index_file_names.push_back(InvertedIndexDescriptor::get_index_file_path_v1(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path), index_id,
                        index.get_index_suffix()));

                status = HttpClient::execute_with_retry(max_retry, 1,
                                                        get_segment_index_file_size_cb);
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
            if (tablet_schema->has_inverted_index()) {
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
                            client->set_timeout_ms(kMaxTimeoutMs);
                            RETURN_IF_ERROR(client->head());
                            return client->get_content_length(&segment_index_file_size);
                        };
                auto segment_path =
                        local_segment_path(local_tablet->tablet_path(),
                                           rowset_meta->rowset_id().to_string(), segment_index);
                segment_index_file_names.push_back(InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)));

                status = HttpClient::execute_with_retry(max_retry, 1,
                                                        get_segment_index_file_size_cb);
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

    // Step 6.3: get all segment index files
    DCHECK(segment_index_file_sizes.size() == segment_index_file_names.size());
    DCHECK(segment_index_file_names.size() == segment_index_file_urls.size());
    for (int64_t i = 0; i < segment_index_file_urls.size(); ++i) {
        auto segment_index_file_size = segment_index_file_sizes[i];
        auto get_segment_index_file_url =
                fmt::format("{}&acquire_md5=true", segment_index_file_urls[i]);

        uint64_t estimate_timeout =
                segment_index_file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        auto local_segment_index_path = segment_index_file_names[i];
        LOG(INFO) << fmt::format("download segment index file from {} to {}",
                                 get_segment_index_file_url, local_segment_index_path);
        auto get_segment_index_file_cb = [&get_segment_index_file_url, &local_segment_index_path,
                                          segment_index_file_size, estimate_timeout,
                                          &download_success_files](HttpClient* client) {
            RETURN_IF_ERROR(client->init(get_segment_index_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(local_segment_index_path));
            download_success_files.push_back(local_segment_index_path);

            std::string remote_file_md5;
            RETURN_IF_ERROR(client->get_content_md5(&remote_file_md5));

            std::error_code ec;
            // Check file length
            uint64_t local_index_file_size =
                    std::filesystem::file_size(local_segment_index_path, ec);
            if (ec) {
                LOG(WARNING) << "download index file error" << ec.message();
                return Status::IOError("can't retrive file_size of {}, due to {}",
                                       local_segment_index_path, ec.message());
            }
            if (local_index_file_size != segment_index_file_size) {
                LOG(WARNING) << "download index file length error"
                             << ", get_segment_index_file_url=" << get_segment_index_file_url
                             << ", index_file_size=" << segment_index_file_size
                             << ", local_index_file_size=" << local_index_file_size;
                return Status::RuntimeError(
                        "downloaded index file size is not equal, local={}, remote={}",
                        local_index_file_size, segment_index_file_size);
            }

            if (!remote_file_md5.empty()) { // keep compatibility
                std::string local_file_md5;
                RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(local_segment_index_path,
                                                                      &local_file_md5));
                if (local_file_md5 != remote_file_md5) {
                    LOG(WARNING) << "download file md5 error"
                                 << ", get_segment_index_file_url=" << get_segment_index_file_url
                                 << ", remote_file_md5=" << remote_file_md5
                                 << ", local_file_md5=" << local_file_md5;
                    return Status::RuntimeError(
                            "download file md5 is not equal, local={}, remote={}", local_file_md5,
                            remote_file_md5);
                }
            }

            return io::global_local_filesystem()->permission(local_segment_index_path,
                                                             io::LocalFileSystem::PERMS_OWNER_RW);
        };

        status = HttpClient::execute_with_retry(max_retry, 1, get_segment_index_file_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment index file from " << get_segment_index_file_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
    }

    // Step 7: create rowset && calculate delete bitmap && commit
    // Step 7.1: create rowset
    RowsetSharedPtr rowset;
    status = RowsetFactory::create_rowset(local_tablet->tablet_schema(),
                                          local_tablet->tablet_path(), rowset_meta, &rowset);

    if (!status) {
        LOG(WARNING) << "failed to create rowset from rowset meta for remote tablet"
                     << ". rowset_id: " << rowset_meta_pb.rowset_id()
                     << ", rowset_type: " << rowset_meta_pb.rowset_type()
                     << ", remote_tablet_id=" << rowset_meta_pb.tablet_id() << ", txn_id=" << txn_id
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    // Step 7.2 calculate delete bitmap before commit
    auto calc_delete_bitmap_token = engine.calc_delete_bitmap_executor()->create_token();
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(local_tablet_id);
    RowsetIdUnorderedSet pre_rowset_ids;
    if (local_tablet->enable_unique_key_merge_on_write()) {
        auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        status = beta_rowset->load_segments(&segments);
        if (!status) {
            LOG(WARNING) << "failed to load segments from rowset"
                         << ". rowset_id: " << beta_rowset->rowset_id() << ", txn_id=" << txn_id
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
        if (segments.size() > 1) {
            // calculate delete bitmap between segments
            status = local_tablet->calc_delete_bitmap_between_segments(rowset, segments,
                                                                       delete_bitmap);
            if (!status) {
                LOG(WARNING) << "failed to calculate delete bitmap"
                             << ". tablet_id: " << local_tablet->tablet_id()
                             << ". rowset_id: " << rowset->rowset_id() << ", txn_id=" << txn_id
                             << ", status=" << status.to_string();
                status.to_thrift(&tstatus);
                return;
            }
        }

        static_cast<void>(BaseTablet::commit_phase_update_delete_bitmap(
                local_tablet, rowset, pre_rowset_ids, delete_bitmap, segments, txn_id,
                calc_delete_bitmap_token.get(), nullptr));
        static_cast<void>(calc_delete_bitmap_token->wait());
    }

    // Step 7.3: commit txn
    Status commit_txn_status = engine.txn_manager()->commit_txn(
            local_tablet->data_dir()->get_meta(), rowset_meta->partition_id(),
            rowset_meta->txn_id(), rowset_meta->tablet_id(), local_tablet->tablet_uid(),
            rowset_meta->load_id(), rowset, std::move(pending_rs_guard), false);
    if (!commit_txn_status && !commit_txn_status.is<ErrorCode::PUSH_TRANSACTION_ALREADY_EXIST>()) {
        auto err_msg = fmt::format(
                "failed to commit txn for remote tablet. rowset_id: {}, remote_tablet_id={}, "
                "txn_id={}, status={}",
                rowset_meta->rowset_id().to_string(), rowset_meta->tablet_id(),
                rowset_meta->txn_id(), commit_txn_status.to_string());
        LOG(WARNING) << err_msg;
        set_tstatus(TStatusCode::RUNTIME_ERROR, std::move(err_msg));
        return;
    }

    if (local_tablet->enable_unique_key_merge_on_write()) {
        engine.txn_manager()->set_txn_related_delete_bitmap(partition_id, txn_id, local_tablet_id,
                                                            local_tablet->tablet_uid(), true,
                                                            delete_bitmap, pre_rowset_ids, nullptr);
    }

    tstatus.__set_status_code(TStatusCode::OK);
}
} // namespace

BaseBackendService::BaseBackendService(ExecEnv* exec_env)
        : _exec_env(exec_env), _agent_server(new AgentServer(exec_env, *exec_env->master_info())) {}

BaseBackendService::~BaseBackendService() = default;

BackendService::BackendService(StorageEngine& engine, ExecEnv* exec_env)
        : BaseBackendService(exec_env), _engine(engine) {}

BackendService::~BackendService() = default;

Status BackendService::create_service(StorageEngine& engine, ExecEnv* exec_env, int port,
                                      std::unique_ptr<ThriftServer>* server,
                                      std::shared_ptr<doris::BackendService> service) {
    service->_agent_server->start_workers(engine, exec_env);
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    // std::shared_ptr<TProcessor> be_processor = std::make_shared<BackendServiceProcessor>(service);
    auto be_processor = std::make_shared<BackendServiceProcessor>(service);

    *server = std::make_unique<ThriftServer>("backend", be_processor, port,
                                             config::be_service_threads);

    LOG(INFO) << "Doris BackendService listening on " << port;

    auto thread_num = config::ingest_binlog_work_pool_size;
    if (thread_num < 0) {
        LOG(INFO) << fmt::format("ingest binlog thread pool size is {}, so we will in sync mode",
                                 thread_num);
        return Status::OK();
    }

    if (thread_num == 0) {
        thread_num = std::thread::hardware_concurrency();
    }
    static_cast<void>(doris::ThreadPoolBuilder("IngestBinlog")
                              .set_min_threads(thread_num)
                              .set_max_threads(thread_num * 2)
                              .build(&(service->_ingest_binlog_workers)));
    LOG(INFO) << fmt::format("ingest binlog thread pool size is {}, in async mode", thread_num);
    return Status::OK();
}

void BaseBackendService::exec_plan_fragment(TExecPlanFragmentResult& return_val,
                                            const TExecPlanFragmentParams& params) {
    LOG(INFO) << "exec_plan_fragment() instance_id=" << print_id(params.params.fragment_instance_id)
              << " coord=" << params.coord << " backend#=" << params.backend_num;
    return_val.__set_status(start_plan_fragment_execution(params).to_thrift());
}

Status BaseBackendService::start_plan_fragment_execution(
        const TExecPlanFragmentParams& exec_params) {
    if (!exec_params.fragment.__isset.output_sink) {
        return Status::InternalError("missing sink in plan fragment");
    }
    return _exec_env->fragment_mgr()->exec_plan_fragment(exec_params,
                                                         QuerySource::INTERNAL_FRONTEND);
}

void BaseBackendService::cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                                              const TCancelPlanFragmentParams& params) {
    LOG(INFO) << "cancel_plan_fragment(): instance_id=" << print_id(params.fragment_instance_id);
    _exec_env->fragment_mgr()->cancel_instance(
            params.fragment_instance_id, Status::InternalError("cancel message received from FE"));
}

void BaseBackendService::transmit_data(TTransmitDataResult& return_val,
                                       const TTransmitDataParams& params) {
    VLOG_ROW << "transmit_data(): instance_id=" << params.dest_fragment_instance_id
             << " node_id=" << params.dest_node_id << " #rows=" << params.row_batch.num_rows
             << " eos=" << (params.eos ? "true" : "false");
    // VLOG_ROW << "transmit_data params: " << apache::thrift::ThriftDebugString(params).c_str();

    if (params.__isset.packet_seq) {
        return_val.__set_packet_seq(params.packet_seq);
        return_val.__set_dest_fragment_instance_id(params.dest_fragment_instance_id);
        return_val.__set_dest_node_id(params.dest_node_id);
    }

    // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
    // of having to copy its data
    if (params.row_batch.num_rows > 0) {
        // Status status = _exec_env->stream_mgr()->add_data(
        //         params.dest_fragment_instance_id,
        //         params.dest_node_id,
        //         params.row_batch,
        //         params.sender_id);
        // status.set_t_status(&return_val);

        // if (!status.ok()) {
        //     // should we close the channel here as well?
        //     return;
        // }
    }

    if (params.eos) {
        // Status status = _exec_env->stream_mgr()->close_sender(
        //        params.dest_fragment_instance_id,
        //        params.dest_node_id,
        //        params.sender_id,
        //        params.be_number);
        //VLOG_ROW << "params.eos: " << (params.eos ? "true" : "false")
        //        << " close_sender status: " << status;
        //status.set_t_status(&return_val);
    }
}

void BaseBackendService::submit_export_task(TStatus& t_status, const TExportTaskRequest& request) {
    //    VLOG_ROW << "submit_export_task. request  is "
    //            << apache::thrift::ThriftDebugString(request).c_str();
    //
    //    Status status = _exec_env->export_task_mgr()->start_task(request);
    //    if (status.ok()) {
    //        VLOG_RPC << "start export task successful id="
    //            << request.params.params.fragment_instance_id;
    //    } else {
    //        VLOG_RPC << "start export task failed id="
    //            << request.params.params.fragment_instance_id
    //            << " and err_msg=" << status;
    //    }
    //    status.to_thrift(&t_status);
}

void BaseBackendService::get_export_status(TExportStatusResult& result, const TUniqueId& task_id) {
    //    VLOG_ROW << "get_export_status. task_id  is " << task_id;
    //    Status status = _exec_env->export_task_mgr()->get_task_state(task_id, &result);
    //    if (!status.ok()) {
    //        LOG(WARNING) << "get export task state failed. [id=" << task_id << "]";
    //    } else {
    //        VLOG_RPC << "get export task state successful. [id=" << task_id
    //            << ",status=" << result.status.status_code
    //            << ",state=" << result.state
    //            << ",files=";
    //        for (auto& item : result.files) {
    //            VLOG_RPC << item << ", ";
    //        }
    //        VLOG_RPC << "]";
    //    }
    //    status.to_thrift(&result.status);
    //    result.__set_state(TExportState::RUNNING);
}

void BaseBackendService::erase_export_task(TStatus& t_status, const TUniqueId& task_id) {
    //    VLOG_ROW << "erase_export_task. task_id  is " << task_id;
    //    Status status = _exec_env->export_task_mgr()->erase_task(task_id);
    //    if (!status.ok()) {
    //        LOG(WARNING) << "delete export task failed. because "
    //            << status << " with task_id " << task_id;
    //    } else {
    //        VLOG_RPC << "delete export task successful with task_id " << task_id;
    //    }
    //    status.to_thrift(&t_status);
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
        uint32_t len = query_plan_info.size();
        // deserialize TQueryPlanInfo
        auto st = deserialize_thrift_msg(buf, &len, false, &t_query_plan_info);
        if (!st.ok()) {
            LOG(WARNING) << "open context error: deserialize TQueryPlanInfo failure";
            std::stringstream msg;
            msg << "query_plan_info: " << query_plan_info
                << " deserialize error, should not be modified after returned Doris FE processed";
            exec_st = Status::InvalidArgument(msg.str());
        }
        p_context->query_id = t_query_plan_info.query_id;
    }
    std::vector<TScanColumnDesc> selected_columns;
    if (exec_st.ok()) {
        // start the scan procedure
        exec_st = _exec_env->fragment_mgr()->exec_external_plan_fragment(
                params, t_query_plan_info, fragment_instance_id, &selected_columns);
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
                strings::Substitute("context_id=$0, send_offset=$1, context_offset=$2", context_id,
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
    LOG(INFO) << "ingest binlog. request: " << apache::thrift::ThriftDebugString(request);

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

    /// Check args: txn_id, remote_tablet_id, binlog_version, remote_host, remote_port, partition_id, load_id
    if (!request.__isset.txn_id) {
        auto error_msg = "txn_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
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
    auto status = _engine.txn_manager()->prepare_txn(partition_id, *local_tablet, txn_id, p_load_id,
                                                     is_ingrest);
    if (!status.ok()) {
        LOG(WARNING) << "prepare txn failed. txn_id=" << txn_id
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    bool is_async = (_ingest_binlog_workers != nullptr);
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
        status = _ingest_binlog_workers->submit_func(std::move(ingest_binlog_func));
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
    Status st = ExecEnv::GetInstance()->fragment_mgr()->get_realtime_exec_status(
            request.id, report_exec_status_params.get());

    if (!st.ok()) {
        response.__set_status(st.to_thrift());
        return;
    }

    report_exec_status_params->__set_query_id(TUniqueId());
    report_exec_status_params->__set_done(false);

    response.__set_status(Status::OK().to_thrift());
    response.__set_report_exec_status_params(*report_exec_status_params);
}

} // namespace doris
