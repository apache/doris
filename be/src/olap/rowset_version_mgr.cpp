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

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <bthread/mutex.h>
#include <bthread/types.h>
#include <bvar/latency_recorder.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <sstream>
#include <utility>

#include "cloud/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader.h"
#include "runtime/client_cache.h"
#include "service/backend_options.h"
#include "service/internal_service.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace doris {

using namespace ErrorCode;
using namespace std::ranges;

static bvar::LatencyRecorder g_remote_fetch_tablet_rowsets_single_request_latency(
        "remote_fetch_rowsets_single_rpc");
static bvar::LatencyRecorder g_remote_fetch_tablet_rowsets_latency("remote_fetch_rowsets");

[[nodiscard]] Result<std::vector<Version>> BaseTablet::capture_consistent_versions_unlocked(
        const Version& version_range, const CaptureRowsetOps& options) const {
    std::vector<Version> version_path;
    auto st =
            _timestamped_version_tracker.capture_consistent_versions(version_range, &version_path);
    if (!st && !options.quiet) {
        auto missed_versions = get_missed_versions_unlocked(version_range.second);
        if (missed_versions.empty()) {
            LOG(WARNING) << fmt::format(
                    "version already has been merged. version_range={}, max_version={}, "
                    "tablet_id={}",
                    version_range.to_string(), _tablet_meta->max_version().second, tablet_id());
            return ResultError(Status::Error<VERSION_ALREADY_MERGED, false>(
                    "missed versions is empty, version_range={}, max_version={}, tablet_id={}",
                    version_range.to_string(), _tablet_meta->max_version().second, tablet_id()));
        }
        LOG(WARNING) << fmt::format("missed version for version_range={}, tablet_id={}, st={}",
                                    version_range.to_string(), tablet_id(), st);
        _print_missed_versions(missed_versions);
        if (!options.skip_missing_versions) {
            return ResultError(std::move(st));
        }
        LOG(WARNING) << "force skipping missing version for tablet:" << tablet_id();
    }
    DBUG_EXECUTE_IF("Tablet::capture_consistent_versions.inject_failure", {
        auto tablet_id = dp->param<int64>("tablet_id", -1);
        auto skip_by_option = dp->param<bool>("skip_by_option", false);
        if (skip_by_option && !options.enable_fetch_rowsets_from_peers) {
            return version_path;
        }
        if ((tablet_id != -1 && tablet_id == _tablet_meta->tablet_id()) || tablet_id == -2) {
            return ResultError(Status::Error<VERSION_ALREADY_MERGED, false>(
                    "versions are already compacted, version_range={}, max_version={}, tablet_id={}",
                    version_range.to_string(), _tablet_meta->max_version().second, tablet_id));
        }
    });
    return version_path;
}

[[nodiscard]] Result<CaptureRowsetResult> BaseTablet::capture_consistent_rowsets_unlocked(
        const Version& version_range, const CaptureRowsetOps& options) const {
    CaptureRowsetResult result;
    auto& rowsets = result.rowsets;
    auto maybe_versions = capture_consistent_versions_unlocked(version_range, options);
    if (maybe_versions) {
        const auto& version_paths = maybe_versions.value();
        rowsets.reserve(version_paths.size());

        auto rowset_for_version = [&](const Version& version,
                                      bool include_stale) -> Result<RowsetSharedPtr> {
            if (auto it = _rs_version_map.find(version); it != _rs_version_map.end()) {
                return it->second;
            } else {
                VLOG_NOTICE << "fail to find Rowset in rs_version for version. tablet="
                            << tablet_id() << ", version='" << version.first << "-"
                            << version.second;
            }
            if (include_stale) {
                if (auto it = _stale_rs_version_map.find(version);
                    it != _stale_rs_version_map.end()) {
                    return it->second;
                } else {
                    LOG(WARNING) << fmt::format(
                            "fail to find Rowset in stale_rs_version for version. tablet={}, "
                            "version={}-{}",
                            tablet_id(), version.first, version.second);
                }
            }
            return ResultError(Status::Error<CAPTURE_ROWSET_ERROR>(
                    "failed to find rowset for version={}", version.to_string()));
        };

        for (const auto& version : version_paths) {
            auto ret = rowset_for_version(version, options.include_stale_rowsets);
            if (!ret) {
                return ResultError(std::move(ret.error()));
            }

            rowsets.push_back(std::move(ret.value()));
        }
        if (keys_type() == KeysType::UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
            result.delete_bitmap = _tablet_meta->delete_bitmap();
        }
        return result;
    }

    if (!config::is_cloud_mode() || !options.enable_fetch_rowsets_from_peers) {
        return ResultError(std::move(maybe_versions.error()));
    }
    auto ret = _remote_capture_rowsets(version_range);
    if (!ret) {
        auto st = Status::Error<VERSION_ALREADY_MERGED, false>(
                "versions are already compacted, meet error during remote capturing rowsets, "
                "error={}, version_range={}",
                ret.error().to_string(), version_range.to_string());
        return ResultError(std::move(st));
    }
    return ret;
}

[[nodiscard]] Result<std::vector<RowSetSplits>> BaseTablet::capture_rs_readers_unlocked(
        const Version& version_range, const CaptureRowsetOps& options) const {
    auto maybe_rs_list = capture_consistent_rowsets_unlocked(version_range, options);
    if (!maybe_rs_list) {
        return ResultError(std::move(maybe_rs_list.error()));
    }
    const auto& rs_list = maybe_rs_list.value().rowsets;
    std::vector<RowSetSplits> rs_splits;
    rs_splits.reserve(rs_list.size());
    for (const auto& rs : rs_list) {
        RowsetReaderSharedPtr rs_reader;
        auto st = rs->create_reader(&rs_reader);
        if (!st) {
            return ResultError(Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                    "failed to create reader for rowset={}, reason={}", rs->rowset_id().to_string(),
                    st.to_string()));
        }
        rs_splits.emplace_back(std::move(rs_reader));
    }
    return rs_splits;
}

[[nodiscard]] Result<TabletReadSource> BaseTablet::capture_read_source(
        const Version& version_range, const CaptureRowsetOps& options) {
    std::shared_lock rdlock(get_header_lock());
    auto maybe_result = capture_consistent_rowsets_unlocked(version_range, options);
    if (!maybe_result) {
        return ResultError(std::move(maybe_result.error()));
    }
    auto rowsets_result = std::move(maybe_result.value());
    TabletReadSource read_source;
    read_source.delete_bitmap = std::move(rowsets_result.delete_bitmap);
    const auto& rowsets = rowsets_result.rowsets;
    read_source.rs_splits.reserve(rowsets.size());
    for (const auto& rs : rowsets) {
        RowsetReaderSharedPtr rs_reader;
        auto st = rs->create_reader(&rs_reader);
        if (!st) {
            return ResultError(Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                    "failed to create reader for rowset={}, reason={}", rs->rowset_id().to_string(),
                    st.to_string()));
        }
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    return read_source;
}

template <typename Fn, typename... Args>
bool call_bthread(bthread_t& th, const bthread_attr_t* attr, Fn&& fn, Args&&... args) {
    auto p_wrap_fn = new auto([=] { fn(args...); });
    auto call_back = [](void* ar) -> void* {
        auto f = reinterpret_cast<decltype(p_wrap_fn)>(ar);
        (*f)();
        delete f;
        return nullptr;
    };
    return bthread_start_background(&th, attr, call_back, p_wrap_fn) == 0;
}

struct GetRowsetsCntl : std::enable_shared_from_this<GetRowsetsCntl> {
    struct RemoteGetRowsetResult {
        std::vector<RowsetMetaSharedPtr> rowsets;
        std::unique_ptr<DeleteBitmap> delete_bitmap;
    };

    Status start_req_bg() {
        task_cnt = req_addrs.size();
        for (const auto& [ip, port] : req_addrs) {
            bthread_t tid;
            bthread_attr_t attr = BTHREAD_ATTR_NORMAL;

            bool succ = call_bthread(tid, &attr, [self = shared_from_this(), &ip, port]() {
                LOG(INFO) << "start to get tablet rowsets from peer BE, ip=" << ip;
                Defer defer_log {[&ip, port]() {
                    LOG(INFO) << "finish to get rowsets from peer BE, ip=" << ip
                              << ", port=" << port;
                }};

                PGetTabletRowsetsRequest req;
                req.set_tablet_id(self->tablet_id);
                req.set_version_start(self->version_range.first);
                req.set_version_end(self->version_range.second);
                if (self->delete_bitmap_keys.has_value()) {
                    req.mutable_delete_bitmap_keys()->CopyFrom(self->delete_bitmap_keys.value());
                }
                brpc::Controller cntl;
                cntl.set_timeout_ms(60000);
                cntl.set_max_retry(3);
                PGetTabletRowsetsResponse response;
                auto start_tm_us = MonotonicMicros();
#ifndef BE_TEST
                std::shared_ptr<PBackendService_Stub> stub =
                        ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(ip, port);
                if (stub == nullptr) {
                    self->result = ResultError(Status::InternalError(
                            "failed to fetch get_tablet_rowsets stub, ip={}, port={}", ip, port));
                    return;
                }
                stub->get_tablet_rowsets(&cntl, &req, &response, nullptr);
#else
                TEST_SYNC_POINT_CALLBACK("get_tablet_rowsets", &response);
#endif
                g_remote_fetch_tablet_rowsets_single_request_latency
                        << MonotonicMicros() - start_tm_us;

                std::unique_lock l(self->butex);
                if (self->done) {
                    return;
                }
                --self->task_cnt;
                auto resp_st = Status::create(response.status());
                DBUG_EXECUTE_IF("GetRowsetCntl::start_req_bg.inject_failure",
                                { resp_st = Status::InternalError("inject error"); });
                if (cntl.Failed() || !resp_st) {
                    if (self->task_cnt != 0) {
                        return;
                    }
                    std::stringstream reason;
                    reason << "failed to get rowsets from all replicas, tablet_id="
                           << self->tablet_id;
                    if (cntl.Failed()) {
                        reason << ", reason=[" << cntl.ErrorCode() << "] " << cntl.ErrorText();
                    } else {
                        reason << ", reason=" << resp_st.to_string();
                    }
                    self->result = ResultError(Status::InternalError(reason.str()));
                    self->done = true;
                    self->event.signal();
                    return;
                }

                Defer done_cb {[&]() {
                    self->done = true;
                    self->event.signal();
                }};
                std::vector<RowsetMetaSharedPtr> rs_metas;
                for (auto&& rs_pb : response.rowsets()) {
                    auto rs_meta = std::make_shared<RowsetMeta>();
                    if (!rs_meta->init_from_pb(rs_pb)) {
                        self->result =
                                ResultError(Status::InternalError("failed to init rowset from pb"));
                        return;
                    }
                    rs_metas.push_back(std::move(rs_meta));
                }
                CaptureRowsetResult result;
                self->result->rowsets = std::move(rs_metas);

                if (response.has_delete_bitmap()) {
                    self->result->delete_bitmap = std::make_unique<DeleteBitmap>(
                            DeleteBitmap::from_pb(response.delete_bitmap(), self->tablet_id));
                }
            });

            if (!succ) {
                return Status::InternalError(
                        "failed to create bthread when request rowsets for tablet={}", tablet_id);
            }
            if (bthread_join(tid, nullptr) != 0) {
                return Status::InternalError("failed to join bthread tid={}", tid);
            }
        }
        return Status::OK();
    }

    Result<RemoteGetRowsetResult> wait_for_ret() {
        event.wait();
        return std::move(result);
    }

    int64_t tablet_id;
    std::vector<std::pair<std::string, int32_t>> req_addrs;
    Version version_range;
    std::optional<DeleteBitmapPB> delete_bitmap_keys = std::nullopt;

private:
    size_t task_cnt;

    bthread::Mutex butex;
    bthread::CountdownEvent event {1};
    bool done = false;

    Result<RemoteGetRowsetResult> result;
};

Result<std::vector<std::pair<std::string, int32_t>>> get_peer_replicas_addresses(
        const int64_t tablet_id) {
    auto* cluster_info = ExecEnv::GetInstance()->cluster_info();
    DCHECK_NE(cluster_info, nullptr);
    auto master_addr = cluster_info->master_fe_addr;
    TGetTabletReplicaInfosRequest req;
    req.tablet_ids.push_back(tablet_id);
    TGetTabletReplicaInfosResult resp;
    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&](FrontendServiceConnection& client) { client->getTabletReplicaInfos(resp, req); });
    if (!st) {
        return ResultError(Status::InternalError(
                "failed to get tablet replica infos, rpc error={}, tablet_id={}", st.to_string(),
                tablet_id));
    }

    auto it = resp.tablet_replica_infos.find(tablet_id);
    if (it == resp.tablet_replica_infos.end()) {
        return ResultError(Status::InternalError("replicas not found, tablet_id={}", tablet_id));
    }
    auto replicas = it->second;
    auto local_host = BackendOptions::get_localhost();
    bool include_local_host = false;
    DBUG_EXECUTE_IF("get_peer_replicas_address.enable_local_host", { include_local_host = true; });
    auto ret_view =
            replicas | std::views::filter([&local_host, include_local_host](const auto& replica) {
                return local_host.find(replica.host) == std::string::npos || include_local_host;
            }) |
            std::views::transform([](auto& replica) {
                return std::make_pair(std::move(replica.host), replica.brpc_port);
            });
    return std::vector(ret_view.begin(), ret_view.end());
}

Result<CaptureRowsetResult> BaseTablet::_remote_capture_rowsets(
        const Version& version_range) const {
    auto start_tm_us = MonotonicMicros();
    Defer defer {
            [&]() { g_remote_fetch_tablet_rowsets_latency << MonotonicMicros() - start_tm_us; }};
#ifndef BE_TEST
    auto maybe_be_addresses = get_peer_replicas_addresses(tablet_id());
#else
    Result<std::vector<std::pair<std::string, int32_t>>> maybe_be_addresses;
    TEST_SYNC_POINT_CALLBACK("get_peer_replicas_addresses", &maybe_be_addresses);
#endif
    DBUG_EXECUTE_IF("Tablet::_remote_get_rowsets_meta.inject_replica_address_fail",
                    { maybe_be_addresses = ResultError(Status::InternalError("inject failure")); });
    if (!maybe_be_addresses) {
        return ResultError(std::move(maybe_be_addresses.error()));
    }
    auto be_addresses = std::move(maybe_be_addresses.value());
    if (be_addresses.empty()) {
        LOG(WARNING) << "no peers replica for tablet=" << tablet_id();
        return ResultError(Status::InternalError("no replicas for tablet={}", tablet_id()));
    }

    auto cntl = std::make_shared<GetRowsetsCntl>();
    cntl->tablet_id = tablet_id();
    cntl->req_addrs = std::move(be_addresses);
    cntl->version_range = version_range;
    bool is_mow = keys_type() == KeysType::UNIQUE_KEYS && enable_unique_key_merge_on_write();
    CaptureRowsetResult result;
    if (is_mow) {
        result.delete_bitmap =
                std::make_unique<DeleteBitmap>(_tablet_meta->delete_bitmap()->snapshot());
        DeleteBitmapPB delete_bitmap_keys;
        auto keyset = result.delete_bitmap->delete_bitmap |
                      std::views::transform([](const auto& kv) { return kv.first; });
        for (const auto& key : keyset) {
            const auto& [rs_id, seg_id, version] = key;
            delete_bitmap_keys.mutable_rowset_ids()->Add(rs_id.to_string());
            delete_bitmap_keys.mutable_segment_ids()->Add(seg_id);
            delete_bitmap_keys.mutable_versions()->Add(version);
        }
        cntl->delete_bitmap_keys = std::move(delete_bitmap_keys);
    }

    RETURN_IF_ERROR_RESULT(cntl->start_req_bg());
    auto maybe_meta = cntl->wait_for_ret();
    if (!maybe_meta) {
        auto err = Status::InternalError(
                "tried to get rowsets from peer replicas and failed, "
                "reason={}",
                maybe_meta.error());
        return ResultError(std::move(err));
    }

    auto& remote_meta = maybe_meta.value();
    const auto& rs_metas = remote_meta.rowsets;
    for (const auto& rs_meta : rs_metas) {
        RowsetSharedPtr rs;
        auto st = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), {}, rs_meta, &rs);
        if (!st) {
            return ResultError(std::move(st));
        }
        result.rowsets.push_back(std::move(rs));
    }
    if (is_mow) {
        DCHECK_NE(result.delete_bitmap, nullptr);
        result.delete_bitmap->merge(*remote_meta.delete_bitmap);
    }
    return result;
}

} // namespace doris
