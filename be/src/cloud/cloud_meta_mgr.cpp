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
#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <type_traits>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "cloud/pb_convert.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet_meta.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/network_util.h"
#include "util/s3_util.h"

namespace doris::cloud {
using namespace ErrorCode;

static bvar::LatencyRecorder g_get_rowset_latency("doris_CloudMetaMgr", "get_rowset");

static constexpr int BRPC_RETRY_TIMES = 3;

class MetaServiceProxy {
public:
    static Status get_client(std::shared_ptr<MetaService_Stub>* stub) {
        SYNC_POINT_RETURN_WITH_VALUE("MetaServiceProxy::get_client", Status::OK(), stub);
        return get_pooled_client(stub);
    }

private:
    static Status get_pooled_client(std::shared_ptr<MetaService_Stub>* stub) {
        static std::once_flag proxies_flag;
        static size_t num_proxies = 1;
        static std::atomic<size_t> index(0);
        static std::unique_ptr<MetaServiceProxy[]> proxies;

        std::call_once(
                proxies_flag, +[]() {
                    if (config::meta_service_connection_pooled) {
                        num_proxies = config::meta_service_connection_pool_size;
                    }
                    proxies = std::make_unique<MetaServiceProxy[]>(num_proxies);
                });

        for (size_t i = 0; i + 1 < num_proxies; ++i) {
            size_t next_index = index.fetch_add(1, std::memory_order_relaxed) % num_proxies;
            Status s = proxies[next_index].get(stub);
            if (s.ok()) return Status::OK();
        }

        size_t next_index = index.fetch_add(1, std::memory_order_relaxed) % num_proxies;
        return proxies[next_index].get(stub);
    }

    static Status init_channel(brpc::Channel* channel) {
        static std::atomic<size_t> index = 1;

        std::string ip;
        uint16_t port;
        Status s = get_meta_service_ip_and_port(&ip, &port);
        if (!s.ok()) {
            LOG(WARNING) << "fail to get meta service ip and port: " << s;
            return s;
        }

        size_t next_id = index.fetch_add(1, std::memory_order_relaxed);
        brpc::ChannelOptions options;
        options.connection_group = fmt::format("ms_{}", next_id);
        if (channel->Init(ip.c_str(), port, &options) != 0) {
            return Status::InternalError("fail to init brpc channel, ip: {}, port: {}", ip, port);
        }
        return Status::OK();
    }

    static Status get_meta_service_ip_and_port(std::string* ip, uint16_t* port) {
        std::string parsed_host;
        if (!parse_endpoint(config::meta_service_endpoint, &parsed_host, port)) {
            return Status::InvalidArgument("invalid meta service endpoint: {}",
                                           config::meta_service_endpoint);
        }
        if (is_valid_ip(parsed_host)) {
            *ip = std::move(parsed_host);
            return Status::OK();
        }
        return hostname_to_ip(parsed_host, *ip);
    }

    bool is_idle_timeout(long now) {
        auto idle_timeout_ms = config::meta_service_idle_connection_timeout_ms;
        return idle_timeout_ms > 0 &&
               _last_access_at_ms.load(std::memory_order_relaxed) + idle_timeout_ms < now;
    }

    Status get(std::shared_ptr<MetaService_Stub>* stub) {
        using namespace std::chrono;

        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        {
            std::shared_lock lock(_mutex);
            if (_deadline_ms >= now && !is_idle_timeout(now)) {
                _last_access_at_ms.store(now, std::memory_order_relaxed);
                *stub = _stub;
                return Status::OK();
            }
        }

        auto channel = std::make_unique<brpc::Channel>();
        Status s = init_channel(channel.get());
        if (UNLIKELY(!s.ok())) {
            return s;
        }

        *stub = std::make_shared<MetaService_Stub>(channel.release(),
                                                   google::protobuf::Service::STUB_OWNS_CHANNEL);

        long deadline = now;
        if (config::meta_service_connection_age_base_minutes > 0) {
            std::default_random_engine rng(static_cast<uint32_t>(now));
            std::uniform_int_distribution<> uni(
                    config::meta_service_connection_age_base_minutes,
                    config::meta_service_connection_age_base_minutes * 2);
            deadline = now + duration_cast<milliseconds>(minutes(uni(rng))).count();
        } else {
            deadline = LONG_MAX;
        }

        // Last one WIN
        std::unique_lock lock(_mutex);
        _last_access_at_ms.store(now, std::memory_order_relaxed);
        _deadline_ms = deadline;
        _stub = *stub;
        return Status::OK();
    }

    std::shared_mutex _mutex;
    std::atomic<long> _last_access_at_ms {0};
    long _deadline_ms {0};
    std::shared_ptr<MetaService_Stub> _stub;
};

template <typename T, typename... Ts>
struct is_any : std::disjunction<std::is_same<T, Ts>...> {};

template <typename T, typename... Ts>
constexpr bool is_any_v = is_any<T, Ts...>::value;

template <typename Request>
static std::string debug_info(const Request& req) {
    if constexpr (is_any_v<Request, CommitTxnRequest, AbortTxnRequest, PrecommitTxnRequest>) {
        return fmt::format(" txn_id={}", req.txn_id());
    } else if constexpr (is_any_v<Request, StartTabletJobRequest, FinishTabletJobRequest>) {
        return fmt::format(" tablet_id={}", req.job().idx().tablet_id());
    } else if constexpr (is_any_v<Request, UpdateDeleteBitmapRequest>) {
        return fmt::format(" tablet_id={}, lock_id={}", req.tablet_id(), req.lock_id());
    } else if constexpr (is_any_v<Request, GetDeleteBitmapUpdateLockRequest>) {
        return fmt::format(" table_id={}, lock_id={}", req.table_id(), req.lock_id());
    } else if constexpr (is_any_v<Request, GetTabletRequest>) {
        return fmt::format(" tablet_id={}", req.tablet_id());
    } else if constexpr (is_any_v<Request, GetObjStoreInfoRequest>) {
        return "";
    } else {
        static_assert(!sizeof(Request));
    }
}

static inline std::default_random_engine make_random_engine() {
    return std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
}

template <typename Request, typename Response>
using MetaServiceMethod = void (MetaService_Stub::*)(::google::protobuf::RpcController*,
                                                     const Request*, Response*,
                                                     ::google::protobuf::Closure*);

template <typename Request, typename Response>
static Status retry_rpc(std::string_view op_name, const Request& req, Response* res,
                        MetaServiceMethod<Request, Response> method) {
    static_assert(std::is_base_of_v<::google::protobuf::Message, Request>);
    static_assert(std::is_base_of_v<::google::protobuf::Message, Response>);

    int retry_times = 0;
    uint32_t duration_ms = 0;
    std::string error_msg;
    std::default_random_engine rng = make_random_engine();
    std::uniform_int_distribution<uint32_t> u(20, 200);
    std::uniform_int_distribution<uint32_t> u2(500, 1000);
    std::shared_ptr<MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));
    while (true) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        res->Clear();
        (stub.get()->*method)(&cntl, &req, res, nullptr);
        if (UNLIKELY(cntl.Failed())) {
            error_msg = cntl.ErrorText();
        } else if (res->status().code() == MetaServiceCode::OK) {
            return Status::OK();
        } else if (res->status().code() != MetaServiceCode::KV_TXN_CONFLICT) {
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>("failed to {}: {}", op_name,
                                                                   res->status().msg());
        } else {
            error_msg = res->status().msg();
        }

        if (++retry_times > config::meta_service_rpc_retry_times) {
            break;
        }

        duration_ms = retry_times <= 100 ? u(rng) : u2(rng);
        LOG(WARNING) << "failed to " << op_name << debug_info(req) << " retry_times=" << retry_times
                     << " sleep=" << duration_ms << "ms : " << cntl.ErrorText();
        bthread_usleep(duration_ms * 1000);
    }
    return Status::RpcError("failed to {}: rpc timeout, last msg={}", op_name, error_msg);
}

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    VLOG_DEBUG << "send GetTabletRequest, tablet_id: " << tablet_id;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::get_tablet_meta", Status::OK(), tablet_id,
                                      tablet_meta);
    GetTabletRequest req;
    GetTabletResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    Status st = retry_rpc("get tablet meta", req, &resp, &MetaService_Stub::get_tablet);
    if (!st.ok()) {
        if (resp.status().code() == MetaServiceCode::TABLET_NOT_FOUND) {
            return Status::NotFound("failed to get tablet meta: {}", resp.status().msg());
        }
        return st;
    }

    *tablet_meta = std::make_shared<TabletMeta>();
    (*tablet_meta)
            ->init_from_pb(cloud_tablet_meta_to_doris(std::move(*resp.mutable_tablet_meta())));
    VLOG_DEBUG << "get tablet meta, tablet_id: " << (*tablet_meta)->tablet_id();
    return Status::OK();
}

Status CloudMetaMgr::sync_tablet_rowsets(CloudTablet* tablet, bool warmup_delta_data) {
    return Status::NotSupported("CloudMetaMgr::sync_tablet_rowsets is not implemented");
}

Status CloudMetaMgr::sync_tablet_delete_bitmap(
        CloudTablet* tablet, int64_t old_max_version,
        const google::protobuf::RepeatedPtrField<RowsetMetaPB>& rs_metas,
        const TabletStatsPB& stats, const TabletIndexPB& idx, DeleteBitmap* delete_bitmap) {
    return Status::NotSupported("CloudMetaMgr::sync_tablet_delete_bitmap is not implemented");
}

Status CloudMetaMgr::prepare_rowset(const RowsetMeta& rs_meta, bool is_tmp,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    return Status::NotSupported("CloudMetaMgr::prepare_rowset is not implemented");
}

Status CloudMetaMgr::commit_rowset(const RowsetMeta& rs_meta, bool is_tmp,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    return Status::NotSupported("CloudMetaMgr::commit_rowset is not implemented");
}

Status CloudMetaMgr::update_tmp_rowset(const RowsetMeta& rs_meta) {
    return Status::NotSupported("CloudMetaMgr::update_tmp_rowset is not implemented");
}

Status CloudMetaMgr::commit_txn(const StreamLoadContext& ctx, bool is_2pc) {
    VLOG_DEBUG << "commit txn, db_id: " << ctx.db_id << ", txn_id: " << ctx.txn_id
               << ", label: " << ctx.label << ", is_2pc: " << is_2pc;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx.db_id);
    req.set_txn_id(ctx.txn_id);
    req.set_is_2pc(is_2pc);
    return retry_rpc("commit txn", req, &res, &MetaService_Stub::commit_txn);
}

Status CloudMetaMgr::abort_txn(const StreamLoadContext& ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx.db_id << ", txn_id: " << ctx.txn_id
               << ", label: " << ctx.label;
    AbortTxnRequest req;
    AbortTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    if (ctx.db_id > 0 && !ctx.label.empty()) {
        req.set_db_id(ctx.db_id);
        req.set_label(ctx.label);
    } else {
        req.set_txn_id(ctx.txn_id);
    }
    return retry_rpc("abort txn", req, &res, &MetaService_Stub::abort_txn);
}

Status CloudMetaMgr::precommit_txn(const StreamLoadContext& ctx) {
    VLOG_DEBUG << "precommit txn, db_id: " << ctx.db_id << ", txn_id: " << ctx.txn_id
               << ", label: " << ctx.label;
    PrecommitTxnRequest req;
    PrecommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx.db_id);
    req.set_txn_id(ctx.txn_id);
    return retry_rpc("precommit txn", req, &res, &MetaService_Stub::precommit_txn);
}

Status CloudMetaMgr::get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
    GetObjStoreInfoRequest req;
    GetObjStoreInfoResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    Status s = retry_rpc("get s3 info", req, &resp, &MetaService_Stub::get_obj_store_info);
    if (!s.ok()) {
        return s;
    }

    for (const auto& obj_store : resp.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_store.ak();
        s3_conf.sk = obj_store.sk();
        s3_conf.endpoint = obj_store.endpoint();
        s3_conf.region = obj_store.region();
        s3_conf.bucket = obj_store.bucket();
        s3_conf.prefix = obj_store.prefix();
        s3_conf.sse_enabled = obj_store.sse_enabled();
        s3_conf.provider = obj_store.provider();
        s3_infos->emplace_back(obj_store.id(), std::move(s3_conf));
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res) {
    VLOG_DEBUG << "prepare_tablet_job: " << job.ShortDebugString();
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::prepare_tablet_job", Status::OK(), job, res);

    StartTabletJobRequest req;
    req.mutable_job()->CopyFrom(job);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("start tablet job", req, res, &MetaService_Stub::start_tablet_job);
}

Status CloudMetaMgr::commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res) {
    VLOG_DEBUG << "commit_tablet_job: " << job.ShortDebugString();
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::commit_tablet_job", Status::OK(), job, res);

    FinishTabletJobRequest req;
    req.mutable_job()->CopyFrom(job);
    req.set_action(FinishTabletJobRequest::COMMIT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("commit tablet job", req, res, &MetaService_Stub::finish_tablet_job);
}

Status CloudMetaMgr::abort_tablet_job(const TabletJobInfoPB& job) {
    VLOG_DEBUG << "abort_tablet_job: " << job.ShortDebugString();
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(FinishTabletJobRequest::ABORT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("abort tablet job", req, &res, &MetaService_Stub::finish_tablet_job);
}

Status CloudMetaMgr::lease_tablet_job(const TabletJobInfoPB& job) {
    VLOG_DEBUG << "lease_tablet_job: " << job.ShortDebugString();
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(FinishTabletJobRequest::LEASE);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("lease tablet job", req, &res, &MetaService_Stub::finish_tablet_job);
}

Status CloudMetaMgr::update_tablet_schema(int64_t tablet_id, const TabletSchema& tablet_schema) {
    VLOG_DEBUG << "send UpdateTabletSchemaRequest, tablet_id: " << tablet_id;

    std::shared_ptr<MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    UpdateTabletSchemaRequest req;
    UpdateTabletSchemaResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);

    TabletSchemaPB tablet_schema_pb;
    tablet_schema.to_schema_pb(&tablet_schema_pb);
    doris_tablet_schema_to_cloud(req.mutable_tablet_schema(), std::move(tablet_schema_pb));
    stub->update_tablet_schema(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to update tablet schema: {}", cntl.ErrorText());
    }
    if (resp.status().code() != MetaServiceCode::OK) {
        return Status::InternalError("failed to update tablet schema: {}", resp.status().msg());
    }
    VLOG_DEBUG << "succeed to update tablet schema, tablet_id: " << tablet_id;
    return Status::OK();
}

Status CloudMetaMgr::update_delete_bitmap(const CloudTablet& tablet, int64_t lock_id,
                                          int64_t initiator, DeleteBitmap* delete_bitmap) {
    VLOG_DEBUG << "update_delete_bitmap , tablet_id: " << tablet.tablet_id();
    UpdateDeleteBitmapRequest req;
    UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet.table_id());
    req.set_partition_id(tablet.partition_id());
    req.set_tablet_id(tablet.tablet_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        req.add_rowset_ids(std::get<0>(iter->first).to_string());
        req.add_segment_ids(std::get<1>(iter->first));
        req.add_versions(std::get<2>(iter->first));
        // To save space, convert array and bitmap containers to run containers
        iter->second.runOptimize();
        std::string bitmap_data(iter->second.getSizeInBytes(), '\0');
        iter->second.write(bitmap_data.data());
        *(req.add_segment_delete_bitmaps()) = std::move(bitmap_data);
    }
    auto st = retry_rpc("update delete bitmap", req, &res, &MetaService_Stub::update_delete_bitmap);
    if (res.status().code() == MetaServiceCode::LOCK_EXPIRED) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                "lock expired when update delete bitmap, tablet_id: {}, lock_id: {}",
                tablet.tablet_id(), lock_id);
    }
    return st;
}

Status CloudMetaMgr::get_delete_bitmap_update_lock(const CloudTablet& tablet, int64_t lock_id,
                                                   int64_t initiator) {
    VLOG_DEBUG << "get_delete_bitmap_update_lock , tablet_id: " << tablet.tablet_id();
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet.table_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_expiration(10); // 10s expiration time for compaction and schema_change
    int retry_times = 0;
    Status st;
    std::default_random_engine rng = make_random_engine();
    std::uniform_int_distribution<uint32_t> u(500, 2000);
    do {
        st = retry_rpc("get delete bitmap update lock", req, &res,
                       &MetaService_Stub::get_delete_bitmap_update_lock);
        if (res.status().code() != MetaServiceCode::LOCK_CONFLICT) {
            break;
        }

        uint32_t duration_ms = u(rng);
        LOG(WARNING) << "get delete bitmap lock conflict. " << debug_info(req)
                     << " retry_times=" << retry_times << " sleep=" << duration_ms
                     << "ms : " << res.status().msg();
        bthread_usleep(duration_ms * 1000);
    } while (++retry_times <= 100);
    return st;
}

} // namespace doris::cloud
