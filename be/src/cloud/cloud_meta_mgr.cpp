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
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "cloud/pb_convert.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/cloud.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "io/fs/obj_storage_client.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/network_util.h"
#include "util/s3_util.h"
#include "util/thrift_rpc_helper.h"

namespace doris::cloud {
using namespace ErrorCode;

Status bthread_fork_join(const std::vector<std::function<Status()>>& tasks, int concurrency) {
    if (tasks.empty()) {
        return Status::OK();
    }

    bthread::Mutex lock;
    bthread::ConditionVariable cond;
    Status status; // Guard by lock
    int count = 0; // Guard by lock

    auto* run_bthread_work = +[](void* arg) -> void* {
        auto* f = reinterpret_cast<std::function<void()>*>(arg);
        (*f)();
        delete f;
        return nullptr;
    };

    for (const auto& task : tasks) {
        {
            std::unique_lock lk(lock);
            // Wait until there are available slots
            while (status.ok() && count >= concurrency) {
                cond.wait(lk);
            }
            if (!status.ok()) {
                break;
            }

            // Increase running task count
            ++count;
        }

        // dispatch task into bthreads
        auto* fn = new std::function<void()>([&, &task = task] {
            auto st = task();
            {
                std::lock_guard lk(lock);
                --count;
                if (!st.ok()) {
                    std::swap(st, status);
                }
                cond.notify_one();
            }
        });

        bthread_t bthread_id;
        if (bthread_start_background(&bthread_id, nullptr, run_bthread_work, fn) != 0) {
            run_bthread_work(fn);
        }
    }

    // Wait until all running tasks have done
    {
        std::unique_lock lk(lock);
        while (count > 0) {
            cond.wait(lk);
        }
    }

    return status;
}

namespace {
constexpr int kBrpcRetryTimes = 3;

bvar::LatencyRecorder _get_rowset_latency("doris_CloudMetaMgr", "get_rowset");
bvar::LatencyRecorder g_cloud_commit_txn_resp_redirect_latency("cloud_table_stats_report_latency");

class MetaServiceProxy {
public:
    static Status get_client(std::shared_ptr<MetaService_Stub>* stub) {
        TEST_SYNC_POINT_RETURN_WITH_VALUE("MetaServiceProxy::get_client", Status::OK(), stub);
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
                    if (config::meta_service_endpoint.find(',') != std::string::npos) {
                        is_meta_service_endpoint_list = true;
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

        const char* load_balancer_name = nullptr;
        std::string endpoint;
        if (is_meta_service_endpoint_list) {
            endpoint = fmt::format("list://{}", config::meta_service_endpoint);
            load_balancer_name = "random";
        } else {
            std::string ip;
            uint16_t port;
            Status s = get_meta_service_ip_and_port(&ip, &port);
            if (!s.ok()) {
                LOG(WARNING) << "fail to get meta service ip and port: " << s;
                return s;
            }

            endpoint = get_host_port(ip, port);
        }

        brpc::ChannelOptions options;
        options.connection_group =
                fmt::format("ms_{}", index.fetch_add(1, std::memory_order_relaxed));
        if (channel->Init(endpoint.c_str(), load_balancer_name, &options) != 0) {
            return Status::InvalidArgument("failed to init brpc channel, endpoint: {}", endpoint);
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
        // idle timeout only works without list endpoint.
        return !is_meta_service_endpoint_list && idle_timeout_ms > 0 &&
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
        if (!s.ok()) [[unlikely]] {
            return s;
        }

        *stub = std::make_shared<MetaService_Stub>(channel.release(),
                                                   google::protobuf::Service::STUB_OWNS_CHANNEL);

        long deadline = now;
        // connection age only works without list endpoint.
        if (!is_meta_service_endpoint_list &&
            config::meta_service_connection_age_base_minutes > 0) {
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

    static std::atomic_bool is_meta_service_endpoint_list;

    std::shared_mutex _mutex;
    std::atomic<long> _last_access_at_ms {0};
    long _deadline_ms {0};
    std::shared_ptr<MetaService_Stub> _stub;
};

std::atomic_bool MetaServiceProxy::is_meta_service_endpoint_list = false;

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
    } else if constexpr (is_any_v<Request, CreateRowsetRequest>) {
        return fmt::format(" tablet_id={}", req.rowset_meta().tablet_id());
    } else {
        static_assert(!sizeof(Request));
    }
}

inline std::default_random_engine make_random_engine() {
    return std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
}

template <typename Request, typename Response>
using MetaServiceMethod = void (MetaService_Stub::*)(::google::protobuf::RpcController*,
                                                     const Request*, Response*,
                                                     ::google::protobuf::Closure*);

template <typename Request, typename Response>
Status retry_rpc(std::string_view op_name, const Request& req, Response* res,
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
        cntl.set_max_retry(kBrpcRetryTimes);
        res->Clear();
        (stub.get()->*method)(&cntl, &req, res, nullptr);
        if (cntl.Failed()) [[unlikely]] {
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

} // namespace

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
    using namespace std::chrono;

    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::sync_tablet_rowsets", Status::OK(), tablet);

    std::shared_ptr<MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int tried = 0;
    while (true) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        GetRowsetRequest req;
        GetRowsetResponse resp;

        int64_t tablet_id = tablet->tablet_id();
        int64_t table_id = tablet->table_id();
        int64_t index_id = tablet->index_id();
        req.set_cloud_unique_id(config::cloud_unique_id);
        auto* idx = req.mutable_idx();
        idx->set_tablet_id(tablet_id);
        idx->set_table_id(table_id);
        idx->set_index_id(index_id);
        idx->set_partition_id(tablet->partition_id());
        {
            std::shared_lock rlock(tablet->get_header_lock());
            req.set_start_version(tablet->max_version_unlocked() + 1);
            req.set_base_compaction_cnt(tablet->base_compaction_cnt());
            req.set_cumulative_compaction_cnt(tablet->cumulative_compaction_cnt());
            req.set_cumulative_point(tablet->cumulative_layer_point());
        }
        req.set_end_version(-1);
        VLOG_DEBUG << "send GetRowsetRequest: " << req.ShortDebugString();

        stub->get_rowset(&cntl, &req, &resp, nullptr);
        int64_t latency = cntl.latency_us();
        _get_rowset_latency << latency;
        int retry_times = config::meta_service_rpc_retry_times;
        if (cntl.Failed()) {
            if (tried++ < retry_times) {
                auto rng = make_random_engine();
                std::uniform_int_distribution<uint32_t> u(20, 200);
                std::uniform_int_distribution<uint32_t> u1(500, 1000);
                uint32_t duration_ms = tried >= 100 ? u(rng) : u1(rng);
                std::this_thread::sleep_for(milliseconds(duration_ms));
                LOG_INFO("failed to get rowset meta")
                        .tag("reason", cntl.ErrorText())
                        .tag("tablet_id", tablet_id)
                        .tag("table_id", table_id)
                        .tag("index_id", index_id)
                        .tag("partition_id", tablet->partition_id())
                        .tag("tried", tried)
                        .tag("sleep", duration_ms);
                continue;
            }
            return Status::RpcError("failed to get rowset meta: {}", cntl.ErrorText());
        }
        if (resp.status().code() == MetaServiceCode::TABLET_NOT_FOUND) {
            return Status::NotFound("failed to get rowset meta: {}", resp.status().msg());
        }
        if (resp.status().code() != MetaServiceCode::OK) {
            return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
        }
        if (latency > 100 * 1000) { // 100ms
            LOG(INFO) << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                      << ", latency=" << latency << "us";
        } else {
            LOG_EVERY_N(INFO, 100)
                    << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                    << ", latency=" << latency << "us";
        }

        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        tablet->last_sync_time_s = now;

        if (tablet->enable_unique_key_merge_on_write()) {
            DeleteBitmap delete_bitmap(tablet_id);
            int64_t old_max_version = req.start_version() - 1;
            auto st = sync_tablet_delete_bitmap(tablet, old_max_version, resp.rowset_meta(),
                                                resp.stats(), req.idx(), &delete_bitmap);
            if (st.is<ErrorCode::ROWSETS_EXPIRED>() && tried++ < retry_times) {
                LOG_WARNING("rowset meta is expired, need to retry")
                        .tag("tablet", tablet->tablet_id())
                        .tag("tried", tried)
                        .error(st);
                continue;
            }
            if (!st.ok()) {
                LOG_WARNING("failed to get delete bitmap")
                        .tag("tablet", tablet->tablet_id())
                        .error(st);
                return st;
            }
            tablet->tablet_meta()->delete_bitmap().merge(delete_bitmap);
        }
        {
            const auto& stats = resp.stats();
            std::unique_lock wlock(tablet->get_header_lock());

            // ATTN: we are facing following data race
            //
            // resp_base_compaction_cnt=0|base_compaction_cnt=0|resp_cumulative_compaction_cnt=0|cumulative_compaction_cnt=1|resp_max_version=11|max_version=8
            //
            //   BE-compaction-thread                 meta-service                                     BE-query-thread
            //            |                                |                                                |
            //    local   |    commit cumu-compaction      |                                                |
            //   cc_cnt=0 |  --------------------------->  |     sync rowset (long rpc, local cc_cnt=0 )    |   local
            //            |                                |  <-----------------------------------------    |  cc_cnt=0
            //            |                                |  -.                                            |
            //    local   |       done cc_cnt=1            |    \                                           |
            //   cc_cnt=1 |  <---------------------------  |     \                                          |
            //            |                                |      \  returned with resp cc_cnt=0 (snapshot) |
            //            |                                |       '------------------------------------>   |   local
            //            |                                |                                                |  cc_cnt=1
            //            |                                |                                                |
            //            |                                |                                                |  CHECK FAIL
            //            |                                |                                                |  need retry
            // To get rid of just retry syncing tablet
            if (stats.base_compaction_cnt() < tablet->base_compaction_cnt() ||
                stats.cumulative_compaction_cnt() < tablet->cumulative_compaction_cnt())
                    [[unlikely]] {
                // stale request, ignore
                LOG_WARNING("stale get rowset meta request")
                        .tag("resp_base_compaction_cnt", stats.base_compaction_cnt())
                        .tag("base_compaction_cnt", tablet->base_compaction_cnt())
                        .tag("resp_cumulative_compaction_cnt", stats.cumulative_compaction_cnt())
                        .tag("cumulative_compaction_cnt", tablet->cumulative_compaction_cnt())
                        .tag("tried", tried);
                if (tried++ < 10) continue;
                return Status::OK();
            }
            std::vector<RowsetSharedPtr> rowsets;
            rowsets.reserve(resp.rowset_meta().size());
            for (const auto& cloud_rs_meta_pb : resp.rowset_meta()) {
                VLOG_DEBUG << "get rowset meta, tablet_id=" << cloud_rs_meta_pb.tablet_id()
                           << ", version=[" << cloud_rs_meta_pb.start_version() << '-'
                           << cloud_rs_meta_pb.end_version() << ']';
                auto existed_rowset = tablet->get_rowset_by_version(
                        {cloud_rs_meta_pb.start_version(), cloud_rs_meta_pb.end_version()});
                if (existed_rowset &&
                    existed_rowset->rowset_id().to_string() == cloud_rs_meta_pb.rowset_id_v2()) {
                    continue; // Same rowset, skip it
                }
                RowsetMetaPB meta_pb = cloud_rowset_meta_to_doris(cloud_rs_meta_pb);
                auto rs_meta = std::make_shared<RowsetMeta>();
                rs_meta->init_from_pb(meta_pb);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                Status s = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
                if (!s.ok()) {
                    LOG_WARNING("create rowset").tag("status", s);
                    return s;
                }
                rowsets.push_back(std::move(rowset));
            }
            if (!rowsets.empty()) {
                // `rowsets.empty()` could happen after doing EMPTY_CUMULATIVE compaction. e.g.:
                //   BE has [0-1][2-11][12-12], [12-12] is delete predicate, cp is 2;
                //   after doing EMPTY_CUMULATIVE compaction, MS cp is 13, get_rowset will return [2-11][12-12].
                bool version_overlap =
                        tablet->max_version_unlocked() >= rowsets.front()->start_version();
                tablet->add_rowsets(std::move(rowsets), version_overlap, wlock, warmup_delta_data);
            }
            tablet->last_base_compaction_success_time_ms = stats.last_base_compaction_time_ms();
            tablet->last_cumu_compaction_success_time_ms = stats.last_cumu_compaction_time_ms();
            tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
            tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
            tablet->set_cumulative_layer_point(stats.cumulative_point());
            tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                            stats.num_rows(), stats.data_size());
        }
        return Status::OK();
    }
}

bool CloudMetaMgr::sync_tablet_delete_bitmap_by_cache(CloudTablet* tablet, int64_t old_max_version,
                                                      std::ranges::range auto&& rs_metas,
                                                      DeleteBitmap* delete_bitmap) {
    std::set<int64_t> txn_processed;
    for (auto& rs_meta : rs_metas) {
        auto txn_id = rs_meta.txn_id();
        if (txn_processed.find(txn_id) != txn_processed.end()) {
            continue;
        }
        txn_processed.insert(txn_id);
        DeleteBitmapPtr tmp_delete_bitmap;
        RowsetIdUnorderedSet tmp_rowset_ids;
        std::shared_ptr<PublishStatus> publish_status =
                std::make_shared<PublishStatus>(PublishStatus::INIT);
        CloudStorageEngine& engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
        Status status = engine.txn_delete_bitmap_cache().get_delete_bitmap(
                txn_id, tablet->tablet_id(), &tmp_delete_bitmap, &tmp_rowset_ids, &publish_status);
        if (status.ok() && *(publish_status.get()) == PublishStatus::SUCCEED) {
            delete_bitmap->merge(*tmp_delete_bitmap);
            engine.txn_delete_bitmap_cache().remove_unused_tablet_txn_info(txn_id,
                                                                           tablet->tablet_id());
        } else {
            LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << tablet->tablet_id()
                         << ", txn_id=" << txn_id << ", status=" << status;
            return false;
        }
    }
    return true;
}

Status CloudMetaMgr::sync_tablet_delete_bitmap(CloudTablet* tablet, int64_t old_max_version,
                                               std::ranges::range auto&& rs_metas,
                                               const TabletStatsPB& stats, const TabletIndexPB& idx,
                                               DeleteBitmap* delete_bitmap) {
    if (rs_metas.empty()) {
        return Status::OK();
    }

    if (sync_tablet_delete_bitmap_by_cache(tablet, old_max_version, rs_metas, delete_bitmap)) {
        return Status::OK();
    } else {
        LOG(WARNING) << "failed to sync delete bitmap by txn info. tablet_id="
                     << tablet->tablet_id();
        DeleteBitmapPtr new_delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        *delete_bitmap = *new_delete_bitmap;
    }

    std::shared_ptr<MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int64_t new_max_version = std::max(old_max_version, rs_metas.rbegin()->end_version());
    brpc::Controller cntl;
    // When there are many delete bitmaps that need to be synchronized, it
    // may take a longer time, especially when loading the tablet for the
    // first time, so set a relatively long timeout time.
    cntl.set_timeout_ms(3 * config::meta_service_brpc_timeout_ms);
    GetDeleteBitmapRequest req;
    GetDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet->tablet_id());
    req.set_base_compaction_cnt(stats.base_compaction_cnt());
    req.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
    req.set_cumulative_point(stats.cumulative_point());
    *(req.mutable_idx()) = idx;
    // New rowset sync all versions of delete bitmap
    for (const auto& rs_meta : rs_metas) {
        req.add_rowset_ids(rs_meta.rowset_id_v2());
        req.add_begin_versions(0);
        req.add_end_versions(new_max_version);
    }

    // old rowset sync incremental versions of delete bitmap
    if (old_max_version > 0 && old_max_version < new_max_version) {
        RowsetIdUnorderedSet all_rs_ids;
        RETURN_IF_ERROR(tablet->get_all_rs_id(old_max_version, &all_rs_ids));
        for (const auto& rs_id : all_rs_ids) {
            req.add_rowset_ids(rs_id.to_string());
            req.add_begin_versions(old_max_version + 1);
            req.add_end_versions(new_max_version);
        }
    }

    VLOG_DEBUG << "send GetDeleteBitmapRequest: " << req.ShortDebugString();
    stub->get_delete_bitmap(&cntl, &req, &res, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get delete bitmap: {}", cntl.ErrorText());
    }
    if (res.status().code() == MetaServiceCode::TABLET_NOT_FOUND) {
        return Status::NotFound("failed to get delete bitmap: {}", res.status().msg());
    }
    // The delete bitmap of stale rowsets will be removed when commit compaction job,
    // then delete bitmap of stale rowsets cannot be obtained. But the rowsets obtained
    // by sync_tablet_rowsets may include these stale rowsets. When this case happend, the
    // error code of ROWSETS_EXPIRED will be returned, we need to retry sync rowsets again.
    //
    // Be query thread             meta-service          Be compaction thread
    //      |                            |                         |
    //      |        get rowset          |                         |
    //      |--------------------------->|                         |
    //      |    return get rowset       |                         |
    //      |<---------------------------|                         |
    //      |                            |        commit job       |
    //      |                            |<------------------------|
    //      |                            |    return commit job    |
    //      |                            |------------------------>|
    //      |      get delete bitmap     |                         |
    //      |--------------------------->|                         |
    //      |  return get delete bitmap  |                         |
    //      |<---------------------------|                         |
    //      |                            |                         |
    if (res.status().code() == MetaServiceCode::ROWSETS_EXPIRED) {
        return Status::Error<ErrorCode::ROWSETS_EXPIRED, false>("failed to get delete bitmap: {}",
                                                                res.status().msg());
    }
    if (res.status().code() != MetaServiceCode::OK) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("failed to get delete bitmap: {}",
                                                               res.status().msg());
    }
    const auto& rowset_ids = res.rowset_ids();
    const auto& segment_ids = res.segment_ids();
    const auto& vers = res.versions();
    const auto& delete_bitmaps = res.segment_delete_bitmaps();
    for (size_t i = 0; i < rowset_ids.size(); i++) {
        RowsetId rst_id;
        rst_id.init(rowset_ids[i]);
        delete_bitmap->merge({rst_id, segment_ids[i], vers[i]},
                             roaring::Roaring::read(delete_bitmaps[i].data()));
    }
    int64_t latency = cntl.latency_us();
    if (latency > 100 * 1000) { // 100ms
        LOG(INFO) << "finish get_delete_bitmap rpc. rowset_ids.size()=" << rowset_ids.size()
                  << ", delete_bitmaps.size()=" << delete_bitmaps.size() << ", latency=" << latency
                  << "us";
    } else {
        LOG_EVERY_N(INFO, 100) << "finish get_delete_bitmap rpc. rowset_ids.size()="
                               << rowset_ids.size()
                               << ", delete_bitmaps.size()=" << delete_bitmaps.size()
                               << ", latency=" << latency << "us";
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_rowset(const RowsetMeta& rs_meta,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "prepare rowset, tablet_id: " << rs_meta.tablet_id()
               << ", rowset_id: " << rs_meta.rowset_id() << " txn_id: " << rs_meta.txn_id();

    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_txn_id(rs_meta.txn_id());

    RowsetMetaPB doris_rs_meta = rs_meta.get_rowset_pb(/*skip_schema=*/true);
    doris_rowset_meta_to_cloud(req.mutable_rowset_meta(), std::move(doris_rs_meta));

    Status st = retry_rpc("prepare rowset", req, &resp, &MetaService_Stub::prepare_rowset);
    if (!st.ok() && resp.status().code() == MetaServiceCode::ALREADY_EXISTED) {
        if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
            RowsetMetaPB doris_rs_meta =
                    cloud_rowset_meta_to_doris(std::move(*resp.mutable_existed_rowset_meta()));
            *existed_rs_meta = std::make_shared<RowsetMeta>();
            (*existed_rs_meta)->init_from_pb(doris_rs_meta);
        }
        return Status::AlreadyExist("failed to prepare rowset: {}", resp.status().msg());
    }
    return st;
}

Status CloudMetaMgr::commit_rowset(const RowsetMeta& rs_meta,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "commit rowset, tablet_id: " << rs_meta.tablet_id()
               << ", rowset_id: " << rs_meta.rowset_id() << " txn_id: " << rs_meta.txn_id();
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_txn_id(rs_meta.txn_id());

    RowsetMetaPB rs_meta_pb = rs_meta.get_rowset_pb();
    doris_rowset_meta_to_cloud(req.mutable_rowset_meta(), std::move(rs_meta_pb));
    Status st = retry_rpc("commit rowset", req, &resp, &MetaService_Stub::commit_rowset);
    if (!st.ok() && resp.status().code() == MetaServiceCode::ALREADY_EXISTED) {
        if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
            RowsetMetaPB doris_rs_meta =
                    cloud_rowset_meta_to_doris(std::move(*resp.mutable_existed_rowset_meta()));
            *existed_rs_meta = std::make_shared<RowsetMeta>();
            (*existed_rs_meta)->init_from_pb(doris_rs_meta);
        }
        return Status::AlreadyExist("failed to commit rowset: {}", resp.status().msg());
    }
    return st;
}

Status CloudMetaMgr::update_tmp_rowset(const RowsetMeta& rs_meta) {
    VLOG_DEBUG << "update committed rowset, tablet_id: " << rs_meta.tablet_id()
               << ", rowset_id: " << rs_meta.rowset_id();
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);

    // Variant schema maybe updated, so we need to update the schema as well.
    // The updated rowset meta after `rowset->merge_rowset_meta` in `BaseTablet::update_delete_bitmap`
    // will be lost in `update_tmp_rowset` if skip_schema.So in order to keep the latest schema we should keep schema in update_tmp_rowset
    // for variant type
    bool skip_schema = rs_meta.tablet_schema()->num_variant_columns() == 0;
    RowsetMetaPB rs_meta_pb = rs_meta.get_rowset_pb(skip_schema);
    doris_rowset_meta_to_cloud(req.mutable_rowset_meta(), std::move(rs_meta_pb));
    Status st =
            retry_rpc("update committed rowset", req, &resp, &MetaService_Stub::update_tmp_rowset);
    if (!st.ok() && resp.status().code() == MetaServiceCode::ROWSET_META_NOT_FOUND) {
        return Status::InternalError("failed to update committed rowset: {}", resp.status().msg());
    }
    return st;
}

// async send TableStats(in res) to FE coz we are in streamload ctx, response to the user ASAP
static void send_stats_to_fe_async(const int64_t db_id, const int64_t txn_id,
                                   const std::string& label, CommitTxnResponse& res) {
    std::string protobufBytes;
    res.SerializeToString(&protobufBytes);
    auto st = ExecEnv::GetInstance()->send_table_stats_thread_pool()->submit_func(
            [db_id, txn_id, label, protobufBytes]() -> Status {
                TReportCommitTxnResultRequest request;
                TStatus result;

                if (protobufBytes.length() <= 0) {
                    LOG(WARNING) << "protobufBytes: " << protobufBytes.length();
                    return Status::OK(); // nobody cares the return status
                }

                request.__set_dbId(db_id);
                request.__set_txnId(txn_id);
                request.__set_label(label);
                request.__set_payload(protobufBytes);

                Status status;
                int64_t duration_ns = 0;
                TNetworkAddress master_addr =
                        ExecEnv::GetInstance()->master_info()->network_address;
                if (master_addr.hostname.empty() || master_addr.port == 0) {
                    status = Status::Error<SERVICE_UNAVAILABLE>(
                            "Have not get FE Master heartbeat yet");
                } else {
                    SCOPED_RAW_TIMER(&duration_ns);

                    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                            master_addr.hostname, master_addr.port,
                            [&request, &result](FrontendServiceConnection& client) {
                                client->reportCommitTxnResult(result, request);
                            }));

                    status = Status::create<false>(result);
                }
                g_cloud_commit_txn_resp_redirect_latency << duration_ns / 1000;

                if (!status.ok()) {
                    LOG(WARNING) << "TableStats report RPC to FE failed, errmsg=" << status
                                 << " dbId=" << db_id << " txnId=" << txn_id << " label=" << label;
                    return Status::OK(); // nobody cares the return status
                } else {
                    LOG(INFO) << "TableStats report RPC to FE success, msg=" << status
                              << " dbId=" << db_id << " txnId=" << txn_id << " label=" << label;
                    return Status::OK();
                }
            });
    if (!st.ok()) {
        LOG(WARNING) << "TableStats report to FE task submission failed: " << st.to_string();
    }
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
    auto st = retry_rpc("commit txn", req, &res, &MetaService_Stub::commit_txn);

    if (st.ok()) {
        send_stats_to_fe_async(ctx.db_id, ctx.txn_id, ctx.label, res);
    }

    return st;
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
    } else if (ctx.txn_id > 0) {
        req.set_txn_id(ctx.txn_id);
    } else {
        LOG(WARNING) << "failed abort txn, with illegal input, db_id=" << ctx.db_id
                     << " txn_id=" << ctx.txn_id << " label=" << ctx.label;
        return Status::InternalError<false>("failed to abort txn");
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

Status CloudMetaMgr::get_storage_vault_info(StorageVaultInfos* vault_infos) {
    GetObjStoreInfoRequest req;
    GetObjStoreInfoResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    Status s =
            retry_rpc("get storage vault info", req, &resp, &MetaService_Stub::get_obj_store_info);
    if (!s.ok()) {
        return s;
    }

    auto add_obj_store = [&vault_infos](const auto& obj_store) {
        vault_infos->emplace_back(obj_store.id(), S3Conf::get_s3_conf(obj_store),
                                  StorageVaultPB_PathFormat {});
    };

    std::ranges::for_each(resp.obj_info(), add_obj_store);
    std::ranges::for_each(resp.storage_vault(), [&](const auto& vault) {
        if (vault.has_hdfs_info()) {
            vault_infos->emplace_back(vault.id(), vault.hdfs_info(), vault.path_format());
        }
        if (vault.has_obj_info()) {
            add_obj_store(vault.obj_info());
        }
    });

    for (int i = 0; i < resp.obj_info_size(); ++i) {
        resp.mutable_obj_info(i)->set_sk(resp.obj_info(i).sk().substr(0, 2) + "xxx");
    }
    for (int i = 0; i < resp.storage_vault_size(); ++i) {
        auto* j = resp.mutable_storage_vault(i);
        if (!j->has_obj_info()) continue;
        j->mutable_obj_info()->set_sk(j->obj_info().sk().substr(0, 2) + "xxx");
    }

    LOG(INFO) << "get storage vault response: " << resp.ShortDebugString();
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
    for (auto& [key, bitmap] : delete_bitmap->delete_bitmap) {
        req.add_rowset_ids(std::get<0>(key).to_string());
        req.add_segment_ids(std::get<1>(key));
        req.add_versions(std::get<2>(key));
        // To save space, convert array and bitmap containers to run containers
        bitmap.runOptimize();
        std::string bitmap_data(bitmap.getSizeInBytes(), '\0');
        bitmap.write(bitmap_data.data());
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
