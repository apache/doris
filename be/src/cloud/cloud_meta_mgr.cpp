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
#include <brpc/errno.pb.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "cloud/pb_convert.h"
#include "common/config.h"
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
#include "olap/rowset/rowset_fwd.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/network_util.h"
#include "util/s3_util.h"
#include "util/thrift_rpc_helper.h"

namespace doris::cloud {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

void* run_bthread_work(void* arg) {
    auto* f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

Status bthread_fork_join(const std::vector<std::function<Status()>>& tasks, int concurrency) {
    if (tasks.empty()) {
        return Status::OK();
    }

    bthread::Mutex lock;
    bthread::ConditionVariable cond;
    Status status; // Guard by lock
    int count = 0; // Guard by lock

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

Status bthread_fork_join(std::vector<std::function<Status()>>&& tasks, int concurrency,
                         std::future<Status>* fut) {
    // std::function will cause `copy`, we need to use heap memory to avoid copy ctor called
    auto prom = std::make_shared<std::promise<Status>>();
    *fut = prom->get_future();
    std::function<void()>* fn = new std::function<void()>(
            [tasks = std::move(tasks), concurrency, p = std::move(prom)]() mutable {
                p->set_value(bthread_fork_join(tasks, concurrency));
            });

    bthread_t bthread_id;
    if (bthread_start_background(&bthread_id, nullptr, run_bthread_work, fn) != 0) {
        delete fn;
        return Status::InternalError<false>("failed to create bthread");
    }
    return Status::OK();
}

namespace {
constexpr int kBrpcRetryTimes = 3;

bvar::LatencyRecorder _get_rowset_latency("doris_cloud_meta_mgr_get_rowset");
bvar::LatencyRecorder g_cloud_commit_txn_resp_redirect_latency("cloud_table_stats_report_latency");
bvar::Adder<uint64_t> g_cloud_meta_mgr_rpc_timeout_count("cloud_meta_mgr_rpc_timeout_count");
bvar::Window<bvar::Adder<uint64_t>> g_cloud_ms_rpc_timeout_count_window(
        "cloud_meta_mgr_rpc_timeout_qps", &g_cloud_meta_mgr_rpc_timeout_count, 30);
bvar::LatencyRecorder g_cloud_be_mow_get_dbm_lock_backoff_sleep_time(
        "cloud_be_mow_get_dbm_lock_backoff_sleep_time");

class MetaServiceProxy {
public:
    static Status get_proxy(MetaServiceProxy** proxy) {
        // The 'stub' is a useless parameter, added only to reuse the `get_pooled_client` function.
        std::shared_ptr<MetaService_Stub> stub;
        return get_pooled_client(&stub, proxy);
    }

    void set_unhealthy() {
        std::unique_lock lock(_mutex);
        maybe_unhealthy = true;
    }

    bool need_reconn(long now) {
        return maybe_unhealthy && ((now - last_reconn_time_ms.front()) >
                                   config::meta_service_rpc_reconnect_interval_ms);
    }

    Status get(std::shared_ptr<MetaService_Stub>* stub) {
        using namespace std::chrono;

        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        {
            std::shared_lock lock(_mutex);
            if (_deadline_ms >= now && !is_idle_timeout(now) && !need_reconn(now)) {
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
        if (config::meta_service_connection_age_base_seconds > 0) {
            std::default_random_engine rng(static_cast<uint32_t>(now));
            std::uniform_int_distribution<> uni(
                    config::meta_service_connection_age_base_seconds,
                    config::meta_service_connection_age_base_seconds * 2);
            deadline = now + duration_cast<milliseconds>(seconds(uni(rng))).count();
        }

        // Last one WIN
        std::unique_lock lock(_mutex);
        _last_access_at_ms.store(now, std::memory_order_relaxed);
        _deadline_ms = deadline;
        _stub = *stub;

        last_reconn_time_ms.push(now);
        last_reconn_time_ms.pop();
        maybe_unhealthy = false;

        return Status::OK();
    }

private:
    static bool is_meta_service_endpoint_list() {
        return config::meta_service_endpoint.find(',') != std::string::npos;
    }

    /**
    * This function initializes a pool of `MetaServiceProxy` objects and selects one using
    * round-robin. It returns a client stub via the selected proxy.
    *
    * @param stub A pointer to a shared pointer of `MetaService_Stub` to be retrieved.
    * @param proxy (Optional) A pointer to store the selected `MetaServiceProxy`.
    *
    * @return Status Returns `Status::OK()` on success or an error status on failure.
    */
    static Status get_pooled_client(std::shared_ptr<MetaService_Stub>* stub,
                                    MetaServiceProxy** proxy) {
        static std::once_flag proxies_flag;
        static size_t num_proxies = 1;
        static std::atomic<size_t> index(0);
        static std::unique_ptr<MetaServiceProxy[]> proxies;
        if (config::meta_service_endpoint.empty()) {
            return Status::InvalidArgument(
                    "Meta service endpoint is empty. Please configure manually or wait for "
                    "heartbeat to obtain.");
        }
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
            if (proxy != nullptr) {
                *proxy = &(proxies[next_index]);
            }
            if (s.ok()) return Status::OK();
        }

        size_t next_index = index.fetch_add(1, std::memory_order_relaxed) % num_proxies;
        if (proxy != nullptr) {
            *proxy = &(proxies[next_index]);
        }
        return proxies[next_index].get(stub);
    }

    static Status init_channel(brpc::Channel* channel) {
        static std::atomic<size_t> index = 1;

        const char* load_balancer_name = nullptr;
        std::string endpoint;
        if (is_meta_service_endpoint_list()) {
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
        return !is_meta_service_endpoint_list() && idle_timeout_ms > 0 &&
               _last_access_at_ms.load(std::memory_order_relaxed) + idle_timeout_ms < now;
    }

    std::shared_mutex _mutex;
    std::atomic<long> _last_access_at_ms {0};
    long _deadline_ms {0};
    std::shared_ptr<MetaService_Stub> _stub;

    std::queue<long> last_reconn_time_ms {std::deque<long> {0, 0, 0}};
    bool maybe_unhealthy = false;
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
    } else if constexpr (is_any_v<Request, CreateRowsetRequest>) {
        return fmt::format(" tablet_id={}", req.rowset_meta().tablet_id());
    } else if constexpr (is_any_v<Request, RemoveDeleteBitmapRequest>) {
        return fmt::format(" tablet_id={}", req.tablet_id());
    } else if constexpr (is_any_v<Request, RemoveDeleteBitmapUpdateLockRequest>) {
        return fmt::format(" table_id={}, tablet_id={}, lock_id={}", req.table_id(),
                           req.tablet_id(), req.lock_id());
    } else if constexpr (is_any_v<Request, GetDeleteBitmapRequest>) {
        return fmt::format(" tablet_id={}", req.tablet_id());
    } else if constexpr (is_any_v<Request, GetSchemaDictRequest>) {
        return fmt::format(" index_id={}", req.index_id());
    } else if constexpr (is_any_v<Request, RestoreJobRequest>) {
        return fmt::format(" tablet_id={}", req.tablet_id());
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

    const_cast<Request&>(req).set_request_ip(BackendOptions::get_be_endpoint());

    int retry_times = 0;
    uint32_t duration_ms = 0;
    std::string error_msg;
    std::default_random_engine rng = make_random_engine();
    std::uniform_int_distribution<uint32_t> u(20, 200);
    std::uniform_int_distribution<uint32_t> u2(500, 1000);
    MetaServiceProxy* proxy;
    RETURN_IF_ERROR(MetaServiceProxy::get_proxy(&proxy));
    while (true) {
        std::shared_ptr<MetaService_Stub> stub;
        RETURN_IF_ERROR(proxy->get(&stub));
        brpc::Controller cntl;
        if (op_name == "get delete bitmap" || op_name == "update delete bitmap") {
            cntl.set_timeout_ms(3 * config::meta_service_brpc_timeout_ms);
        } else {
            cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        }
        cntl.set_max_retry(kBrpcRetryTimes);
        res->Clear();
        int error_code = 0;
        (stub.get()->*method)(&cntl, &req, res, nullptr);
        if (cntl.Failed()) [[unlikely]] {
            error_msg = cntl.ErrorText();
            error_code = cntl.ErrorCode();
            proxy->set_unhealthy();
        } else if (res->status().code() == MetaServiceCode::OK) {
            return Status::OK();
        } else if (res->status().code() == MetaServiceCode::INVALID_ARGUMENT) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("failed to {}: {}", op_name,
                                                                     res->status().msg());
        } else if (res->status().code() != MetaServiceCode::KV_TXN_CONFLICT) {
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>("failed to {}: {}", op_name,
                                                                   res->status().msg());
        } else {
            error_msg = res->status().msg();
        }

        if (error_code == brpc::ERPCTIMEDOUT) {
            g_cloud_meta_mgr_rpc_timeout_count << 1;
        }

        ++retry_times;
        if (retry_times > config::meta_service_rpc_retry_times ||
            (retry_times > config::meta_service_rpc_timeout_retry_times &&
             error_code == brpc::ERPCTIMEDOUT) ||
            (retry_times > config::meta_service_conflict_error_retry_times &&
             res->status().code() == MetaServiceCode::KV_TXN_CONFLICT)) {
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

Status CloudMetaMgr::sync_tablet_rowsets(CloudTablet* tablet, const SyncOptions& options,
                                         SyncRowsetStats* sync_stats) {
    std::unique_lock lock {tablet->get_sync_meta_lock()};
    return sync_tablet_rowsets_unlocked(tablet, lock, options, sync_stats);
}

Status CloudMetaMgr::sync_tablet_rowsets_unlocked(CloudTablet* tablet,
                                                  std::unique_lock<bthread::Mutex>& lock,
                                                  const SyncOptions& options,
                                                  SyncRowsetStats* sync_stats) {
    using namespace std::chrono;

    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::sync_tablet_rowsets", Status::OK(), tablet);

    MetaServiceProxy* proxy;
    RETURN_IF_ERROR(MetaServiceProxy::get_proxy(&proxy));
    std::string tablet_info =
            fmt::format("tablet_id={} table_id={} index_id={} partition_id={}", tablet->tablet_id(),
                        tablet->table_id(), tablet->index_id(), tablet->partition_id());
    int tried = 0;
    while (true) {
        std::shared_ptr<MetaService_Stub> stub;
        RETURN_IF_ERROR(proxy->get(&stub));
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
            if (options.full_sync) {
                req.set_start_version(0);
            } else {
                req.set_start_version(tablet->max_version_unlocked() + 1);
            }
            req.set_base_compaction_cnt(tablet->base_compaction_cnt());
            req.set_cumulative_compaction_cnt(tablet->cumulative_compaction_cnt());
            req.set_cumulative_point(tablet->cumulative_layer_point());
        }
        req.set_end_version(-1);
        VLOG_DEBUG << "send GetRowsetRequest: " << req.ShortDebugString();
        auto start = std::chrono::steady_clock::now();
        stub->get_rowset(&cntl, &req, &resp, nullptr);
        auto end = std::chrono::steady_clock::now();
        int64_t latency = cntl.latency_us();
        _get_rowset_latency << latency;
        int retry_times = config::meta_service_rpc_retry_times;
        if (cntl.Failed()) {
            proxy->set_unhealthy();
            if (tried++ < retry_times) {
                auto rng = make_random_engine();
                std::uniform_int_distribution<uint32_t> u(20, 200);
                std::uniform_int_distribution<uint32_t> u1(500, 1000);
                uint32_t duration_ms = tried >= 100 ? u(rng) : u1(rng);
                bthread_usleep(duration_ms * 1000);
                LOG_INFO("failed to get rowset meta, " + tablet_info)
                        .tag("reason", cntl.ErrorText())
                        .tag("tried", tried)
                        .tag("sleep", duration_ms);
                continue;
            }
            return Status::RpcError("failed to get rowset meta: {}", cntl.ErrorText());
        }
        if (resp.status().code() == MetaServiceCode::TABLET_NOT_FOUND) {
            LOG(WARNING) << "failed to get rowset meta, err=" << resp.status().msg() << " "
                         << tablet_info;
            return Status::NotFound("failed to get rowset meta: {}, {}", resp.status().msg(),
                                    tablet_info);
        }
        if (resp.status().code() != MetaServiceCode::OK) {
            LOG(WARNING) << " failed to get rowset meta, err=" << resp.status().msg() << " "
                         << tablet_info;
            return Status::InternalError("failed to get rowset meta: {}, {}", resp.status().msg(),
                                         tablet_info);
        }
        if (latency > 100 * 1000) { // 100ms
            LOG(INFO) << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                      << ", latency=" << latency << "us"
                      << " " << tablet_info;
        } else {
            LOG_EVERY_N(INFO, 100)
                    << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                    << ", latency=" << latency << "us"
                    << " " << tablet_info;
        }

        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        tablet->last_sync_time_s = now;

        if (sync_stats) {
            sync_stats->get_remote_rowsets_rpc_ns +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            sync_stats->get_remote_rowsets_num += resp.rowset_meta().size();
        }

        // If is mow, the tablet has no delete bitmap in base rowsets.
        // So dont need to sync it.
        if (options.sync_delete_bitmap && tablet->enable_unique_key_merge_on_write() &&
            tablet->tablet_state() == TABLET_RUNNING) {
            DBUG_EXECUTE_IF("CloudMetaMgr::sync_tablet_rowsets.sync_tablet_delete_bitmap.block",
                            DBUG_BLOCK);
            DeleteBitmap delete_bitmap(tablet_id);
            int64_t old_max_version = req.start_version() - 1;
            auto st = sync_tablet_delete_bitmap(tablet, old_max_version, resp.rowset_meta(),
                                                resp.stats(), req.idx(), &delete_bitmap,
                                                options.full_sync, sync_stats);
            if (st.is<ErrorCode::ROWSETS_EXPIRED>() && tried++ < retry_times) {
                LOG_INFO("rowset meta is expired, need to retry, " + tablet_info)
                        .tag("tried", tried)
                        .error(st);
                continue;
            }
            if (!st.ok()) {
                LOG_WARNING("failed to get delete bitmap, " + tablet_info).error(st);
                return st;
            }
            tablet->tablet_meta()->delete_bitmap().merge(delete_bitmap);
            if (config::enable_mow_verbose_log && !resp.rowset_meta().empty() &&
                delete_bitmap.cardinality() > 0) {
                std::vector<std::string> new_rowset_msgs;
                std::vector<std::string> old_rowset_msgs;
                std::unordered_set<RowsetId> new_rowset_ids;
                int64_t new_max_version = resp.rowset_meta().rbegin()->end_version();
                for (const auto& rs : resp.rowset_meta()) {
                    RowsetId rowset_id;
                    rowset_id.init(rs.rowset_id_v2());
                    new_rowset_ids.insert(rowset_id);
                    DeleteBitmap rowset_dbm(tablet_id);
                    delete_bitmap.subset(
                            {rowset_id, 0, 0},
                            {rowset_id, std::numeric_limits<DeleteBitmap::SegmentId>::max(),
                             std::numeric_limits<DeleteBitmap::Version>::max()},
                            &rowset_dbm);
                    size_t cardinality = rowset_dbm.cardinality();
                    size_t count = rowset_dbm.get_delete_bitmap_count();
                    if (cardinality > 0) {
                        new_rowset_msgs.push_back(fmt::format(
                                "({}[{}-{}],{},{})", rs.rowset_id_v2(), rs.start_version(),
                                rs.end_version(), count, cardinality));
                    }
                }

                if (old_max_version > 0) {
                    std::vector<RowsetSharedPtr> old_rowsets;
                    RowsetIdUnorderedSet old_rowset_ids;
                    {
                        std::lock_guard<std::shared_mutex> rlock(tablet->get_header_lock());
                        RETURN_IF_ERROR(
                                tablet->get_all_rs_id_unlocked(old_max_version, &old_rowset_ids));
                        old_rowsets = tablet->get_rowset_by_ids(&old_rowset_ids);
                    }
                    for (const auto& rs : old_rowsets) {
                        if (!new_rowset_ids.contains(rs->rowset_id())) {
                            DeleteBitmap rowset_dbm(tablet_id);
                            delete_bitmap.subset(
                                    {rs->rowset_id(), 0, 0},
                                    {rs->rowset_id(),
                                     std::numeric_limits<DeleteBitmap::SegmentId>::max(),
                                     std::numeric_limits<DeleteBitmap::Version>::max()},
                                    &rowset_dbm);
                            size_t cardinality = rowset_dbm.cardinality();
                            size_t count = rowset_dbm.get_delete_bitmap_count();
                            if (cardinality > 0) {
                                old_rowset_msgs.push_back(
                                        fmt::format("({}{},{},{})", rs->rowset_id().to_string(),
                                                    rs->version().to_string(), count, cardinality));
                            }
                        }
                    }
                }

                LOG_INFO("[verbose] sync tablet delete bitmap " + tablet_info)
                        .tag("full_sync", options.full_sync)
                        .tag("old_max_version", old_max_version)
                        .tag("new_max_version", new_max_version)
                        .tag("cumu_compaction_cnt", resp.stats().cumulative_compaction_cnt())
                        .tag("base_compaction_cnt", resp.stats().base_compaction_cnt())
                        .tag("cumu_point", resp.stats().cumulative_point())
                        .tag("rowset_num", resp.rowset_meta().size())
                        .tag("delete_bitmap_cardinality", delete_bitmap.cardinality())
                        .tag("old_rowsets(rowset,count,cardinality)",
                             fmt::format("[{}]", fmt::join(old_rowset_msgs, ", ")))
                        .tag("new_rowsets(rowset,count,cardinality)",
                             fmt::format("[{}]", fmt::join(new_rowset_msgs, ", ")));
            }
        }
        DBUG_EXECUTE_IF("CloudMetaMgr::sync_tablet_rowsets.before.modify_tablet_meta", {
            auto target_tablet_id = dp->param<int64_t>("tablet_id", -1);
            if (target_tablet_id == tablet->tablet_id()) {
                DBUG_BLOCK
            }
        });
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
                LOG_WARNING("stale get rowset meta request " + tablet_info)
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
                tablet->add_rowsets(std::move(rowsets), version_overlap, wlock,
                                    options.warmup_delta_data);
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
        std::shared_ptr<PublishStatus> publish_status =
                std::make_shared<PublishStatus>(PublishStatus::INIT);
        CloudStorageEngine& engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
        Status status = engine.txn_delete_bitmap_cache().get_delete_bitmap(
                txn_id, tablet->tablet_id(), &tmp_delete_bitmap, nullptr, &publish_status);
        // CloudMetaMgr::sync_tablet_delete_bitmap_by_cache() is called after we sync rowsets from meta services.
        // If the control flows reaches here, it's gauranteed that the rowsets is commited in meta services, so we can
        // use the delete bitmap from cache directly if *publish_status == PublishStatus::SUCCEED without checking other
        // stats(version or compaction stats)
        if (status.ok() && *publish_status == PublishStatus::SUCCEED) {
            // tmp_delete_bitmap contains sentinel marks, we should remove it before merge it to delete bitmap.
            // Also, the version of delete bitmap key in tmp_delete_bitmap is DeleteBitmap::TEMP_VERSION_COMMON,
            // we should replace it with the rowset's real version
            DCHECK(rs_meta.start_version() == rs_meta.end_version());
            int64_t rowset_version = rs_meta.start_version();
            for (const auto& [delete_bitmap_key, bitmap_value] : tmp_delete_bitmap->delete_bitmap) {
                // skip sentinel mark, which is used for delete bitmap correctness check
                if (std::get<1>(delete_bitmap_key) != DeleteBitmap::INVALID_SEGMENT_ID) {
                    delete_bitmap->merge({std::get<0>(delete_bitmap_key),
                                          std::get<1>(delete_bitmap_key), rowset_version},
                                         bitmap_value);
                }
            }
            engine.txn_delete_bitmap_cache().remove_unused_tablet_txn_info(txn_id,
                                                                           tablet->tablet_id());
        } else {
            LOG_EVERY_N(INFO, 20)
                    << "delete bitmap not found in cache, will sync rowset to get. tablet_id= "
                    << tablet->tablet_id() << ", txn_id=" << txn_id << ", status=" << status;
            return false;
        }
    }
    return true;
}

Status CloudMetaMgr::sync_tablet_delete_bitmap(CloudTablet* tablet, int64_t old_max_version,
                                               std::ranges::range auto&& rs_metas,
                                               const TabletStatsPB& stats, const TabletIndexPB& idx,
                                               DeleteBitmap* delete_bitmap, bool full_sync,
                                               SyncRowsetStats* sync_stats) {
    if (rs_metas.empty()) {
        return Status::OK();
    }

    if (!full_sync &&
        sync_tablet_delete_bitmap_by_cache(tablet, old_max_version, rs_metas, delete_bitmap)) {
        if (sync_stats) {
            sync_stats->get_local_delete_bitmap_rowsets_num += rs_metas.size();
        }
        return Status::OK();
    } else {
        DeleteBitmapPtr new_delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        *delete_bitmap = *new_delete_bitmap;
    }

    int64_t new_max_version = std::max(old_max_version, rs_metas.rbegin()->end_version());
    // When there are many delete bitmaps that need to be synchronized, it
    // may take a longer time, especially when loading the tablet for the
    // first time, so set a relatively long timeout time.
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
    if (sync_stats) {
        sync_stats->get_remote_delete_bitmap_rowsets_num += req.rowset_ids_size();
    }

    VLOG_DEBUG << "send GetDeleteBitmapRequest: " << req.ShortDebugString();

    auto start = std::chrono::steady_clock::now();
    auto st = retry_rpc("get delete bitmap", req, &res, &MetaService_Stub::get_delete_bitmap);
    auto end = std::chrono::steady_clock::now();
    if (st.code() == ErrorCode::THRIFT_RPC_ERROR) {
        return st;
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
    if (rowset_ids.size() != segment_ids.size() || rowset_ids.size() != vers.size() ||
        rowset_ids.size() != delete_bitmaps.size()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "get delete bitmap data wrong,"
                "rowset_ids.size={},segment_ids.size={},vers.size={},delete_bitmaps.size={}",
                rowset_ids.size(), segment_ids.size(), vers.size(), delete_bitmaps.size());
    }
    if (sync_stats) {
        sync_stats->get_remote_delete_bitmap_rpc_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        sync_stats->get_remote_delete_bitmap_key_count += delete_bitmaps.size();
        for (const auto& dbm : delete_bitmaps) {
            sync_stats->get_remote_delete_bitmap_bytes += dbm.length();
        }
    }
    for (int i = 0; i < rowset_ids.size(); i++) {
        RowsetId rst_id;
        rst_id.init(rowset_ids[i]);
        delete_bitmap->merge(
                {rst_id, segment_ids[i], vers[i]},
                roaring::Roaring::readSafe(delete_bitmaps[i].data(), delete_bitmaps[i].length()));
    }
    int64_t latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
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

Status CloudMetaMgr::prepare_rowset(const RowsetMeta& rs_meta, const std::string& job_id,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "prepare rowset, tablet_id: " << rs_meta.tablet_id()
               << ", rowset_id: " << rs_meta.rowset_id() << " txn_id: " << rs_meta.txn_id();
    {
        Status ret_st;
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("CloudMetaMgr::prepare_rowset", ret_st);
    }
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_txn_id(rs_meta.txn_id());
    req.set_tablet_job_id(job_id);

    RowsetMetaPB doris_rs_meta = rs_meta.get_rowset_pb(/*skip_schema=*/true);
    doris_rowset_meta_to_cloud(req.mutable_rowset_meta(), std::move(doris_rs_meta));

    Status st = retry_rpc("prepare rowset", req, &resp, &MetaService_Stub::prepare_rowset);
    if (!st.ok() && resp.status().code() == MetaServiceCode::ALREADY_EXISTED) {
        if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
            RowsetMetaPB doris_rs_meta_tmp =
                    cloud_rowset_meta_to_doris(std::move(*resp.mutable_existed_rowset_meta()));
            *existed_rs_meta = std::make_shared<RowsetMeta>();
            (*existed_rs_meta)->init_from_pb(doris_rs_meta_tmp);
        }
        return Status::AlreadyExist("failed to prepare rowset: {}", resp.status().msg());
    }
    return st;
}

Status CloudMetaMgr::commit_rowset(RowsetMeta& rs_meta, const std::string& job_id,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "commit rowset, tablet_id: " << rs_meta.tablet_id()
               << ", rowset_id: " << rs_meta.rowset_id() << " txn_id: " << rs_meta.txn_id();
    {
        Status ret_st;
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("CloudMetaMgr::commit_rowset", ret_st);
    }
    check_table_size_correctness(rs_meta);
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_txn_id(rs_meta.txn_id());
    req.set_tablet_job_id(job_id);

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
    int64_t timeout_ms = -1;
    // if the `job_id` is not empty, it means this rowset was produced by a compaction job.
    if (config::enable_compaction_delay_commit_for_warm_up && !job_id.empty()) {
        // 1. assume the download speed is 100MB/s
        // 2. we double the download time as timeout for safety
        // 3. for small rowsets, the timeout we calculate maybe quite small, so we need a min_time_out
        const double speed_mbps = 100.0; // 100MB/s
        const double safety_factor = 2.0;
        timeout_ms = std::min(
                std::max(static_cast<int64_t>(static_cast<double>(rs_meta.data_disk_size()) /
                                              (speed_mbps * 1024 * 1024) * safety_factor * 1000),
                         config::warm_up_rowset_sync_wait_min_timeout_ms),
                config::warm_up_rowset_sync_wait_max_timeout_ms);
        LOG(INFO) << "warm up rowset: " << rs_meta.version() << ", job_id: " << job_id
                  << ", with timeout: " << timeout_ms << " ms";
    }
    auto& manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
    manager.warm_up_rowset(rs_meta, timeout_ms);
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
                        ExecEnv::GetInstance()->cluster_info()->master_fe_addr;
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
    {
        Status ret_st;
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("CloudMetaMgr::commit_txn", ret_st);
    }
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx.db_id);
    req.set_txn_id(ctx.txn_id);
    req.set_is_2pc(is_2pc);
    req.set_enable_txn_lazy_commit(config::enable_cloud_txn_lazy_commit);
    auto st = retry_rpc("commit txn", req, &res, &MetaService_Stub::commit_txn);

    if (st.ok()) {
        send_stats_to_fe_async(ctx.db_id, ctx.txn_id, ctx.label, res);
    }

    return st;
}

Status CloudMetaMgr::abort_txn(const StreamLoadContext& ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx.db_id << ", txn_id: " << ctx.txn_id
               << ", label: " << ctx.label;
    {
        Status ret_st;
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("CloudMetaMgr::abort_txn", ret_st);
    }
    AbortTxnRequest req;
    AbortTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_reason(std::string(ctx.status.msg().substr(0, 1024)));
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
    {
        Status ret_st;
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("CloudMetaMgr::precommit_txn", ret_st);
    }
    PrecommitTxnRequest req;
    PrecommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx.db_id);
    req.set_txn_id(ctx.txn_id);
    return retry_rpc("precommit txn", req, &res, &MetaService_Stub::precommit_txn);
}

Status CloudMetaMgr::prepare_restore_job(const TabletMetaPB& tablet_meta) {
    VLOG_DEBUG << "prepare restore job, tablet_id: " << tablet_meta.tablet_id();
    RestoreJobRequest req;
    RestoreJobResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_meta.tablet_id());
    req.set_expiration(config::snapshot_expire_time_sec);

    doris_tablet_meta_to_cloud(req.mutable_tablet_meta(), std::move(tablet_meta));
    return retry_rpc("prepare restore job", req, &resp, &MetaService_Stub::prepare_restore_job);
}

Status CloudMetaMgr::commit_restore_job(const int64_t tablet_id) {
    VLOG_DEBUG << "commit restore job, tablet_id: " << tablet_id;
    RestoreJobRequest req;
    RestoreJobResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);

    return retry_rpc("commit restore job", req, &resp, &MetaService_Stub::commit_restore_job);
}

Status CloudMetaMgr::finish_restore_job(const int64_t tablet_id, bool is_completed) {
    VLOG_DEBUG << "finish restore job, tablet_id: " << tablet_id
               << ", is_completed: " << is_completed;
    RestoreJobRequest req;
    RestoreJobResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    req.set_is_completed(is_completed);

    return retry_rpc("finish restore job", req, &resp, &MetaService_Stub::finish_restore_job);
}

Status CloudMetaMgr::get_storage_vault_info(StorageVaultInfos* vault_infos, bool* is_vault_mode) {
    GetObjStoreInfoRequest req;
    GetObjStoreInfoResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    Status s =
            retry_rpc("get storage vault info", req, &resp, &MetaService_Stub::get_obj_store_info);
    if (!s.ok()) {
        return s;
    }

    *is_vault_mode = resp.enable_storage_vault();

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

    // desensitization, hide secret
    for (int i = 0; i < resp.obj_info_size(); ++i) {
        resp.mutable_obj_info(i)->set_sk(resp.obj_info(i).sk().substr(0, 2) + "xxx");
    }
    for (int i = 0; i < resp.storage_vault_size(); ++i) {
        auto* j = resp.mutable_storage_vault(i);
        if (!j->has_obj_info()) continue;
        j->mutable_obj_info()->set_sk(j->obj_info().sk().substr(0, 2) + "xxx");
    }

    LOG(INFO) << "get storage vault, enable_storage_vault=" << *is_vault_mode
              << " response=" << resp.ShortDebugString();
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
    auto st = retry_rpc("commit tablet job", req, res, &MetaService_Stub::finish_tablet_job);
    if (res->status().code() == MetaServiceCode::KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                "txn conflict when commit tablet job {}", job.ShortDebugString());
    }
    return st;
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

Status CloudMetaMgr::update_delete_bitmap(const CloudTablet& tablet, int64_t lock_id,
                                          int64_t initiator, DeleteBitmap* delete_bitmap,
                                          int64_t txn_id, bool is_explicit_txn,
                                          int64_t next_visible_version) {
    VLOG_DEBUG << "update_delete_bitmap , tablet_id: " << tablet.tablet_id();
    UpdateDeleteBitmapRequest req;
    UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet.table_id());
    req.set_partition_id(tablet.partition_id());
    req.set_tablet_id(tablet.tablet_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_is_explicit_txn(is_explicit_txn);
    if (txn_id > 0) {
        req.set_txn_id(txn_id);
    }
    if (next_visible_version > 0) {
        req.set_next_visible_version(next_visible_version);
    }
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
    DBUG_EXECUTE_IF("CloudMetaMgr::test_update_big_delete_bitmap", {
        LOG(INFO) << "test_update_big_delete_bitmap for tablet " << tablet.tablet_id();
        auto count = dp->param<int>("count", 30000);
        if (!delete_bitmap->delete_bitmap.empty()) {
            auto& key = delete_bitmap->delete_bitmap.begin()->first;
            auto& bitmap = delete_bitmap->delete_bitmap.begin()->second;
            for (int i = 1000; i < (1000 + count); i++) {
                req.add_rowset_ids(std::get<0>(key).to_string());
                req.add_segment_ids(std::get<1>(key));
                req.add_versions(i);
                // To save space, convert array and bitmap containers to run containers
                bitmap.runOptimize();
                std::string bitmap_data(bitmap.getSizeInBytes(), '\0');
                bitmap.write(bitmap_data.data());
                *(req.add_segment_delete_bitmaps()) = std::move(bitmap_data);
            }
        }
    });
    DBUG_EXECUTE_IF("CloudMetaMgr::test_update_delete_bitmap_fail", {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR>(
                "test update delete bitmap failed, tablet_id: {}, lock_id: {}", tablet.tablet_id(),
                lock_id);
    });
    auto st = retry_rpc("update delete bitmap", req, &res, &MetaService_Stub::update_delete_bitmap);
    if (config::enable_update_delete_bitmap_kv_check_core &&
        res.status().code() == MetaServiceCode::UPDATE_OVERRIDE_EXISTING_KV) {
        auto& msg = res.status().msg();
        LOG_WARNING(msg);
        CHECK(false) << msg;
    }
    if (res.status().code() == MetaServiceCode::LOCK_EXPIRED) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                "lock expired when update delete bitmap, tablet_id: {}, lock_id: {}, initiator: "
                "{}, error_msg: {}",
                tablet.tablet_id(), lock_id, initiator, res.status().msg());
    }
    return st;
}

Status CloudMetaMgr::cloud_update_delete_bitmap_without_lock(
        const CloudTablet& tablet, DeleteBitmap* delete_bitmap,
        std::map<std::string, int64_t>& rowset_to_versions, int64_t pre_rowset_agg_start_version,
        int64_t pre_rowset_agg_end_version) {
    LOG(INFO) << "cloud_update_delete_bitmap_without_lock, tablet_id: " << tablet.tablet_id()
              << ", delete_bitmap size: " << delete_bitmap->delete_bitmap.size();
    UpdateDeleteBitmapRequest req;
    UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet.table_id());
    req.set_partition_id(tablet.partition_id());
    req.set_tablet_id(tablet.tablet_id());
    // use a fake lock id to resolve compatibility issues
    req.set_lock_id(-3);
    req.set_without_lock(true);
    for (auto& [key, bitmap] : delete_bitmap->delete_bitmap) {
        req.add_rowset_ids(std::get<0>(key).to_string());
        req.add_segment_ids(std::get<1>(key));
        req.add_versions(std::get<2>(key));
        if (pre_rowset_agg_end_version > 0) {
            DCHECK(rowset_to_versions.find(std::get<0>(key).to_string()) !=
                   rowset_to_versions.end())
                    << "rowset_to_versions not found for key=" << std::get<0>(key).to_string();
            req.add_pre_rowset_versions(rowset_to_versions[std::get<0>(key).to_string()]);
        }
        DCHECK(pre_rowset_agg_end_version <= 0 || pre_rowset_agg_end_version == std::get<2>(key))
                << "pre_rowset_agg_end_version=" << pre_rowset_agg_end_version
                << " not equal to version=" << std::get<2>(key);
        // To save space, convert array and bitmap containers to run containers
        bitmap.runOptimize();
        std::string bitmap_data(bitmap.getSizeInBytes(), '\0');
        bitmap.write(bitmap_data.data());
        *(req.add_segment_delete_bitmaps()) = std::move(bitmap_data);
    }
    if (pre_rowset_agg_start_version > 0 && pre_rowset_agg_end_version > 0) {
        req.set_pre_rowset_agg_start_version(pre_rowset_agg_start_version);
        req.set_pre_rowset_agg_end_version(pre_rowset_agg_end_version);
    }
    return retry_rpc("update delete bitmap", req, &res, &MetaService_Stub::update_delete_bitmap);
}

Status CloudMetaMgr::get_delete_bitmap_update_lock(const CloudTablet& tablet, int64_t lock_id,
                                                   int64_t initiator) {
    DBUG_EXECUTE_IF("get_delete_bitmap_update_lock.inject_fail", {
        auto p = dp->param("percent", 0.01);
        std::mt19937 gen {std::random_device {}()};
        std::bernoulli_distribution inject_fault {p};
        if (inject_fault(gen)) {
            return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR>(
                    "injection error when get get_delete_bitmap_update_lock, "
                    "tablet_id={}, lock_id={}, initiator={}",
                    tablet.tablet_id(), lock_id, initiator);
        }
    });
    VLOG_DEBUG << "get_delete_bitmap_update_lock , tablet_id: " << tablet.tablet_id()
               << ",lock_id:" << lock_id;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet.table_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    // set expiration time for compaction and schema_change
    req.set_expiration(config::delete_bitmap_lock_expiration_seconds);
    int retry_times = 0;
    Status st;
    std::default_random_engine rng = make_random_engine();
    std::uniform_int_distribution<uint32_t> u(500, 2000);
    uint64_t backoff_sleep_time_ms {0};
    do {
        bool test_conflict = false;
        st = retry_rpc("get delete bitmap update lock", req, &res,
                       &MetaService_Stub::get_delete_bitmap_update_lock);
        DBUG_EXECUTE_IF("CloudMetaMgr::test_get_delete_bitmap_update_lock_conflict",
                        { test_conflict = true; });
        if (!test_conflict && res.status().code() != MetaServiceCode::LOCK_CONFLICT) {
            break;
        }

        uint32_t duration_ms = u(rng);
        LOG(WARNING) << "get delete bitmap lock conflict. " << debug_info(req)
                     << " retry_times=" << retry_times << " sleep=" << duration_ms
                     << "ms : " << res.status().msg();
        auto start = std::chrono::steady_clock::now();
        bthread_usleep(duration_ms * 1000);
        auto end = std::chrono::steady_clock::now();
        backoff_sleep_time_ms += duration_cast<std::chrono::milliseconds>(end - start).count();
    } while (++retry_times <= config::get_delete_bitmap_lock_max_retry_times);
    g_cloud_be_mow_get_dbm_lock_backoff_sleep_time << backoff_sleep_time_ms;
    DBUG_EXECUTE_IF("CloudMetaMgr.get_delete_bitmap_update_lock.inject_sleep", {
        auto p = dp->param("percent", 0.01);
        // 100s > Config.calculate_delete_bitmap_task_timeout_seconds = 60s
        auto sleep_time = dp->param("sleep", 15);
        std::mt19937 gen {std::random_device {}()};
        std::bernoulli_distribution inject_fault {p};
        if (inject_fault(gen)) {
            LOG_INFO("injection sleep for {} seconds, tablet_id={}", sleep_time,
                     tablet.tablet_id());
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
    });
    if (res.status().code() == MetaServiceCode::KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                "txn conflict when get delete bitmap update lock, table_id {}, lock_id {}, "
                "initiator {}",
                tablet.table_id(), lock_id, initiator);
    } else if (res.status().code() == MetaServiceCode::LOCK_CONFLICT) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                "lock conflict when get delete bitmap update lock, table_id {}, lock_id {}, "
                "initiator {}",
                tablet.table_id(), lock_id, initiator);
    }
    return st;
}

void CloudMetaMgr::remove_delete_bitmap_update_lock(int64_t table_id, int64_t lock_id,
                                                    int64_t initiator, int64_t tablet_id) {
    LOG(INFO) << "remove_delete_bitmap_update_lock ,table_id: " << table_id
              << ",lock_id:" << lock_id << ",initiator:" << initiator << ",tablet_id:" << tablet_id;
    RemoveDeleteBitmapUpdateLockRequest req;
    RemoveDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(table_id);
    req.set_tablet_id(tablet_id);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    auto st = retry_rpc("remove delete bitmap update lock", req, &res,
                        &MetaService_Stub::remove_delete_bitmap_update_lock);
    if (!st.ok()) {
        LOG(WARNING) << "remove delete bitmap update lock fail,table_id=" << table_id
                     << ",tablet_id=" << tablet_id << ",lock_id=" << lock_id
                     << ",st=" << st.to_string();
    }
}

void CloudMetaMgr::check_table_size_correctness(const RowsetMeta& rs_meta) {
    if (!config::enable_table_size_correctness_check) {
        return;
    }
    int64_t total_segment_size = get_segment_file_size(rs_meta);
    int64_t total_inverted_index_size = get_inverted_index_file_szie(rs_meta);
    if (rs_meta.data_disk_size() != total_segment_size ||
        rs_meta.index_disk_size() != total_inverted_index_size ||
        rs_meta.data_disk_size() + rs_meta.index_disk_size() != rs_meta.total_disk_size()) {
        LOG(WARNING) << "[Cloud table table size check failed]:"
                     << " tablet id: " << rs_meta.tablet_id()
                     << ", rowset id:" << rs_meta.rowset_id()
                     << ", rowset data disk size:" << rs_meta.data_disk_size()
                     << ", rowset real data disk size:" << total_segment_size
                     << ", rowset index disk size:" << rs_meta.index_disk_size()
                     << ", rowset real index disk size:" << total_inverted_index_size
                     << ", rowset total disk size:" << rs_meta.total_disk_size()
                     << ", rowset segment path:"
                     << StorageResource().remote_segment_path(rs_meta.tablet_id(),
                                                              rs_meta.rowset_id().to_string(), 0);
        DCHECK(false);
    }
}

int64_t CloudMetaMgr::get_segment_file_size(const RowsetMeta& rs_meta) {
    int64_t total_segment_size = 0;
    const auto fs = const_cast<RowsetMeta&>(rs_meta).fs();
    if (!fs) {
        LOG(WARNING) << "get fs failed, resource_id={}" << rs_meta.resource_id();
    }
    for (int64_t seg_id = 0; seg_id < rs_meta.num_segments(); seg_id++) {
        std::string segment_path = StorageResource().remote_segment_path(
                rs_meta.tablet_id(), rs_meta.rowset_id().to_string(), seg_id);
        int64_t segment_file_size = 0;
        auto st = fs->file_size(segment_path, &segment_file_size);
        if (!st.ok()) {
            segment_file_size = 0;
            if (st.is<NOT_FOUND>()) {
                LOG(INFO) << "cloud table size correctness check get segment size 0 because "
                             "file not exist! msg:"
                          << st.msg() << ", segment path:" << segment_path;
            } else {
                LOG(WARNING) << "cloud table size correctness check get segment size failed! msg:"
                             << st.msg() << ", segment path:" << segment_path;
            }
        }
        total_segment_size += segment_file_size;
    }
    return total_segment_size;
}

int64_t CloudMetaMgr::get_inverted_index_file_szie(const RowsetMeta& rs_meta) {
    int64_t total_inverted_index_size = 0;
    const auto fs = const_cast<RowsetMeta&>(rs_meta).fs();
    if (!fs) {
        LOG(WARNING) << "get fs failed, resource_id={}" << rs_meta.resource_id();
    }
    if (rs_meta.tablet_schema()->get_inverted_index_storage_format() ==
        InvertedIndexStorageFormatPB::V1) {
        const auto& indices = rs_meta.tablet_schema()->inverted_indexes();
        for (auto& index : indices) {
            for (int seg_id = 0; seg_id < rs_meta.num_segments(); ++seg_id) {
                std::string segment_path = StorageResource().remote_segment_path(
                        rs_meta.tablet_id(), rs_meta.rowset_id().to_string(), seg_id);
                int64_t file_size = 0;

                std::string inverted_index_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(segment_path),
                                index->index_id(), index->get_index_suffix());
                auto st = fs->file_size(inverted_index_file_path, &file_size);
                if (!st.ok()) {
                    file_size = 0;
                    if (st.is<NOT_FOUND>()) {
                        LOG(INFO) << "cloud table size correctness check get inverted index v1 "
                                     "0 because file not exist! msg:"
                                  << st.msg()
                                  << ", inverted index path:" << inverted_index_file_path;
                    } else {
                        LOG(WARNING)
                                << "cloud table size correctness check get inverted index v1 "
                                   "size failed! msg:"
                                << st.msg() << ", inverted index path:" << inverted_index_file_path;
                    }
                }
                total_inverted_index_size += file_size;
            }
        }
    } else {
        for (int seg_id = 0; seg_id < rs_meta.num_segments(); ++seg_id) {
            int64_t file_size = 0;
            std::string segment_path = StorageResource().remote_segment_path(
                    rs_meta.tablet_id(), rs_meta.rowset_id().to_string(), seg_id);

            std::string inverted_index_file_path = InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(segment_path));
            auto st = fs->file_size(inverted_index_file_path, &file_size);
            if (!st.ok()) {
                file_size = 0;
                if (st.is<NOT_FOUND>()) {
                    LOG(INFO) << "cloud table size correctness check get inverted index v2 "
                                 "0 because file not exist! msg:"
                              << st.msg() << ", inverted index path:" << inverted_index_file_path;
                } else {
                    LOG(WARNING) << "cloud table size correctness check get inverted index v2 "
                                    "size failed! msg:"
                                 << st.msg()
                                 << ", inverted index path:" << inverted_index_file_path;
                }
            }
            total_inverted_index_size += file_size;
        }
    }
    return total_inverted_index_size;
}

#include "common/compile_check_end.h"
} // namespace doris::cloud
