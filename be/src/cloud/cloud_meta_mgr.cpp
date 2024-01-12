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
#include <vector>

#include "cloud/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/network_util.h"
#include "util/s3_util.h"

namespace doris::cloud {
using namespace ErrorCode;

bvar::LatencyRecorder g_get_rowset_latency("doris_CloudMetaMgr", "get_rowset");

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

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    VLOG_DEBUG << "send GetTabletRequest, tablet_id: " << tablet_id;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::get_tablet_meta", Status::OK(), tablet_id,
                                      tablet_meta);

    std::shared_ptr<MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int tried = 0;
    while (true) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        GetTabletRequest req;
        GetTabletResponse resp;
        req.set_cloud_unique_id(config::cloud_unique_id);
        req.set_tablet_id(tablet_id);
        stub->get_tablet(&cntl, &req, &resp, nullptr);
        int retry_times = config::meta_service_rpc_retry_times;
        if (cntl.Failed()) {
            if (tried++ < retry_times) {
                auto rng = std::default_random_engine(static_cast<uint32_t>(
                        std::chrono::steady_clock::now().time_since_epoch().count()));
                std::uniform_int_distribution<uint32_t> u(20, 200);
                std::uniform_int_distribution<uint32_t> u1(500, 1000);
                uint32_t duration_ms = tried >= 100 ? u(rng) : u1(rng);
                std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
                LOG_INFO("failed to get tablet meta")
                        .tag("reason", cntl.ErrorText())
                        .tag("tablet_id", tablet_id)
                        .tag("tried", tried)
                        .tag("sleep", duration_ms);
                continue;
            }
            return Status::RpcError("failed to get tablet meta: {}", cntl.ErrorText());
        }
        if (resp.status().code() == MetaServiceCode::TABLET_NOT_FOUND) {
            return Status::NotFound("failed to get tablet meta: {}", resp.status().msg());
        }
        if (resp.status().code() != MetaServiceCode::OK) {
            return Status::InternalError("failed to get tablet meta: {}", resp.status().msg());
        }
        *tablet_meta = std::make_shared<TabletMeta>();
        (*tablet_meta)->init_from_pb(resp.tablet_meta());
        VLOG_DEBUG << "get tablet meta, tablet_id: " << (*tablet_meta)->tablet_id();
        return Status::OK();
    }
}

Status CloudMetaMgr::sync_tablet_rowsets(Tablet* tablet, bool warmup_delta_data) {
    return Status::NotSupported("CloudMetaMgr::sync_tablet_rowsets is not implemented");
}

Status CloudMetaMgr::sync_tablet_delete_bitmap(
        Tablet* tablet, int64_t old_max_version,
        const google::protobuf::RepeatedPtrField<RowsetMetaPB>& rs_metas,
        const TabletStatsPB& stats, const TabletIndexPB& idx, DeleteBitmap* delete_bitmap) {
    return Status::NotSupported("CloudMetaMgr::sync_tablet_delete_bitmap is not implemented");
}

Status CloudMetaMgr::prepare_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    return Status::NotSupported("CloudMetaMgr::prepare_rowset is not implemented");
}

Status CloudMetaMgr::commit_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    return Status::NotSupported("CloudMetaMgr::commit_rowset is not implemented");
}

Status CloudMetaMgr::update_tmp_rowset(const RowsetMeta& rs_meta) {
    return Status::NotSupported("CloudMetaMgr::update_tmp_rowset is not implemented");
}

Status CloudMetaMgr::commit_txn(StreamLoadContext* ctx, bool is_2pc) {
    return Status::NotSupported("CloudMetaMgr::commit_txn is not implemented");
}

Status CloudMetaMgr::abort_txn(StreamLoadContext* ctx) {
    return Status::NotSupported("CloudMetaMgr::abort_txn is not implemented");
}

Status CloudMetaMgr::precommit_txn(StreamLoadContext* ctx) {
    return Status::NotSupported("CloudMetaMgr::precommit_txn is not implemented");
}

Status CloudMetaMgr::get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
    return Status::NotSupported("CloudMetaMgr::get_s3_info is not implemented");
}

Status CloudMetaMgr::prepare_tablet_job(const TabletJobInfoPB& job, StartTabletJobResponse* res) {
    return Status::NotSupported("CloudMetaMgr::prepare_tablet_job is not implemented");
}

Status CloudMetaMgr::commit_tablet_job(const TabletJobInfoPB& job, FinishTabletJobResponse* res) {
    return Status::NotSupported("CloudMetaMgr::commit_tablet_job is not implemented");
}

Status CloudMetaMgr::abort_tablet_job(const TabletJobInfoPB& job) {
    return Status::NotSupported("CloudMetaMgr::alter_tablet_job is not implemented");
}

Status CloudMetaMgr::lease_tablet_job(const TabletJobInfoPB& job) {
    return Status::NotSupported("CloudMetaMgr::lease_tablet_job is not implemented");
}

Status CloudMetaMgr::update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) {
    return Status::NotSupported("CloudMetaMgr::update_tablet_schema is not implemented");
}

Status CloudMetaMgr::update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                          DeleteBitmap* delete_bitmap) {
    return Status::NotSupported("CloudMetaMgr::update_delete_bitmap is not implemented");
}

Status CloudMetaMgr::get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                                   int64_t initiator) {
    return Status::NotSupported("CloudMetaMgr::get_delete_bitmap_update_lock is not implemented");
}

} // namespace doris::cloud
