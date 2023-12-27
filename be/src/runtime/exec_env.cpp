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

#include "runtime/exec_env.h"

#include <gen_cpp/HeartbeatService_types.h>
#include <glog/logging.h>

#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/fragment_mgr.h"
#include "runtime/frontend_info.h"
#include "time.h"
#include "util/debug_util.h"
#include "util/time.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_stub_pool.h"

namespace doris {

ExecEnv::ExecEnv() = default;

ExecEnv::~ExecEnv() {
    destroy();
}

// TODO(plat1ko): template <class Engine>
Result<BaseTabletSPtr> ExecEnv::get_tablet(int64_t tablet_id) {
    BaseTabletSPtr tablet;
    std::string err;
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
    if (tablet == nullptr) {
        return unexpected(
                Status::InternalError("failed to get tablet: {}, reason: {}", tablet_id, err));
    }
    return tablet;
}

const std::string& ExecEnv::token() const {
    return _master_info->token;
}

std::map<TNetworkAddress, FrontendInfo> ExecEnv::get_frontends() {
    std::lock_guard<std::mutex> lg(_frontends_lock);
    return _frontends;
}

void ExecEnv::update_frontends(const std::vector<TFrontendInfo>& new_fe_infos) {
    std::lock_guard<std::mutex> lg(_frontends_lock);

    std::set<TNetworkAddress> dropped_fes;

    for (const auto& cur_fe : _frontends) {
        dropped_fes.insert(cur_fe.first);
    }

    for (const auto& coming_fe_info : new_fe_infos) {
        auto itr = _frontends.find(coming_fe_info.coordinator_address);

        if (itr == _frontends.end()) {
            LOG(INFO) << "A completely new frontend, " << PrintFrontendInfo(coming_fe_info);

            _frontends.insert(std::pair<TNetworkAddress, FrontendInfo>(
                    coming_fe_info.coordinator_address,
                    FrontendInfo {coming_fe_info, GetCurrentTimeMicros() / 1000, /*first time*/
                                  GetCurrentTimeMicros() / 1000 /*last time*/}));

            continue;
        }

        dropped_fes.erase(coming_fe_info.coordinator_address);

        if (coming_fe_info.process_uuid == 0) {
            LOG(WARNING) << "Frontend " << PrintFrontendInfo(coming_fe_info)
                         << " is in an unknown state.";
        }

        if (coming_fe_info.process_uuid == itr->second.info.process_uuid) {
            itr->second.last_reveiving_time_ms = GetCurrentTimeMicros() / 1000;
            continue;
        }

        // If we get here, means this frontend has already restarted.
        itr->second.info.process_uuid = coming_fe_info.process_uuid;
        itr->second.first_receiving_time_ms = GetCurrentTimeMicros() / 1000;
        itr->second.last_reveiving_time_ms = GetCurrentTimeMicros() / 1000;
        LOG(INFO) << "Update frontend " << PrintFrontendInfo(coming_fe_info);
    }

    for (const auto& dropped_fe : dropped_fes) {
        LOG(INFO) << "Frontend " << PrintThriftNetworkAddress(dropped_fe)
                  << " has already been dropped, remove it";
        _frontends.erase(dropped_fe);
    }
}

std::map<TNetworkAddress, FrontendInfo> ExecEnv::get_running_frontends() {
    std::lock_guard<std::mutex> lg(_frontends_lock);
    std::map<TNetworkAddress, FrontendInfo> res;
    const int expired_duration = config::fe_expire_duration_seconds * 1000;
    const auto now = GetCurrentTimeMicros() / 1000;

    for (const auto& pair : _frontends) {
        auto& brpc_addr = pair.first;
        auto& fe_info = pair.second;

        if (fe_info.info.process_uuid == 0) {
            // FE is in an unknown state, regart it as alive. conservative
            res[brpc_addr] = fe_info;
        } else {
            if (now - fe_info.last_reveiving_time_ms < expired_duration) {
                // If fe info has just been update in last expired_duration, regard it as running.
                res[brpc_addr] = fe_info;
            } else {
                // Fe info has not been udpate for more than expired_duration, regard it as an abnormal.
                // Abnormal means this fe can not connect to master, and it is not dropped from cluster.
                // or fe do not have master yet.
                LOG_EVERY_N(WARNING, 50) << fmt::format(
                        "Frontend {}:{} has not update its hb for more than {} secs, regard it as "
                        "abnormal",
                        brpc_addr.hostname, brpc_addr.port, config::fe_expire_duration_seconds);
            }
        }
    }

    return res;
}

void ExecEnv::wait_for_all_tasks_done() {
    // For graceful shutdown, need to wait for all running queries to stop
    int32_t wait_seconds_passed = 0;
    while (true) {
        int num_queries = _fragment_mgr->running_query_num();
        if (num_queries < 1) {
            break;
        }
        if (wait_seconds_passed > doris::config::grace_shutdown_wait_seconds) {
            LOG(INFO) << "There are still " << num_queries << " queries running, but "
                      << wait_seconds_passed << " seconds passed, has to exist now";
            break;
        }
        LOG(INFO) << "There are still " << num_queries << " queries running, waiting...";
        sleep(1);
        ++wait_seconds_passed;
    }
}

} // namespace doris
