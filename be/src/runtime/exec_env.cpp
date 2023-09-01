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

#include <mutex>
#include <utility>

#include "common/config.h"
#include "runtime/frontend_info.h"
#include "time.h"
#include "util/debug_util.h"
#include "util/time.h"

namespace doris {

ExecEnv::ExecEnv() = default;

ExecEnv::~ExecEnv() {
    _destroy();
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
        if (pair.second.info.process_uuid != 0) {
            if (now - pair.second.last_reveiving_time_ms < expired_duration) {
                // If fe info has just been update in last expired_duration, regard it as running.
                res[pair.first] = pair.second;
            } else {
                // Fe info has not been udpate for more than expired_duration, regard it as an abnormal.
                // Abnormal means this fe can not connect to master, and it is not dropped from cluster.
                // or fe do not have master yet.
                LOG(INFO) << "Frontend " << PrintFrontendInfo(pair.second.info)
                          << " has not update its hb "
                          << "for more than " << config::fe_expire_duration_seconds
                          << " secs, regard it as abnormal.";
            }

            continue;
        }

        if (pair.second.last_reveiving_time_ms - pair.second.first_receiving_time_ms >
            expired_duration) {
            // A zero process-uuid that sustains more than 60 seconds(default).
            // We will regard this fe as a abnormal frontend.
            LOG(INFO) << "Frontend " << PrintFrontendInfo(pair.second.info)
                      << " has not update its hb "
                      << "for more than " << config::fe_expire_duration_seconds
                      << " secs, regard it as abnormal.";
            continue;
        } else {
            res[pair.first] = pair.second;
        }
    }

    return res;
}

} // namespace doris
