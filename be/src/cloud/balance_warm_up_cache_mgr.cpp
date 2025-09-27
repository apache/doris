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

#include "cloud/balance_warm_up_cache_mgr.h"

#include <algorithm>

#include "common/logging.h"

namespace doris::cloud {

BalanceWarmUpCacheMgr& BalanceWarmUpCacheMgr::instance() {
    static BalanceWarmUpCacheMgr instance;
    return instance;
}

void BalanceWarmUpCacheMgr::record_tablet(int64_t tablet_id, const std::string& host,
                                          int32_t brpc_port) {
    std::lock_guard<std::mutex> lock(_mtx);
    _balanced_tablets.emplace(tablet_id, WarmUpCacheTabletInfo(host, brpc_port));
    VLOG_DEBUG << "Recorded warm up cache tablet: tablet_id=" << tablet_id << ", host=" << host
               << ":" << brpc_port;
}

std::optional<WarmUpCacheTabletInfo> BalanceWarmUpCacheMgr::get_tablet_info(
        int64_t tablet_id) const {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _balanced_tablets.find(tablet_id);
    if (it == _balanced_tablets.end()) {
        return std::nullopt;
    }
    return it->second;
}

void BalanceWarmUpCacheMgr::remove_tablet(int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _balanced_tablets.find(tablet_id);
    if (it != _balanced_tablets.end()) {
        VLOG_DEBUG << "Removed warm up cache tablet: tablet_id=" << tablet_id;
        _balanced_tablets.erase(it);
    }
}

void BalanceWarmUpCacheMgr::remove_balanced_tablets(const std::vector<int64_t>& tablet_ids) {
    std::lock_guard<std::mutex> lock(_mtx);
    for (int64_t tablet_id : tablet_ids) {
        auto it = _balanced_tablets.find(tablet_id);
        if (it != _balanced_tablets.end()) {
            VLOG_DEBUG << "Removed warm up cache tablet: tablet_id=" << tablet_id;
            _balanced_tablets.erase(it);
        }
    }
}

std::unordered_map<int64_t, WarmUpCacheTabletInfo> BalanceWarmUpCacheMgr::get_all_balanced_tablets()
        const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _balanced_tablets;
}

void BalanceWarmUpCacheMgr::clear_expired_balanced_tablets(std::chrono::milliseconds max_age) {
    std::lock_guard<std::mutex> lock(_mtx);
    auto now = std::chrono::steady_clock::now();
    auto it = _balanced_tablets.begin();
    size_t removed_count = 0;

    while (it != _balanced_tablets.end()) {
        if (now - it->second.timestamp > max_age) {
            VLOG_DEBUG << "Clearing expired warm up cache tablet: tablet_id=" << it->first
                       << ", age="
                       << std::chrono::duration_cast<std::chrono::milliseconds>(
                                  now - it->second.timestamp)
                                  .count()
                       << "ms";
            it = _balanced_tablets.erase(it);
            removed_count++;
        } else {
            ++it;
        }
    }

    if (removed_count > 0) {
        VLOG_DEBUG << "Cleared " << removed_count << " expired warm up cache tablets";
    }
}

} // namespace doris::cloud
