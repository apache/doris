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

#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace doris::cloud {

struct WarmUpCacheTabletInfo {
    std::string host;
    int32_t brpc_port;
    std::chrono::steady_clock::time_point timestamp;

    WarmUpCacheTabletInfo() = default;
    WarmUpCacheTabletInfo(const std::string& h, int32_t port)
            : host(h), brpc_port(port), timestamp(std::chrono::steady_clock::now()) {}
};

class BalanceWarmUpCacheMgr {
public:
    static BalanceWarmUpCacheMgr& instance();

    /**
     * Record tablet warm up cache request
     * @param tablet_id tablet id to warm up
     * @param host peer host
     * @param brpc_port peer brpc port
     */
    void record_tablet(int64_t tablet_id, const std::string& host, int32_t brpc_port);

    /**
     * Get tablet information by tablet_id
     * @param tablet_id tablet id
     * @return optional copy of tablet info if found, std::nullopt otherwise
     */
    std::optional<WarmUpCacheTabletInfo> get_tablet_info(int64_t tablet_id) const;

    /**
     * Remove tablet from tracking
     * @param tablet_id tablet id
     */
    void remove_tablet(int64_t tablet_id);

    /**
     * Remove multiple tablets from tracking
     * @param tablet_ids list of tablet ids to remove
     */
    void remove_balanced_tablets(const std::vector<int64_t>& tablet_ids);

    /**
     * Get all tracked tablets
     * @return map of tablet_id -> tablet info
     */
    std::unordered_map<int64_t, WarmUpCacheTabletInfo> get_all_balanced_tablets() const;

    /**
     * Clear expired tablets older than specified duration
     * @param max_age maximum age before considering a tablet expired
     */
    void clear_expired_balanced_tablets(std::chrono::milliseconds max_age);

private:
    BalanceWarmUpCacheMgr() = default;
    ~BalanceWarmUpCacheMgr() = default;

    mutable std::mutex _mtx;
    std::unordered_map<int64_t, WarmUpCacheTabletInfo> _balanced_tablets;
};

} // namespace doris::cloud
