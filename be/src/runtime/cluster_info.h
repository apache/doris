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

#include <gen_cpp/Types_types.h>

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace doris {

// This class is used to save the cluster info
// like cluster id, epoch, cloud_unique_id, etc.
// These info are usually in heartbeat from Master FE.
class ClusterInfo {
public:
    struct MasterFeEndpoint {
        TNetworkAddress rpc_addr;
        int32_t http_port = 0;
    };

    TNetworkAddress master_fe_addr() const {
        std::shared_lock lock(_master_fe_mutex);
        return _master_fe_endpoint.rpc_addr;
    }

    MasterFeEndpoint master_fe_endpoint() const {
        std::shared_lock lock(_master_fe_mutex);
        return _master_fe_endpoint;
    }

    void set_master_fe_endpoint(const MasterFeEndpoint& endpoint) {
        std::unique_lock lock(_master_fe_mutex);
        _master_fe_endpoint = endpoint;
    }

    int64_t changelog_ttl_reference_tso() const {
        return _changelog_ttl_reference_tso.load(std::memory_order_acquire);
    }

    // Heartbeat identity validation must precede this call. Never persist this value:
    // after a restart cleanup waits for a fresh, authenticated master heartbeat.
    void advance_changelog_ttl_reference_tso(int64_t tso) {
        auto previous = _changelog_ttl_reference_tso.load(std::memory_order_acquire);
        while (tso > previous && !_changelog_ttl_reference_tso.compare_exchange_weak(
                                         previous, tso, std::memory_order_acq_rel)) {
        }
    }

    // Unique cluster id
    int32_t cluster_id = 0;
    // Unique cluster token
    std::string token = "";
    // Backend ID
    int64_t backend_id = 0;

    // Auth token for internal authentication
    // Save the last 2 tokens to avoid token invalid during token update
    std::string curr_auth_token = "";
    std::string last_auth_token = "";

private:
    mutable std::shared_mutex _master_fe_mutex;
    MasterFeEndpoint _master_fe_endpoint;
    std::atomic<int64_t> _changelog_ttl_reference_tso {0};
};

} // namespace doris
