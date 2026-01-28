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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "runtime/cluster_info.h"

namespace doris {

class CloudTablet;

// Cached cluster status information
struct ClusterStatusCache {
    int32_t status {0};   // ClusterStatus enum value
    int64_t mtime_ms {0}; // Timestamp when status was last changed
};

class CloudClusterInfo : public ClusterInfo {
public:
    ~CloudClusterInfo();

    bool is_in_standby() const { return _is_in_standby; }
    void set_is_in_standby(bool flag) { _is_in_standby = flag; }

    // Get this BE's cluster ID
    std::string my_cluster_id() const {
        std::shared_lock lock(_mutex);
        return _my_cluster_id;
    }
    void set_my_cluster_id(const std::string& id) {
        std::unique_lock lock(_mutex);
        _my_cluster_id = id;
    }

    // Get cached cluster status, returns false if not found
    bool get_cluster_status(const std::string& id, ClusterStatusCache* cache) const {
        std::shared_lock lock(_mutex);
        auto it = _cluster_status_cache.find(id);
        if (it != _cluster_status_cache.end()) {
            *cache = it->second;
            return true;
        }
        return false;
    }

    // Update cluster status cache
    void set_cluster_status(const std::string& id, int32_t status, int64_t mtime_ms) {
        std::unique_lock lock(_mutex);
        _cluster_status_cache[id] = {status, mtime_ms};
    }

    // Clear all cached cluster status
    void clear_cluster_status_cache() {
        std::unique_lock lock(_mutex);
        _cluster_status_cache.clear();
    }

    // Start background refresh thread
    void start_bg_worker();
    // Stop background refresh thread
    void stop_bg_worker();

    // Check if this cluster should skip compaction for the given tablet
    // Returns true if should skip (i.e., another cluster should do the compaction)
    bool should_skip_compaction(CloudTablet* tablet) const;

private:
    void _bg_worker_func();
    void _refresh_cluster_status();

    bool _is_in_standby = false;

    mutable std::shared_mutex _mutex;
    std::string _my_cluster_id;
    std::unordered_map<std::string, ClusterStatusCache> _cluster_status_cache;

    // Background worker
    std::thread _bg_worker;
    std::atomic<bool> _bg_worker_stopped {true};
    std::mutex _bg_worker_mutex;
    std::condition_variable _bg_worker_cv;
};

} // namespace doris
