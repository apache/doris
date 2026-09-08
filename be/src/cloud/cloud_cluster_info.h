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
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "runtime/cluster_info.h"

namespace doris {

class CloudTablet;
class CloudClusterInfoTestPeer;
namespace cloud {
class TabletCompactionJobPB;
}

// Cached cluster status information
struct ClusterStatusCache {
    int32_t status {0};
    // Raw status generation from meta-service. Zero means it was not reported.
    int64_t reported_status_mtime_ms {0};
    // Raw generic cluster metadata generation. It changes for any cluster metadata mutation on
    // meta-service versions that implement the monotonic update contract.
    int64_t reported_cluster_mtime_ms {0};
    // False when generic cluster metadata is newer than the status generation, so the BE starts
    // the takeover window at first observation instead of trusting the older timestamp.
    bool status_mtime_trusted {false};
    // Stable start of this status generation's takeover window on this BE.
    int64_t takeover_start_time_ms {0};
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

    // Resolve the requester identity reported independently by meta-service and FE heartbeat.
    // An empty result is fail-closed: neither source knows the identity, or they conflict.
    std::string current_cluster_id() const;

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
    void set_cluster_status(const std::string& id, int32_t status, int64_t status_mtime_ms) {
        std::unique_lock lock(_mutex);
        _cluster_status_cache[id] = {
                .status = status,
                .reported_status_mtime_ms = status_mtime_ms,
                .reported_cluster_mtime_ms = status_mtime_ms,
                .status_mtime_trusted = true,
                .takeover_start_time_ms = status_mtime_ms,
        };
        _cluster_status_cache_initialized = true;
    }

    // Clear all cached cluster status
    void clear_cluster_status_cache() {
        std::unique_lock lock(_mutex);
        _cluster_status_cache.clear();
        _cluster_status_cache_initialized = false;
    }

    // Start background refresh thread
    void start_bg_worker();
    // Stop background refresh thread
    void stop_bg_worker();

    // Check if this cluster should skip compaction for the given tablet
    // Returns true if should skip (i.e., another cluster should do the compaction)
    bool should_skip_compaction(CloudTablet* tablet) const;
    // Recheck compaction ownership and fill the request token from the same authorization
    // snapshot. False means the caller must not contact meta-service.
    bool prepare_compaction_job(CloudTablet* tablet,
                                cloud::TabletCompactionJobPB* compaction_job) const;
    std::string cloud_compute_group_id() const {
        std::shared_lock lock(_mutex);
        return _cloud_compute_group_id;
    }
    void set_cloud_compute_group_id(const std::string& id) {
        std::unique_lock lock(_mutex);
        _cloud_compute_group_id = id;
    }

private:
    friend class CloudClusterInfoTestPeer;

    void _bg_worker_func();
    void _refresh_cluster_status();
    void _reconcile_cluster_status_cache(
            const std::unordered_map<std::string, std::tuple<int32_t, int64_t, int64_t, bool>>&
                    cluster_status,
            int64_t observed_time_ms);
    bool _should_skip_compaction(CloudTablet* tablet, int64_t now_ms) const;
    bool _authorize_compaction(CloudTablet* tablet, cloud::TabletCompactionJobPB* compaction_job,
                               int64_t now_ms) const;
    bool _authorize_foreign_compaction(CloudTablet* tablet, const std::string& owner_cluster_id,
                                       int64_t owner_epoch, const std::string& requester_cluster_id,
                                       cloud::TabletCompactionJobPB* compaction_job,
                                       int64_t now_ms) const;

    bool _is_in_standby = false;
    std::string _cloud_compute_group_id;

    mutable std::shared_mutex _mutex;
    std::string _my_cluster_id;
    std::unordered_map<std::string, ClusterStatusCache> _cluster_status_cache;
    bool _cluster_status_cache_initialized {false};

    // Background worker
    std::thread _bg_worker;
    std::atomic<bool> _bg_worker_stopped {true};
    std::mutex _bg_worker_mutex;
    std::condition_variable _bg_worker_cv;
};

} // namespace doris
