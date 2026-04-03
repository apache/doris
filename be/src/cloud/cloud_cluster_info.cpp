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

#include "cloud/cloud_cluster_info.h"

#include <glog/logging.h>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "gen_cpp/cloud.pb.h"
#include "runtime/exec_env.h"
#include "util/time.h"

namespace doris {

CloudClusterInfo::~CloudClusterInfo() {
    stop_bg_worker();
}

void CloudClusterInfo::start_bg_worker() {
    bool expected = true;
    if (!_bg_worker_stopped.compare_exchange_strong(expected, false)) {
        // Already running
        return;
    }

    _bg_worker = std::thread(&CloudClusterInfo::_bg_worker_func, this);
    LOG(INFO) << "CloudClusterInfo background worker started, "
              << "refresh_interval=" << config::cluster_status_cache_refresh_interval_sec << "s";
}

void CloudClusterInfo::stop_bg_worker() {
    bool expected = false;
    if (!_bg_worker_stopped.compare_exchange_strong(expected, true)) {
        // Already stopped
        return;
    }

    {
        std::lock_guard lock(_bg_worker_mutex);
        _bg_worker_cv.notify_all();
    }

    if (_bg_worker.joinable()) {
        _bg_worker.join();
    }

    LOG(INFO) << "CloudClusterInfo background worker stopped";
}

void CloudClusterInfo::_bg_worker_func() {
    LOG(INFO) << "CloudClusterInfo background worker thread running";

    while (!_bg_worker_stopped.load()) {
        _refresh_cluster_status();

        std::unique_lock lock(_bg_worker_mutex);
        _bg_worker_cv.wait_for(
                lock, std::chrono::seconds(config::cluster_status_cache_refresh_interval_sec),
                [this] { return _bg_worker_stopped.load(); });
    }
}

void CloudClusterInfo::_refresh_cluster_status() {
    if (!config::is_cloud_mode()) {
        return;
    }
    auto* cloud_engine =
            dynamic_cast<CloudStorageEngine*>(&ExecEnv::GetInstance()->storage_engine());
    if (!cloud_engine) {
        return;
    }

    std::unordered_map<std::string, std::pair<int32_t, int64_t>> cluster_status;
    std::string resolved_cluster_id;
    Status st = cloud_engine->meta_mgr().get_cluster_status(
            &cluster_status, my_cluster_id().empty() ? &resolved_cluster_id : nullptr);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to refresh cluster status: " << st;
        return;
    }

    // Update cache
    {
        std::unique_lock lock(_mutex);
        _cluster_status_cache.clear();
        for (const auto& [cluster_id, status_pair] : cluster_status) {
            _cluster_status_cache[cluster_id] = {status_pair.first, status_pair.second};
        }
    }

    VLOG_DEBUG << "Refreshed cluster status cache, " << cluster_status.size() << " clusters";

    // Set our own cluster_id if resolved from the response
    if (my_cluster_id().empty() && !resolved_cluster_id.empty()) {
        set_my_cluster_id(resolved_cluster_id);
        LOG(INFO) << "Resolved my cluster_id: " << resolved_cluster_id;
    }
}

bool CloudClusterInfo::should_skip_compaction(CloudTablet* tablet) const {
    if (!config::enable_compaction_rw_separation) {
        return false;
    }

    std::string last_active_cluster = tablet->last_active_cluster_id();
    std::string my_cluster = my_cluster_id();
    int64_t tablet_id = tablet->tablet_id();

    // Case 1: No active cluster record, any cluster can compact
    if (last_active_cluster.empty()) {
        VLOG_DEBUG << "tablet " << tablet_id << " has no last_active_cluster record, "
                   << "my_cluster=" << my_cluster << ", allow compaction";
        return false;
    }

    // Case 2: This is the active cluster, allow compaction
    if (last_active_cluster == my_cluster) {
        VLOG_DEBUG << "tablet " << tablet_id << " last_active_cluster=" << last_active_cluster
                   << " equals my_cluster=" << my_cluster << ", allow compaction";
        return false;
    }

    // Case 3: Check if the last active cluster is available
    ClusterStatusCache cache;
    if (!get_cluster_status(last_active_cluster, &cache)) {
        // Cluster not found in cache, might be deleted, allow takeover
        LOG(INFO) << "compaction_rw_separation: tablet " << tablet_id
                  << " last_active_cluster=" << last_active_cluster
                  << " not found in cache (maybe deleted), my_cluster=" << my_cluster
                  << ", allow takeover";
        return false;
    }

    // Force compaction if tablet has too many rowsets (>80% of max_tablet_version_num),
    // even on read clusters, to prevent version count from growing unbounded
    // when the write cluster can't keep up or has compaction disabled.
    int64_t num_rowsets = tablet->fetch_add_approximate_num_rowsets(0);
    auto threshold = static_cast<int64_t>(tablet->max_version_config() *
                                          config::compaction_rw_separation_version_threshold_ratio);
    if (num_rowsets > threshold) {
        LOG(INFO) << "compaction_rw_separation: force compaction on tablet " << tablet_id
                  << ", num_rowsets=" << num_rowsets << " > threshold=" << threshold << " (80% of "
                  << tablet->max_version_config() << ")"
                  << ", my_cluster=" << my_cluster;
        return false;
    }

    auto status = static_cast<cloud::ClusterStatus>(cache.status);
    int64_t status_mtime = cache.mtime_ms;
    int64_t now = UnixMillis();
    int64_t elapsed = now - status_mtime;
    int64_t timeout = config::compaction_cluster_takeover_timeout_ms;

    // Case 4: Original cluster is NORMAL (still active), cannot takeover
    if (status == cloud::ClusterStatus::NORMAL) {
        LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet_id
                               << ", last_active_cluster=" << last_active_cluster
                               << " is NORMAL (active), my_cluster=" << my_cluster;
        return true;
    }

    // Case 5: Original cluster is unavailable (SUSPENDED/MANUAL_SHUTDOWN/deleted)
    if (elapsed > timeout) {
        // Takeover successful
        LOG(INFO) << "compaction_rw_separation: takeover tablet " << tablet_id
                  << ", last_active_cluster=" << last_active_cluster
                  << " status=" << cloud::ClusterStatus_Name(status)
                  << " status_mtime=" << status_mtime << " elapsed=" << elapsed
                  << "ms > timeout=" << timeout << "ms"
                  << ", my_cluster=" << my_cluster;
        return false;
    } else {
        // Timeout not reached yet, waiting
        LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet_id
                               << ", last_active_cluster=" << last_active_cluster
                               << " status=" << cloud::ClusterStatus_Name(status)
                               << " status_mtime=" << status_mtime << " elapsed=" << elapsed
                               << "ms <= timeout=" << timeout << "ms"
                               << ", my_cluster=" << my_cluster << ", waiting for takeover";
        return true;
    }
}

} // namespace doris
