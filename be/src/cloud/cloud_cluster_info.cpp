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

#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <algorithm>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "runtime/exec_env.h"
#include "util/time.h"

namespace doris {

namespace {

void fill_compaction_authorization(cloud::TabletCompactionJobPB* compaction_job,
                                   const std::string& owner_cluster_id, int64_t owner_epoch,
                                   const std::string& requester_cluster_id,
                                   const ClusterStatusCache* owner_status,
                                   bool force_by_version_count) {
    if (compaction_job == nullptr) {
        return;
    }
    compaction_job->set_observed_last_active_cluster_id(owner_cluster_id);
    compaction_job->set_observed_last_active_epoch(owner_epoch);
    compaction_job->set_requester_cluster_id(requester_cluster_id);
    compaction_job->set_force_compaction_by_version_count(force_by_version_count);
    if (owner_status != nullptr) {
        compaction_job->set_observed_owner_cluster_status(
                static_cast<cloud::ClusterStatus>(owner_status->status));
        compaction_job->set_observed_owner_cluster_status_mtime_ms(
                owner_status->reported_status_mtime_ms);
        compaction_job->set_observed_owner_cluster_mtime_ms(
                owner_status->reported_cluster_mtime_ms);
    }
}

} // namespace

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

    std::unordered_map<std::string, std::tuple<int32_t, int64_t, int64_t, bool>> cluster_status;
    std::string resolved_cluster_id;
    Status st = cloud_engine->meta_mgr().get_cluster_status(&cluster_status, &resolved_cluster_id);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to refresh cluster status: " << st;
        return;
    }

    _reconcile_cluster_status_cache(cluster_status, UnixMillis());

    VLOG_DEBUG << "Refreshed cluster status cache, " << cluster_status.size() << " clusters";

    // Reconcile our cluster identity on every refresh because a running node can be moved between
    // compute groups.
    const auto previous_cluster_id = my_cluster_id();
    if (resolved_cluster_id != previous_cluster_id) {
        set_my_cluster_id(resolved_cluster_id);
        LOG(INFO) << "Resolved my cluster_id: " << previous_cluster_id << " -> "
                  << resolved_cluster_id;
    }
}

bool CloudClusterInfo::should_skip_compaction(CloudTablet* tablet) const {
    return _should_skip_compaction(tablet, UnixMillis());
}

std::string CloudClusterInfo::current_cluster_id() const {
    std::shared_lock lock(_mutex);
    if (!_my_cluster_id.empty() && !_cloud_compute_group_id.empty() &&
        _my_cluster_id != _cloud_compute_group_id) {
        LOG_EVERY_N(WARNING, 100) << "conflicting compute group identities, meta-service="
                                  << _my_cluster_id << ", heartbeat=" << _cloud_compute_group_id;
        return "";
    }
    return !_cloud_compute_group_id.empty() ? _cloud_compute_group_id : _my_cluster_id;
}

bool CloudClusterInfo::prepare_compaction_job(CloudTablet* tablet,
                                              cloud::TabletCompactionJobPB* compaction_job) const {
    return _authorize_compaction(tablet, compaction_job, UnixMillis());
}

void CloudClusterInfo::_reconcile_cluster_status_cache(
        const std::unordered_map<std::string, std::tuple<int32_t, int64_t, int64_t, bool>>&
                cluster_status,
        int64_t observed_time_ms) {
    std::unique_lock lock(_mutex);
    std::unordered_map<std::string, ClusterStatusCache> next_cache;
    next_cache.reserve(cluster_status.size());
    for (const auto& [cluster_id, status_info] : cluster_status) {
        const auto& [status, status_mtime_ms, cluster_mtime_ms, status_mtime_trusted] = status_info;
        int64_t takeover_start_time_ms = status_mtime_trusted && status_mtime_ms > 0
                                                 ? std::min(status_mtime_ms, observed_time_ms)
                                                 : observed_time_ms;
        auto old = _cluster_status_cache.find(cluster_id);
        if (old != _cluster_status_cache.end()) {
            const auto& previous = old->second;
            if (previous.status == status && previous.reported_status_mtime_ms == status_mtime_ms &&
                previous.reported_cluster_mtime_ms == cluster_mtime_ms &&
                previous.status_mtime_trusted == status_mtime_trusted) {
                takeover_start_time_ms = previous.takeover_start_time_ms;
            } else if (previous.status != status && status_mtime_ms > 0 &&
                       previous.reported_status_mtime_ms == status_mtime_ms) {
                // An old MetaService can preserve this unknown field while changing the status.
                takeover_start_time_ms = observed_time_ms;
            }
        }
        next_cache.emplace(cluster_id,
                           ClusterStatusCache {.status = status,
                                               .reported_status_mtime_ms = status_mtime_ms,
                                               .reported_cluster_mtime_ms = cluster_mtime_ms,
                                               .status_mtime_trusted = status_mtime_trusted,
                                               .takeover_start_time_ms = takeover_start_time_ms});
    }
    _cluster_status_cache.swap(next_cache);
    _cluster_status_cache_initialized = true;
}

bool CloudClusterInfo::_should_skip_compaction(CloudTablet* tablet, int64_t now_ms) const {
    return !_authorize_compaction(tablet, nullptr, now_ms);
}

bool CloudClusterInfo::_authorize_compaction(CloudTablet* tablet,
                                             cloud::TabletCompactionJobPB* compaction_job,
                                             int64_t now_ms) const {
    if (!config::enable_compaction_rw_separation) {
        return true;
    }

    const auto owner = tablet->last_active_cluster_info();
    const std::string requester_cluster_id = current_cluster_id();
    if (requester_cluster_id.empty()) {
        LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet->tablet_id()
                               << ", requester cluster is not initialized";
        return false;
    }

    if (owner.cluster_id.empty() || owner.cluster_id == requester_cluster_id) {
        fill_compaction_authorization(compaction_job, owner.cluster_id, owner.epoch,
                                      requester_cluster_id, nullptr, false);
        return true;
    }

    return _authorize_foreign_compaction(tablet, owner.cluster_id, owner.epoch,
                                         requester_cluster_id, compaction_job, now_ms);
}

bool CloudClusterInfo::_authorize_foreign_compaction(CloudTablet* tablet,
                                                     const std::string& owner_cluster_id,
                                                     int64_t owner_epoch,
                                                     const std::string& requester_cluster_id,
                                                     cloud::TabletCompactionJobPB* compaction_job,
                                                     int64_t now_ms) const {
    const int64_t tablet_id = tablet->tablet_id();
    const int64_t num_rowsets = tablet->fetch_add_approximate_num_rowsets(0);
    const auto threshold =
            static_cast<int64_t>(tablet->max_version_config() *
                                 config::compaction_rw_separation_version_threshold_ratio);
    if (num_rowsets > threshold) {
        LOG(INFO) << "compaction_rw_separation: force compaction on tablet " << tablet_id
                  << ", num_rowsets=" << num_rowsets << " > threshold=" << threshold
                  << ", my_cluster=" << requester_cluster_id;
        fill_compaction_authorization(compaction_job, owner_cluster_id, owner_epoch,
                                      requester_cluster_id, nullptr, true);
        return true;
    }

    ClusterStatusCache cache;
    bool cache_initialized = false;
    bool cluster_found = false;
    {
        std::shared_lock lock(_mutex);
        cache_initialized = _cluster_status_cache_initialized;
        auto it = _cluster_status_cache.find(owner_cluster_id);
        if (it != _cluster_status_cache.end()) {
            cache = it->second;
            cluster_found = true;
        }
    }
    if (!cache_initialized) {
        LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet_id
                               << ", cluster status cache is not initialized"
                               << ", last_active_cluster=" << owner_cluster_id
                               << ", my_cluster=" << requester_cluster_id;
        return false;
    }
    if (!cluster_found) {
        LOG(INFO) << "compaction_rw_separation: tablet " << tablet_id
                  << " last_active_cluster=" << owner_cluster_id
                  << " not found in cache (maybe deleted), my_cluster=" << requester_cluster_id
                  << ", allow takeover";
        fill_compaction_authorization(compaction_job, owner_cluster_id, owner_epoch,
                                      requester_cluster_id, nullptr, false);
        return true;
    }

    const auto status = static_cast<cloud::ClusterStatus>(cache.status);
    const int64_t elapsed = now_ms - cache.takeover_start_time_ms;
    const int64_t timeout = config::compaction_cluster_takeover_timeout_ms;

    if (status == cloud::ClusterStatus::NORMAL) {
        LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet_id
                               << ", last_active_cluster=" << owner_cluster_id
                               << " is NORMAL (active), my_cluster=" << requester_cluster_id;
        return false;
    }

    if (elapsed > timeout) {
        LOG(INFO) << "compaction_rw_separation: takeover tablet " << tablet_id
                  << ", last_active_cluster=" << owner_cluster_id
                  << " status=" << cloud::ClusterStatus_Name(status)
                  << " reported_status_mtime_ms=" << cache.reported_status_mtime_ms
                  << " takeover_start_time_ms=" << cache.takeover_start_time_ms
                  << " elapsed=" << elapsed << "ms > timeout=" << timeout << "ms"
                  << ", my_cluster=" << requester_cluster_id;
        fill_compaction_authorization(compaction_job, owner_cluster_id, owner_epoch,
                                      requester_cluster_id, &cache, false);
        return true;
    }

    LOG_EVERY_N(INFO, 100) << "compaction_rw_separation: skip tablet " << tablet_id
                           << ", last_active_cluster=" << owner_cluster_id
                           << " status=" << cloud::ClusterStatus_Name(status)
                           << " reported_status_mtime_ms=" << cache.reported_status_mtime_ms
                           << " takeover_start_time_ms=" << cache.takeover_start_time_ms
                           << " elapsed=" << elapsed << "ms <= timeout=" << timeout << "ms"
                           << ", my_cluster=" << requester_cluster_id << ", waiting for takeover";
    return false;
}

} // namespace doris
