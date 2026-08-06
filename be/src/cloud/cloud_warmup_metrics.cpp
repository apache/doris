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

#include "cloud/cloud_warmup_metrics.h"

#include <algorithm>

namespace doris {

WarmUpEdDownstreamProgressTracker g_warmup_ed_downstream_progress_tracker;

void WarmUpEdDownstreamProgressTracker::record_task_submit(const std::string& job_id_str,
                                                           int64_t upstream_trigger_ts_ms) {
    if (upstream_trigger_ts_ms <= 0) {
        return;
    }
    std::lock_guard lock(_mtx);
    auto& progress = _progress_by_job[job_id_str];
    ++progress.pending_trigger_ts_counts[upstream_trigger_ts_ms];
}

void WarmUpEdDownstreamProgressTracker::record_task_done(const std::string& job_id_str,
                                                         int64_t upstream_trigger_ts_ms) {
    if (upstream_trigger_ts_ms <= 0) {
        return;
    }
    std::lock_guard lock(_mtx);
    auto& progress = _progress_by_job[job_id_str];
    auto pending_it = progress.pending_trigger_ts_counts.find(upstream_trigger_ts_ms);
    if (pending_it != progress.pending_trigger_ts_counts.end()) {
        --pending_it->second;
        if (pending_it->second <= 0) {
            progress.pending_trigger_ts_counts.erase(pending_it);
        }
    }
    progress.last_finished_trigger_ts =
            std::max(progress.last_finished_trigger_ts, upstream_trigger_ts_ms);
}

int64_t WarmUpEdDownstreamProgressTracker::get_progress_ts(const std::string& job_id_str) const {
    std::lock_guard lock(_mtx);
    auto progress_it = _progress_by_job.find(job_id_str);
    if (progress_it == _progress_by_job.end()) {
        return 0;
    }
    const auto& progress = progress_it->second;
    if (!progress.pending_trigger_ts_counts.empty()) {
        return progress.pending_trigger_ts_counts.begin()->first;
    }
    return progress.last_finished_trigger_ts;
}

std::vector<std::string> WarmUpEdDownstreamProgressTracker::list_job_ids() const {
    std::lock_guard lock(_mtx);
    std::vector<std::string> job_ids;
    job_ids.reserve(_progress_by_job.size());
    for (const auto& entry : _progress_by_job) {
        job_ids.emplace_back(entry.first);
    }
    return job_ids;
}

void WarmUpEdDownstreamProgressTracker::reset_for_test() {
    std::lock_guard lock(_mtx);
    _progress_by_job.clear();
}

} // namespace doris
