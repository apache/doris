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

#include <bvar/bvar.h>
#include <bvar/multi_dimension.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "util/bvar_windowed_adder.h"

namespace doris {

// Source BE metrics keyed by job_id (defined in cloud_warm_up_manager.cpp).
extern MBvarWindowedAdder g_warmup_ed_requested_segment_num;
extern MBvarWindowedAdder g_warmup_ed_requested_segment_size;
extern MBvarWindowedAdder g_warmup_ed_requested_index_num;
extern MBvarWindowedAdder g_warmup_ed_requested_index_size;
extern bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_trigger_ts;

// Target BE metrics keyed by job_id (defined in cloud_internal_service.cpp).
extern MBvarWindowedAdder g_warmup_ed_finish_segment_num;
extern MBvarWindowedAdder g_warmup_ed_finish_segment_size;
extern MBvarWindowedAdder g_warmup_ed_finish_index_num;
extern MBvarWindowedAdder g_warmup_ed_finish_index_size;
extern MBvarWindowedAdder g_warmup_ed_fail_segment_num;
extern MBvarWindowedAdder g_warmup_ed_fail_segment_size;
extern MBvarWindowedAdder g_warmup_ed_fail_index_num;
extern MBvarWindowedAdder g_warmup_ed_fail_index_size;
extern bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_finish_ts;

// Tracks the target BE's event-driven warm-up progress by upstream trigger timestamp.
// If there are unfinished downloads for a job, progress is the earliest pending upstream trigger
// time. If the job has no pending downloads, progress falls back to the latest completed upstream
// trigger time, so FE can report a zero trigger gap once the target side catches up.
class WarmUpEdDownstreamProgressTracker {
public:
    void record_task_submit(const std::string& job_id_str, int64_t upstream_trigger_ts_ms);
    void record_task_done(const std::string& job_id_str, int64_t upstream_trigger_ts_ms);
    int64_t get_progress_ts(const std::string& job_id_str) const;
    std::vector<std::string> list_job_ids() const;
    void reset_for_test();

private:
    struct JobProgress {
        std::map<int64_t, int64_t> pending_trigger_ts_counts;
        int64_t last_finished_trigger_ts = 0;
    };

    mutable std::mutex _mtx;
    std::unordered_map<std::string, JobProgress> _progress_by_job;
};

extern WarmUpEdDownstreamProgressTracker g_warmup_ed_downstream_progress_tracker;

} // namespace doris
