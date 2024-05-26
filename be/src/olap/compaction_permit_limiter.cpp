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

#include "olap/compaction_permit_limiter.h"

#include "common/config.h"
#include "util/doris_metrics.h"

namespace doris {

CompactionPermitLimiter::CompactionPermitLimiter() : _used_permits(0) {}

void CompactionPermitLimiter::request(int64_t permits) {
    DorisMetrics::instance()->compaction_waitting_permits->set_value(permits);
    // 1. config::total_permits_for_compaction_score = 20000
    // 2. Thread-B requests permits 11000， used_permits = 11000
    // 3. Thread-A requests permits 12000，wait for used_permits + 12000 <= 20000
    // 4. adjust config::total_permits_for_compaction_score = 10000
    // 5. Thread-B releases permits，used_permits = 0，notify Thread-A，used_permits + 12000 <= 10000
    // we need to initialize total_permits instead of using the config.
    int64_t total_permits = config::total_permits_for_compaction_score;
    if (permits > total_permits) {
        // when tablet's compaction score is larger than "config::total_permits_for_compaction_score",
        // it's necessary to do compaction for this tablet because this tablet will not get "permits"
        // anyway. otherwise, compaction task for this tablet will not be executed forever.
        std::unique_lock<std::mutex> lock(_permits_mutex);
        _permits_cv.wait(lock, [permits, total_permits, this] {
            return _used_permits == 0 || _used_permits + permits <= total_permits;
        });
    } else {
        if (_used_permits + permits > total_permits) {
            std::unique_lock<std::mutex> lock(_permits_mutex);
            _permits_cv.wait(lock, [permits, total_permits, this] {
                return _used_permits + permits <= total_permits;
            });
        }
    }
    _used_permits += permits;
    DorisMetrics::instance()->compaction_waitting_permits->set_value(0);
    DorisMetrics::instance()->compaction_used_permits->set_value(_used_permits);
}

void CompactionPermitLimiter::release(int64_t permits) {
    std::unique_lock<std::mutex> lock(_permits_mutex);
    _used_permits -= permits;
    _permits_cv.notify_one();
    DorisMetrics::instance()->compaction_used_permits->set_value(_used_permits);
}
} // namespace doris
