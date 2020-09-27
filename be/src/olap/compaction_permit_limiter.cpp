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

namespace doris {

CompactionPermitLimiter::CompactionPermitLimiter() : _used_permits(0) {}

bool CompactionPermitLimiter::request(int64_t permits) {
    if (permits > config::total_permits_for_compaction_score) {
        if (config::enable_compaction_permit_over_sold) {
            std::unique_lock<std::mutex> lock(_over_sold_mutex);
            _cv.wait(lock, [=] {
                return _used_permits == 0 ||
                       _used_permits + permits < config::total_permits_for_compaction_score;
            });
            _used_permits += permits;
            return true;
        } else {
            return false;
        }
    } else {
        if (_used_permits + permits > config::total_permits_for_compaction_score) {
            return false;
        }
        _used_permits += permits;
        return true;
    }
}

void CompactionPermitLimiter::release(int64_t permits) {
    _used_permits -= permits;
    _cv.notify_one();
}
} // namespace doris
