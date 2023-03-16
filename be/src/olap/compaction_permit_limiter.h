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

#include <condition_variable>
#include <mutex>

#include "common/config.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"

namespace doris {

/*
    This class is used to control compaction permission. To some extent, it can be used to control memory consumption.
    "permits" should be applied before a compaction task can execute. When the sum of "permites" held by executing
    compaction tasks reaches a threshold, subsequent compaction task will be no longer allowed, until some "permits"
    are released by some finished compaction tasks. "compaction score" for tablet is used as "permits" here.
*/
class CompactionPermitLimiter {
public:
    CompactionPermitLimiter();
    virtual ~CompactionPermitLimiter() {}

    bool request(int64_t permits);

    void release(int64_t permits);

    int64_t usage() const { return _used_permits; }

private:
    // sum of "permits" held by executing compaction tasks currently
    std::atomic<int64_t> _used_permits;
    std::mutex _permits_mutex;
    std::condition_variable _permits_cv;
};
} // namespace doris
