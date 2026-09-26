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

#include "exec/spill/spill_remote_upload_budget.h"

#include <glog/logging.h>

#include <chrono>

#include "util/stopwatch.hpp"

namespace doris {

Status SpillRemoteUploadBudget::acquire(int64_t bytes, const std::function<bool()>& is_cancelled,
                                        int64_t* wait_ns) {
    MonotonicStopWatch watch;
    watch.start();
    std::unique_lock<std::mutex> lock(_mutex);
    // A cancelled query must not start a new (billed) upload, whether the budget is available
    // right away, or became available while it was waiting.
    if (is_cancelled && is_cancelled()) {
        return Status::Cancelled("query cancelled before acquiring spill upload budget");
    }
    while (_inflight_bytes > 0 && _inflight_bytes + bytes > _limit_bytes) {
        _cv.wait_for(lock, std::chrono::milliseconds(100));
        if (is_cancelled && is_cancelled()) {
            return Status::Cancelled("query cancelled while waiting for spill upload budget");
        }
    }
    _inflight_bytes += bytes;
    _total_acquired_bytes += bytes;
    if (wait_ns != nullptr) {
        *wait_ns = static_cast<int64_t>(watch.elapsed_time());
    }
    return Status::OK();
}

void SpillRemoteUploadBudget::release(int64_t bytes) {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _inflight_bytes -= bytes;
        _total_released_bytes += bytes;
        // Releasing more than was acquired means a callback was paired twice; the budget would
        // silently over-admit from then on, so this must hold in release builds too.
        DORIS_CHECK_GE(_inflight_bytes, 0) << "spill upload budget released more than acquired";
    }
    _cv.notify_all();
}

int64_t SpillRemoteUploadBudget::inflight_bytes() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _inflight_bytes;
}

int64_t SpillRemoteUploadBudget::total_acquired_bytes() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _total_acquired_bytes;
}

int64_t SpillRemoteUploadBudget::total_released_bytes() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _total_released_bytes;
}

int64_t SpillRemoteUploadBudget::limit_bytes() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _limit_bytes;
}

void SpillRemoteUploadBudget::set_limit_bytes(int64_t limit_bytes) {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _limit_bytes = limit_bytes;
    }
    _cv.notify_all();
}

} // namespace doris
