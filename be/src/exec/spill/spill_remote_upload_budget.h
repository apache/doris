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
#include <cstdint>
#include <functional>
#include <mutex>

#include "common/status.h"

namespace doris {

/// BE-wide budget of spill bytes that have been SUBMITTED for upload to object storage and
/// not yet acknowledged. S3FileWriter allocates a 5 MB upload buffer per submitted part and
/// never blocks on its own, so without this budget a spilling query could keep as many
/// buffers in flight as it has bytes to spill.
///
/// Only submitted buffers are counted: the writer's not-yet-full pending buffer is not, because
/// nothing but a further append or close can ever submit it. Counting it would let a set of
/// idle writers hold the whole budget and block every other writer forever. The memory bound
/// is therefore `limit + open writers x s3_write_buffer_size`.
///
/// S3FileWriter calls acquire() through FileWriterOptions::upload_submit_gate right before a
/// buffer is submitted and release() through upload_done_callback when its upload finished.
/// Every counted byte is drained by the upload thread pool, so acquire() always makes progress.
/// SpillFileWriter reconciles any difference when a part reached its final state, so buffers
/// that fail before the upload starts cannot leak budget.
class SpillRemoteUploadBudget {
public:
    explicit SpillRemoteUploadBudget(int64_t limit_bytes) : _limit_bytes(limit_bytes) {}

    /// Block until `bytes` fits into the budget. A request larger than the whole budget is
    /// admitted once nothing else is in flight so a single oversized buffer cannot deadlock.
    /// Returns Cancelled when `is_cancelled` (optional) reports true while waiting.
    /// `wait_ns` (optional) receives the time spent blocked.
    Status acquire(int64_t bytes, const std::function<bool()>& is_cancelled, int64_t* wait_ns);

    void release(int64_t bytes);

    int64_t inflight_bytes() const;
    int64_t limit_bytes() const;
    void set_limit_bytes(int64_t limit_bytes);

    /// Monotonic totals, for tests and metrics: acquired - released == inflight.
    int64_t total_acquired_bytes() const;
    int64_t total_released_bytes() const;

private:
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    int64_t _limit_bytes;
    int64_t _inflight_bytes = 0;
    int64_t _total_acquired_bytes = 0;
    int64_t _total_released_bytes = 0;
};

} // namespace doris
