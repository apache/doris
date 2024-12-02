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

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

class MemoryContext : public std::enable_shared_from_this<MemoryContext> {
    ENABLE_FACTORY_CREATOR(MemoryContext);

public:
    // Used to collect memory execution stats.
    // The stats class is not thread safe, should not do concurrent modifications.
    class MemoryStats {
    public:
        MemoryStats() = default;
        virtual ~MemoryStats() = default;
        void merge(MemoryStats& stats);
        void reset();
        std::string debug_string();
        int64_t revoke_attempts() { return revoke_attempts_; }
        int64_t revoke_wait_time_ms() { return revoke_wait_time_ms_; }
        int64_t revoked_bytes() { return revoked_bytes_; }
        int64_t max_peak_memory_bytes() { return max_peak_memory_bytes_; }
        int64_t current_used_memory_bytes() { return current_used_memory_bytes_; }

    private:
        // Maximum memory peak for all backends.
        // only set once by result sink when closing.
        int64_t max_peak_memory_bytes_ = 0;
        int64_t current_used_memory_bytes_ = 0;
        // The total number of times that the revoke method is called.
        int64_t revoke_attempts_ = 0;
        // The time that waiting for revoke finished.
        int64_t revoke_wait_time_ms_ = 0;
        // The revoked bytes
        int64_t revoked_bytes_ = 0;
    };

public:
    MemoryContext(std::shared_ptr<MemtrackerLimiter> memtracker)
            : memtracker_limiter_(memtracker) {}

    virtual ~MemoryContext() = default;

    MemtrackerLimiter* memtracker_limiter() { return memtracker_limiter_.get(); }

    MemoryStats* stats() { return &stats_; }

    // Following method is related with spill disk.
    // Compute the number of bytes could be released.
    virtual int64_t revokable_bytes() { return 0; }

    virtual bool ready_do_revoke() { return true; }

    // Begin to do revoke memory task.
    virtual Status revoke(int64_t bytes) { return Status::OK(); }

    virtual Status enter_arbitration(Status reason) { return Status::OK(); }

    virtual Status leave_arbitration(Status reason) { return Status::OK(); }

private:
    MemoryStats stats_;
    std::shared_ptr<MemtrackerLimiter> memtracker_limiter_;
};

} // namespace doris
