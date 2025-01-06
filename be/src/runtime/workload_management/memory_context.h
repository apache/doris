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

#include <cstdint>

#include <string>

#include "common/factory_creator.h"
#include "common/status.h"
#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;

class MemoryContext : public std::enable_shared_from_this<MemoryContext> {
    ENABLE_FACTORY_CREATOR(MemoryContext);

public:
    /*
    * --------------------------------
    * |          Property            |
    * --------------------------------
    * 1. operate them thread-safe.
    * 2. all tasks are unified.
    * 3. should not be operated frequently, use local variables to update Counter.
    */

    RuntimeProfile::Counter* current_memory_bytes_counter_;
    RuntimeProfile::Counter* peak_memory_bytes_counter_;
    // Maximum memory peak for all backends.
    // only set once by result sink when closing.
    RuntimeProfile::Counter* max_peak_memory_bytes_counter_;
    // The total number of times that the revoke method is called.
    RuntimeProfile::Counter* revoke_attempts_counter_;
    // The time that waiting for revoke finished.
    RuntimeProfile::Counter* revoke_wait_time_ms_counter_;
    // The revoked bytes
    RuntimeProfile::Counter* revoked_bytes_counter_;

    RuntimeProfile* profile() { return profile_.get(); }
    std::shared_ptr<MemTrackerLimiter> memtracker_limiter() { return memtracker_limiter_; }
    void set_memtracker_limiter(const std::shared_ptr<MemTrackerLimiter>& memtracker_limiter) {
        memtracker_limiter_ = memtracker_limiter;
    }
    std::string debug_string() { return profile_->pretty_print(); }

    /*
    * --------------------------------
    * |           Action             |
    * --------------------------------
    */

    // Following method is related with spill disk.
    // Compute the number of bytes could be released.
    virtual int64_t revokable_bytes() { return 0; }

    virtual bool ready_do_revoke() { return true; }

    // Begin to do revoke memory task.
    virtual Status revoke(int64_t bytes) { return Status::OK(); }

    virtual Status enter_arbitration(Status reason) { return Status::OK(); }

    virtual Status leave_arbitration(Status reason) { return Status::OK(); }

protected:
    MemoryContext() { init_profile(); }
    virtual ~MemoryContext() = default;

private:
    void init_profile() {
        profile_ = std::make_unique<RuntimeProfile>("MemoryContext");
        current_memory_bytes_counter_ = ADD_COUNTER(profile_, "CurrentMemoryBytes", TUnit::BYTES);
        peak_memory_bytes_counter_ = ADD_COUNTER(profile_, "PeakMemoryBytes", TUnit::BYTES);
        max_peak_memory_bytes_counter_ = ADD_COUNTER(profile_, "MaxPeakMemoryBytes", TUnit::BYTES);
        revoke_attempts_counter_ = ADD_COUNTER(profile_, "RevokeAttempts", TUnit::UNIT);
        revoke_wait_time_ms_counter_ = ADD_COUNTER(profile_, "RevokeWaitTimeMs", TUnit::TIME_MS);
        revoked_bytes_counter_ = ADD_COUNTER(profile_, "RevokedBytes", TUnit::BYTES);
    }

    // Used to collect memory execution stats.
    std::unique_ptr<RuntimeProfile> profile_;
    std::shared_ptr<MemTrackerLimiter> memtracker_limiter_;
};

class QueryMemoryContext : public MemoryContext {
    QueryMemoryContext() = default;
};

class LoadMemoryContext : public MemoryContext {
    LoadMemoryContext() = default;
};

class CompactionMemoryContext : public MemoryContext {
    CompactionMemoryContext() = default;
};

} // namespace doris
