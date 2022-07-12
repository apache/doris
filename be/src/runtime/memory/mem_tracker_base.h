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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.h
// and modified by Doris

#pragma once

#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;

// A MemTracker tracks memory consumption.
// This class is thread-safe.
class MemTrackerBase {
public:
    const std::string& label() const { return _label; }

    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

    MemTrackerBase(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile);

    // this is used for creating an orphan mem tracker, or for unit test.
    // If a mem tracker has parent, it should be created by `create_tracker()`
    MemTrackerBase(const std::string& label = std::string());

    MemTrackerLimiter* parent() const { return _parent; }
    int64_t id() { return _id; }
    bool is_limited() { return _is_limited; } // MemTrackerLimiter
    bool is_observed() { return _is_observed; }
    void set_is_limited() { _is_limited = true; } // MemTrackerObserve
    void set_is_observed() { _is_observed = true; }

    // Usually, a negative values means that the statistics are not accurate,
    // 1. The released memory is not consumed.
    // 2. The same block of memory, tracker A calls consume, and tracker B calls release.
    // 3. Repeated releases of MemTacker. When the consume is called on the child MemTracker,
    //    after the release is called on the parent MemTracker,
    //    the child ~MemTracker will cause repeated releases.
    void memory_leak_check() { DCHECK_EQ(_consumption->current_value(), 0); }

    static const std::string COUNTER_NAME;

protected:
    // label used in the usage string (log_usage())
    std::string _label;

    // Automatically generated, unique for each mem tracker.
    int64_t _id;

    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> _consumption; // in bytes

    bool _is_limited = false; // is MemTrackerLimiter

    bool _is_observed = false; // is MemTrackerObserve

    MemTrackerLimiter* _parent; // The parent of this tracker.
};

} // namespace doris
