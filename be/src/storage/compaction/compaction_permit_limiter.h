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

#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "storage/olap_common.h"

namespace doris {

class Status;

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

    void request(int64_t permits);

    bool try_request(int64_t permits);

    void release(int64_t permits);

    int64_t usage() const { return _used_permits.load(std::memory_order_relaxed); }

private:
    // sum of "permits" held by executing compaction tasks currently
    std::atomic<int64_t> _used_permits;
    std::mutex _permits_mutex;
    std::condition_variable _permits_cv;
};

class DataDir;

// Only instantiated when cumulative isolation is enabled. Disk slots and permits are granted
// together so no task can hold either resource while waiting for the other one.
class CompactionAdmission {
public:
    class Lease {
    public:
        ~Lease();
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

    private:
        friend class CompactionAdmission;
        Lease(CompactionAdmission& owner, const DataDir* dir, CompactionType type, int64_t charge)
                : _owner(owner), _dir(dir), _type(type), _charge(charge) {}

        CompactionAdmission& _owner;
        const DataDir* _dir;
        CompactionType _type;
        int64_t _charge;
    };

    explicit CompactionAdmission(int64_t reserved_permits);

    // A zero score denotes a force request: it still counts towards the disk slot limit.
    Status acquire(const DataDir* dir, CompactionType type, int disk_limit, int64_t score,
                   bool wait, std::unique_ptr<Lease>& lease);
    bool has_disk_slot(const DataDir* dir, CompactionType type, int disk_limit);
    void close();

private:
    struct DiskUsage {
        int base = 0;
        int cumulative = 0;
        int changelog = 0;
    };

    bool _has_disk_slot(const DataDir* dir, CompactionType type, int limit) const;
    void _release(const DataDir* dir, CompactionType type, int64_t charge);
    void _update_usage_metrics();

    const int64_t _reserved_permits;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::unordered_map<const DataDir*, DiskUsage> _disk_usage;
    int64_t _cumulative_used = 0;
    int64_t _other_used = 0;
    bool _closed = false;
};
} // namespace doris
