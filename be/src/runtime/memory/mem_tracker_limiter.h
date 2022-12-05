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

#include <atomic>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/perf_counters.h"

namespace doris {

class RuntimeState;

// Track and limit the memory usage of process and query.
// Contains an limit, arranged into a tree structure.
//
// Automatically track every once malloc/free of the system memory allocator (Currently, based on TCMlloc hook).
// Put Query MemTrackerLimiter into SCOPED_ATTACH_TASK when the thread starts,all memory used by this thread
// will be recorded on this Query, otherwise it will be recorded in Orphan Tracker by default.
class MemTrackerLimiter final : public MemTracker {
public:
    enum Type {
        GLOBAL = 0,        // Life cycle is the same as the process, e.g. Cache and default Orphan
        QUERY = 1,         // Count the memory consumption of all Query tasks.
        LOAD = 2,          // Count the memory consumption of all Load tasks.
        COMPACTION = 3,    // Count the memory consumption of all Base and Cumulative tasks.
        SCHEMA_CHANGE = 4, // Count the memory consumption of all SchemaChange tasks.
        CLONE = 5, // Count the memory consumption of all EngineCloneTask. Note: Memory that does not contain make/release snapshots.
        BATCHLOAD = 6,  // Count the memory consumption of all EngineBatchLoadTask.
        CONSISTENCY = 7 // Count the memory consumption of all EngineChecksumTask.
    };

    inline static std::unordered_map<Type, std::shared_ptr<RuntimeProfile::HighWaterMarkCounter>>
            TypeMemSum = {{Type::GLOBAL,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::QUERY,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::LOAD,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::COMPACTION,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::SCHEMA_CHANGE,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::CLONE,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::BATCHLOAD,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)},
                          {Type::CONSISTENCY,
                           std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES)}};

    inline static const std::string TypeString[] = {"global",     "query",         "load",
                                                    "compaction", "schema_change", "clone",
                                                    "batch_load", "consistency"};

public:
    // byte_limit equal to -1 means no consumption limit, only participate in process memory statistics.
    MemTrackerLimiter(Type type, const std::string& label = std::string(), int64_t byte_limit = -1,
                      RuntimeProfile* profile = nullptr,
                      const std::string& profile_counter_name = "PeakMemoryUsage");

    ~MemTrackerLimiter();

    static bool sys_mem_exceed_limit_check(int64_t bytes) {
        if (!_oom_avoidance) {
            return false;
        }
        // Limit process memory usage using the actual physical memory of the process in `/proc/self/status`.
        // This is independent of the consumption value of the mem tracker, which counts the virtual memory
        // of the process malloc.
        // for fast, expect MemInfo::initialized() to be true.
        //
        // tcmalloc/jemalloc allocator cache does not participate in the mem check as part of the process physical memory.
        // because `new/malloc` will trigger mem hook when using tcmalloc/jemalloc allocator cache,
        // but it may not actually alloc physical memory, which is not expected in mem hook fail.
        if (MemInfo::proc_mem_no_allocator_cache() + bytes >= MemInfo::mem_limit() ||
            MemInfo::sys_mem_available() < MemInfo::sys_mem_available_low_water_mark()) {
            print_log_process_usage(
                    fmt::format("System Mem Exceed Limit Check Faild, Try Alloc: {}", bytes));
            return true;
        }
        return false;
    }

    void set_consumption() { LOG(FATAL) << "MemTrackerLimiter set_consumption not supported"; }
    Type type() const { return _type; }
    int64_t group_num() const { return _group_num; }
    bool has_limit() const { return _limit >= 0; }
    int64_t limit() const { return _limit; }
    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }

    Status check_limit(int64_t bytes);

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker limiter.
    int64_t spare_capacity() const { return _limit - consumption(); }

    static void disable_oom_avoidance() { _oom_avoidance = false; }

public:
    // If need to consume the tracker frequently, use it
    void cache_consume(int64_t bytes);

    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(int64_t size, MemTrackerLimiter* dst) {
        cache_consume(-size);
        dst->cache_consume(size);
    }

    static void refresh_global_counter();
    Snapshot make_snapshot() const;
    // Returns a list of all the valid tracker snapshots.
    static void make_process_snapshots(std::vector<MemTracker::Snapshot>* snapshots);
    static void make_type_snapshots(std::vector<MemTracker::Snapshot>* snapshots, Type type);

    static std::string log_usage(MemTracker::Snapshot snapshot);
    std::string log_usage() { return log_usage(make_snapshot()); }
    static std::string type_log_usage(MemTracker::Snapshot snapshot);
    void print_log_usage(const std::string& msg);
    void enable_print_log_usage() { _enable_print_log_usage = true; }
    static void enable_print_log_process_usage() { _enable_print_log_process_usage = true; }
    static void print_log_process_usage(const std::string& msg, bool with_stacktrace = true);

    // Log the memory usage when memory limit is exceeded.
    std::string mem_limit_exceeded(const std::string& msg,
                                   const std::string& limit_exceeded_errmsg);
    Status fragment_mem_limit_exceeded(RuntimeState* state, const std::string& msg,
                                       int64_t failed_allocation_size = 0);

    static std::string process_mem_log_str() {
        return fmt::format(
                "physical memory {}, process memory used {} limit {}, sys mem available {} low "
                "water mark {}",
                PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES),
                PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(),
                MemInfo::sys_mem_available_str(),
                PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES));
    }

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "type: " << TypeString[_type] << "; ";
        return msg.str();
    }

private:
    friend class ThreadMemTrackerMgr;

    // Increases consumption of this tracker by 'bytes' only if will not exceeding limit.
    // Returns true if the consumption was successfully updated.
    WARN_UNUSED_RESULT
    bool try_consume(int64_t bytes, std::string& failed_msg);

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes);

    static std::string tracker_limit_exceeded_errmsg_str(int64_t bytes,
                                                         MemTrackerLimiter* exceed_tracker) {
        return fmt::format(
                "failed alloc size {}, exceeded tracker:<{}>, limit {}, peak "
                "used {}, current used {}",
                print_bytes(bytes), exceed_tracker->label(), print_bytes(exceed_tracker->limit()),
                print_bytes(exceed_tracker->_consumption->value()),
                print_bytes(exceed_tracker->_consumption->current_value()));
    }

    static std::string process_limit_exceeded_errmsg_str(int64_t bytes) {
        return fmt::format(
                "process memory used {} exceed limit {} or sys mem available {} less than low "
                "water mark {}, failed alloc size {}",
                PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(),
                MemInfo::sys_mem_available_str(),
                PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES),
                print_bytes(bytes));
    }

private:
    Type _type;

    // Limit on memory consumption, in bytes.
    int64_t _limit;

    // Group number in MemTracker::mem_tracker_limiter_pool and MemTracker::mem_tracker_pool, generated by the timestamp.
    int64_t _group_num;

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    std::atomic<int64_t> _untracked_mem = 0;

    // Avoid frequent printing.
    bool _enable_print_log_usage = false;
    static std::atomic<bool> _enable_print_log_process_usage;
    static bool _oom_avoidance;

    // Iterator into mem_tracker_limiter_pool for this object. Stored to have O(1) remove.
    std::list<MemTrackerLimiter*>::iterator _tracker_limiter_group_it;
};

inline int64_t MemTrackerLimiter::add_untracked_mem(int64_t bytes) {
    _untracked_mem += bytes;
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
        return _untracked_mem.exchange(0);
    }
    return 0;
}

inline void MemTrackerLimiter::cache_consume(int64_t bytes) {
    if (bytes == 0) return;
    int64_t consume_bytes = add_untracked_mem(bytes);
    consume(consume_bytes);
}

inline bool MemTrackerLimiter::try_consume(int64_t bytes, std::string& failed_msg) {
    if (bytes <= 0) {
        release(-bytes);
        failed_msg = std::string();
        return true;
    }

    if (config::memory_debug && bytes > 1073741824) { // 1G
        print_log_process_usage(fmt::format("Alloc Large Memory, Try Alloc: {}", bytes));
    }

    if (sys_mem_exceed_limit_check(bytes)) {
        failed_msg = process_limit_exceeded_errmsg_str(bytes);
        return false;
    }

    if (_limit < 0) {
        _consumption->add(bytes); // No limit at this tracker.
    } else {
        if (!_consumption->try_add(bytes, _limit)) {
            failed_msg = tracker_limit_exceeded_errmsg_str(bytes, this);
            return false;
        }
    }
    failed_msg = std::string();
    return true;
}

inline Status MemTrackerLimiter::check_limit(int64_t bytes) {
    if (sys_mem_exceed_limit_check(bytes)) {
        return Status::MemoryLimitExceeded(process_limit_exceeded_errmsg_str(bytes));
    }
    if (bytes <= 0) return Status::OK();
    if (_limit > 0 && _consumption->current_value() + bytes > _limit) {
        return Status::MemoryLimitExceeded(tracker_limit_exceeded_errmsg_str(bytes, this));
    }
    return Status::OK();
}

} // namespace doris
