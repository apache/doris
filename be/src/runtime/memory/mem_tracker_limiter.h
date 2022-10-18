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
// Contains an limit, arranged into a tree structure, the consumption also tracked by its ancestors.
//
// Automatically track every once malloc/free of the system memory allocator (Currently, based on TCMlloc hook).
// Put Query MemTrackerLimiter into SCOPED_ATTACH_TASK when the thread starts,all memory used by this thread
// will be recorded on this Query, otherwise it will be recorded in Process Tracker by default.
//
// We use a five-level hierarchy of mem trackers: process, query pool, query, instance, node.
// The first four layers are MemTrackerLimiter with limit, and the fifth layer is MemTracker without limit.
// Specific parts of the fragment (exec nodes, sinks, etc) will add a fifth level when they are initialized.
class MemTrackerLimiter final : public MemTracker {
public:
    // Creates and adds the tracker limiter to the tree
    MemTrackerLimiter(
            int64_t byte_limit = -1, const std::string& label = std::string(),
            const std::shared_ptr<MemTrackerLimiter>& parent = std::shared_ptr<MemTrackerLimiter>(),
            RuntimeProfile* profile = nullptr);

    // If the final consumption is not as expected, this usually means that the same memory is calling
    // consume and release on different trackers. If the two trackers have a parent-child relationship,
    // the parent tracker consumption is correct, and the child tracker is wrong; if the two trackers have
    // no parent-child relationship, the two tracker consumptions are wrong.
    ~MemTrackerLimiter();

    std::shared_ptr<MemTrackerLimiter> parent() const { return _parent; }

    size_t remain_child_count() const { return _child_tracker_limiters.size(); }
    size_t had_child_count() const { return _had_child_count; }

    Snapshot make_snapshot(size_t level) const;
    // Returns a list of all the valid tracker snapshots.
    void make_snapshot(std::vector<MemTracker::Snapshot>* snapshots, size_t cur_level,
                       size_t upper_level) const;

public:
    static bool sys_mem_exceed_limit_check(int64_t bytes) {
        // Limit process memory usage using the actual physical memory of the process in `/proc/self/status`.
        // This is independent of the consumption value of the mem tracker, which counts the virtual memory
        // of the process malloc.
        // for fast, expect MemInfo::initialized() to be true.
        // tcmalloc/jemalloc allocator cache does not participate in the mem check as part of the process physical memory.
        // because `new/malloc` will trigger mem hook when using tcmalloc/jemalloc allocator cache,
        // but it may not actually alloc physical memory, which is not expected in mem hook fail.
        //
        // TODO: In order to ensure no OOM, currently reserve 200M, and then use the free mem in /proc/meminfo to ensure no OOM.
        if (MemInfo::proc_mem_no_allocator_cache() + bytes >= MemInfo::mem_limit() ||
            PerfCounters::get_vm_rss() + bytes >= MemInfo::hard_mem_limit()) {
            return true;
        }
        return false;
    }

    int64_t group_num() const { return _group_num; }
    bool has_limit() const { return _limit >= 0; }
    int64_t limit() const { return _limit; }
    void update_limit(int64_t limit) {
        DCHECK(has_limit());
        _limit = limit;
    }
    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }

    // Returns true if a valid limit of this tracker limiter or one of its ancestors is exceeded.
    bool any_limit_exceeded() const {
        for (const auto& tracker : _limited_ancestors) {
            if (tracker->limit_exceeded()) {
                return true;
            }
        }
        return false;
    }

    Status check_limit(int64_t bytes);

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker limiter or any of its parents. Returns int64_t::max() if there are no
    // limits and a negative value if any limit is already exceeded.
    int64_t spare_capacity() const;

    // Returns the lowest limit for this tracker limiter and its ancestors. Returns -1 if there is no limit.
    int64_t get_lowest_limit() const;

public:
    // up to (but not including) end_tracker.
    // This happens when we want to update tracking on a particular mem tracker but the consumption
    // against the limit recorded in one of its ancestors already happened.
    // It is used for revise mem tracker consumption.
    // If the location of memory alloc and free is different, the consumption value of mem tracker will be inaccurate.
    // But the consumption value of the process mem tracker is not affecte
    void cache_consume_local(int64_t bytes);

    // Will not change the value of process_mem_tracker, even though mem_tracker == process_mem_tracker.
    void transfer_to(int64_t size, MemTrackerLimiter* dst) {
        cache_consume_local(-size);
        dst->cache_consume_local(size);
    }

    void enable_print_log_usage() { _print_log_usage = true; }
    void enable_reset_zero() { _reset_zero = true; }

    void reset_zero() {
        ExecEnv::GetInstance()->orphan_mem_tracker_raw()->cache_consume_local(
                _consumption->current_value());
        cache_consume_local(-_consumption->current_value());
    }

    // Logs the usage of this tracker limiter and optionally its children (recursively).
    // If 'logged_consumption' is non-nullptr, sets the consumption value logged.
    // 'max_recursive_depth' specifies the maximum number of levels of children
    // to include in the dump. If it is zero, then no children are dumped.
    // Limiting the recursive depth reduces the cost of dumping, particularly
    // for the process tracker limiter.
    std::string log_usage(int max_recursive_depth = INT_MAX, int64_t* logged_consumption = nullptr);
    void print_log_usage(const std::string& msg);

    // Log the memory usage when memory limit is exceeded and return a status object with
    // msg of the allocation which caused the limit to be exceeded.
    // If 'failed_allocation_size' is greater than zero, logs the allocation size. If
    // 'failed_allocation_size' is zero, nothing about the allocation size is logged.
    // If 'state' is non-nullptr, logs the error to 'state'.
    std::string mem_limit_exceeded(const std::string& msg, int64_t failed_allocation_size = 0);
    std::string mem_limit_exceeded(const std::string& msg, MemTrackerLimiter* failed_tracker,
                                   const std::string& limit_exceeded_errmsg_prefix);
    Status mem_limit_exceeded(RuntimeState* state, const std::string& msg,
                              int64_t failed_allocation_size = 0);

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "all ancestor size: " << _all_ancestors.size() - 1 << "; "
            << "limited ancestor size: " << _limited_ancestors.size() - 1 << "; ";
        return msg.str();
    }

    static std::string print_bytes(int64_t bytes) {
        return PrettyPrinter::print(bytes, TUnit::BYTES);
    }

private:
    // The following func, for automatic memory tracking and limiting based on system memory allocation.
    friend class ThreadMemTrackerMgr;

    // Increases consumption of this tracker and its ancestors by 'bytes'.
    void consume(int64_t bytes);

    // Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) { consume(-bytes); }

    // Increases consumption of this tracker and its ancestors by 'bytes' only if
    // they can all consume 'bytes' without exceeding limit. If limit would be exceed,
    // no MemTrackerLimiters are updated. Returns true if the consumption was successfully updated.
    WARN_UNUSED_RESULT
    bool try_consume(int64_t bytes, std::string& failed_msg);

    void consume_local(int64_t bytes);

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes);

    // Log consumption of all the trackers provided. Returns the sum of consumption in
    // 'logged_consumption'. 'max_recursive_depth' specifies the maximum number of levels
    // of children to include in the dump. If it is zero, then no children are dumped.
    static std::string log_usage(int max_recursive_depth,
                                 const std::list<MemTrackerLimiter*>& trackers,
                                 int64_t* logged_consumption);

    static std::string limit_exceeded_errmsg_prefix_str(int64_t bytes,
                                                        MemTrackerLimiter* exceed_tracker) {
        return fmt::format(
                "failed alloc size {}, exceeded tracker:<{}>, limit {}, peak "
                "used {}, current used {}",
                print_bytes(bytes), exceed_tracker->label(), print_bytes(exceed_tracker->limit()),
                print_bytes(exceed_tracker->_consumption->value()),
                print_bytes(exceed_tracker->_consumption->current_value()));
    }

    static std::string limit_exceeded_errmsg_suffix_str(const std::string& msg) {
        return fmt::format(
                "{}. backend {} process memory used {}, limit {}. If query tracker exceed, `set "
                "exec_mem_limit=8G` to change limit, details mem usage see be.INFO.",
                msg, BackendOptions::get_localhost(), PerfCounters::get_vm_rss_str(),
                MemInfo::mem_limit_str());
    }

    static std::string limit_exceeded_errmsg_sys_str(int64_t bytes) {
        auto err_msg = fmt::format(
                "process memory used {}, tc/jemalloc allocator cache {}, exceed limit {}, failed "
                "alloc size {}",
                PerfCounters::get_vm_rss_str(), MemInfo::allocator_cache_mem_str(),
                MemInfo::mem_limit_str(), print_bytes(bytes));
        ExecEnv::GetInstance()->process_mem_tracker()->print_log_usage(err_msg);
        return err_msg;
    }

private:
    // Limit on memory consumption, in bytes. If limit_ == -1, there is no consumption limit. Used in log_usage。
    int64_t _limit;

    // Group number in MemTracker::mem_tracker_pool, generated by the timestamp.
    int64_t _group_num;

    std::shared_ptr<MemTrackerLimiter> _parent; // The parent of this tracker.

    // this tracker limiter plus all of its ancestors
    std::vector<MemTrackerLimiter*> _all_ancestors;
    // _all_ancestors with valid limits, except process tracker
    std::vector<MemTrackerLimiter*> _limited_ancestors;

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    std::atomic<int64_t> _untracked_mem = 0;

    // Child trackers of this tracker limiter. Used for error reporting and
    // listing only (i.e. updating the consumption of a parent tracker limiter does not
    // update that of its children).
    mutable std::mutex _child_tracker_limiter_lock;
    std::list<MemTrackerLimiter*> _child_tracker_limiters;
    // Iterator into parent_->_child_tracker_limiters for this object. Stored to have O(1) remove.
    std::list<MemTrackerLimiter*>::iterator _child_tracker_it;

    // The number of child trackers that have been added.
    std::atomic_size_t _had_child_count = 0;

    bool _print_log_usage = false;
    // mem hook record tracker cannot guarantee that the final consumption is 0,
    // nor can it guarantee that the memory alloc and free are recorded in a one-to-one correspondence.
    // In some cases, in order to avoid the cumulative error of the upper global tracker,
    // the consumption of the current tracker is reset to zero.
    bool _reset_zero = false;
};

inline void MemTrackerLimiter::consume(int64_t bytes) {
    if (bytes == 0) return;
    for (auto& tracker : _all_ancestors) {
        tracker->_consumption->add(bytes);
    }
}

inline int64_t MemTrackerLimiter::add_untracked_mem(int64_t bytes) {
    _untracked_mem += bytes;
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
        return _untracked_mem.exchange(0);
    }
    return 0;
}

inline void MemTrackerLimiter::consume_local(int64_t bytes) {
    if (bytes == 0) return;
    for (auto& tracker : _all_ancestors) {
        if (tracker->label() == "Process") return;
        tracker->_consumption->add(bytes);
    }
}

inline void MemTrackerLimiter::cache_consume_local(int64_t bytes) {
    if (bytes == 0) return;
    int64_t consume_bytes = add_untracked_mem(bytes);
    consume_local(consume_bytes);
}

inline bool MemTrackerLimiter::try_consume(int64_t bytes, std::string& failed_msg) {
    if (bytes <= 0) {
        release(-bytes);
        failed_msg = std::string();
        return true;
    }
    if (sys_mem_exceed_limit_check(bytes)) {
        failed_msg = limit_exceeded_errmsg_sys_str(bytes);
        return false;
    }
    int i;
    // Walk the tracker tree top-down.
    for (i = _all_ancestors.size() - 1; i >= 0; --i) {
        MemTrackerLimiter* tracker = _all_ancestors[i];
        // Process tracker does not participate in the process memory limit, process tracker consumption is virtual memory,
        // and there is a diff between the real physical memory value of the process. It is replaced by check_sys_mem_info.
        if (tracker->limit() < 0 || tracker->label() == "Process") {
            tracker->_consumption->add(bytes); // No limit at this tracker.
        } else {
            if (!tracker->_consumption->try_add(bytes, tracker->limit())) {
                // Failed for this mem tracker. Roll back the ones that succeeded.
                for (int j = _all_ancestors.size() - 1; j > i; --j) {
                    _all_ancestors[j]->_consumption->add(-bytes);
                }
                failed_msg = limit_exceeded_errmsg_prefix_str(bytes, tracker);
                return false;
            }
        }
    }
    // Everyone succeeded, return.
    DCHECK_EQ(i, -1);
    failed_msg = std::string();
    return true;
}

inline Status MemTrackerLimiter::check_limit(int64_t bytes) {
    if (bytes <= 0) return Status::OK();
    if (sys_mem_exceed_limit_check(bytes)) {
        return Status::MemoryLimitExceeded(limit_exceeded_errmsg_sys_str(bytes));
    }
    int i;
    // Walk the tracker tree top-down.
    for (i = _limited_ancestors.size() - 1; i >= 0; --i) {
        MemTrackerLimiter* tracker = _limited_ancestors[i];
        if (tracker->_consumption->current_value() + bytes > tracker->limit()) {
            return Status::MemoryLimitExceeded(limit_exceeded_errmsg_prefix_str(bytes, tracker));
        }
    }
    return Status::OK();
}

} // namespace doris
