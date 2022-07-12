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

#include "common/config.h"
#include "runtime/memory/mem_tracker_base.h"
#include "runtime/runtime_state.h"
#include "util/mem_info.h"

namespace doris {

class MemTrackerObserve;

// Tracker contains an limit, and can be arranged into a tree structure such that the consumption
// tracked by a MemTracker is also tracked by its ancestors.
// Used for:
// 1. Track and limit the memory usage of process and query.
//    Automatic memory consume based on system memory allocation (Currently, based on TCMlloc hook).
// 2. Execution logic that requires memory size to participate in control.
//    Manual consumption, but will not affect the overall statistics of the process.
//
// We use a five-level hierarchy of mem trackers: process, query pool, query, instance,
// node. Specific parts of the fragment (exec nodes, sinks, etc) will add a
// fifth level when they are initialized.
//
// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
// reached. If limit_exceeded() is called and the limit is exceeded, it will first call
// the GcFunctions to try to free memory and recheck the limit. For example, the process
// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
// this will be called before the process limit is reported as exceeded. GcFunctions are
// called in the order they are added, so expensive functions should be added last.
// GcFunctions are called with a global lock held, so should be non-blocking and not
// call back into MemTrackers, except to release memory.
class MemTrackerLimiter final : public MemTrackerBase {
public:
    // Creates and adds the tracker to the tree
    static MemTrackerLimiter* create_tracker(int64_t byte_limit, const std::string& label,
                                             MemTrackerLimiter* parent = nullptr,
                                             RuntimeProfile* profile = nullptr);

    // Walks the MemTrackerLimiter hierarchy and populates _ancestor_all_trackers and limit_trackers_
    void init(int64_t limit);

    ~MemTrackerLimiter();

    // Adds tracker to _child_trackers
    void add_child_tracker(MemTrackerLimiter* tracker);
    void add_child_tracker(MemTrackerObserve* tracker);
    // Remove tracker from _child_trackers
    void remove_child_tracker(MemTrackerLimiter* tracker);
    void remove_child_tracker(MemTrackerObserve* tracker);

    // Leaf tracker, without any child
    bool is_leaf() { _child_limiter_trackers.size() + _child_observe_trackers.size() == 0; }

    // Gets a "process" tracker, creating it if necessary.
    static MemTrackerLimiter* get_process_tracker();

    // Returns a list of all the valid trackers.
    static void list_process_trackers(std::vector<MemTrackerBase*>* trackers);

public:
    // The following func, for execution logic that requires memory size to participate in control.
    // this does not change the value of process tracker.

    // only consume self, will not sync to parent. Usually used to manually record the specified memory,
    // It is independent of the automatically recording of thread local tracker, so the same block of memory
    // will be recorded in the thread local tracker and the current tracker at the same time.
    void consume_self(int64_t bytes);
    void release_self(int64_t bytes) { consume_self(-bytes); }

    // up to (but not including) end_tracker.
    // This is useful if we want to move tracking between trackers that share a common (i.e. end_tracker)
    // ancestor. This happens when we want to update tracking on a particular mem tracker but the consumption
    // against the limit recorded in one of its ancestors already happened.
    void consume_local(int64_t bytes, MemTrackerLimiter* end_tracker);
    void release_local(int64_t bytes, MemTrackerLimiter* end_tracker) {
        consume_local(-bytes, end_tracker);
    }

    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    // Forced transfer, 'dst' may limit exceed, and more ancestor trackers will be updated.
    void transfer_to(MemTrackerLimiter* dst, int64_t bytes);

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes);

    // In most cases, no need to call flush_untracked_mem on the child tracker,
    // because when it is destructed, theoretically all its children have been destructed.
    void flush_untracked_mem() { consume(_untracked_mem.exchange(0)); }

    // Find the common ancestor and update trackers between 'this'/'dst' and
    // the common ancestor. This logic handles all cases, including the
    // two trackers being the same or being ancestors of each other because
    // 'all_trackers_' includes the current tracker.
    MemTrackerLimiter* common_ancestor(MemTrackerLimiter* dst);

public:
    // The following func, for mem limit.

    Status check_sys_mem_info(int64_t bytes) {
        // TODO add mmap
        if (MemInfo::initialized() && MemInfo::current_mem() + bytes >= MemInfo::mem_limit()) {
            return Status::MemoryLimitExceeded(fmt::format(
                    "{}: TryConsume failed, bytes={} process whole consumption={}  mem limit={}",
                    _label, bytes, MemInfo::current_mem(), MemInfo::mem_limit()));
        }
        return Status::OK();
    }

    bool has_limit() const { return _limit >= 0; }
    int64_t limit() const { return _limit; }
    void update_limit(int64_t limit) {
        DCHECK(has_limit());
        _limit = limit;
    }
    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }
    bool any_limit_exceeded() const { return limit_exceeded_tracker() != nullptr; }

    // Returns true if a valid limit of this tracker or one of its ancestors is exceeded.
    MemTrackerLimiter* limit_exceeded_tracker() const;

    Status check_limit(int64_t bytes);

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker or any of its parents. Returns int64_t::max() if there are no
    // limits and a negative value if any limit is already exceeded.
    int64_t spare_capacity() const;

    // Returns the lowest limit for this tracker and its ancestors. Returns -1 if there is no limit.
    int64_t get_lowest_limit() const;

    typedef std::function<void(int64_t bytes_to_free)> GcFunction;
    /// Add a function 'f' to be called if the limit is reached, if none of the other
    /// previously-added GC functions were successful at freeing up enough memory.
    /// 'f' does not need to be thread-safe as long as it is added to only one MemTrackerLimiter.
    /// Note that 'f' must be valid for the lifetime of this MemTrackerLimiter.
    void add_gc_function(GcFunction f) { _gc_functions.push_back(f); }

    // If consumption is higher than max_consumption, attempts to free memory by calling
    // any added GC functions.  Returns true if max_consumption is still exceeded. Takes gc_lock.
    // Note: If the cache of segment/chunk is released due to insufficient query memory at a certain moment,
    // the performance of subsequent queries may be degraded, so the use of gc function should be careful enough.
    bool gc_memory(int64_t max_consumption);
    Status try_gc_memory(int64_t bytes);

    /// Logs the usage of this tracker and optionally its children (recursively).
    /// If 'logged_consumption' is non-nullptr, sets the consumption value logged.
    /// 'max_recursive_depth' specifies the maximum number of levels of children
    /// to include in the dump. If it is zero, then no children are dumped.
    /// Limiting the recursive depth reduces the cost of dumping, particularly
    /// for the process MemTracker.
    std::string log_usage(int max_recursive_depth = INT_MAX, int64_t* logged_consumption = nullptr);

    // Log the memory usage when memory limit is exceeded and return a status object with
    // details of the allocation which caused the limit to be exceeded.
    // If 'failed_allocation_size' is greater than zero, logs the allocation size. If
    // 'failed_allocation_size' is zero, nothing about the allocation size is logged.
    // If 'state' is non-nullptr, logs the error to 'state'.
    Status mem_limit_exceeded(RuntimeState* state, const std::string& details = std::string(),
                              int64_t failed_allocation = -1, Status failed_alloc = Status::OK());

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "all tracker size: " << _ancestor_all_trackers.size() << "; "
            << "limit trackers size: " << _ancestor_limiter_trackers.size() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

private:
    // The following func, for automatic memory tracking and limiting based on system memory allocation.
    friend class ThreadMemTrackerMgr;

    MemTrackerLimiter(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile)
            : MemTrackerBase(label, parent, profile) {}

    // Creates the process tracker.
    static void create_process_tracker();

    // Increases consumption of this tracker and its ancestors by 'bytes'.
    void consume(int64_t bytes);

    // Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) { consume(-bytes); }

    // Increases consumption of this tracker and its ancestors by 'bytes' only if
    // they can all consume 'bytes' without exceeding limit. If limit would be exceed,
    // no MemTrackers are updated. Returns true if the consumption was successfully updated.
    WARN_UNUSED_RESULT
    Status try_consume(int64_t bytes);

    /// Log consumption of all the trackers provided. Returns the sum of consumption in
    /// 'logged_consumption'. 'max_recursive_depth' specifies the maximum number of levels
    /// of children to include in the dump. If it is zero, then no children are dumped.
    static std::string log_usage(int max_recursive_depth,
                                 const std::list<MemTrackerLimiter*>& trackers,
                                 int64_t* logged_consumption);

private:
    // Limit on memory consumption, in bytes. If limit_ == -1, there is no consumption limit. Used in log_usage。
    int64_t _limit;

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    std::atomic<int64_t> _untracked_mem = 0;

    // All the child trackers of this tracker. Used for error reporting and
    // listing only (i.e. updating the consumption of a parent tracker does not
    // update that of its children).
    SpinLock _child_trackers_lock;
    std::list<MemTrackerLimiter*> _child_limiter_trackers;
    std::list<MemTrackerObserve*> _child_observe_trackers;
    // Iterator into parent_->_child_limiter_trackers for this object. Stored to have O(1) remove.
    std::list<MemTrackerLimiter*>::iterator _child_tracker_it;

    // this tracker plus all of its ancestors
    std::vector<MemTrackerLimiter*> _ancestor_all_trackers;
    // _ancestor_all_trackers with valid limits
    std::vector<MemTrackerLimiter*> _ancestor_limiter_trackers;

    // Lock to protect gc_memory(). This prevents many GCs from occurring at once.
    std::mutex _gc_lock;
    // Functions to call after the limit is reached to free memory.
    std::vector<GcFunction> _gc_functions;
};

inline void MemTrackerLimiter::consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    } else {
        for (auto& tracker : _ancestor_all_trackers) {
            tracker->_consumption->add(bytes);
        }
    }
}

inline Status MemTrackerLimiter::try_consume(int64_t bytes) {
    if (bytes <= 0) {
        release(-bytes);
        return Status::OK();
    }
    RETURN_IF_ERROR(check_sys_mem_info(bytes));
    int i;
    // Walk the tracker tree top-down.
    for (i = _ancestor_all_trackers.size() - 1; i >= 0; --i) {
        MemTrackerLimiter* tracker = _ancestor_all_trackers[i];
        if (tracker->limit() < 0) {
            tracker->_consumption->add(bytes); // No limit at this tracker.
        } else {
            // If TryConsume fails, we can try to GC, but we may need to try several times if
            // there are concurrent consumers because we don't take a lock before trying to
            // update _consumption.
            while (true) {
                if (LIKELY(tracker->_consumption->try_add(bytes, tracker->limit()))) break;
                Status st = tracker->try_gc_memory(bytes);
                if (!st) {
                    // Failed for this mem tracker. Roll back the ones that succeeded.
                    for (int j = _ancestor_all_trackers.size() - 1; j > i; --j) {
                        _ancestor_all_trackers[j]->_consumption->add(-bytes);
                    }
                    return st;
                }
            }
        }
    }
    // Everyone succeeded, return.
    DCHECK_EQ(i, -1);
    return Status::OK();
}

inline void MemTrackerLimiter::consume_self(int64_t bytes) {
    int64_t consume_bytes = add_untracked_mem(bytes);
    if (consume_bytes != 0) {
        _consumption->add(consume_bytes);
    }
}

inline void MemTrackerLimiter::consume_local(int64_t bytes, MemTrackerLimiter* end_tracker) {
    DCHECK(end_tracker);
    if (bytes == 0) return;
    for (auto& tracker : _ancestor_all_trackers) {
        if (tracker == end_tracker) return;
        tracker->consume_self(bytes);
    }
}

inline void MemTrackerLimiter::transfer_to(MemTrackerLimiter* dst, int64_t bytes) {
    DCHECK(dst->is_limited());
    if (id() == dst->id()) return;
    release_local(bytes, MemTrackerLimiter::get_process_tracker());
    dst->consume_local(bytes, MemTrackerLimiter::get_process_tracker());
}

inline int64_t MemTrackerLimiter::add_untracked_mem(int64_t bytes) {
    _untracked_mem += bytes;
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
        return _untracked_mem.exchange(0);
    }
    return 0;
}

inline Status MemTrackerLimiter::check_limit(int64_t bytes) {
    if (bytes <= 0) return Status::OK();
    RETURN_IF_ERROR(check_sys_mem_info(bytes));
    int i;
    // Walk the tracker tree top-down.
    for (i = _ancestor_all_trackers.size() - 1; i >= 0; --i) {
        MemTrackerLimiter* tracker = _ancestor_all_trackers[i];
        if (tracker->limit() > 0) {
            while (true) {
                if (LIKELY(tracker->_consumption->current_value() + bytes < tracker->limit()))
                    break;
                RETURN_IF_ERROR(tracker->try_gc_memory(bytes));
            }
        }
    }
    return Status::OK();
}

#define RETURN_LIMIT_EXCEEDED(tracker, ...) return tracker->mem_limit_exceeded(__VA_ARGS__);
#define RETURN_IF_LIMIT_EXCEEDED(tracker, state, msg) \
    if (tracker->any_limit_exceeded()) RETURN_LIMIT_EXCEEDED(tracker, state, msg);
#define RETURN_IF_INSTANCE_LIMIT_EXCEEDED(state, msg)        \
    if (state->instance_mem_tracker()->any_limit_exceeded()) \
        RETURN_LIMIT_EXCEEDED(state->instance_mem_tracker(), state, msg);

} // namespace doris
