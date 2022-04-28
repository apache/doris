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

#include <cstdint>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/status.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace doris {

// The Level use to decide whether to show it in web page,
// each MemTracker have a Level less than or equal to parent, only be set explicit,
// TASK contains query, import, compaction, etc.
enum class MemTrackerLevel { OVERVIEW = 0, TASK, INSTANCE, VERBOSE };

class MemTracker;
class RuntimeState;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// We use a five-level hierarchy of mem trackers: process, pool, query, fragment
/// instance. Specific parts of the fragment (exec nodes, sinks, etc) will add a
/// fifth level when they are initialized. This function also initializes a user
/// function mem tracker (in the fifth level).
///
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can be specified, and then the metric's value is used as the consumption rather than
/// the tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
/// Other consumption metrics are used in trackers below the process level to account
/// for memory (such as free buffer pool buffers) that is not tracked by Consume() and
/// Release().
///
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If limit_exceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.
class MemTracker {
public:
    // Creates and adds the tracker to the tree
    static std::shared_ptr<MemTracker> create_tracker(
            int64_t byte_limit = -1, const std::string& label = std::string(),
            const std::shared_ptr<MemTracker>& parent = std::shared_ptr<MemTracker>(),
            MemTrackerLevel level = MemTrackerLevel::VERBOSE, RuntimeProfile* profile = nullptr);

    // Cosume/release will not sync to parent.Usually used to manually record the specified memory,
    // It is independent of the recording of TCMalloc Hook in the thread local tracker, so the same
    // block of memory is recorded independently in these two trackers.
    static std::shared_ptr<MemTracker> create_virtual_tracker(
            int64_t byte_limit = -1, const std::string& label = std::string(),
            const std::shared_ptr<MemTracker>& parent = std::shared_ptr<MemTracker>(),
            MemTrackerLevel level = MemTrackerLevel::VERBOSE);

    // this is used for creating an orphan mem tracker, or for unit test.
    // If a mem tracker has parent, it should be created by `create_tracker()`
    MemTracker(int64_t byte_limit = -1, const std::string& label = std::string());

    ~MemTracker();

    // Returns a list of all the valid trackers.
    static void list_process_trackers(std::vector<std::shared_ptr<MemTracker>>* trackers);

    // Gets a shared_ptr to the "process" tracker, creating it if necessary.
    static std::shared_ptr<MemTracker> get_process_tracker();
    static MemTracker* get_raw_process_tracker();
    // Gets a shared_ptr to the "brpc server" tracker, creating it if necessary.
    static std::shared_ptr<MemTracker> get_brpc_server_tracker();

    Status check_sys_mem_info(int64_t bytes) {
        if (MemInfo::initialized() && MemInfo::current_mem() + bytes >= MemInfo::mem_limit()) {
            return Status::MemoryLimitExceeded(fmt::format(
                    "{}: TryConsume failed, bytes={} process whole consumption={}  mem limit={}",
                    _label, bytes, MemInfo::current_mem(), MemInfo::mem_limit()));
        }
        return Status::OK();
    }

    // Increases consumption of this tracker and its ancestors by 'bytes'.
    void consume(int64_t bytes) {
        if (bytes <= 0) {
            release(-bytes);
            return;
        }
        for (auto& tracker : _all_trackers) {
            tracker->_consumption->add(bytes);
        }
    }

    // Increases consumption of this tracker and its ancestors by 'bytes' only if
    // they can all consume 'bytes' without exceeding limit. If limit would be exceed,
    // no MemTrackers are updated. Returns true if the consumption was successfully updated.
    WARN_UNUSED_RESULT
    Status try_consume(int64_t bytes) {
        if (bytes <= 0) {
            release(-bytes);
            return Status::OK();
        }
        RETURN_IF_ERROR(check_sys_mem_info(bytes));
        int i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
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
                        for (int j = _all_trackers.size() - 1; j > i; --j) {
                            _all_trackers[j]->_consumption->add(-bytes);
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

    // Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) {
        if (bytes < 0) {
            consume(-bytes);
            return;
        }
        if (bytes == 0) {
            return;
        }
        for (auto& tracker : _all_trackers) {
            tracker->_consumption->add(-bytes);
        }
    }

    static void batch_consume(int64_t bytes,
                              const std::vector<std::shared_ptr<MemTracker>>& trackers) {
        for (auto& tracker : trackers) {
            tracker->consume(bytes);
        }
    }

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes) {
        _untracked_mem += bytes;
        if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
            return _untracked_mem.exchange(0);
        }
        return 0;
    }

    // In most cases, no need to call flush_untracked_mem on the child tracker,
    // because when it is destructed, theoretically all its children have been destructed.
    void flush_untracked_mem() {
        consume(_untracked_mem.exchange(0));
        for (const auto& tracker_weak : _child_trackers) {
            std::shared_ptr<MemTracker> tracker = tracker_weak.lock();
            if (tracker) tracker->flush_untracked_mem();
        }
    }

    void release_cache(int64_t bytes) {
        int64_t consume_bytes = add_untracked_mem(-bytes);
        if (consume_bytes != 0) {
            release(-consume_bytes);
        }
    }

    void consume_cache(int64_t bytes) {
        int64_t consume_bytes = add_untracked_mem(bytes);
        if (consume_bytes != 0) {
            consume(consume_bytes);
        }
    }

    WARN_UNUSED_RESULT
    Status try_consume_cache(int64_t bytes) {
        if (bytes <= 0) {
            release_cache(-bytes);
            return Status::OK();
        }
        int64_t consume_bytes = add_untracked_mem(bytes);
        if (consume_bytes != 0) {
            Status st = try_consume(consume_bytes);
            if (!st) {
                _untracked_mem += consume_bytes;
                return st;
            }
        }
        return Status::OK();
    }

    // up to (but not including) end_tracker.
    // This is useful if we want to move tracking between trackers that share a common (i.e. end_tracker)
    // ancestor. This happens when we want to update tracking on a particular mem tracker but the consumption
    // against the limit recorded in one of its ancestors already happened.
    void consume_local(int64_t bytes, MemTracker* end_tracker) {
        DCHECK(end_tracker);
        if (bytes == 0) return;
        for (auto& tracker : _all_trackers) {
            if (tracker == end_tracker) return;
            tracker->_consumption->add(bytes);
        }
    }

    // up to (but not including) end_tracker.
    void release_local(int64_t bytes, MemTracker* end_tracker) {
        DCHECK(end_tracker);
        if (bytes == 0) return;
        for (auto& tracker : _all_trackers) {
            if (tracker == end_tracker) return;
            tracker->_consumption->add(-bytes);
        }
    }

    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    // updating all ancestors up to the first shared ancestor. Must not be used if
    // 'dst' has a limit, or an ancestor with a limit, that is not a common
    // ancestor with the tracker, because this does not check memory limits.
    void transfer_to_relative(MemTracker* dst, int64_t bytes);

    WARN_UNUSED_RESULT
    Status try_transfer_to(MemTracker* dst, int64_t bytes) {
        if (id() == dst->id()) return Status::OK();
        // Must release first, then consume
        release_cache(bytes);
        Status st = dst->try_consume_cache(bytes);
        if (!st) {
            consume_cache(bytes);
            return st;
        }
        return Status::OK();
    }

    // Forced transfer, 'dst' may limit exceed, and more ancestor trackers will be updated.
    void transfer_to(MemTracker* dst, int64_t bytes) {
        if (id() == dst->id()) return;
        release_cache(bytes);
        dst->consume_cache(bytes);
    }

    // Returns true if a valid limit of this tracker or one of its ancestors is exceeded.
    MemTracker* limit_exceeded_tracker() const {
        for (const auto& tracker : _limit_trackers) {
            if (tracker->limit_exceeded()) {
                return tracker;
            }
        }
        return nullptr;
    }

    bool any_limit_exceeded() const { return limit_exceeded_tracker() != nullptr; }

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker or any of its parents. Returns int64_t::max() if there are no
    // limits and a negative value if any limit is already exceeded.
    int64_t spare_capacity() const {
        int64_t result = std::numeric_limits<int64_t>::max();
        for (const auto& tracker : _limit_trackers) {
            int64_t mem_left = tracker->limit() - tracker->consumption();
            result = std::min(result, mem_left);
        }
        return result;
    }

    // Returns the lowest limit for this tracker and its ancestors. Returns -1 if there is no limit.
    int64_t get_lowest_limit() const {
        if (_limit_trackers.empty()) return -1;
        int64_t min_limit = std::numeric_limits<int64_t>::max();
        for (const auto& tracker : _limit_trackers) {
            DCHECK(tracker->has_limit());
            min_limit = std::min(min_limit, tracker->limit());
        }
        return min_limit;
    }

    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }
    int64_t limit() const { return _limit; }
    void set_limit(int64_t limit) {
        DCHECK_GE(limit, -1);
        DCHECK(!_virtual);
        _limit = limit;
        _limit_trackers.push_back(this);
        for (const auto& tracker_weak : _child_trackers) {
            std::shared_ptr<MemTracker> tracker = tracker_weak.lock();
            if (tracker) tracker->_limit_trackers.push_back(this);
        }
    }
    bool has_limit() const { return _limit >= 0; }

    Status check_limit(int64_t bytes) {
        if (bytes <= 0) return Status::OK();
        RETURN_IF_ERROR(check_sys_mem_info(bytes));
        int i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
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

    const std::string& label() const { return _label; }

    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

    std::shared_ptr<MemTracker> parent() const { return _parent; }

    typedef std::function<void(int64_t bytes_to_free)> GcFunction;
    /// Add a function 'f' to be called if the limit is reached, if none of the other
    /// previously-added GC functions were successful at freeing up enough memory.
    /// 'f' does not need to be thread-safe as long as it is added to only one MemTracker.
    /// Note that 'f' must be valid for the lifetime of this MemTracker.
    void add_gc_function(GcFunction f) { _gc_functions.push_back(f); }

    /// Logs the usage of this tracker and optionally its children (recursively).
    /// If 'logged_consumption' is non-nullptr, sets the consumption value logged.
    /// 'max_recursive_depth' specifies the maximum number of levels of children
    /// to include in the dump. If it is zero, then no children are dumped.
    /// Limiting the recursive depth reduces the cost of dumping, particularly
    /// for the process MemTracker.
    std::string log_usage(int max_recursive_depth = INT_MAX, int64_t* logged_consumption = nullptr);

    /// Log the memory usage when memory limit is exceeded and return a status object with
    /// details of the allocation which caused the limit to be exceeded.
    /// If 'failed_allocation_size' is greater than zero, logs the allocation size. If
    /// 'failed_allocation_size' is zero, nothing about the allocation size is logged.
    /// If 'state' is non-nullptr, logs the error to 'state'.
    Status mem_limit_exceeded(RuntimeState* state, const std::string& details = std::string(),
                              int64_t failed_allocation = -1,
                              Status failed_alloc = Status::OK()) WARN_UNUSED_RESULT;

    // Usually, a negative values means that the statistics are not accurate,
    // 1. The released memory is not consumed.
    // 2. The same block of memory, tracker A calls consume, and tracker B calls release.
    // 3. Repeated releases of MemTacker. When the consume is called on the child MemTracker,
    //    after the release is called on the parent MemTracker,
    //    the child ~MemTracker will cause repeated releases.
    static void memory_leak_check(MemTracker* tracker, bool flush = true) {
        if (flush) tracker->flush_untracked_mem();
        DCHECK_EQ(tracker->_consumption->current_value(), 0) << std::endl << tracker->log_usage();
    }

    // If an ancestor of this tracker is a Task MemTracker, return that tracker. Otherwise return nullptr.
    MemTracker* parent_task_mem_tracker() {
        MemTracker* tracker = this;
        while (tracker != nullptr && tracker->_level != MemTrackerLevel::TASK) {
            tracker = tracker->_parent.get();
        }
        return tracker;
    }

    bool has_virtual_ancestor() {
        MemTracker* tracker = this;
        while (tracker != nullptr && tracker->_virtual == false) {
            tracker = tracker->_parent.get();
        }
        return tracker == nullptr ? false : true;
    }

    int64_t id() { return _id; }

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "all tracker size: " << _all_trackers.size() << "; "
            << "limit trackers size: " << _limit_trackers.size() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    static const std::string COUNTER_NAME;

private:
    /// 'byte_limit' < 0 means no limit
    /// 'label' is the label used in the usage string (log_usage())
    MemTracker(int64_t byte_limit, const std::string& label,
               const std::shared_ptr<MemTracker>& parent, MemTrackerLevel, RuntimeProfile* profile);

private:
    // If consumption is higher than max_consumption, attempts to free memory by calling
    // any added GC functions.  Returns true if max_consumption is still exceeded. Takes gc_lock.
    // Note: If the cache of segment/chunk is released due to insufficient query memory at a certain moment,
    // the performance of subsequent queries may be degraded, so the use of gc function should be careful enough.
    bool gc_memory(int64_t max_consumption);

    inline Status try_gc_memory(int64_t bytes) {
        if (UNLIKELY(gc_memory(_limit - bytes))) {
            return Status::MemoryLimitExceeded(
                    fmt::format("label={} TryConsume failed size={}, used={}, limit={}", label(),
                                bytes, _consumption->current_value(), _limit));
        }
        VLOG_NOTICE << "GC succeeded, TryConsume bytes=" << bytes
                    << " consumption=" << _consumption->current_value() << " limit=" << _limit;
        return Status::OK();
    }

    // Walks the MemTracker hierarchy and populates _all_trackers and
    // limit_trackers_
    void init();
    void init_virtual();

    // Adds tracker to _child_trackers
    void add_child_tracker(const std::shared_ptr<MemTracker>& tracker) {
        std::lock_guard<SpinLock> l(_child_trackers_lock);
        tracker->_child_tracker_it = _child_trackers.insert(_child_trackers.end(), tracker);
    }

    /// Log consumption of all the trackers provided. Returns the sum of consumption in
    /// 'logged_consumption'. 'max_recursive_depth' specifies the maximum number of levels
    /// of children to include in the dump. If it is zero, then no children are dumped.
    static std::string log_usage(int max_recursive_depth,
                                 const std::list<std::weak_ptr<MemTracker>>& trackers,
                                 int64_t* logged_consumption);

    // Creates the process tracker.
    static void create_process_tracker();
    // Creates the brpc server tracker.
    static void create_brpc_server_tracker();

    // Limit on memory consumption, in bytes. If limit_ == -1, there is no consumption limit.
    int64_t _limit;

    std::string _label;

    int64_t _id;

    std::shared_ptr<MemTracker> _parent; // The parent of this tracker.

    MemTrackerLevel _level;

    bool _virtual = false;

    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> _consumption; // in bytes

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    // TODO(zxy) It may be more performant to use thread_local static, which is inherently thread-safe.
    // Test after introducing TCMalloc hook
    std::atomic<int64_t> _untracked_mem = 0;

    std::vector<MemTracker*> _all_trackers;   // this tracker plus all of its ancestors
    std::vector<MemTracker*> _limit_trackers; // _all_trackers with valid limits

    // All the child trackers of this tracker. Used for error reporting and
    // listing only (i.e. updating the consumption of a parent tracker does not
    // update that of its children).
    SpinLock _child_trackers_lock;
    std::list<std::weak_ptr<MemTracker>> _child_trackers;
    // Iterator into parent_->child_trackers_ for this object. Stored to have O(1) remove.
    std::list<std::weak_ptr<MemTracker>>::iterator _child_tracker_it;

    // Lock to protect gc_memory(). This prevents many GCs from occurring at once.
    std::mutex _gc_lock;
    // Functions to call after the limit is reached to free memory.
    std::vector<GcFunction> _gc_functions;
};

#define RETURN_LIMIT_EXCEEDED(tracker, ...) return tracker->mem_limit_exceeded(__VA_ARGS__);
#define RETURN_IF_LIMIT_EXCEEDED(tracker, state, msg) \
    if (tracker->any_limit_exceeded()) RETURN_LIMIT_EXCEEDED(tracker, state, msg);
#define RETURN_IF_INSTANCE_LIMIT_EXCEEDED(state, msg)        \
    if (state->instance_mem_tracker()->any_limit_exceeded()) \
        RETURN_LIMIT_EXCEEDED(state->instance_mem_tracker(), state, msg);

} // namespace doris
