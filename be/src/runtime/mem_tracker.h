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

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_MEM_LIMIT_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_MEM_LIMIT_H

#include <stdint.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "gen_cpp/Types_types.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "common/status.h"

namespace doris {

class ObjectPool;
class MemTracker;
class ReservationTrackerCounters;
class RuntimeState;
class TQueryOptions;

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
/// can specified, and then the metric's value is used as the consumption rather than the
/// tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
//
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call
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
    /// 'byte_limit' < 0 means no limit
    /// 'label' is the label used in the usage string (LogUsage())
    /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be included
    /// in LogUsage() output if consumption is 0.
    MemTracker(int64_t byte_limit = -1, const std::string& label = std::string(),
               MemTracker* parent = NULL, bool log_usage_if_zero = true);

    /// C'tor for tracker for which consumption counter is created as part of a profile.
    /// The counter is created with name COUNTER_NAME.
    MemTracker(RuntimeProfile* profile, int64_t byte_limit,
            const std::string& label = std::string(), MemTracker* parent = NULL);

    /// C'tor for tracker that uses consumption_metric as the consumption value.
    /// Consume()/Release() can still be called. This is used for the process tracker.
    MemTracker(UIntGauge* consumption_metric, int64_t byte_limit = -1,
      const std::string& label = std::string());

    ~MemTracker();

    /// Closes this MemTracker. After closing it is invalid to consume memory on this
    /// tracker and the tracker's consumption counter (which may be owned by a
    /// RuntimeProfile, not this MemTracker) can be safely destroyed. MemTrackers without
    /// consumption metrics in the context of a daemon must always be closed.
    /// Idempotent: calling multiple times has no effect.
    void close();

    // Removes this tracker from _parent->_child_trackers.
    void unregister_from_parent() {
        DCHECK(_parent != NULL);
        std::lock_guard<std::mutex> l(_parent->_child_trackers_lock);
        _parent->_child_trackers.erase(_child_tracker_it);
        _child_tracker_it = _parent->_child_trackers.end();
    }

    /// Include counters from a ReservationTracker in logs and other diagnostics.
    /// The counters should be owned by the fragment's RuntimeProfile.
    void enable_reservation_reporting(const ReservationTrackerCounters& counters);

    /// Construct a MemTracker object for query 'id'. The query limits are determined based
    /// on 'query_options'. The MemTracker is a child of the request pool MemTracker for
    /// 'pool_name', which is created if needed. The returned MemTracker is owned by
    /// 'obj_pool'.
    static MemTracker* CreateQueryMemTracker(const TUniqueId& id,
            const TQueryOptions& query_options, const std::string& pool_name,
            ObjectPool* obj_pool);

    // Returns a MemTracker object for query 'id'.  Calling this with the same id will
    // return the same MemTracker object.  An example of how this is used is to pass it
    // the same query id for all fragments of that query running on this machine.  This
    // way, we have per-query limits rather than per-fragment.
    // The first time this is called for an id, a new MemTracker object is created with
    // 'parent' as the parent tracker.
    // byte_limit and parent must be the same for all GetMemTracker() calls with the
    // same id.
    static std::shared_ptr<MemTracker> get_query_mem_tracker(const TUniqueId& id,
            int64_t byte_limit, MemTracker* parent);

    void consume(int64_t bytes) {
        if (bytes <= 0) {
            if (bytes < 0) release(-bytes);
            return;
        }

        if (_consumption_metric != NULL) {
            RefreshConsumptionFromMetric();
            return;
        }
        for (std::vector<MemTracker*>::iterator tracker = _all_trackers.begin();
             tracker != _all_trackers.end(); ++tracker) {
            (*tracker)->_consumption->add(bytes);
            if ((*tracker)->_consumption_metric == NULL) {
                DCHECK_GE((*tracker)->_consumption->current_value(), 0);
            }
        }
    }

    /// Increases/Decreases the consumption of this tracker and the ancestors up to (but
    /// not including) end_tracker. This is useful if we want to move tracking between
    /// trackers that share a common (i.e. end_tracker) ancestor. This happens when we want
    /// to update tracking on a particular mem tracker but the consumption against
    /// the limit recorded in one of its ancestors already happened.
    void consume_local(int64_t bytes, MemTracker* end_tracker) {
        DCHECK(_consumption_metric == NULL) << "Should not be called on root.";
        for (int i = 0; i < _all_trackers.size(); ++i) {
            if (_all_trackers[i] == end_tracker) return;
            DCHECK(!_all_trackers[i]->has_limit());
            _all_trackers[i]->_consumption->add(bytes);
        }
        DCHECK(false) << "end_tracker is not an ancestor";
    }

    void release_local(int64_t bytes, MemTracker* end_tracker) {
        consume_local(-bytes, end_tracker);
    }

    /// Increases consumption of this tracker and its ancestors by 'bytes' only if
    /// they can all consume 'bytes'. If this brings any of them over, none of them
    /// are updated.
    /// Returns true if the try succeeded.
    WARN_UNUSED_RESULT
    bool try_consume(int64_t bytes) {
        if (_consumption_metric != NULL) RefreshConsumptionFromMetric();
        if (UNLIKELY(bytes <= 0)) return true;
        int i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
            const int64_t limit = tracker->limit();
            if (limit < 0) {
                tracker->_consumption->add(bytes); // No limit at this tracker.
            } else {
                // If TryConsume fails, we can try to GC, but we may need to try several times if
                // there are concurrent consumers because we don't take a lock before trying to
                // update _consumption.
                while (true) {
                    if (LIKELY(tracker->_consumption->try_add(bytes, limit))) break;

                    VLOG_RPC << "TryConsume failed, bytes=" << bytes
                        << " consumption=" << tracker->_consumption->current_value()
                        << " limit=" << limit << " attempting to GC";
                    if (UNLIKELY(tracker->GcMemory(limit - bytes))) {
                        DCHECK_GE(i, 0);
                        // Failed for this mem tracker. Roll back the ones that succeeded.
                        for (int j = _all_trackers.size() - 1; j > i; --j) {
                            _all_trackers[j]->_consumption->add(-bytes);
                        }
                        return false;
                    }
                    VLOG_RPC << "GC succeeded, TryConsume bytes=" << bytes
                        << " consumption=" << tracker->_consumption->current_value()
                        << " limit=" << limit;
                }
            }
        }
        // Everyone succeeded, return.
        DCHECK_EQ(i, -1);
        return true;
    }

    /// Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) {
        if (bytes <= 0) {
            if (bytes < 0) consume(-bytes);
            return;
        }

        if (_consumption_metric != NULL) {
            RefreshConsumptionFromMetric();
            return;
        }
        for (std::vector<MemTracker*>::iterator tracker = _all_trackers.begin();
             tracker != _all_trackers.end(); ++tracker) {
            (*tracker)->_consumption->add(-bytes);
            /// If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
            /// reported amount, the subsequent call to FunctionContext::Free() may cause the
            /// process mem tracker to go negative until it is synced back to the tcmalloc
            /// metric. Don't blow up in this case. (Note that this doesn't affect non-process
            /// trackers since we can enforce that the reported memory usage is internally
            /// consistent.)
            if ((*tracker)->_consumption_metric == NULL) {
                DCHECK_GE((*tracker)->_consumption->current_value(), 0)
                    << std::endl << (*tracker)->LogUsage();
            }
        }

        /// TODO: Release brokered memory?
    }

    // Returns true if a valid limit of this tracker or one of its ancestors is exceeded.
    bool any_limit_exceeded() {
        for (std::vector<MemTracker*>::iterator tracker = _limit_trackers.begin();
                tracker != _limit_trackers.end(); ++tracker) {
            if ((*tracker)->limit_exceeded()) {
                return true;
            }
        }
        return false;
    }

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker or any of its parents. Returns int64_t::max() if there are no
    // limits and a negative value if any limit is already exceeded.
    int64_t spare_capacity() const {
        int64_t result = std::numeric_limits<int64_t>::max();
        for (std::vector<MemTracker*>::const_iterator tracker = _limit_trackers.begin();
                tracker != _limit_trackers.end(); ++tracker) {
            int64_t mem_left = (*tracker)->limit() - (*tracker)->consumption();
            result = std::min(result, mem_left);
        }
        return result;
    }

    /// Refresh the memory consumption value from the consumption metric. Only valid to
    /// call if this tracker has a consumption metric.
    void RefreshConsumptionFromMetric() {
        DCHECK(_consumption_metric != nullptr);
        DCHECK(_parent == nullptr);
        _consumption->set(_consumption_metric->value());
    }


    bool limit_exceeded() const{
        return _limit >= 0 && _limit < consumption();
    }

    int64_t limit() const {
        return _limit;
    }

    bool has_limit() const {
        return _limit >= 0;
    }

    const std::string& label() const {
        return _label;
    }

    /// Returns the lowest limit for this tracker and its ancestors. Returns
    /// -1 if there is no limit.
    int64_t lowest_limit() const {
        if (_limit_trackers.empty()) return -1;
        int64_t v = std::numeric_limits<int64_t>::max();
        for (int i = 0; i < _limit_trackers.size(); ++i) {
            DCHECK(_limit_trackers[i]->has_limit());
            v = std::min(v, _limit_trackers[i]->limit());
        }
        return v;
    }

    /// Returns the memory 'reserved' by this resource pool mem tracker, which is the sum
    /// of the memory reserved by the queries in it (i.e. its child trackers). The mem
    /// reserved for a query is its limit_, if set (which should be the common case with
    /// admission control). Otherwise the current consumption is used.
    int64_t GetPoolMemReserved() const;

    int64_t consumption() const {
        return _consumption->current_value();;
    }


    /// Note that if _consumption is based on _consumption_metric, this will the max value
    /// we've recorded in consumption(), not necessarily the highest value
    /// _consumption_metric has ever reached.
    int64_t peak_consumption() const { return _consumption->value(); }

    MemTracker* parent() const {
        return _parent;
    }

    /// Signature for function that can be called to free some memory after limit is
    /// reached. The function should try to free at least 'bytes_to_free' bytes of
    /// memory. See the class header for further details on the expected behaviour of
    /// these functions.
    typedef std::function<void(int64_t bytes_to_free)> GcFunction;

    /// Add a function 'f' to be called if the limit is reached, if none of the other
    /// previously-added GC functions were successful at freeing up enough memory.
    /// 'f' does not need to be thread-safe as long as it is added to only one MemTracker.
    /// Note that 'f' must be valid for the lifetime of this MemTracker.
    void AddGcFunction(GcFunction f);

    /// Register this MemTracker's metrics. Each key will be of the form
    /// "<prefix>.<metric name>".
    void RegisterMetrics(MetricRegistry* metrics, const std::string& prefix);

    /// Logs the usage of this tracker and all of its children (recursively).
    /// If 'logged_consumption' is non-NULL, sets the consumption value logged.
    /// TODO: once all memory is accounted in ReservationTracker hierarchy, move
    /// reporting there.
    std::string LogUsage(
            const std::string& prefix = "", int64_t* logged_consumption = nullptr) const;

    /// Log the memory usage when memory limit is exceeded and return a status object with
    /// details of the allocation which caused the limit to be exceeded.
    /// If 'failed_allocation_size' is greater than zero, logs the allocation size. If
    /// 'failed_allocation_size' is zero, nothing about the allocation size is logged.
    Status MemLimitExceeded(RuntimeState* state, const std::string& details,
            int64_t failed_allocation = 0);

    static const int UNLIMITED_DEPTH = INT_MAX;

    static const std::string COUNTER_NAME;

    static void update_limits(int64_t bytes, std::vector<MemTracker*>* limits) {
        for (std::vector<MemTracker*>::iterator i = limits->begin(); i != limits->end(); ++i) {
            (*i)->consume(bytes);
        }
    }

    static bool limit_exceeded(const std::vector<MemTracker*>& limits) {
        for (std::vector<MemTracker*>::const_iterator i = limits.begin(); i != limits.end();
                ++i) {
            if ((*i)->limit_exceeded()) {
                // TODO: remove logging
                LOG(WARNING) << "exceeded limit: limit=" << (*i)->limit() << " consumption="
                             << (*i)->consumption();
                return true;
            }
        }

        return false;
    }

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "all tracker size: " << _all_trackers.size() << "; "
            << "limit trackers size: " << _limit_trackers.size() << "; "
            << "parent is null: " << ((_parent == NULL) ? "true" : "false") << "; ";
        return msg.str();
    }

    bool is_consumption_metric_null() {
        return _consumption_metric == nullptr;
    }
    
private:
    friend class PoolMemTrackerRegistry;

    /// If consumption is higher than max_consumption, attempts to free memory by calling
    /// any added GC functions.  Returns true if max_consumption is still exceeded. Takes
    /// gc_lock. Updates metrics if initialized.
    bool GcMemory(int64_t max_consumption);

    // Walks the MemTracker hierarchy and populates _all_trackers and _limit_trackers
    void Init();

    // Adds tracker to _child_trackers
    void add_child_tracker(MemTracker* tracker) {
        std::lock_guard<std::mutex> l(_child_trackers_lock);
        tracker->_child_tracker_it = _child_trackers.insert(_child_trackers.end(), tracker);
    }

    /// Log consumption of all the trackers provided. Returns the sum of consumption in
    /// 'logged_consumption'.
    static std::string LogUsage(const std::string& prefix,
            const std::list<MemTracker*>& trackers, int64_t* logged_consumption);

    /// Lock to protect GcMemory(). This prevents many GCs from occurring at once.
    std::mutex _gc_lock;

    // Protects _request_to_mem_trackers and _pool_to_mem_trackers
    static std::mutex _s_mem_trackers_lock;

    // All per-request MemTracker objects that are in use.  For memory management, this map
    // contains only weak ptrs.  MemTrackers that are handed out via get_query_mem_tracker()
    // are shared ptrs.  When all the shared ptrs are no longer referenced, the MemTracker
    // d'tor will be called at which point the weak ptr will be removed from the map.
    typedef std::unordered_map<TUniqueId, std::weak_ptr<MemTracker> > RequestTrackersMap;
    static RequestTrackersMap _s_request_to_mem_trackers;

    // Only valid for MemTrackers returned from get_query_mem_tracker()
    /// Only valid for MemTrackers returned from CreateQueryMemTracker()
    TUniqueId _query_id;

    /// Only valid for MemTrackers returned from GetRequestPoolMemTracker()
    std::string _pool_name;

    int64_t _limit;  // in bytes
    //int64_t _consumption;  // in bytes

    std::string _label;
    MemTracker* _parent;

    /// in bytes; not owned
    RuntimeProfile::HighWaterMarkCounter* _consumption;

    /// holds _consumption counter if not tied to a profile
    RuntimeProfile::HighWaterMarkCounter _local_counter;

    /// If non-NULL, used to measure consumption (in bytes) rather than the values provided
    /// to Consume()/Release(). Only used for the process tracker, thus parent_ should be
    /// NULL if _consumption_metric is set.
    UIntGauge* _consumption_metric;

    /// If non-NULL, counters from a corresponding ReservationTracker that should be
    /// reported in logs and other diagnostics. Owned by this MemTracker. The counters
    /// are owned by the fragment's RuntimeProfile.
    AtomicPtr<ReservationTrackerCounters> _reservation_counters;

    std::vector<MemTracker*> _all_trackers;  // this tracker plus all of its ancestors
    std::vector<MemTracker*> _limit_trackers;  // _all_trackers with valid limits

    // All the child trackers of this tracker. Used for error reporting only.
    // i.e., Updating a parent tracker does not update the children.
    mutable std::mutex _child_trackers_lock;
    std::list<MemTracker*> _child_trackers;
    // Iterator into _parent->_child_trackers for this object. Stored to have O(1)
    // remove.
    std::list<MemTracker*>::iterator _child_tracker_it;

    /// Functions to call after the limit is reached to free memory.
    std::vector<GcFunction> _gc_functions;

    /// If false, this tracker (and its children) will not be included in LogUsage() output
    /// if consumption is 0.
    bool _log_usage_if_zero;

    /// The number of times the GcFunctions were called.
    IntCounter* _num_gcs_metric;

    /// The number of bytes freed by the last round of calling the GcFunctions (-1 before any
    /// GCs are performed).
    IntGauge* _bytes_freed_by_last_gc_metric;

    /// The number of bytes over the limit we were the last time LimitExceeded() was called
    /// and the limit was exceeded pre-GC. -1 if there is no limit or the limit was never
    /// exceeded.
    IntGauge* _bytes_over_limit_metric;

    /// Metric for limit_.
    IntGauge* _limit_metric;

    // If true, calls unregister_from_parent() in the dtor. This is only used for
    // the query wide trackers to remove it from the process mem tracker. The
    // process tracker never gets deleted so it is safe to reference it in the dtor.
    // The query tracker has lifetime shared by multiple plan fragments so it's hard
    // to do cleanup another way.
    bool _auto_unregister;
};

/// Global registry for query and pool MemTrackers. Owned by ExecEnv.
class PoolMemTrackerRegistry {
 public:
  /// Returns a MemTracker object for request pool 'pool_name'. Calling this with the same
  /// 'pool_name' will return the same MemTracker object. This is used to track the local
  /// memory usage of all requests executing in this pool. If 'create_if_not_present' is
  /// true, the first time this is called for a pool, a new MemTracker object is created
  /// with the process tracker as its parent. There is no explicit per-pool byte_limit
  /// set at any particular impalad, so newly created trackers will always have a limit
  /// of -1.
  MemTracker* GetRequestPoolMemTracker(
      const std::string& pool_name, bool create_if_not_present);

 private:
  /// All per-request pool MemTracker objects. It is assumed that request pools will live
  /// for the entire duration of the process lifetime so MemTrackers are never removed
  /// from this map. Protected by '_pool_to_mem_trackers_lock'
  typedef std::unordered_map<std::string, std::unique_ptr<MemTracker>> PoolTrackersMap;
  PoolTrackersMap _pool_to_mem_trackers;
  /// IMPALA-3068: Use SpinLock instead of std::mutex so that the lock won't
  /// automatically destroy itself as part of process teardown, which could cause races.
  SpinLock _pool_to_mem_trackers_lock;
};

}

#endif
