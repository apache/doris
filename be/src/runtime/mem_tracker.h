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
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace doris {

/// Mode argument passed to various MemTracker methods to indicate whether a soft or hard
/// limit should be used.
enum class MemLimit { HARD, SOFT };

/// The Level use to decide whether to show it in web page
/// each MemTracker have a Level equals to parent, only be set explicit
enum class MemTrackerLevel { OVERVIEW = 0, TASK, VERBOSE };

class ObjectPool;
class MemTracker;
struct ReservationTrackerCounters;
class RuntimeState;
class TQueryOptions;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// A MemTracker has a hard and a soft limit derived from the limit. If the hard limit
/// is exceeded, all memory allocations and queries should fail until we are under the
/// limit again. The soft limit can be exceeded without causing query failures, but
/// consumers of memory that can tolerate running without more memory should not allocate
/// memory in excess of the soft limit.
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
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.
class MemTracker : public std::enable_shared_from_this<MemTracker> {
public:
    // Creates and adds the tracker to the tree so that it can be retrieved with
    // FindTracker/FindOrCreateTracker.
    static std::shared_ptr<MemTracker> CreateTracker(
            int64_t byte_limit = -1, const std::string& label = std::string(),
            std::shared_ptr<MemTracker> parent = std::shared_ptr<MemTracker>(),
            bool log_usage_if_zero = true, bool reset_label_name = true,
            MemTrackerLevel level = MemTrackerLevel::VERBOSE);

    static std::shared_ptr<MemTracker> CreateTracker(
            RuntimeProfile* profile, int64_t byte_limit, const std::string& label = std::string(),
            const std::shared_ptr<MemTracker>& parent = std::shared_ptr<MemTracker>(),
            bool reset_label_name = true, MemTrackerLevel level = MemTrackerLevel::VERBOSE);

    // this is used for creating an orphan mem tracker, or for unit test.
    // If a mem tracker has parent, it should be created by `CreateTracker()`
    MemTracker(int64_t byte_limit = -1, const std::string& label = std::string());

    ~MemTracker();

    // Returns a list of all the valid trackers.
    static void ListTrackers(std::vector<std::shared_ptr<MemTracker>>* trackers);

    /// Include counters from a ReservationTracker in logs and other diagnostics.
    /// The counters should be owned by the fragment's RuntimeProfile.
    void EnableReservationReporting(const ReservationTrackerCounters& counters);

    // Gets a shared_ptr to the "root" tracker, creating it if necessary.
    static std::shared_ptr<MemTracker> GetRootTracker();

    // delete static CreateQueryMemTracker(), cuz it cannot use shared tracker

    /// Increases consumption of this tracker and its ancestors by 'bytes'.
    void Consume(int64_t bytes) {
        // DCHECK_GE(bytes, 0);
        if (bytes < 0) {
            Release(-bytes);
            return;
        }
        if (bytes == 0) {
            return;
        }

        if (UNLIKELY(consumption_metric_ != nullptr)) {
            RefreshConsumptionFromMetric();
            return; // TODO(yingchun): why return not update tracker?
        }
        for (auto& tracker : all_trackers_) {
            tracker->consumption_->add(bytes);
            if (LIKELY(tracker->consumption_metric_ == nullptr)) {
                DCHECK_GE(tracker->consumption_->current_value(), 0);
            }
        }
    }

    /// Increases the consumption of this tracker and the ancestors up to (but
    /// not including) end_tracker. This is useful if we want to move tracking between
    /// trackers that share a common (i.e. end_tracker) ancestor. This happens when we want
    /// to update tracking on a particular mem tracker but the consumption against
    /// the limit recorded in one of its ancestors already happened.
    void ConsumeLocal(int64_t bytes, MemTracker* end_tracker) {
        DCHECK_GE(bytes, 0);
        if (UNLIKELY(bytes < 0)) return; // needed in RELEASE, hits DCHECK in DEBUG
        ChangeConsumption(bytes, end_tracker);
    }

    /// Same as above, but it decreases the consumption.
    void ReleaseLocal(int64_t bytes, MemTracker* end_tracker) {
        DCHECK_GE(bytes, 0);
        if (UNLIKELY(bytes < 0)) return; // needed in RELEASE, hits DCHECK in DEBUG
        ChangeConsumption(-bytes, end_tracker);
    }

    /// Increases consumption of this tracker and its ancestors by 'bytes' only if
    /// they can all consume 'bytes' without exceeding limit (hard or soft) specified
    /// by 'mode'. If any limit would be exceed, no MemTrackers are updated. If the
    /// caller can tolerate an allocation failing, it should set mode=SOFT so that
    /// other callers that may not tolerate allocation failures have a better chance
    /// of success. Returns true if the consumption was successfully updated.
    WARN_UNUSED_RESULT
    Status TryConsume(int64_t bytes, MemLimit mode = MemLimit::HARD) {
        // DCHECK_GE(bytes, 0);
        if (bytes <= 0) {
            Release(-bytes);
            return Status::OK();
        }
        if (MemInfo::current_mem() + bytes >= MemInfo::mem_limit()) {
            return Status::MemoryLimitExceeded(fmt::format(
                    "{}: TryConsume failed, bytes={} process whole consumption={}  mem limit={}",
                    label_, bytes, MemInfo::current_mem(), MemInfo::mem_limit()));
        }
        // if (UNLIKELY(bytes == 0)) return true;
        // if (UNLIKELY(bytes < 0)) return false; // needed in RELEASE, hits DCHECK in DEBUG
        if (UNLIKELY(consumption_metric_ != nullptr)) RefreshConsumptionFromMetric();
        int i;
        // Walk the tracker tree top-down.
        for (i = all_trackers_.size() - 1; i >= 0; --i) {
            MemTracker* tracker = all_trackers_[i];
            const int64_t limit = tracker->GetLimit(mode);
            if (limit < 0) {
                tracker->consumption_->add(bytes); // No limit at this tracker.
            } else {
                // If TryConsume fails, we can try to GC, but we may need to try several times if
                // there are concurrent consumers because we don't take a lock before trying to
                // update consumption_.
                while (true) {
                    if (LIKELY(tracker->consumption_->try_add(bytes, limit))) break;

                    if (UNLIKELY(tracker->GcMemory(limit - bytes))) {
                        DCHECK_GE(i, 0);
                        // Failed for this mem tracker. Roll back the ones that succeeded.
                        for (int j = all_trackers_.size() - 1; j > i; --j) {
                            all_trackers_[j]->consumption_->add(-bytes);
                        }
                        return Status::MemoryLimitExceeded(fmt::format(
                                "{}: TryConsume failed, bytes={} consumption={}  imit={} "
                                "attempting to GC",
                                tracker->label(), bytes, tracker->consumption_->current_value(),
                                limit));
                    }
                    VLOG_NOTICE << "GC succeeded, TryConsume bytes=" << bytes
                                << " consumption=" << tracker->consumption_->current_value()
                                << " limit=" << limit;
                }
            }
        }
        // Everyone succeeded, return.
        DCHECK_EQ(i, -1);
        return Status::OK();
    }

    /// Decreases consumption of this tracker and its ancestors by 'bytes'.
    void Release(int64_t bytes) {
        // DCHECK_GE(bytes, 0);
        if (bytes < 0) {
            Consume(-bytes);
            return;
        }

        if (bytes == 0) {
            return;
        }

        // if (UNLIKELY(bytes <= 0)) return; // < 0 needed in RELEASE, hits DCHECK in DEBUG

        if (UNLIKELY(consumption_metric_ != nullptr)) {
            RefreshConsumptionFromMetric();
            return;
        }
        for (auto& tracker : all_trackers_) {
            tracker->consumption_->add(-bytes);
            /// If a UDF calls FunctionContext::TrackAllocation() but allocates less than the
            /// reported amount, the subsequent call to FunctionContext::Free() may cause the
            /// process mem tracker to go negative until it is synced back to the tcmalloc
            /// metric. Don't blow up in this case. (Note that this doesn't affect non-process
            /// trackers since we can enforce that the reported memory usage is internally
            /// consistent.)
            if (LIKELY(tracker->consumption_metric_ == nullptr)) {
                DCHECK_GE(tracker->consumption_->current_value(), 0)
                        << std::endl
                        << tracker->LogUsage(UNLIMITED_DEPTH);
            }
        }
    }

    /// Transfer 'bytes' of consumption from this tracker to 'dst', updating
    /// all ancestors up to the first shared ancestor. Must not be used if
    /// 'dst' has a limit, or an ancestor with a limit, that is not a common
    /// ancestor with the tracker, because this does not check memory limits.
    void TransferTo(MemTracker* dst, int64_t bytes);

    /// Returns true if a valid limit of this tracker or one of its ancestors is
    /// exceeded.
    bool AnyLimitExceeded(MemLimit mode) {
        for (const auto& tracker : limit_trackers_) {
            if (tracker->LimitExceeded(mode)) {
                return true;
            }
        }
        return false;
    }

    /// If this tracker has a limit, checks the limit and attempts to free up some memory if
    /// the hard limit is exceeded by calling any added GC functions. Returns true if the
    /// limit is exceeded after calling the GC functions. Returns false if there is no limit
    /// or consumption is under the limit.
    bool LimitExceeded(MemLimit mode) {
        if (UNLIKELY(CheckLimitExceeded(mode))) return LimitExceededSlow(mode);
        return false;
    }

    // Return limit exceeded tracker or null
    MemTracker* find_limit_exceeded_tracker() {
        for (const auto& tracker : limit_trackers_) {
            if (tracker->limit_exceeded()) {
                return tracker;
            }
        }
        return nullptr;
    }

    /// Returns the maximum consumption that can be made without exceeding the limit on
    /// this tracker or any of its parents. Returns int64_t::max() if there are no
    /// limits and a negative value if any limit is already exceeded.
    int64_t SpareCapacity(MemLimit mode) const;

    /// Refresh the memory consumption value from the consumption metric. Only valid to
    /// call if this tracker has a consumption metric.
    void RefreshConsumptionFromMetric();

    // TODO(yingchun): following functions are old style which have no MemLimit parameter
    bool limit_exceeded() const { return limit_ >= 0 && limit_ < consumption(); }

    int64_t limit() const { return limit_; }
    bool has_limit() const { return limit_ >= 0; }

    int64_t soft_limit() const { return soft_limit_; }
    int64_t GetLimit(MemLimit mode) const {
        if (mode == MemLimit::SOFT) return soft_limit();
        DCHECK_ENUM_EQ(mode, MemLimit::HARD);
        return limit();
    }
    const std::string& label() const { return label_; }

    /// Returns the lowest limit for this tracker and its ancestors. Returns
    /// -1 if there is no limit.
    int64_t GetLowestLimit(MemLimit mode) const;

    /// Returns the memory 'reserved' by this resource pool mem tracker, which is the sum
    /// of the memory reserved by the queries in it (i.e. its child trackers). The mem
    /// reserved for a query that is currently executing is its limit_, if set (which
    /// should be the common case with admission control). Otherwise, if the query has
    /// no limit or the query is finished executing, the current consumption is used.
    int64_t GetPoolMemReserved();

    /// Returns the memory consumed in bytes.
    int64_t consumption() const { return consumption_->current_value(); }

    /// Note that if consumption_ is based on consumption_metric_, this will the max value
    /// we've recorded in consumption(), not necessarily the highest value
    /// consumption_metric_ has ever reached.
    int64_t peak_consumption() const { return consumption_->value(); }

    std::shared_ptr<MemTracker> parent() const { return parent_; }

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
    // TODO(yingchun): remove comments
    //void RegisterMetrics(MetricGroup* metrics, const std::string& prefix);

    /// Logs the usage of this tracker and optionally its children (recursively).
    /// If 'logged_consumption' is non-nullptr, sets the consumption value logged.
    /// 'max_recursive_depth' specifies the maximum number of levels of children
    /// to include in the dump. If it is zero, then no children are dumped.
    /// Limiting the recursive depth reduces the cost of dumping, particularly
    /// for the process MemTracker.
    /// TODO: once all memory is accounted in ReservationTracker hierarchy, move
    /// reporting there.
    std::string LogUsage(int max_recursive_depth, const std::string& prefix = "",
                         int64_t* logged_consumption = nullptr);
    /// Dumping the process MemTracker is expensive. Limiting the recursive depth
    /// to two levels limits the level of detail to a one-line summary for each query
    /// MemTracker, avoiding all MemTrackers below that level. This provides a summary
    /// of process usage with substantially lower cost than the full dump.
    static const int PROCESS_MEMTRACKER_LIMITED_DEPTH = 2;
    /// Unlimited dumping is useful for query memtrackers or error conditions that
    /// are not performance sensitive
    static const int UNLIMITED_DEPTH = INT_MAX;

    /// Logs the usage of 'limit' number of queries based on maximum total memory
    /// consumption.
    std::string LogTopNQueries(int limit);

    /// Log the memory usage when memory limit is exceeded and return a status object with
    /// details of the allocation which caused the limit to be exceeded.
    /// If 'failed_allocation_size' is greater than zero, logs the allocation size. If
    /// 'failed_allocation_size' is zero, nothing about the allocation size is logged.
    /// If 'state' is non-nullptr, logs the error to 'state'.
    Status MemLimitExceeded(RuntimeState* state, const std::string& details,
                            int64_t failed_allocation = 0) WARN_UNUSED_RESULT {
        return MemLimitExceeded(this, state, details, failed_allocation);
    }

    /// Makes MemLimitExceeded callable for nullptr MemTrackers.
    static Status MemLimitExceeded(MemTracker* mtracker, RuntimeState* state,
                                   const std::string& details,
                                   int64_t failed_allocation = 0) WARN_UNUSED_RESULT;

    static void update_limits(int64_t bytes,
                              const std::vector<std::shared_ptr<MemTracker>>& trackers) {
        for (auto& tracker : trackers) {
            tracker->Consume(bytes);
        }
    }

    static bool limit_exceeded(const std::vector<std::shared_ptr<MemTracker>>& trackers) {
        for (const auto& tracker : trackers) {
            if (tracker->limit_exceeded()) {
                // TODO: remove logging
                LOG(WARNING) << "exceeded limit: limit=" << tracker->limit()
                             << " consumption=" << tracker->consumption();
                return true;
            }
        }

        return false;
    }

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << limit_ << "; "
            << "consumption: " << consumption_->current_value() << "; "
            << "label: " << label_ << "; "
            << "all tracker size: " << all_trackers_.size() << "; "
            << "limit trackers size: " << limit_trackers_.size() << "; "
            << "parent is null: " << ((parent_ == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    bool is_consumption_metric_null() const { return consumption_metric_ == nullptr; }

    static const std::string COUNTER_NAME;

private:
    /// 'byte_limit' < 0 means no limit
    /// 'label' is the label used in the usage string (LogUsage())
    /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be
    /// included in LogUsage() output if consumption is 0.
    MemTracker(RuntimeProfile* profile, int64_t byte_limit, const std::string& label,
               const std::shared_ptr<MemTracker>& parent, bool log_usage_if_zero, MemTrackerLevel);

private:
    friend class PoolMemTrackerRegistry;

    // TODO(HW): remove later
    /// Closes this MemTracker. After closing it is invalid to consume memory on this
    /// tracker and the tracker's consumption counter (which may be owned by a
    /// RuntimeProfile, not this MemTracker) can be safely destroyed. MemTrackers without
    /// consumption metrics in the context of a daemon must always be closed.
    /// Idempotent: calling multiple times has no effect.
    void Close();

    /// Returns true if the current memory tracker's limit is exceeded.
    bool CheckLimitExceeded(MemLimit mode) const {
        int64_t limit = GetLimit(mode);
        return limit >= 0 && limit < consumption();
    }

    /// Slow path for LimitExceeded().
    bool LimitExceededSlow(MemLimit mode);

    /// If consumption is higher than max_consumption, attempts to free memory by calling
    /// any added GC functions.  Returns true if max_consumption is still exceeded. Takes
    /// gc_lock. Updates metrics if initialized.
    bool GcMemory(int64_t max_consumption);

    /// Walks the MemTracker hierarchy and populates all_trackers_ and
    /// limit_trackers_
    void Init();

    /// Adds tracker to child_trackers_
    void AddChildTracker(const std::shared_ptr<MemTracker>& tracker);

    /// Log consumption of all the trackers provided. Returns the sum of consumption in
    /// 'logged_consumption'. 'max_recursive_depth' specifies the maximum number of levels
    /// of children to include in the dump. If it is zero, then no children are dumped.
    static std::string LogUsage(int max_recursive_depth, const std::string& prefix,
                                const std::list<std::weak_ptr<MemTracker>>& trackers,
                                int64_t* logged_consumption);

    /// Helper function for LogTopNQueries that iterates through the MemTracker hierarchy
    /// and populates 'min_pq' with 'limit' number of elements (that contain state related
    /// to query MemTrackers) based on maximum total memory consumption.
    void GetTopNQueries(std::priority_queue<std::pair<int64_t, std::string>,
                                            std::vector<std::pair<int64_t, std::string>>,
                                            std::greater<std::pair<int64_t, std::string>>>& min_pq,
                        int limit);

    /// If an ancestor of this tracker is a query MemTracker, return that tracker.
    /// Otherwise return nullptr.
    MemTracker* GetQueryMemTracker();

    /// Increases/Decreases the consumption of this tracker and the ancestors up to (but
    /// not including) end_tracker.
    void ChangeConsumption(int64_t bytes, MemTracker* end_tracker) {
        DCHECK(consumption_metric_ == nullptr) << "Should not be called on root.";
        for (MemTracker* tracker : all_trackers_) {
            if (tracker == end_tracker) return;
            DCHECK(!tracker->has_limit()) << tracker->label() << " have limit:" << tracker->limit();
            tracker->consumption_->add(bytes);
        }
        DCHECK(false) << "end_tracker is not an ancestor";
    }

    // Creates the root tracker.
    static void CreateRootTracker();

    /// Lock to protect GcMemory(). This prevents many GCs from occurring at once.
    std::mutex gc_lock_;

    /// Only used if 'is_query_mem_tracker_' is true.
    /// 0 if the query is still executing or 1 if it has finished executing. Before
    /// it has finished executing, the tracker limit is treated as "reserved memory"
    /// for the purpose of admission control - see GetPoolMemReserved().
    std::atomic<int32_t> query_exec_finished_ {0};

    /// Only valid for MemTrackers returned from GetRequestPoolMemTracker()
    std::string pool_name_;

    /// Hard limit on memory consumption, in bytes. May not be exceeded. If limit_ == -1,
    /// there is no consumption limit.
    const int64_t limit_;

    /// Soft limit on memory consumption, in bytes. Can be exceeded but callers to
    /// TryConsume() can opt not to exceed this limit. If -1, there is no consumption limit.
    const int64_t soft_limit_;

    std::string label_;

    /// The parent of this tracker. The pointer is never modified, even after this tracker
    /// is unregistered.
    std::shared_ptr<MemTracker> parent_;

    /// in bytes
    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> consumption_;

    /// If non-nullptr, used to measure consumption (in bytes) rather than the values provided
    /// to Consume()/Release(). Only used for the process tracker, thus parent_ should be
    /// nullptr if consumption_metric_ is set.
    IntGauge* consumption_metric_;

    /// If non-nullptr, counters from a corresponding ReservationTracker that should be
    /// reported in logs and other diagnostics. Owned by this MemTracker. The counters
    /// are owned by the fragment's RuntimeProfile.
    AtomicPtr<ReservationTrackerCounters> reservation_counters_;

    std::vector<MemTracker*> all_trackers_;   // this tracker plus all of its ancestors
    std::vector<MemTracker*> limit_trackers_; // all_trackers_ with valid limits

    // All the child trackers of this tracker. Used for error reporting and
    // listing only (i.e. updating the consumption of a parent tracker does not
    // update that of its children).
    SpinLock child_trackers_lock_;
    std::list<std::weak_ptr<MemTracker>> child_trackers_;

    /// Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
    /// remove.
    std::list<std::weak_ptr<MemTracker>>::iterator child_tracker_it_;

    /// Functions to call after the limit is reached to free memory.
    std::vector<GcFunction> gc_functions_;

    /// If false, this tracker (and its children) will not be included in LogUsage() output
    /// if consumption is 0.
    bool log_usage_if_zero_;

    MemTrackerLevel _level;

    /// The number of times the GcFunctions were called.
    IntCounter* num_gcs_metric_;

    /// The number of bytes freed by the last round of calling the GcFunctions (-1 before any
    /// GCs are performed).
    IntGauge* bytes_freed_by_last_gc_metric_;

    /// The number of bytes over the limit we were the last time LimitExceeded() was called
    /// and the limit was exceeded pre-GC. -1 if there is no limit or the limit was never
    /// exceeded.
    IntGauge* bytes_over_limit_metric_;

    /// Metric for limit_.
    IntGauge* limit_metric_;
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
    /// TODO(cmy): this function is not used for now. the memtracker returned from here is
    ///            got from a shared_ptr in `pool_to_mem_trackers_`.
    ///            This funtion is from
    ///            https://github.com/cloudera/Impala/blob/495397101e5807c701df71ea288f4815d69c2c8a/be/src/runtime/mem-tracker.h#L497
    ///            And in impala this function will return a raw pointer.
    std::shared_ptr<MemTracker> GetRequestPoolMemTracker(const std::string& pool_name,
                                                         bool create_if_not_present);

private:
    /// All per-request pool MemTracker objects. It is assumed that request pools will live
    /// for the entire duration of the process lifetime so MemTrackers are never removed
    /// from this map. Protected by '_pool_to_mem_trackers_lock'
    typedef std::unordered_map<std::string, std::shared_ptr<MemTracker>> PoolTrackersMap;
    PoolTrackersMap pool_to_mem_trackers_;
    /// IMPALA-3068: Use SpinLock instead of std::mutex so that the lock won't
    /// automatically destroy itself as part of process teardown, which could cause races.
    SpinLock pool_to_mem_trackers_lock_;
};

} // namespace doris
