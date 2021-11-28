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

#include "runtime/mem_tracker.h"

#include <cstdint>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>

#include <limits>
#include <memory>

#include "exec/exec_node.h"
#include "gutil/once.h"
#include "gutil/strings/substitute.h"
#include "runtime/bufferpool/reservation_tracker_counters.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

using boost::join;
using std::deque;
using std::endl;
using std::greater;
using std::list;
using std::pair;
using std::priority_queue;
using std::shared_ptr;
using std::string;

using std::vector;
using std::weak_ptr;
using strings::Substitute;

namespace doris {

const std::string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

// Name for query MemTrackers. '$0' is replaced with the query id.
const std::string QUERY_MEM_TRACKER_LABEL_FORMAT = "queryId=$0";

/// Calculate the soft limit for a MemTracker based on the hard limit 'limit'.
static int64_t CalcSoftLimit(int64_t limit) {
    if (limit < 0) {
        return -1;
    }
    if (MemInfo::initialized() && limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(limit, TUnit::BYTES)
                     << " exceeds physical memory of "
                     << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES);
        limit = MemInfo::physical_mem();
    }
    double frac = std::max(0.0, std::min(1.0, config::soft_mem_limit_frac));
    return static_cast<int64_t>(limit * frac);
}

// The ancestor for all trackers. Every tracker is visible from the root down.
static std::shared_ptr<MemTracker> root_tracker;
static GoogleOnceType root_tracker_once = GOOGLE_ONCE_INIT;

void MemTracker::CreateRootTracker() {
    root_tracker.reset(new MemTracker(nullptr, -1, "Root", nullptr, true, MemTrackerLevel::OVERVIEW));
    root_tracker->Init();
}

// An independent tracker, no parent and child,
// used in tcmalloc new/delete hook to count the real memory of the process
static std::shared_ptr<MemTracker> global_hook_mem_tracker;
static GoogleOnceType global_hook_mem_tracker_once = GOOGLE_ONCE_INIT;

void MemTracker::CreateGlobalHookTracker() {
    global_hook_mem_tracker.reset(new MemTracker(nullptr, -1, "Global Hook", nullptr, true, MemTrackerLevel::OVERVIEW));
    global_hook_mem_tracker->Init();
}

std::shared_ptr<MemTracker> MemTracker::CreateTracker(RuntimeProfile* profile, int64_t byte_limit,
                                                      const std::string& label, const std::shared_ptr<MemTracker>& parent,
                                                      bool reset_label_name, MemTrackerLevel level,
                                                      const std::string& query_id) {
    std::shared_ptr<MemTracker> real_parent;
    std::string label_name;
    // if parent is not null, reset label name to query id.
    // The parent label always: RuntimeState:instance:8ca5a59e3aa84f74-84bb0d0466193736
    // we just need the last id of it: 8ca5a59e3aa84f74-84bb0d0466193736
    // to build the new label name of tracker: `label`: 8ca5a59e3aa84f74-84bb0d0466193736
    // else if parent is null
    //  just use the root is parent and keep the label_name as label
    if (parent) {
        real_parent = parent;
        if (reset_label_name) {
            std::vector<string> tmp_result;
            boost::split(tmp_result, parent->label(), boost::is_any_of(":"));
            label_name = label + ":" + tmp_result[tmp_result.size() - 1];
        } else {
            label_name = label;
        }
    } else {
        real_parent = GetRootTracker();
        label_name = label;
    }

    shared_ptr<MemTracker> tracker(new MemTracker(profile, byte_limit, label_name, real_parent, true,
            level > real_parent->_level ? level : real_parent->_level));
    real_parent->AddChildTracker(tracker);
    tracker->Init();
    tracker->set_query_id(query_id);

    return tracker;
}

std::shared_ptr<MemTracker> MemTracker::CreateTracker(int64_t byte_limit, const std::string& label,
        std::shared_ptr<MemTracker> parent, bool log_usage_if_zero, bool reset_label_name,
        MemTrackerLevel level, const std::string& query_id) {
    std::shared_ptr<MemTracker> real_parent;
    std::string label_name;
    // if parent is not null, reset label name to query id.
    // The parent label always: RuntimeState:instance:8ca5a59e3aa84f74-84bb0d0466193736
    // we just need the last id of it: 8ca5a59e3aa84f74-84bb0d0466193736
    // to build the new label name of tracker: `label`: 8ca5a59e3aa84f74-84bb0d0466193736
    // else if parent is null
    //  just use the root is parent and keep the label_name as label
    if (parent) {
        real_parent = parent;
        if (reset_label_name) {
            std::vector<string> tmp_result;
            boost::split(tmp_result, parent->label(), boost::is_any_of(":"));
            label_name = label + ":" + tmp_result[tmp_result.size() - 1];
        } else {
            label_name = label;
        }
    } else {
        real_parent = GetRootTracker();
        label_name = label;
    }

    shared_ptr<MemTracker> tracker(
            new MemTracker(nullptr, byte_limit, label_name, real_parent, log_usage_if_zero,
                    level > real_parent->_level ? level : real_parent->_level));
    real_parent->AddChildTracker(tracker);
    tracker->Init();
    tracker->set_query_id(query_id);

    return tracker;
}

MemTracker::MemTracker(int64_t byte_limit, const std::string& label)
        : MemTracker(nullptr, byte_limit, label, std::shared_ptr<MemTracker>(), true, MemTrackerLevel::VERBOSE) {}

MemTracker::MemTracker(RuntimeProfile* profile, int64_t byte_limit, const string& label,
                       const std::shared_ptr<MemTracker>& parent, bool log_usage_if_zero, MemTrackerLevel level)
        : limit_(byte_limit),
          soft_limit_(CalcSoftLimit(byte_limit)),
          label_(label),
          parent_(parent),
          consumption_metric_(nullptr),
          log_usage_if_zero_(log_usage_if_zero),
          _level(level),
          num_gcs_metric_(nullptr),
          bytes_freed_by_last_gc_metric_(nullptr),
          bytes_over_limit_metric_(nullptr),
          limit_metric_(nullptr) {
    if (profile == nullptr) {
        consumption_ = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        consumption_ = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
}

void MemTracker::Init() {
    DCHECK_GE(limit_, -1);
    DCHECK_LE(soft_limit_, limit_);
    // populate all_trackers_ and limit_trackers_
    MemTracker* tracker = this;
    while (tracker != nullptr) {
        all_trackers_.push_back(tracker);
        if (tracker->has_limit()) limit_trackers_.push_back(tracker);
        tracker = tracker->parent_.get();
    }
    DCHECK_GT(all_trackers_.size(), 0);
    DCHECK_EQ(all_trackers_[0], this);
}

void MemTracker::AddChildTracker(const std::shared_ptr<MemTracker>& tracker) {
    lock_guard<SpinLock> l(child_trackers_lock_);
    tracker->child_tracker_it_ = child_trackers_.insert(child_trackers_.end(), tracker);
}

void MemTracker::EnableReservationReporting(const ReservationTrackerCounters& counters) {
    delete reservation_counters_.swap(new ReservationTrackerCounters(counters));
}

int64_t MemTracker::GetLowestLimit(MemLimit mode) const {
    if (limit_trackers_.empty()) return -1;
    int64_t min_limit = numeric_limits<int64_t>::max();
    for (MemTracker* limit_tracker : limit_trackers_) {
        DCHECK(limit_tracker->has_limit());
        min_limit = std::min(min_limit, limit_tracker->GetLimit(mode));
    }
    return min_limit;
}

int64_t MemTracker::SpareCapacity(MemLimit mode) const {
    int64_t result = std::numeric_limits<int64_t>::max();
    for (const auto& tracker : limit_trackers_) {
        int64_t mem_left = tracker->GetLimit(mode) - tracker->consumption();
        result = std::min(result, mem_left);
    }
    return result;
}

void MemTracker::RefreshConsumptionFromMetric() {
    DCHECK(consumption_metric_ != nullptr);
    consumption_->set(consumption_metric_->value());
}

int64_t MemTracker::GetPoolMemReserved() {
    DCHECK_EQ(limit_, -1) << LogUsage(UNLIMITED_DEPTH);

    // Use cache to avoid holding child_trackers_lock_
    list<weak_ptr<MemTracker>> children;
    {
        lock_guard<SpinLock> l(child_trackers_lock_);
        children = child_trackers_;
    }

    int64_t mem_reserved = 0L;
    for (const auto& child_weak : children) {
        std::shared_ptr<MemTracker> child = child_weak.lock();
        if (child) {
            int64_t child_limit = child->limit();
            if (child_limit > 0) {
                // Make sure we don't overflow if the query limits are set to ridiculous values.
                mem_reserved += std::min(child_limit, MemInfo::physical_mem());
            } else {
                DCHECK(child_limit == -1)
                        << child->LogUsage(UNLIMITED_DEPTH);
                mem_reserved += child->consumption();
            }
        }
    }
    return mem_reserved;
}

std::shared_ptr<MemTracker> QueryMemTrackerRegistry::RegisterQueryMemTracker(
        const std::string& query_id, int64_t mem_limit) {
    DCHECK(!query_id.empty());
    VLOG(2) << "Register query memory tracker, query id: " << query_id
            << " limit: " << PrettyPrinter::print(mem_limit, TUnit::BYTES);

    // First time this query_id registered, make a new object, otherwise do nothing.
    // Combine CreateTracker and emplace into one operation to avoid the use of locks
    _query_mem_trackers.try_emplace_l(
            query_id, [](std::shared_ptr<MemTracker>) {},
            MemTracker::CreateTracker(mem_limit,
                                      strings::Substitute(QUERY_MEM_TRACKER_LABEL_FORMAT, query_id),
                                      ExecEnv::GetInstance()->all_query_mem_tracker(), false,
                                      false, MemTrackerLevel::OVERVIEW, query_id));
    std::shared_ptr<MemTracker> tracker = GetQueryMemTracker(query_id);
    if (tracker != nullptr) {
        tracker->set_exist_transfer_control();
    }
    return tracker;
}

std::shared_ptr<MemTracker> QueryMemTrackerRegistry::GetQueryMemTracker(const std::string& query_id) {
    DCHECK(!query_id.empty());
    std::shared_ptr<MemTracker> tracker = nullptr;
    // Avoid using locks to resolve erase conflicts
    _query_mem_trackers.if_contains(query_id,
                                    [&tracker](std::shared_ptr<MemTracker> v) { tracker = v; });
    return tracker;
}

void QueryMemTrackerRegistry::DeregisterQueryMemTracker() {
    std::vector<std::string> expired_querys;
    for (auto it = _query_mem_trackers.begin(); it != _query_mem_trackers.end(); it++) {
        // No RuntimeState uses this query MemTracker, it is only referenced by this map, delete it
        if (it->second.use_count() == 1) {
           expired_querys.emplace_back(it->first);
        }
    }
    for (auto qid: expired_querys) {
        DCHECK(_query_mem_trackers[qid].use_count() == 1);
        _query_mem_trackers.erase(qid);
        VLOG(2) << "Deregister query memory tracker, query id: " << qid;
    }
}

MemTracker::~MemTracker() {
    delete reservation_counters_.load();

    if (parent()) {
        if (!exist_transfer_control()) {
            DCHECK(consumption() == 0) << "Memory tracker " << debug_string()
                                       << " has unreleased consumption " << consumption();
        }
        parent_->Release(consumption());

        lock_guard<SpinLock> l(parent_->child_trackers_lock_);
        if (child_tracker_it_ != parent_->child_trackers_.end()) {
            parent_->child_trackers_.erase(child_tracker_it_);
            child_tracker_it_ = parent_->child_trackers_.end();
        }
    }
}

void MemTracker::ListTrackers(vector<shared_ptr<MemTracker>>* trackers) {
    trackers->clear();
    deque<shared_ptr<MemTracker>> to_process;
    to_process.push_front(GetRootTracker());
    to_process.push_front(GetGlobalHookTracker());
    while (!to_process.empty()) {
        shared_ptr<MemTracker> t = to_process.back();
        to_process.pop_back();

        trackers->push_back(t);
        list<weak_ptr<MemTracker>> children;
        {
            lock_guard<SpinLock> l(t->child_trackers_lock_);
            children = t->child_trackers_;
        }
        for (const auto& child_weak : children) {
            shared_ptr<MemTracker> child = child_weak.lock();
            if (child && static_cast<decltype(config::mem_tracker_level)>(child->_level) <= config::mem_tracker_level) {
                to_process.emplace_back(std::move(child));
            }
        }
    }
}

//void MemTracker::RegisterMetrics(MetricGroup* metrics, const string& prefix) {
//  num_gcs_metric_ = metrics->AddCounter(strings::Substitute("$0.num-gcs", prefix), 0);
//
//  // TODO: Consider a total amount of bytes freed counter
//  bytes_freed_by_last_gc_metric_ = metrics->AddGauge(
//      strings::Substitute("$0.bytes-freed-by-last-gc", prefix), -1);
//
//  bytes_over_limit_metric_ = metrics->AddGauge(
//      strings::Substitute("$0.bytes-over-limit", prefix), -1);
//
//  limit_metric_ = metrics->AddGauge(strings::Substitute("$0.limit", prefix), limit_);
//}

void MemTracker::TransferTo(MemTracker* dst, int64_t bytes) {
    DCHECK_EQ(all_trackers_.back(), dst->all_trackers_.back()) << "Must have same root";
    // Find the common ancestor and update trackers between 'this'/'dst' and
    // the common ancestor. This logic handles all cases, including the
    // two trackers being the same or being ancestors of each other because
    // 'all_trackers_' includes the current tracker.
    int ancestor_idx = all_trackers_.size() - 1;
    int dst_ancestor_idx = dst->all_trackers_.size() - 1;
    while (ancestor_idx > 0 && dst_ancestor_idx > 0 &&
           all_trackers_[ancestor_idx - 1] == dst->all_trackers_[dst_ancestor_idx - 1]) {
        --ancestor_idx;
        --dst_ancestor_idx;
    }
    MemTracker* common_ancestor = all_trackers_[ancestor_idx];
    ReleaseLocal(bytes, common_ancestor);
    dst->ConsumeLocal(bytes, common_ancestor);
}

// Calling this on the query tracker results in output like:
//
//  Query(4a4c81fedaed337d:4acadfda00000000) Limit=10.00 GB Total=508.28 MB Peak=508.45 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000000: Total=8.00 KB Peak=8.00 KB
//      EXCHANGE_NODE (id=4): Total=0 Peak=0
//      DataStreamRecvr: Total=0 Peak=0
//    Block Manager: Limit=6.68 GB Total=394.00 MB Peak=394.00 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000006: Total=233.72 MB Peak=242.24 MB
//      AGGREGATION_NODE (id=1): Total=139.21 MB Peak=139.84 MB
//      HDFS_SCAN_NODE (id=0): Total=93.94 MB Peak=102.24 MB
//      DataStreamSender (dst_id=2): Total=45.99 KB Peak=85.99 KB
//    Fragment 4a4c81fedaed337d:4acadfda00000003: Total=274.55 MB Peak=274.62 MB
//      AGGREGATION_NODE (id=3): Total=274.50 MB Peak=274.50 MB
//      EXCHANGE_NODE (id=2): Total=0 Peak=0
//      DataStreamRecvr: Total=45.91 KB Peak=684.07 KB
//      DataStreamSender (dst_id=4): Total=680.00 B Peak=680.00 B
//
// If 'reservation_metrics_' are set, we ge a more granular breakdown:
//   TrackerName: Limit=5.00 MB Reservation=5.00 MB OtherMemory=1.04 MB
//                Total=6.04 MB Peak=6.45 MB
//
std::string MemTracker::LogUsage(int max_recursive_depth, const string& prefix,
                                 int64_t* logged_consumption) {
    // Make sure the consumption is up to date.
    if (consumption_metric_ != nullptr) RefreshConsumptionFromMetric();
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = consumption_->value();
    if (logged_consumption != nullptr) *logged_consumption = curr_consumption;

    if (!log_usage_if_zero_ && curr_consumption == 0) return "";

    std::stringstream ss;
    ss << prefix << label_ << ":";
    if (CheckLimitExceeded(MemLimit::HARD)) ss << " memory limit exceeded.";
    if (limit_ > 0) ss << " Limit=" << PrettyPrinter::print(limit_, TUnit::BYTES);

    // TODO(zxy): ReservationTrackerCounters is not actually used in the current Doris. 
    // Printing here ReservationTrackerCounters may cause BE crash when high concurrency.
    // The memory tracker in Doris will be redesigned in the future.
    // ReservationTrackerCounters* reservation_counters = reservation_counters_.load();
    // if (reservation_counters != nullptr) {
    //     int64_t reservation = reservation_counters->peak_reservation->current_value();
    //     ss << " Reservation=" << PrettyPrinter::print(reservation, TUnit::BYTES);
    //     if (reservation_counters->reservation_limit != nullptr) {
    //         int64_t limit = reservation_counters->reservation_limit->value();
    //         ss << " ReservationLimit=" << PrettyPrinter::print(limit, TUnit::BYTES);
    //     }
    //     ss << " OtherMemory=" << PrettyPrinter::print(curr_consumption - reservation, TUnit::BYTES);
    // }
    ss << " Total=" << PrettyPrinter::print(curr_consumption, TUnit::BYTES);
    // Peak consumption is not accurate if the metric is lazily updated (i.e.
    // this is a non-root tracker that exists only for reporting purposes).
    // Only report peak consumption if we actually call Consume()/Release() on
    // this tracker or an descendent.
    if (consumption_metric_ == nullptr || parent_ == nullptr) {
        ss << " Peak=" << PrettyPrinter::print(peak_consumption, TUnit::BYTES);
    }

    // This call does not need the children, so return early.
    if (max_recursive_depth == 0) return ss.str();

    // Recurse and get information about the children
    std::string new_prefix = strings::Substitute("  $0", prefix);
    int64_t child_consumption;
    std::string child_trackers_usage;
    list<weak_ptr<MemTracker>> children;
    {
        lock_guard<SpinLock> l(child_trackers_lock_);
        children = child_trackers_;
    }
    child_trackers_usage =
            LogUsage(max_recursive_depth - 1, new_prefix, children, &child_consumption);
    if (!child_trackers_usage.empty()) ss << "\n" << child_trackers_usage;

    if (parent_ == nullptr) {
        // Log the difference between the metric value and children as "untracked" memory so
        // that the values always add up. This value is not always completely accurate because
        // we did not necessarily get a consistent snapshot of the consumption values for all
        // children at a single moment in time, but is good enough for our purposes.
        int64_t untracked_bytes = curr_consumption - child_consumption;
        ss << "\n"
           << new_prefix
           << "Untracked Memory: Total=" << PrettyPrinter::print(untracked_bytes, TUnit::BYTES);
    }
    return ss.str();
}

std::string MemTracker::LogUsage(int max_recursive_depth, const string& prefix,
                                 const list<weak_ptr<MemTracker>>& trackers,
                                 int64_t* logged_consumption) {
    *logged_consumption = 0;
    std::vector<string> usage_strings;
    for (const auto& tracker_weak : trackers) {
        shared_ptr<MemTracker> tracker = tracker_weak.lock();
        if (tracker) {
            int64_t tracker_consumption;
            std::string usage_string =
                    tracker->LogUsage(max_recursive_depth, prefix, &tracker_consumption);
            if (!usage_string.empty()) usage_strings.push_back(usage_string);
            *logged_consumption += tracker_consumption;
        }
    }
    return join(usage_strings, "\n");
}

std::string MemTracker::LogTopNQueries(int limit) {
    if (limit == 0) return "";
    priority_queue<pair<int64_t, string>, std::vector<pair<int64_t, string>>,
                   std::greater<pair<int64_t, string>>>
            min_pq;
    GetTopNQueries(min_pq, limit);
    std::vector<string> usage_strings(min_pq.size());
    while (!min_pq.empty()) {
        usage_strings.push_back(min_pq.top().second);
        min_pq.pop();
    }
    std::reverse(usage_strings.begin(), usage_strings.end());
    return join(usage_strings, "\n");
}

void MemTracker::GetTopNQueries(
        priority_queue<pair<int64_t, string>, std::vector<pair<int64_t, string>>,
                       greater<pair<int64_t, string>>>& min_pq,
        int limit) {
    list<weak_ptr<MemTracker>> children;
    {
        lock_guard<SpinLock> l(child_trackers_lock_);
        children = child_trackers_;
    }
    for (const auto& child_weak : children) {
        shared_ptr<MemTracker> child = child_weak.lock();
        if (child) {
            child->GetTopNQueries(min_pq, limit);
        }
    }
}

MemTracker* MemTracker::GetQueryMemTracker() {
    MemTracker* tracker = this;
    while (tracker != nullptr && !tracker->_is_query_mem_tracker) {
        tracker = tracker->parent_.get();
    }
    return tracker;
}

Status MemTracker::MemLimitExceeded(MemTracker* mtracker, RuntimeState* state,
                                    const std::string& details, int64_t failed_allocation_size) {
    DCHECK_GE(failed_allocation_size, 0);
    std::stringstream ss;
    if (!details.empty()) ss << details << std::endl;
    if (failed_allocation_size != 0) {
        if (mtracker != nullptr) ss << mtracker->label();
        ss << " could not allocate " << PrettyPrinter::print(failed_allocation_size, TUnit::BYTES)
           << " without exceeding limit." << std::endl;
    }
    ss << "Error occurred on backend " << BackendOptions::get_localhost();
    if (state != nullptr) ss << " by fragment " << print_id(state->fragment_instance_id());
    ss << std::endl;
    ExecEnv* exec_env = ExecEnv::GetInstance();
    MemTracker* process_tracker = exec_env->process_mem_tracker().get();
    const int64_t process_capacity = process_tracker->SpareCapacity(MemLimit::HARD);
    ss << "Memory left in process limit: " << PrettyPrinter::print(process_capacity, TUnit::BYTES)
       << std::endl;
    Status status = Status::MemoryLimitExceeded(ss.str());

    // only print the query tracker in be log(if available).
    MemTracker* query_tracker = nullptr;
    if (mtracker != nullptr) {
        query_tracker = mtracker->GetQueryMemTracker();
        if (query_tracker != nullptr) {
            if (query_tracker->has_limit()) {
                const int64_t query_capacity =
                        query_tracker->limit() - query_tracker->consumption();
                ss << "Memory left in query limit: "
                   << PrettyPrinter::print(query_capacity, TUnit::BYTES) << std::endl;
            }
            ss << query_tracker->LogUsage(UNLIMITED_DEPTH);
        }
    }

    // Log the process level if the process tracker is close to the limit or
    // if this tracker is not within a query's MemTracker hierarchy.
    if (process_capacity < failed_allocation_size || query_tracker == nullptr) {
        // IMPALA-5598: For performance reasons, limit the levels of recursion when
        // dumping the process tracker to only two layers.
        ss << process_tracker->LogUsage(PROCESS_MEMTRACKER_LIMITED_DEPTH);
    }
    if (state != nullptr) state->log_error(ss.str());
    LOG(WARNING) << ss.str();
    return status;
}

void MemTracker::AddGcFunction(GcFunction f) {
    gc_functions_.push_back(f);
}

bool MemTracker::LimitExceededSlow(MemLimit mode) {
    if (mode == MemLimit::HARD && bytes_over_limit_metric_ != nullptr) {
        bytes_over_limit_metric_->set_value(consumption() - limit_);
    }
    return GcMemory(GetLimit(mode));
}

bool MemTracker::GcMemory(int64_t max_consumption) {
    if (max_consumption < 0) return true;
    lock_guard<std::mutex> l(gc_lock_);
    if (consumption_metric_ != nullptr) RefreshConsumptionFromMetric();
    int64_t pre_gc_consumption = consumption();
    // Check if someone gc'd before us
    if (pre_gc_consumption < max_consumption) return false;
    if (num_gcs_metric_ != nullptr) num_gcs_metric_->increment(1);

    int64_t curr_consumption = pre_gc_consumption;
    // Try to free up some memory
    for (int i = 0; i < gc_functions_.size(); ++i) {
        // Try to free up the amount we are over plus some extra so that we don't have to
        // immediately GC again. Don't free all the memory since that can be unnecessarily
        // expensive.
        const int64_t EXTRA_BYTES_TO_FREE = 512L * 1024L * 1024L;
        int64_t bytes_to_free = curr_consumption - max_consumption + EXTRA_BYTES_TO_FREE;
        gc_functions_[i](bytes_to_free);
        if (consumption_metric_ != nullptr) RefreshConsumptionFromMetric();
        curr_consumption = consumption();
        if (max_consumption - curr_consumption <= EXTRA_BYTES_TO_FREE) break;
    }

    if (bytes_freed_by_last_gc_metric_ != nullptr) {
        bytes_freed_by_last_gc_metric_->set_value(pre_gc_consumption - curr_consumption);
    }
    return curr_consumption > max_consumption;
}

std::shared_ptr<MemTracker> MemTracker::GetRootTracker() {
    GoogleOnceInit(&root_tracker_once, &MemTracker::CreateRootTracker);
    return root_tracker;
}

std::shared_ptr<MemTracker> MemTracker::GetGlobalHookTracker() {
    GoogleOnceInit(&global_hook_mem_tracker_once, &MemTracker::CreateGlobalHookTracker);
    return global_hook_mem_tracker;
}

} // namespace doris
