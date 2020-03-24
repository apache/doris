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

#include <stdint.h>
#include <limits>
#include <memory>
//#include <boost/lexical_cast.hpp>
//#include <boost/shared_ptr.hpp>
//include <boost/weak_ptr.hpp>

#include "exec/exec_node.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"
#include "util/debug_util.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"
#include "util/stack_util.h"

//using std::shared_ptr;
//using std::weak_ptr;
//using std::lexical_cast;
#include <boost/algorithm/string/join.hpp>

#include "runtime/bufferpool/reservation_tracker_counters.h"

namespace doris {

const std::string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

// Name for request pool MemTrackers. '$0' is replaced with the pool name.
const std::string REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT = "RequestPool=$0";

MemTracker::MemTracker(
        int64_t byte_limit, const std::string& label, MemTracker* parent, bool log_usage_if_zero)
    : _limit(byte_limit),
    _label(label),
    _parent(parent),
    _consumption(&_local_counter),
    _local_counter(TUnit::BYTES),
    _consumption_metric(NULL),
    _log_usage_if_zero(log_usage_if_zero),
    _num_gcs_metric(NULL),
    _bytes_freed_by_last_gc_metric(NULL),
    _bytes_over_limit_metric(NULL),
    _limit_metric(NULL) {
        if (parent != NULL) _parent->add_child_tracker(this);
        Init();
    }

MemTracker::MemTracker(RuntimeProfile* profile, int64_t byte_limit,
        const std::string& label, MemTracker* parent)
    : _limit(byte_limit),
    _label(label),
    _parent(parent),
    _consumption(profile->AddHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES)),
    _local_counter(TUnit::BYTES),
    _consumption_metric(NULL),
    _log_usage_if_zero(true),
    _num_gcs_metric(NULL),
    _bytes_freed_by_last_gc_metric(NULL),
    _bytes_over_limit_metric(NULL),
    _limit_metric(NULL) {
        if (parent != NULL) _parent->add_child_tracker(this);
        Init();
    }

MemTracker::MemTracker(
        UIntGauge* consumption_metric, int64_t byte_limit, const std::string& label)
    : _limit(byte_limit),
    _label(label),
    _parent(NULL),
    _consumption(&_local_counter),
    _local_counter(TUnit::BYTES),
    _consumption_metric(consumption_metric),
    _log_usage_if_zero(true),
    _num_gcs_metric(NULL),
    _bytes_freed_by_last_gc_metric(NULL),
    _bytes_over_limit_metric(NULL),
    _limit_metric(NULL) {
        Init();
    }

void MemTracker::Init() {
    DCHECK_GE(_limit, -1);
    // populate _all_trackers and _limit_trackers
    MemTracker* tracker = this;
    while (tracker != NULL) {
        _all_trackers.push_back(tracker);
        if (tracker->has_limit()) _limit_trackers.push_back(tracker);
        tracker = tracker->_parent;
    }
    DCHECK_GT(_all_trackers.size(), 0);
    DCHECK_EQ(_all_trackers[0], this);
}

// TODO chenhao , set MemTracker close state
void MemTracker::close() {
}

void MemTracker::enable_reservation_reporting(const ReservationTrackerCounters& counters) {
    ReservationTrackerCounters* new_counters = new ReservationTrackerCounters(counters);
    _reservation_counters.store(new_counters);
}

int64_t MemTracker::GetPoolMemReserved() const {
    // Pool trackers should have a _pool_name and no limit.
    DCHECK(!_pool_name.empty());
    DCHECK_EQ(_limit, -1) << LogUsage("");

    int64_t mem_reserved = 0L;
    std::lock_guard<std::mutex> l(_child_trackers_lock);
    for (MemTracker* child : _child_trackers) {
        int64_t child_limit = child->limit();
        if (child_limit > 0) {
            // Make sure we don't overflow if the query limits are set to ridiculous values.
            mem_reserved += std::min(child_limit, MemInfo::physical_mem());
        } else {
            DCHECK_EQ(child_limit, -1) << child->LogUsage("");
            mem_reserved += child->consumption();
        }
    }
    return mem_reserved;
}

MemTracker* PoolMemTrackerRegistry::GetRequestPoolMemTracker(
        const std::string& pool_name, bool create_if_not_present) {
    DCHECK(!pool_name.empty());
    std::lock_guard<SpinLock> l(_pool_to_mem_trackers_lock);
    PoolTrackersMap::iterator it = _pool_to_mem_trackers.find(pool_name);
    if (it != _pool_to_mem_trackers.end()) {
        MemTracker* tracker = it->second.get();
        DCHECK(pool_name == tracker->_pool_name);
        return tracker;
    }
    if (!create_if_not_present) return nullptr;
    // First time this pool_name registered, make a new object.
    MemTracker* tracker =
        new MemTracker(-1, strings::Substitute(REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT, pool_name),
                ExecEnv::GetInstance()->process_mem_tracker());
    tracker->_pool_name = pool_name;
    _pool_to_mem_trackers.emplace(pool_name, std::unique_ptr<MemTracker>(tracker));
    return tracker;
}

MemTracker* MemTracker::CreateQueryMemTracker(const TUniqueId& id,
        const TQueryOptions& query_options, const std::string& pool_name, ObjectPool* obj_pool) {
    int64_t byte_limit = -1;
    if (query_options.__isset.mem_limit && query_options.mem_limit > 0) {
        byte_limit = query_options.mem_limit;
    }
    if (byte_limit != -1) {
        if (byte_limit > MemInfo::physical_mem()) {
            LOG(WARNING) << "Memory limit " << PrettyPrinter::print(byte_limit, TUnit::BYTES)
                << " exceeds physical memory of "
                << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES);
        }
        VLOG_QUERY << "Using query memory limit: "
            << PrettyPrinter::print(byte_limit, TUnit::BYTES);
    }

    MemTracker* pool_tracker =
        ExecEnv::GetInstance()->pool_mem_trackers()->GetRequestPoolMemTracker(
                pool_name, true);
    return pool_tracker;
}

MemTracker::~MemTracker() {
    int64_t remaining_bytes = consumption();
    // work around some scenario where consume() is not paired with release()
    // e.g., in the initialization of hll and bitmap aggregator (see aggregate_func.h)
    // TODO(gaodayue) should be replaced with `DCHECK_EQ(consumption(), 0);` when
    // we fixed thoses invalid usages
    if (remaining_bytes > 0) {
        for (auto tracker : _all_trackers) {
            tracker->_consumption->add(-remaining_bytes);
        }
    }
    delete _reservation_counters.load();
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
//   TrackerName: Limit=5.00 MB BufferPoolUsed/Reservation=0/5.00 MB OtherMemory=1.04 MB
//                Total=6.04 MB Peak=6.45 MB
//
std::string MemTracker::LogUsage(const std::string& prefix, int64_t* logged_consumption) const {
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    if (logged_consumption != nullptr) *logged_consumption = curr_consumption;

    if (!_log_usage_if_zero && curr_consumption == 0) return "";

    std::stringstream ss;
    ss << prefix << _label << ":";
    //if (CheckLimitExceeded()) ss << " memory limit exceeded.";
    if (limit_exceeded()) ss << " memory limit exceeded.";
    if (_limit > 0) ss << " Limit=" << PrettyPrinter::print(_limit, TUnit::BYTES);

    ReservationTrackerCounters* reservation_counters = _reservation_counters.load();
    if (reservation_counters != nullptr) {
        int64_t reservation = reservation_counters->peak_reservation->current_value();
        int64_t used_reservation =
            reservation_counters->peak_used_reservation->current_value();
        int64_t reservation_limit = 0;
        //TODO chenhao, reservation_limit is null when ReservationTracker 
        // does't have reservation limit
        if (reservation_counters->reservation_limit != nullptr) {
             reservation_limit = reservation_counters->reservation_limit->value();
        }
        ss << " BufferPoolUsed/Reservation="
            << PrettyPrinter::print(used_reservation, TUnit::BYTES) << "/"
            << PrettyPrinter::print(reservation, TUnit::BYTES);
        if (reservation_limit != std::numeric_limits<int64_t>::max()) {
            ss << " BufferPoolLimit=" << PrettyPrinter::print(reservation_limit, TUnit::BYTES);
        }
        ss << " OtherMemory="
            << PrettyPrinter::print(curr_consumption - reservation, TUnit::BYTES);
    }
    ss << " Total=" << PrettyPrinter::print(curr_consumption, TUnit::BYTES)
        << " Peak=" << PrettyPrinter::print(peak_consumption, TUnit::BYTES);

    std::string new_prefix = strings::Substitute("  $0", prefix);
    int64_t child_consumption;
    std::string child_trackers_usage;
    {
        std::lock_guard<std::mutex> l(_child_trackers_lock);
        child_trackers_usage = LogUsage(new_prefix, _child_trackers, &child_consumption);
    }
    if (!child_trackers_usage.empty()) ss << "\n" << child_trackers_usage;

    if (_consumption_metric != nullptr) {
        // Log the difference between the metric value and children as "untracked" memory so
        // that the values always add up. This value is not always completely accurate because
        // we did not necessarily get a consistent snapshot of the consumption values for all
        // children at a single moment in time, but is good enough for our purposes.
        int64_t untracked_bytes = curr_consumption - child_consumption;
        ss << "\n"
            << new_prefix << "Untracked Memory: Total=";
        ss << "\n"
            << new_prefix << "Untracked Memory: Total="
            << PrettyPrinter::print(untracked_bytes, TUnit::BYTES);
    }

    return ss.str();
}

std::string MemTracker::LogUsage(const std::string& prefix, const std::list<MemTracker*>& trackers,
        int64_t* logged_consumption) {
    *logged_consumption = 0;
    std::vector<std::string> usage_strings;
    for (MemTracker* tracker : trackers) {
        int64_t tracker_consumption;
        std::string usage_string = tracker->LogUsage(prefix, &tracker_consumption);
        if (!usage_string.empty()) usage_strings.push_back(usage_string);
        *logged_consumption += tracker_consumption;
    }
    return boost::join(usage_strings, "\n");
}

Status MemTracker::MemLimitExceeded(RuntimeState* state, const std::string& details,
        int64_t failed_allocation_size) {
    DCHECK_GE(failed_allocation_size, 0);
    std::stringstream ss;
    if (details.size() != 0) ss << details << std::endl;
    if (failed_allocation_size != 0) {
        ss << label() << " could not allocate "
            << PrettyPrinter::print(failed_allocation_size, TUnit::BYTES)
            << " without exceeding limit." << std::endl;
    }
    //ss << "Error occurred on backend " << GetBackendString();
    if (state != nullptr) ss << " by fragment " << state->fragment_instance_id();
    ss << std::endl;
    ExecEnv* exec_env = ExecEnv::GetInstance();
    //ExecEnv* exec_env = nullptr;
    MemTracker* process_tracker = exec_env->process_mem_tracker();
    const int64_t process_capacity = process_tracker->spare_capacity();
    ss << "Memory left in process limit: "
        << PrettyPrinter::print(process_capacity, TUnit::BYTES) << std::endl;

    // Choose which tracker to log the usage of. Default to the process tracker so we can
    // get the full view of memory consumption.
    // FIXME(cmy): call LogUsage() lead to crash here, fix it later
    // MemTracker* tracker_to_log = process_tracker;
    // if (state != nullptr && state->query_mem_tracker()->has_limit()) {
    //     MemTracker* query_tracker = state->query_mem_tracker();
    //     const int64_t query_capacity = query_tracker->limit() - query_tracker->consumption();
    //     ss << "Memory left in query limit: "
    //         << PrettyPrinter::print(query_capacity, TUnit::BYTES) << std::endl;
    //     // Log the query tracker only if the query limit was closer to being exceeded.
    //     if (query_capacity < process_capacity) tracker_to_log = query_tracker;
    // }
    // ss << tracker_to_log->LogUsage();
    // Status status = Status::MemLimitExceeded(ss.str());
    LIMIT_EXCEEDED(this, state, ss.str());
}

void MemTracker::AddGcFunction(GcFunction f) {
    _gc_functions.push_back(f);
}

bool MemTracker::GcMemory(int64_t max_consumption) {
    if (max_consumption < 0) return true;
    std::lock_guard<std::mutex> l(_gc_lock);
    if (_consumption_metric != NULL) RefreshConsumptionFromMetric();
    int64_t pre_gc_consumption = consumption();
    // Check if someone gc'd before us
    if (pre_gc_consumption < max_consumption) return false;
    if (_num_gcs_metric != NULL) _num_gcs_metric->increment(1);

    int64_t curr_consumption = pre_gc_consumption;
    // Try to free up some memory
    for (int i = 0; i < _gc_functions.size(); ++i) {
        // Try to free up the amount we are over plus some extra so that we don't have to
        // immediately GC again. Don't free all the memory since that can be unnecessarily
        // expensive.
        const int64_t EXTRA_BYTES_TO_FREE = 512L * 1024L * 1024L;
        int64_t bytes_to_free = curr_consumption - max_consumption + EXTRA_BYTES_TO_FREE;
        _gc_functions[i](bytes_to_free);
        if (_consumption_metric != NULL) RefreshConsumptionFromMetric();
        curr_consumption = consumption();
        if (max_consumption - curr_consumption <= EXTRA_BYTES_TO_FREE) break;
    }

    if (_bytes_freed_by_last_gc_metric != NULL) {
        _bytes_freed_by_last_gc_metric->set_value(pre_gc_consumption - curr_consumption);
    }
    return curr_consumption > max_consumption;
}

} // end namespace doris

