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

#include "runtime/memory/mem_tracker_task_pool.h"

#include "common/config.h"
#include "runtime/exec_env.h"
#include "util/pretty_printer.h"

namespace doris {

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::register_task_mem_tracker_impl(
        const std::string& task_id, int64_t mem_limit, const std::string& label,
        const std::shared_ptr<MemTrackerLimiter>& parent) {
    DCHECK(!task_id.empty());
    std::lock_guard<std::mutex> l(_task_tracker_lock);
    // First time this task_id registered, make a new object, otherwise do nothing.
    // Combine new tracker and emplace into one operation to avoid the use of locks
    // Name for task MemTrackers. '$0' is replaced with the task id.
    std::shared_ptr<MemTrackerLimiter> tracker;
    bool new_emplace = _task_mem_trackers.lazy_emplace_l(
            task_id, [&](const std::shared_ptr<MemTrackerLimiter>& v) { tracker = v; },
            [&](const auto& ctor) {
                tracker = std::make_shared<MemTrackerLimiter>(mem_limit, label, parent);
                ctor(task_id, tracker);
            });
    if (new_emplace) {
        LOG(INFO) << "Register query/load memory tracker, query/load id: " << task_id
                  << " limit: " << PrettyPrinter::print(mem_limit, TUnit::BYTES);
    }
    return tracker;
}

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::register_query_mem_tracker(
        const std::string& query_id, int64_t mem_limit) {
    return register_task_mem_tracker_impl(query_id, mem_limit, fmt::format("Query#Id={}", query_id),
                                          ExecEnv::GetInstance()->query_pool_mem_tracker());
}

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::register_query_scanner_mem_tracker(
        const std::string& query_id) {
    return register_task_mem_tracker_impl("Scanner#" + query_id, -1,
                                          fmt::format("Scanner#Query#Id={}", query_id),
                                          ExecEnv::GetInstance()->query_pool_mem_tracker());
}

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::register_load_mem_tracker(
        const std::string& load_id, int64_t mem_limit) {
    // In load, the query id of the fragment is executed, which is the same as the load id of the load channel.
    return register_task_mem_tracker_impl(load_id, mem_limit, fmt::format("Load#Id={}", load_id),
                                          ExecEnv::GetInstance()->load_pool_mem_tracker());
}

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::register_load_scanner_mem_tracker(
        const std::string& load_id) {
    return register_task_mem_tracker_impl("Scanner#" + load_id, -1,
                                          fmt::format("Scanner#Load#Id={}", load_id),
                                          ExecEnv::GetInstance()->load_pool_mem_tracker());
}

std::shared_ptr<MemTrackerLimiter> MemTrackerTaskPool::get_task_mem_tracker(
        const std::string& task_id) {
    DCHECK(!task_id.empty());
    std::shared_ptr<MemTrackerLimiter> tracker = nullptr;
    // Avoid using locks to resolve erase conflicts
    _task_mem_trackers.if_contains(
            task_id, [&tracker](const std::shared_ptr<MemTrackerLimiter>& v) { tracker = v; });
    return tracker;
}

void MemTrackerTaskPool::logout_task_mem_tracker() {
    std::lock_guard<std::mutex> l(_task_tracker_lock);
    std::vector<std::string> expired_task_ids;
    for (auto it = _task_mem_trackers.begin(); it != _task_mem_trackers.end(); it++) {
        if (!it->second) {
            // Unknown exception case with high concurrency, after _task_mem_trackers.erase,
            // the key still exists in _task_mem_trackers. https://github.com/apache/incubator-doris/issues/10006
            expired_task_ids.emplace_back(it->first);
        } else if (it->second.use_count() == 1 && it->second->had_child_count() != 0) {
            // No RuntimeState uses this task MemTrackerLimiter, it is only referenced by this map,
            // and tracker was not created soon, delete it.
            //
            // If consumption is not equal to 0 before query mem tracker is destructed,
            // there are two possibilities in theory.
            // 1. A memory leak occurs.
            // 2. memory consumed on query mem tracker, released on other trackers, and no manual transfer
            //  between the two trackers.
            // At present, it is impossible to effectively locate which memory consume and release on different trackers,
            // so query memory leaks cannot be found.
            LOG(INFO) << fmt::format(
                    "Deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                    "PeakUsed={}",
                    it->first, PrettyPrinter::print(it->second->limit(), TUnit::BYTES),
                    PrettyPrinter::print(it->second->consumption(), TUnit::BYTES),
                    PrettyPrinter::print(it->second->peak_consumption(), TUnit::BYTES));
            expired_task_ids.emplace_back(it->first);
        } else if (config::memory_verbose_track) {
            it->second->print_log_usage("query routine");
            it->second->enable_print_log_usage();
        }
    }
    for (auto tid : expired_task_ids) {
        // Verify the condition again to make sure the tracker is not being used again.
        _task_mem_trackers.erase_if(tid, [&](const std::shared_ptr<MemTrackerLimiter>& v) {
            return !v || v.use_count() == 1;
        });
    }
}

// TODO(zxy) More observable methods
// /// Logs the usage of 'limit' number of queries based on maximum total memory
// /// consumption.
// std::string MemTracker::LogTopNQueries(int limit) {
//     if (limit == 0) return "";
//     priority_queue<pair<int64_t, string>, std::vector<pair<int64_t, string>>,
//                    std::greater<pair<int64_t, string>>>
//             min_pq;
//     GetTopNQueries(min_pq, limit);
//     std::vector<string> usage_strings(min_pq.size());
//     while (!min_pq.empty()) {
//         usage_strings.push_back(min_pq.top().second);
//         min_pq.pop();
//     }
//     std::reverse(usage_strings.begin(), usage_strings.end());
//     return join(usage_strings, "\n");
// }

// /// Helper function for LogTopNQueries that iterates through the MemTracker hierarchy
// /// and populates 'min_pq' with 'limit' number of elements (that contain state related
// /// to query MemTrackers) based on maximum total memory consumption.
// void MemTracker::GetTopNQueries(
//         priority_queue<pair<int64_t, string>, std::vector<pair<int64_t, string>>,
//                        greater<pair<int64_t, string>>>& min_pq,
//         int limit) {
//     list<weak_ptr<MemTracker>> children;
//     {
//         lock_guard<SpinLock> l(child_trackers_lock_);
//         children = child_trackers_;
//     }
//     for (const auto& child_weak : children) {
//         shared_ptr<MemTracker> child = child_weak.lock();
//         if (child) {
//             child->GetTopNQueries(min_pq, limit);
//         }
//     }
// }

} // namespace doris
