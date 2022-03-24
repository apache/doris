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

#include "runtime/mem_tracker_task_pool.h"

#include "common/config.h"
#include "runtime/exec_env.h"
#include "util/pretty_printer.h"

namespace doris {

std::shared_ptr<MemTracker> MemTrackerTaskPool::register_task_mem_tracker_impl(
        const std::string& task_id, int64_t mem_limit, const std::string& label,
        std::shared_ptr<MemTracker> parent) {
    DCHECK(!task_id.empty());
    // First time this task_id registered, make a new object, otherwise do nothing.
    // Combine create_tracker and emplace into one operation to avoid the use of locks
    // Name for task MemTrackers. '$0' is replaced with the task id.
    _task_mem_trackers.try_emplace_l(
            task_id, [](std::shared_ptr<MemTracker>) {},
            MemTracker::create_tracker(mem_limit, label, parent, MemTrackerLevel::TASK));
    std::shared_ptr<MemTracker> tracker = get_task_mem_tracker(task_id);
    return tracker;
}

std::shared_ptr<MemTracker> MemTrackerTaskPool::register_query_mem_tracker(
        const std::string& query_id, int64_t mem_limit) {
    VLOG_FILE << "Register Query memory tracker, query id: " << query_id
              << " limit: " << PrettyPrinter::print(mem_limit, TUnit::BYTES);
    return register_task_mem_tracker_impl(query_id, mem_limit, fmt::format("queryId={}", query_id),
                                          ExecEnv::GetInstance()->query_pool_mem_tracker());
}

std::shared_ptr<MemTracker> MemTrackerTaskPool::register_load_mem_tracker(
        const std::string& load_id, int64_t mem_limit) {
    VLOG_FILE << "Register Load memory tracker, load id: " << load_id
              << " limit: " << PrettyPrinter::print(mem_limit, TUnit::BYTES);
    return register_task_mem_tracker_impl(load_id, mem_limit, fmt::format("loadId={}", load_id),
                                          ExecEnv::GetInstance()->load_pool_mem_tracker());
}

std::shared_ptr<MemTracker> MemTrackerTaskPool::get_task_mem_tracker(const std::string& task_id) {
    DCHECK(!task_id.empty());
    std::shared_ptr<MemTracker> tracker = nullptr;
    // Avoid using locks to resolve erase conflicts
    _task_mem_trackers.if_contains(task_id,
                                   [&tracker](std::shared_ptr<MemTracker> v) { tracker = v; });
    return tracker;
}

void MemTrackerTaskPool::logout_task_mem_tracker() {
    std::vector<std::string> expired_tasks;
    for (auto it = _task_mem_trackers.begin(); it != _task_mem_trackers.end(); it++) {
        // No RuntimeState uses this task MemTracker, it is only referenced by this map, delete it
        if (it->second.use_count() == 1) {
            if (config::memory_leak_detection && it->second->consumption() != 0) {
                // If consumption is not equal to 0 before query mem tracker is destructed,
                // there are two possibilities in theory.
                // 1. A memory leak occurs.
                // 2. Some of the memory consumed/released on the query mem tracker is actually released/consume on
                // other trackers such as the process mem tracker, and there is no manual transfer between the two trackers.
                //
                // The second case should be eliminated in theory, but it has not been done so far, so the query memory leak
                // cannot be located, and the value of the query pool mem tracker statistics will be inaccurate.
                //
                // In order to ensure that the query pool mem tracker is the sum of all currently running query mem trackers,
                // the effect of the ended query mem tracker on the query pool mem tracker should be cleared, that is,
                // the negative number of the current value of consume.
                LOG(WARNING) << "Task memory tracker memory leak:" << it->second->debug_string();
            }
            it->second->parent()->consume_local(-it->second->consumption(),
                                                MemTracker::get_process_tracker().get());
            expired_tasks.emplace_back(it->first);
        }
    }
    for (auto tid : expired_tasks) {
        DCHECK(_task_mem_trackers[tid].use_count() == 1);
        _task_mem_trackers.erase(tid);
        VLOG_FILE << "Deregister task memory tracker, task id: " << tid;
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
