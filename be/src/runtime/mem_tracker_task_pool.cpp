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

std::shared_ptr<MemTracker> MemTrackerTaskPool::register_query_mem_tracker(
        const std::string& query_id, int64_t mem_limit) {
    DCHECK(!query_id.empty());
    VLOG_FILE << "Register query memory tracker, query id: " << query_id
              << " limit: " << PrettyPrinter::print(mem_limit, TUnit::BYTES);

    // First time this query_id registered, make a new object, otherwise do nothing.
    // Combine create_tracker and emplace into one operation to avoid the use of locks
    // Name for query MemTrackers. '$0' is replaced with the query id.
    _query_mem_trackers.try_emplace_l(
            query_id, [](std::shared_ptr<MemTracker>) {},
            MemTracker::create_tracker(mem_limit, fmt::format("queryId={}", query_id),
                                       ExecEnv::GetInstance()->query_pool_mem_tracker(),
                                       MemTrackerLevel::TASK));
    std::shared_ptr<MemTracker> tracker = get_query_mem_tracker(query_id);
    return tracker;
}

std::shared_ptr<MemTracker> MemTrackerTaskPool::get_query_mem_tracker(const std::string& query_id) {
    DCHECK(!query_id.empty());
    std::shared_ptr<MemTracker> tracker = nullptr;
    // Avoid using locks to resolve erase conflicts
    _query_mem_trackers.if_contains(query_id,
                                    [&tracker](std::shared_ptr<MemTracker> v) { tracker = v; });
    return tracker;
}

void MemTrackerTaskPool::logout_query_mem_tracker() {
    std::vector<std::string> expired_querys;
    for (auto it = _query_mem_trackers.begin(); it != _query_mem_trackers.end(); it++) {
        // No RuntimeState uses this query MemTracker, it is only referenced by this map, delete it
        if (it->second.use_count() == 1) {
            if (!config::memory_leak_detection || it->second->consumption() == 0) {
                expired_querys.emplace_back(it->first);
            } else {
                LOG(WARNING) << "Memory tracker " << it->second->debug_string() << " Memory leak "
                             << it->second->consumption();
            }
        }
    }
    for (auto qid : expired_querys) {
        DCHECK(_query_mem_trackers[qid].use_count() == 1);
        _query_mem_trackers.erase(qid);
        VLOG_FILE << "Deregister query memory tracker, query id: " << qid;
    }
}

// TODO(zxy)
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
