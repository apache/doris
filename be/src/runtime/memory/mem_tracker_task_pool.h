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

#include <parallel_hashmap/phmap.h>

#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

// TODO: phmap `parallel_flat_hash_map` is not thread-safe. If it is not fixed in the future,
//       can consider using other maps instead.
using TaskTrackersMap = phmap::parallel_flat_hash_map<
        std::string, std::shared_ptr<MemTrackerLimiter>,
        phmap::priv::hash_default_hash<std::string>, phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<const std::string, std::shared_ptr<MemTrackerLimiter>>>, 12,
        std::mutex>;

// Global task pool for query MemTrackers. Owned by ExecEnv.
class MemTrackerTaskPool {
public:
    // Construct a MemTrackerLimiter object for 'task_id' with 'mem_limit' as the memory limit.
    // The MemTrackerLimiter is a child of the pool MemTrackerLimiter, Calling this with the same
    // 'task_id' will return the same MemTrackerLimiter object. This is used to track the local
    // memory usage of all tasks executing. The first time this is called for a task,
    // a new MemTrackerLimiter object is created with the pool tracker as its parent.
    // Newly created trackers will always have a limit of -1.
    std::shared_ptr<MemTrackerLimiter> register_task_mem_tracker_impl(
            const std::string& task_id, int64_t mem_limit, const std::string& label,
            const std::shared_ptr<MemTrackerLimiter>& parent);
    std::shared_ptr<MemTrackerLimiter> register_query_mem_tracker(const std::string& query_id,
                                                                  int64_t mem_limit);
    std::shared_ptr<MemTrackerLimiter> register_query_scanner_mem_tracker(
            const std::string& query_id);
    std::shared_ptr<MemTrackerLimiter> register_load_mem_tracker(const std::string& load_id,
                                                                 int64_t mem_limit);
    std::shared_ptr<MemTrackerLimiter> register_load_scanner_mem_tracker(
            const std::string& load_id);

    std::shared_ptr<MemTrackerLimiter> get_task_mem_tracker(const std::string& task_id);

    // Remove the mem tracker that has ended the query.
    void logout_task_mem_tracker();

private:
    // All per-task MemTrackerLimiter objects.
    // The life cycle of task MemTrackerLimiter in the process is the same as task runtime state,
    // MemTrackers will be removed from this map after query finish or cancel.
    TaskTrackersMap _task_mem_trackers;
    std::mutex _task_tracker_lock;
};

} // namespace doris