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

#include "runtime/mem_tracker.h"

namespace doris {

// Global task pool for query MemTrackers. Owned by ExecEnv.
class MemTrackerTaskPool {
public:
    // Construct a MemTracker object for 'query_id' with 'mem_limit' as the memory limit.
    // The MemTracker is a child of the process MemTracker, Calling this with the same
    // 'query_id' will return the same MemTracker object. This is used to track the local
    // memory usage of all querys executing. The first time this is called for a query,
    // a new MemTracker object is created with the process tracker as its parent.
    // Newly created trackers will always have a limit of -1.
    std::shared_ptr<MemTracker> register_query_mem_tracker(const std::string& query_id,
                                                           int64_t mem_limit = -1);

    std::shared_ptr<MemTracker> get_query_mem_tracker(const std::string& query_id);

    void logout_query_mem_tracker();

private:
    // All per-query MemTracker objects.
    // The life cycle of query memtracker in the process is the same as query runtime state,
    // MemTrackers will be removed from this map after query finish or cancel.
    using TaskTrackersMap = phmap::parallel_flat_hash_map<
            std::string, std::shared_ptr<MemTracker>, phmap::priv::hash_default_hash<std::string>,
            phmap::priv::hash_default_eq<std::string>,
            std::allocator<std::pair<const std::string, std::shared_ptr<MemTracker>>>, 12,
            std::mutex>;

    TaskTrackersMap _query_mem_trackers;
};

} // namespace doris