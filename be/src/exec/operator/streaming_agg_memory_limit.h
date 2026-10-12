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

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "exec/operator/streaming_agg_min_reduction.h"

namespace doris {

// Per-task memory budget of a streaming pre-aggregation (hash table + arenas).
//
// 1. The budget is one fifth of the current query memory limit, shared evenly among the
//    `parallel_tasks` instances of the operator on this BE. The query limit is read on every
//    call, so a limit lowered or restored by the workload group manager takes effect at once.
// 2. It never drops below `min_memory_limit`, which is the last cache tier of the min-reduction
//    table doubled (the budget also counts the key/state arenas, not only the bucket array), so a
//    small query limit does not disable pre-aggregation altogether. The floor itself is capped by
//    the per-task share of the query limit, so the pre-aggregation alone can never exceed it.
// 3. `fixed_limit` is an explicit upper bound on top of that; 0 means "no fixed bound". Callers
//    pass the session variable `spill_streaming_agg_mem_limit` when spilling is enabled (the
//    downstream agg can spill, the pre-agg cannot, so it must stay small) and 0 otherwise. It is
//    applied last so that a user who sets it explicitly always gets what they asked for.
//
// Returns 0 when neither limit is known, which callers treat as "no cap".
inline size_t streaming_agg_memory_limit(int64_t query_memory_limit, int parallel_tasks,
                                         int64_t fixed_limit) {
    if (query_memory_limit <= 0) {
        return fixed_limit > 0 ? static_cast<size_t>(fixed_limit) : 0;
    }

    constexpr int64_t memory_limit_divisor = 5;
    constexpr int64_t min_memory_limit =
            2LL * STREAMING_HT_MIN_REDUCTION[STREAMING_HT_MIN_REDUCTION_SIZE - 1].min_ht_mem;

    // A known positive query limit must never collapse to the "no cap" sentinel 0, even when it is
    // smaller than the number of tasks.
    const int64_t per_task_query_limit =
            std::max<int64_t>(query_memory_limit / std::max(parallel_tasks, 1), 1);
    int64_t limit = per_task_query_limit / memory_limit_divisor;
    limit = std::max(limit, std::min(min_memory_limit, per_task_query_limit));
    if (fixed_limit > 0) {
        limit = std::min(limit, fixed_limit);
    }
    return static_cast<size_t>(limit);
}

} // namespace doris
