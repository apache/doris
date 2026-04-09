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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/formatIPv6.cpp
// and modified by Doris

#include "exec/common/memory.h"

#include "exec/pipeline/dependency.h"
#include "util/time.h"

namespace doris {

// ==================== MemShareArbitrator Implementation ====================

MemShareArbitrator::MemShareArbitrator(const TUniqueId& qid, int64_t query_mem_limit,
                                       double max_ratio)
        : query_id(qid),
          query_mem_limit(query_mem_limit),
          // Ensure at least 1 byte to avoid division by zero in ratio calculations
          mem_limit(std::max<int64_t>(
                  1, static_cast<int64_t>(static_cast<double>(query_mem_limit) * max_ratio))) {}

// Register a new operator with default memory allocation.
// Called during operator initialization to account for its presence in memory distribution.
void MemShareArbitrator::register_operator() {
    total_mem_bytes.fetch_add(DEFAULT_BLOCK_MEM_BYTES);
}

// Update memory allocation using proportional sharing algorithm.
// Returns the new memory limit for the calling operator based on its share of total usage.
//
// Algorithm:
//   1. Update total tracked memory: total += (new_value - old_value)
//   2. Calculate this operator's ratio: ratio = new_value / total
//   3. Allocate proportional share: limit = mem_limit * ratio
//
// This ensures operators with higher memory demands get proportionally more resources,
// while the total allocation stays within the query's memory budget.
int64_t MemShareArbitrator::update_mem_bytes(int64_t old_value, int64_t new_value) {
    int64_t diff = new_value - old_value;
    int64_t total = total_mem_bytes.fetch_add(diff) + diff;
    if (new_value == 0) return 0;
    if (total <= 0) return mem_limit;
    // Proportional sharing: allocate based on this context's share of total usage
    double ratio = static_cast<double>(new_value) / static_cast<double>(std::max(total, new_value));
    return static_cast<int64_t>(static_cast<double>(mem_limit) * ratio);
}

// ==================== MemLimiter Implementation ====================

// Calculate available task slots for a specific instance based on memory constraints.
// Distributes slots fairly across parallel instances, with remainder going to lower indices.
//
// For serial operators, all slots go to a single instance.
// For parallel operators, slots are divided evenly with remainder distributed round-robin.
int MemLimiter::empty_task_slots(int ins_idx) const {
    int64_t mem_limit_value = mem_limit.load();
    int64_t running_tasks_count_value = running_tasks_count.load();
    int64_t estimated_block_mem_bytes_value = estimated_block_mem_bytes;
    DCHECK(estimated_block_mem_bytes_value > 0 ||
           (open_tasks_count == 0 && estimated_block_mem_bytes_value == 0))
            << "estimated_block_mem_bytes should be > 0 when there are open tasks. debug_string: "
            << debug_string();

    if (estimated_block_mem_bytes_value == 0) {
        return 1;
    }
    int64_t max_count = std::max(1L, mem_limit_value / estimated_block_mem_bytes_value);
    int64_t avail_count = max_count;
    int64_t per_count = avail_count / parallelism;
    if (serial_operator) {
        per_count += (avail_count - per_count * parallelism);
    } else if (ins_idx < avail_count - per_count * parallelism) {
        per_count += 1;
    }

    VLOG_DEBUG << "empty_task_slots. max_count=" << max_count << "(" << running_tasks_count_value
               << "/" << estimated_block_mem_bytes_value
               << "), operator_mem_limit = " << operator_mem_limit
               << ", running_tasks_count = " << running_tasks_count_value
               << ", parallelism = " << parallelism << ", avail_count = " << avail_count
               << ", ins_id = " << ins_idx << ", per_count = " << per_count
               << " debug_string: " << debug_string();

    return cast_set<int>(per_count);
}

void MemLimiter::reestimated_block_mem_bytes(int64_t value) {
    if (value == 0) return;
    value = std::min(value, operator_mem_limit);

    std::lock_guard<std::mutex> L(lock);
    auto old_value = estimated_block_mem_bytes.load();
    int64_t total = estimated_block_mem_bytes * estimated_block_mem_bytes_update_count + value;
    estimated_block_mem_bytes_update_count += 1;
    estimated_block_mem_bytes = total / estimated_block_mem_bytes_update_count;
    VLOG_DEBUG << fmt::format(
            "reestimated_block_mem_bytes. MemLimiter = {}, estimated_block_mem_bytes = {}, "
            "old_value = {}, value: {}",
            debug_string(), estimated_block_mem_bytes, old_value, value);
}

// Report current memory usage to the arbitrator and receive updated memory limit.
// Called periodically (typically by instance 0) to adjust allocation based on actual usage.
void MemLimiter::update_mem_limit() {
    int64_t old_value = arb_mem_bytes;
    int64_t new_value = estimated_block_mem_bytes;

    new_value = std::min(new_value, operator_mem_limit);
    arb_mem_bytes = new_value;

    int64_t new_mem_limit = mem_share_arb->update_mem_bytes(old_value, new_value);
    mem_limit = new_mem_limit;

    VLOG_DEBUG << fmt::format(
            "update_mem_limit. context = {}, new mem limit = {}, mem bytes = {} "
            "-> {}, mem_share_arb: {}",
            debug_string(), new_mem_limit, old_value, new_value, mem_share_arb->debug_string());
}

// ==================== AdaptiveTaskProcessor Implementation ====================

// Calculate available task slots with adaptive adjustment.
// Combines memory-based limits from MemLimiter with configured min/max bounds.
//
// Instance 0 is responsible for triggering memory limit updates to avoid
// redundant arbitrator calls from parallel instances.
//
// Rate limiting: Adjustments only occur every `doris_adaptive_tasks_interval_ms`
// to prevent rapid oscillation in concurrency levels.
int AdaptiveTaskProcessor::available_task_slots(int ins_idx) {
    int min_tasks = std::max(1, min_concurrency);
    int max_tasks = mem_limiter->empty_task_slots(ins_idx);
    max_tasks = std::min(max_tasks, max_concurrency);
    if (ins_idx == 0) {
        // Adjust memory limit via memory share arbitrator
        mem_limiter->update_mem_limit();
    }

    int& expected = expected_tasks;
    int64_t now = UnixMillis();
    // Avoid frequent adjustment - only adjust every 100ms
    if (now - adjust_tasks_last_timestamp <= config::doris_adaptive_tasks_interval_ms) {
        return expected;
    }
    adjust_tasks_last_timestamp = now;
    auto old_tasks = expected_tasks;

    expected = std::max(max_tasks, min_tasks);
    VLOG_DEBUG << fmt::format(
            "available_task_slots.  old_tasks = {}, expected tasks = {} "
            ", min_tasks: {}, max_tasks: {}",
            old_tasks, expected, min_tasks, max_tasks);

    // TODO(gabriel): Tasks are scheduled adaptively based on the memory usage now.
    return expected;
}

// Adjust parallelism by blocking/unblocking dependencies based on memory pressure.
//
// When running_sink_operators > expected:
//   - Block excess dependencies to reduce parallelism
//   - Dependencies are stored in a stack for LIFO release
//
// When running_sink_operators <= expected:
//   - Unblock dependencies to allow more parallelism
//   - LIFO order ensures recently blocked tasks resume first (cache locality)
void AdaptiveTaskProcessor::adjust_parallelism(int running_sink_operators, int expected,
                                               std::shared_ptr<Dependency> dep,
                                               std::unique_lock<std::mutex>& /* lc */) {
    int remain = std::max(0, running_sink_operators - expected);
    if (blocking_dependencies.size() < remain && dep) {
        dep->block();
        blocking_dependencies.push(dep);
    } else {
        int sz = cast_set<int>(blocking_dependencies.size());
        while (sz > remain) {
            const auto top = blocking_dependencies.top();
            blocking_dependencies.pop();
            sz--;
            top->set_ready();
        }
    }
    DCHECK_GE(remain, blocking_dependencies.size()) << debug_string();
}

std::string AdaptiveTaskProcessor::debug_string() const {
    return fmt::format(
            "max_concurrency: {}, min_concurrency: {}, expected_tasks: {}, blocking_deps_size: {}, "
            "mem_limiter: {}",
            max_concurrency, min_concurrency, expected_tasks, blocking_dependencies.size(),
            mem_limiter->debug_string());
}

} // namespace doris
