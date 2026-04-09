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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>

#include <atomic>
#include <stack>
#include <string>

#include "common/cast_set.h"
#include "common/factory_creator.h"
#include "common/logging.h"
#include "util/uid_util.h"

namespace doris {
// Default memory estimate per block/task when actual usage is unknown.
// Used as initial estimate before runtime statistics are collected.
static constexpr int64_t DEFAULT_BLOCK_MEM_BYTES = 64 * 1024 * 1024; // 64MB default

struct MemLimiter;
struct AdaptiveTaskProcessor;

// ==================== MemShareArbitrator ====================
// Query-level memory arbitrator that distributes memory fairly across all operators.
//
// This arbitrator implements proportional memory sharing: each operator gets a share
// of the total memory budget proportional to its current memory usage relative to
// all operators' total usage. This ensures fair distribution while allowing operators
// with higher memory demands to get more resources.
//
// Thread-safety: All public methods are thread-safe. The arbitrator can be accessed
// concurrently by multiple operators without external synchronization.
//
// Usage:
//   1. Create one arbitrator per query with the query's memory limit
//   2. Each operator calls register_operator() during initialization
//   3. MemLimiter instances call update_mem_bytes() to report usage changes
//      and receive their new memory allocation
struct MemShareArbitrator {
public:
    ENABLE_FACTORY_CREATOR(MemShareArbitrator)
    MemShareArbitrator(const TUniqueId& qid, int64_t query_mem_limit, double max_ratio);
    void register_operator();
    std::string debug_string() const {
        return fmt::format("query_id: {}, query_mem_limit: {}, mem_limit: {}", print_id(query_id),
                           query_mem_limit, mem_limit);
    }

private:
    friend struct MemLimiter;
    // Update memory allocation when memory usage changes
    // Returns new operator memory limit for this context
    int64_t update_mem_bytes(int64_t old_value, int64_t new_value);

    const TUniqueId query_id;
    const int64_t query_mem_limit = 0;
    const int64_t mem_limit = 0;
    std::atomic<int64_t> total_mem_bytes = 0;
};

// ==================== MemLimiter ====================
// Operator-level memory limiter that controls task concurrency based on memory constraints.
//
// Each operator (scan, local exchange, etc.) has a MemLimiter that:
//   1. Tracks estimated memory usage per task (block)
//   2. Calculates how many concurrent tasks can run within memory limits
//   3. Coordinates with MemShareArbitrator for fair memory distribution
//
// The limiter uses a running average of block memory sizes to estimate future usage,
// which helps adapt to varying data characteristics during query execution.
//
// Lifecycle:
//   - init(): Called when first task starts, registers with arbitrator
//   - close(): Called when last task completes, releases arbitrator resources
//
// Thread-safety: All public methods are thread-safe.
struct MemLimiter {
public:
    ENABLE_FACTORY_CREATOR(MemLimiter)
    MemLimiter(const TUniqueId& qid, int64_t parallelism, bool serial_operator_, int64_t mem_limit,
               std::shared_ptr<MemShareArbitrator> mem_arb)
            : query_id(qid),
              parallelism(parallelism),
              serial_operator(serial_operator_),
              operator_mem_limit(mem_limit),
              mem_share_arb(mem_arb) {}
    ~MemLimiter() {
        if (open_tasks_count > 0) {
            // This operator shutdown with failure, memory should be reset here.
            estimated_block_mem_bytes = 0;
            update_mem_limit();
        }
    }
    int64_t init() {
        // TODO(gabriel): set estimated block size
        reestimated_block_mem_bytes(DEFAULT_BLOCK_MEM_BYTES);
        auto num = open_tasks_count.fetch_add(1);
        if (num == 0) {
            arb_mem_bytes = std::min(DEFAULT_BLOCK_MEM_BYTES, operator_mem_limit);
            update_mem_limit();
        }
        return num;
    }
    void close() {
        // Reset memory usage if all tasks to run this operator are done.
        if (open_tasks_count.fetch_add(-1) == 1) {
            estimated_block_mem_bytes = 0;
            update_mem_limit();
        }
    }

    int64_t update_running_tasks_count(int delta) { return running_tasks_count += delta; }
    // Re-estimated the average memory usage of a block, and update the estimated_block_mem_bytes accordingly.
    void reestimated_block_mem_bytes(int64_t value);

    std::string debug_string() const {
        return fmt::format(
                "query_id: {}, parallelism: {}, serial_operator: {}, operator_mem_limit: {}, "
                "running_tasks_count: {}, estimated_block_mem_bytes: {}, "
                "estimated_block_mem_bytes_update_count: {}, arb_mem_bytes: {}, "
                "open_tasks_count: {}, mem_limit: {}",
                print_id(query_id), parallelism, serial_operator, operator_mem_limit,
                running_tasks_count.load(), estimated_block_mem_bytes.load(),
                estimated_block_mem_bytes_update_count, arb_mem_bytes, open_tasks_count, mem_limit);
    }

private:
    friend struct AdaptiveTaskProcessor;
    // Update the memory usage of this context to memory share arbitrator.
    void update_mem_limit();
    // Calculate available tasks count based on memory limit
    int empty_task_slots(int ins_idx) const;

    const TUniqueId query_id;
    mutable std::mutex lock;
    // Parallelism of the query
    const int64_t parallelism = 0;
    const bool serial_operator = false;
    const int64_t operator_mem_limit;
    std::atomic<int64_t> running_tasks_count = 0;

    std::atomic<int64_t> estimated_block_mem_bytes = 0;
    int64_t estimated_block_mem_bytes_update_count = 0;
    int64_t arb_mem_bytes = 0;
    std::atomic<int64_t> open_tasks_count = 0;

    // Memory limit for this operator (shared by all instances), updated by memory share arbitrator
    std::atomic<int64_t> mem_limit = 0;
    std::shared_ptr<MemShareArbitrator> mem_share_arb;
};

class Dependency;

// ==================== AdaptiveTaskProcessor ====================
// Adaptive processor for dynamic task concurrency adjustment based on memory pressure.
//
// This processor sits between the operator and MemLimiter to provide:
//   1. Concurrency bounds enforcement (min/max limits)
//   2. Rate-limited adjustments to avoid thrashing (configurable interval)
//   3. Dependency-based backpressure when memory is constrained
//
// When memory pressure is high, the processor can block dependencies to reduce
// parallelism. When pressure decreases, blocked dependencies are released in
// LIFO order to resume execution.
//
// The adjustment interval (default 100ms) prevents rapid oscillation in
// concurrency levels, which could cause performance instability.
struct AdaptiveTaskProcessor {
public:
    ENABLE_FACTORY_CREATOR(AdaptiveTaskProcessor)
    AdaptiveTaskProcessor(int32_t max_concurrency, int32_t min_concurrency,
                          std::shared_ptr<MemLimiter> mem_limiter)
            : max_concurrency(max_concurrency),
              min_concurrency(min_concurrency),
              mem_limiter(mem_limiter) {}
    ~AdaptiveTaskProcessor() = default;
    // Calculate available tasks count for adaptive scheduling
    int available_task_slots(int ins_idx);
    // Expected tasks in this cycle
    int expected_tasks = 0;
    void adjust_parallelism(int running_sink_operators, int expected_tasks,
                            std::shared_ptr<Dependency> dep,
                            std::unique_lock<std::mutex>& /* lc */);
    std::string debug_string() const;

private:
    const int32_t max_concurrency;
    const int32_t min_concurrency;
    std::shared_ptr<MemLimiter> mem_limiter;
    // Timing metrics
    // int64_t context_start_time = 0;
    // int64_t scanner_total_halt_time = 0;
    // int64_t scanner_gen_blocks_time = 0;
    // std::atomic_int64_t scanner_total_io_time = 0;
    // std::atomic_int64_t scanner_total_running_time = 0;
    // std::atomic_int64_t scanner_total_scan_bytes = 0;

    // Timestamps
    // std::atomic_int64_t last_scanner_finish_timestamp = 0;
    // int64_t check_all_scanners_last_timestamp = 0;
    // int64_t last_driver_output_full_timestamp = 0;
    int64_t adjust_tasks_last_timestamp = 0;

    // Adjustment strategy fields
    // bool try_add_scanners = false;
    // double expected_speedup_ratio = 0;
    // double last_scanner_scan_speed = 0;
    // int64_t last_scanner_total_scan_bytes = 0;
    // int try_add_scanners_fail_count = 0;
    // int check_slow_io = 0;
    // int32_t slow_io_latency_ms = 100; // Default from config
    std::stack<std::shared_ptr<Dependency>> blocking_dependencies;
};

} // namespace doris
