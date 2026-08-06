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
#include <string>

#include "common/cast_set.h"
#include "common/factory_creator.h"
#include "common/logging.h"
#include "util/uid_util.h"

namespace doris {
static constexpr int64_t DEFAULT_SCANNER_MEM_BYTES = 64 * 1024 * 1024; // 64MB default

// Query-level memory arbitrator that distributes memory fairly across all scan contexts
struct MemShareArbitrator {
    ENABLE_FACTORY_CREATOR(MemShareArbitrator)
    TUniqueId query_id;
    int64_t query_mem_limit = 0;
    int64_t mem_limit = 0;
    std::atomic<int64_t> total_mem_bytes = 0;

    MemShareArbitrator(const TUniqueId& qid, int64_t query_mem_limit, double max_scan_ratio);

    // Update memory allocation when scanner memory usage changes
    // Returns new scan memory limit for this context
    int64_t update_mem_bytes(int64_t old_value, int64_t new_value);
    void register_scan_node();
    std::string debug_string() const {
        return fmt::format("query_id: {}, query_mem_limit: {}, mem_limit: {}", print_id(query_id),
                           query_mem_limit, mem_limit);
    }
};

// Scan-context-level memory limiter that controls scanner concurrency based on memory
struct MemLimiter {
private:
    TUniqueId query_id;
    mutable std::mutex lock;
    // Parallelism of the scan operator
    const int64_t parallelism = 0;
    const bool serial_operator = false;
    const int64_t operator_mem_limit;
    std::atomic<int64_t> running_tasks_count = 0;

    std::atomic<int64_t> estimated_block_mem_bytes = 0;
    int64_t estimated_block_mem_bytes_update_count = 0;
    int64_t arb_mem_bytes = 0;
    std::atomic<int64_t> open_tasks_count = 0;

    // Memory limit for this scan node (shared by all instances), updated by memory share arbitrator
    std::atomic<int64_t> mem_limit = 0;

public:
    ENABLE_FACTORY_CREATOR(MemLimiter)
    MemLimiter(const TUniqueId& qid, int64_t parallelism, bool serial_operator_, int64_t mem_limit)
            : query_id(qid),
              parallelism(parallelism),
              serial_operator(serial_operator_),
              operator_mem_limit(mem_limit) {}
    ~MemLimiter() { DCHECK_LE(open_tasks_count, 0); }

    // Calculate available scanner count based on memory limit
    int available_scanner_count(int ins_idx) const;

    int64_t update_running_tasks_count(int delta) { return running_tasks_count += delta; }

    // Re-estimated the average memory usage of a block, and update the estimated_block_mem_bytes accordingly.
    void reestimated_block_mem_bytes(int64_t value);
    void update_mem_limit(int64_t value) { mem_limit = value; }
    // Update the memory usage of this context to memory share arbitrator.
    // NOTE: This could be accessed by parallel tasks without synchronization, but it's fine
    // since the memory share arbitrator will do proportional sharing based on the ratio of this
    // context's memory usage to total memory usage, so even if there are some fluctuations in the
    // memory usage, the overall proportional sharing will still work.
    void update_arb_mem_bytes(int64_t value) {
        value = std::min(value, operator_mem_limit);
        arb_mem_bytes = value;
    }
    int64_t get_arb_scanner_mem_bytes() const { return arb_mem_bytes; }

    int64_t get_estimated_block_mem_bytes() const { return estimated_block_mem_bytes; }

    int64_t update_open_tasks_count(int delta) { return open_tasks_count.fetch_add(delta); }
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
};

} // namespace doris
