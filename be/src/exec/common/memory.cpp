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

namespace doris {

MemShareArbitrator::MemShareArbitrator(const TUniqueId& qid, int64_t query_mem_limit,
                                       double max_scan_ratio)
        : query_id(qid),
          query_mem_limit(query_mem_limit),
          mem_limit(std::max<int64_t>(
                  1, static_cast<int64_t>(static_cast<double>(query_mem_limit) * max_scan_ratio))) {
}

void MemShareArbitrator::register_scan_node() {
    total_mem_bytes.fetch_add(DEFAULT_SCANNER_MEM_BYTES);
}

int64_t MemShareArbitrator::update_mem_bytes(int64_t old_value, int64_t new_value) {
    int64_t diff = new_value - old_value;
    int64_t total = total_mem_bytes.fetch_add(diff) + diff;
    if (new_value == 0) return 0;
    if (total <= 0) return mem_limit;
    // Proportional sharing: allocate based on this context's share of total usage
    double ratio = static_cast<double>(new_value) / static_cast<double>(std::max(total, new_value));
    return static_cast<int64_t>(static_cast<double>(mem_limit) * ratio);
}

// ==================== MemLimiter ====================
int MemLimiter::available_scanner_count(int ins_idx) const {
    int64_t mem_limit_value = mem_limit.load();
    int64_t running_tasks_count_value = running_tasks_count.load();
    int64_t estimated_block_mem_bytes_value = get_estimated_block_mem_bytes();
    DCHECK_GT(estimated_block_mem_bytes_value, 0);

    int64_t max_count = std::max(1L, mem_limit_value / estimated_block_mem_bytes_value);
    int64_t avail_count = max_count;
    int64_t per_count = avail_count / parallelism;
    if (serial_operator) {
        per_count += (avail_count - per_count * parallelism);
    } else if (ins_idx < avail_count - per_count * parallelism) {
        per_count += 1;
    }

    VLOG_DEBUG << "available_scanner_count. max_count=" << max_count << "("
               << running_tasks_count_value << "/" << estimated_block_mem_bytes_value
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
    int64_t total =
            get_estimated_block_mem_bytes() * estimated_block_mem_bytes_update_count + value;
    estimated_block_mem_bytes_update_count += 1;
    estimated_block_mem_bytes = total / estimated_block_mem_bytes_update_count;
    VLOG_DEBUG << fmt::format(
            "reestimated_block_mem_bytes. MemLimiter = {}, estimated_block_mem_bytes = {}, "
            "old_value = {}, value: {}",
            debug_string(), estimated_block_mem_bytes, old_value, value);
}

} // namespace doris
