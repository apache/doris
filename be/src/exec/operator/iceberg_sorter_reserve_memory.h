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
#include <limits>
#include <vector>

namespace doris {

struct IcebergSorterReserveMemory {
    size_t retained_growth = 0;
    size_t transient_workspace = 0;
};

inline size_t bounded_iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations) {
    size_t retained_growth = 0;
    size_t transient_workspace = 0;
    for (const auto& reservation : per_partition_reservations) {
        retained_growth = std::min(std::numeric_limits<size_t>::max() - retained_growth,
                                   reservation.retained_growth) +
                          retained_growth;
        transient_workspace = std::max(transient_workspace, reservation.transient_workspace);
    }
    return std::min(std::numeric_limits<size_t>::max() - retained_growth, transient_workspace) +
           retained_growth;
}

inline size_t iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations,
        size_t incoming_block_bytes) {
    size_t sorter_reserve = bounded_iceberg_reserve_size(per_partition_reservations);
    // The incoming block creates cold partition writers before they can appear in the published snapshot.
    return std::min(std::numeric_limits<size_t>::max() - sorter_reserve, incoming_block_bytes) +
           sorter_reserve;
}

inline size_t iceberg_spill_merge_workspace(size_t spill_file_count, size_t spill_buffer_bytes,
                                            size_t merge_limit_bytes) {
    if (spill_file_count == 0 || spill_buffer_bytes == 0) {
        return 0;
    }
    const size_t max_fan_in = std::max<size_t>(2, merge_limit_bytes / spill_buffer_bytes);
    const size_t input_count = std::min(spill_file_count, max_fan_in);
    const size_t max_size = std::numeric_limits<size_t>::max();
    const size_t input_bytes = input_count > max_size / spill_buffer_bytes
                                       ? max_size
                                       : input_count * spill_buffer_bytes;
    // VSortedRunMerger materializes one block per input cursor plus the block being emitted.
    return input_bytes > max_size - spill_buffer_bytes ? max_size
                                                       : input_bytes + spill_buffer_bytes;
}

} // namespace doris
