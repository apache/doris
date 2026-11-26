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

class Block;

struct IcebergSorterReserveMemory {
    size_t retained_growth = 0;
    size_t retained_growth_trigger_bytes = 0;
    size_t transient_workspace = 0;
};

inline size_t iceberg_saturating_add(size_t lhs, size_t rhs) {
    return std::min(std::numeric_limits<size_t>::max() - lhs, rhs) + lhs;
}

inline size_t bounded_iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations,
        size_t incoming_rows = std::numeric_limits<size_t>::max(),
        size_t incoming_bytes = std::numeric_limits<size_t>::max()) {
    size_t transient_workspace = 0;
    for (const auto& reservation : per_partition_reservations) {
        transient_workspace = std::max(transient_workspace, reservation.transient_workspace);
    }

    std::vector<const IcebergSorterReserveMemory*> growth_candidates;
    growth_candidates.reserve(per_partition_reservations.size());
    for (const auto& reservation : per_partition_reservations) {
        if (reservation.retained_growth > 0) {
            growth_candidates.push_back(&reservation);
        }
    }

    std::sort(growth_candidates.begin(), growth_candidates.end(),
              [](const auto* lhs, const auto* rhs) {
                  return lhs->retained_growth > rhs->retained_growth;
              });
    size_t row_bound = 0;
    for (size_t i = 0; i < std::min(incoming_rows, growth_candidates.size()); ++i) {
        row_bound = iceberg_saturating_add(row_bound, growth_candidates[i]->retained_growth);
    }

    size_t byte_bound = 0;
    std::vector<const IcebergSorterReserveMemory*> positive_trigger_candidates;
    positive_trigger_candidates.reserve(growth_candidates.size());
    for (const auto* reservation : growth_candidates) {
        if (reservation->retained_growth_trigger_bytes == 0) {
            byte_bound = iceberg_saturating_add(byte_bound, reservation->retained_growth);
        } else {
            positive_trigger_candidates.push_back(reservation);
        }
    }
    std::sort(positive_trigger_candidates.begin(), positive_trigger_candidates.end(),
              [](const auto* lhs, const auto* rhs) {
                  return static_cast<unsigned __int128>(lhs->retained_growth) *
                                 rhs->retained_growth_trigger_bytes >
                         static_cast<unsigned __int128>(rhs->retained_growth) *
                                 lhs->retained_growth_trigger_bytes;
              });
    size_t remaining_bytes = incoming_bytes;
    for (const auto* reservation : positive_trigger_candidates) {
        if (reservation->retained_growth_trigger_bytes <= remaining_bytes) {
            byte_bound = iceberg_saturating_add(byte_bound, reservation->retained_growth);
            remaining_bytes -= reservation->retained_growth_trigger_bytes;
            continue;
        }
        const auto numerator =
                static_cast<unsigned __int128>(reservation->retained_growth) * remaining_bytes +
                reservation->retained_growth_trigger_bytes - 1;
        const auto fractional_growth =
                std::min<unsigned __int128>(numerator / reservation->retained_growth_trigger_bytes,
                                            std::numeric_limits<size_t>::max());
        byte_bound = iceberg_saturating_add(byte_bound, static_cast<size_t>(fractional_growth));
        break;
    }

    // A block's rows and bytes are divided across partition sorters. The two fractional-relaxation
    // bounds avoid charging the complete input block to every active partition while remaining safe.
    const size_t retained_growth = std::min(row_bound, byte_bound);
    return iceberg_saturating_add(retained_growth, transient_workspace);
}

inline size_t iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations,
        size_t incoming_block_reserve, size_t incoming_rows = std::numeric_limits<size_t>::max(),
        size_t incoming_bytes = std::numeric_limits<size_t>::max()) {
    size_t sorter_reserve =
            bounded_iceberg_reserve_size(per_partition_reservations, incoming_rows, incoming_bytes);
    // The incoming block creates cold partition writers before they can appear in the published snapshot.
    return iceberg_saturating_add(sorter_reserve, incoming_block_reserve);
}

size_t iceberg_cold_writer_reserve_size(const Block& block, size_t writer_workspace_bytes);

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
