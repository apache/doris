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
#include <functional>
#include <limits>
#include <vector>

namespace doris {

class Block;

struct IcebergSorterReserveMemory {
    size_t retained_growth = 0;
    size_t retained_growth_trigger_bytes = 0;
    size_t retained_sorted_destination = 0;
    size_t transient_workspace = 0;
};

inline size_t iceberg_saturating_add(size_t lhs, size_t rhs) {
    return std::min(std::numeric_limits<size_t>::max() - lhs, rhs) + lhs;
}

inline size_t iceberg_saturating_multiply(size_t lhs, size_t rhs) {
    return lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs
                   ? std::numeric_limits<size_t>::max()
                   : lhs * rhs;
}

inline size_t bounded_iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations,
        size_t incoming_rows = std::numeric_limits<size_t>::max(),
        size_t incoming_bytes = std::numeric_limits<size_t>::max()) {
    size_t transient_workspace = 0;
    for (const auto& reservation : per_partition_reservations) {
        transient_workspace = std::max(transient_workspace, reservation.transient_workspace);
    }

    std::vector<size_t> sorted_destinations;
    sorted_destinations.reserve(per_partition_reservations.size());
    for (const auto& reservation : per_partition_reservations) {
        if (reservation.retained_sorted_destination > 0) {
            sorted_destinations.push_back(reservation.retained_sorted_destination);
        }
    }
    std::sort(sorted_destinations.begin(), sorted_destinations.end(), std::greater<>());
    // A row can touch only one partition, but each destination survives serial dispatch. At EOS
    // every nonempty sorter is closed, so a zero-row final item must retain all destinations.
    const size_t destination_count = incoming_rows == 0
                                             ? sorted_destinations.size()
                                             : std::min(incoming_rows, sorted_destinations.size());
    size_t retained_sorted_destinations = 0;
    for (size_t i = 0; i < destination_count; ++i) {
        retained_sorted_destinations =
                iceberg_saturating_add(retained_sorted_destinations, sorted_destinations[i]);
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
    return iceberg_saturating_add(
            iceberg_saturating_add(retained_growth, retained_sorted_destinations),
            transient_workspace);
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

inline size_t iceberg_spill_merge_fan_in(size_t spill_buffer_bytes, size_t merge_limit_bytes) {
    if (spill_buffer_bytes == 0) {
        return 0;
    }
    const size_t reader_bytes = iceberg_saturating_multiply(3, spill_buffer_bytes);
    const size_t available_reader_bytes =
            merge_limit_bytes > spill_buffer_bytes ? merge_limit_bytes - spill_buffer_bytes : 0;
    return std::max<size_t>(2, reader_bytes == 0 ? 0 : available_reader_bytes / reader_bytes);
}

inline size_t iceberg_spill_merge_workspace(size_t spill_file_count, size_t spill_buffer_bytes,
                                            size_t merge_limit_bytes) {
    if (spill_file_count == 0 || spill_buffer_bytes == 0) {
        return 0;
    }
    const size_t reader_bytes = iceberg_saturating_multiply(3, spill_buffer_bytes);
    const size_t input_count = std::min(
            spill_file_count, iceberg_spill_merge_fan_in(spill_buffer_bytes, merge_limit_bytes));
    // Each primed reader retains a serialized read buffer, parsed protobuf storage, and one
    // deserialized cursor block; the merger additionally owns the block being emitted.
    return iceberg_saturating_add(iceberg_saturating_multiply(input_count, reader_bytes),
                                  spill_buffer_bytes);
}

inline size_t iceberg_final_merge_batch_rows(size_t spill_buffer_rows, size_t runtime_batch_rows) {
    // The output block is covered by one spill-buffer reservation, so its row count must use the
    // observed spill bound instead of the unrelated query-wide batch size.
    return std::max<size_t>(1, std::min(spill_buffer_rows, runtime_batch_rows));
}

inline size_t iceberg_merge_output_workspace(size_t observed_row_bytes, size_t spill_buffer_bytes) {
    // A row wider than the byte budget is indivisible, so admission must still cover one such row.
    return std::max(observed_row_bytes, spill_buffer_bytes);
}

inline size_t iceberg_merge_output_batch_rows(size_t observed_row_bytes, size_t spill_buffer_bytes,
                                              size_t runtime_batch_rows) {
    const size_t row_bytes = std::max<size_t>(1, observed_row_bytes);
    const size_t byte_bounded_rows = std::max<size_t>(1, spill_buffer_bytes / row_bytes);
    return std::max<size_t>(1, std::min(byte_bounded_rows, runtime_batch_rows));
}

} // namespace doris
