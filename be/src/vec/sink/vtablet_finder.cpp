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

#include "vec/sink/vtablet_finder.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <set>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
Status OlapTabletFinder::find_tablets(RuntimeState* state, Block* block, int rows,
                                      std::vector<VOlapTablePartition*>& partitions,
                                      std::vector<uint32_t>& tablet_index, std::vector<bool>& skip,
                                      std::vector<int64_t>* miss_rows) {
    for (int index = 0; index < rows; index++) {
        _vpartition->find_partition(block, index, partitions[index]);
    }

    std::vector<uint32_t> qualified_rows;
    qualified_rows.reserve(rows);

    for (int row_index = 0; row_index < rows; row_index++) {
        if (partitions[row_index] == nullptr) [[unlikely]] {
            if (miss_rows != nullptr) {          // auto partition table
                miss_rows->push_back(row_index); // already reserve memory outside
                skip[row_index] = true;
                continue;
            }
            _num_filtered_rows++;
            _filter_bitmap.Set(row_index, true);
            skip[row_index] = true;
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple=\n{}",
                                       block->dump_data(row_index, 1));
                        return fmt::to_string(buf);
                    }));
            continue;
        }
        if (!partitions[row_index]->is_mutable) [[unlikely]] {
            _num_immutable_partition_filtered_rows++;
            skip[row_index] = true;
            continue;
        }
        if (partitions[row_index]->num_buckets <= 0) [[unlikely]] {
            std::stringstream ss;
            ss << "num_buckets must be greater than 0, num_buckets="
               << partitions[row_index]->num_buckets;
            return Status::InternalError(ss.str());
        }

        _partition_ids.emplace(partitions[row_index]->id);

        qualified_rows.push_back(row_index);
    }

    if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_ROW) {
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index);
    } else {
        // for random distribution
        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_ROW_BASED) {
            _find_tablets_with_row_based_switching(block, qualified_rows, partitions, tablet_index);
        } else {
            _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                                      &_partition_to_tablet_map);
        }

        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
            for (auto it : _partition_to_tablet_map) {
                // do round-robin for next batch
                if (it.first->load_tablet_idx != -1) {
                    it.first->load_tablet_idx++;
                }
            }
            _partition_to_tablet_map.clear();
        }
        // For ROW_BASED mode, don't clear the map to maintain tablet assignment consistency
    }

    return Status::OK();
}


void OlapTabletFinder::_find_tablets_with_row_based_switching(
        vectorized::Block* block, const std::vector<uint32_t>& qualified_rows,
        const std::vector<VOlapTablePartition*>& partitions, std::vector<uint32_t>& tablet_index) {
    // Simplified logic: first write all data to one tablet, then check if we need to switch for next batch

    // Get unique partitions in this batch
    std::set<VOlapTablePartition*> unique_partitions;
    for (auto row_index : qualified_rows) {
        unique_partitions.insert(partitions[row_index]);
    }

    // Check if we need to switch tablets before processing this batch
    for (auto* partition : unique_partitions) {
        int64_t current_accumulated_rows = _partition_row_counts[partition];

        LOG(INFO) << "TABLET_SWITCH_DEBUG: partition_id=" << partition->id
                  << ", load_tablet_idx=" << partition->load_tablet_idx
                  << ", current_batch_size=" << qualified_rows.size()
                  << ", accumulated_rows=" << current_accumulated_rows
                  << ", threshold=" << _tablet_switch_row_threshold;

        // If accumulated rows exceed threshold, switch to next tablet before processing this batch
        if (current_accumulated_rows >= _tablet_switch_row_threshold) {
            if (partition->load_tablet_idx != -1) {
                int64_t old_idx = partition->load_tablet_idx;
                partition->load_tablet_idx++;
                LOG(INFO) << "TABLET_SWITCH_DEBUG: Switching tablet from " << old_idx << " to "
                          << partition->load_tablet_idx << " for partition " << partition->id;
                // Reset row count for this partition after switching
                _partition_row_counts[partition] = 0;
            }
        }
    }

    // Process the entire batch with current tablet assignment
    _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                              &_partition_to_tablet_map);

    // Update row counts after processing this batch
    for (auto* partition : unique_partitions) {
        _partition_row_counts[partition] += qualified_rows.size();
    }
}

} // namespace doris::vectorized