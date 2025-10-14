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

#include <mutex>
#include <string>
#include <unordered_map>
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
                        fmt::format_to(buf, "no partition for this tuple. tuple={}",
                                       block->dump_data_json(row_index, 1));
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
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                                  &_partition_to_tablet_map);
        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
            // Count rows per partition for accurate tablet switching
            std::unordered_map<VOlapTablePartition*, int64_t> partition_row_counts;
            for (size_t i = 0; i < qualified_rows.size(); ++i) {
                auto row_idx = qualified_rows[i];
                auto* partition = partitions[row_idx];
                if (partition != nullptr) {
                    partition_row_counts[partition]++;
                }
            }

            // Update local state for each partition involved in this batch
            for (auto it : _partition_to_tablet_map) {
                auto* partition = it.first;
                if (partition->load_tablet_idx == -1) continue;

                // Use actual row count for this partition
                int64_t partition_rows = partition_row_counts[partition];
                partition->current_tablet_rows += partition_rows;

                LOG(INFO) << "Tablet switching: partition_id=" << partition->id
                          << ", current_tablet_rows=" << partition->current_tablet_rows
                          << ", switching_threshold=" << partition->switching_threshold
                          << ", load_tablet_idx=" << partition->load_tablet_idx
                          << ", num_buckets=" << partition->num_buckets
                          << ", partition_rows_this_batch=" << partition_rows;

                // Check if we need to switch to next tablet based on threshold
                if (partition->switching_threshold > 0 &&
                    partition->current_tablet_rows >= partition->switching_threshold) {
                    // Switch to next tablet in round-robin fashion
                    int64_t old_tablet_idx = partition->load_tablet_idx;
                    partition->load_tablet_idx =
                            (partition->load_tablet_idx + 1) % partition->num_buckets;
                    partition->current_tablet_rows = 0;
                    LOG(INFO) << "Tablet switched: partition_id=" << partition->id
                              << ", from tablet_idx=" << old_tablet_idx
                              << " to tablet_idx=" << partition->load_tablet_idx;
                } else if (partition->switching_threshold == 0) {
                    // Legacy behavior: increment tablet index for each batch
                    partition->load_tablet_idx++;
                }
            }
            _partition_to_tablet_map.clear();
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
