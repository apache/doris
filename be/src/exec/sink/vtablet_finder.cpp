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

#include "exec/sink/vtablet_finder.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"
#include "storage/tablet_info.h"

namespace doris {
Status OlapTabletFinder::find_tablets(RuntimeState* state, Block* block, int rows,
                                      std::vector<VOlapTablePartition*>& partitions,
                                      std::vector<uint32_t>& tablet_index, std::vector<bool>& skip,
                                      std::vector<uint32_t>* miss_rows) {
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
    } else if (_find_tablet_mode == FindTabletMode::FIND_TABLET_RANDOM_BUCKET) {
        // Use per-batch caching same as FIND_TABLET_EVERY_BATCH
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                                  &_partition_to_tablet_map);

        int64_t bytes_per_row = rows > 0 ? block->bytes() / rows : 0;
        for (auto& [partition, _] : _partition_to_tablet_map) {
            if (partition->indexes.empty()) {
                continue;
            }
            int64_t cur_bucket = partition->load_tablet_idx % partition->num_buckets;
            int64_t cur_tablet_id = partition->indexes[0].tablets[cur_bucket];
            _tablet_written_bytes[cur_tablet_id] += bytes_per_row * rows;

            // Switch to the next local bucket when the threshold is reached.
            // local_bucket_seqs carries the exact set of buckets assigned to this BE by the FE,
            // which handles both single-replica and multi-replica correctly.
            const auto& seqs = partition->local_bucket_seqs;
            if (_tablet_written_bytes[cur_tablet_id] >= BUCKET_SWITCH_THRESHOLD_BYTES &&
                !seqs.empty()) {
                auto it = std::find(seqs.begin(), seqs.end(), static_cast<int32_t>(cur_bucket));
                if (it != seqs.end()) {
                    ++it;
                    if (it == seqs.end()) {
                        it = seqs.begin();
                    }
                    partition->load_tablet_idx = *it;
                }
            }
        }
        _partition_to_tablet_map.clear();
    } else {
        // FIND_TABLET_EVERY_BATCH / FIND_TABLET_EVERY_SINK
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                                  &_partition_to_tablet_map);
        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
            for (auto it : _partition_to_tablet_map) {
                // do round-robin for next batch
                if (it.first->load_tablet_idx != -1) {
                    it.first->load_tablet_idx++;
                }
            }
            _partition_to_tablet_map.clear();
        }
    }

    return Status::OK();
}

} // namespace doris
