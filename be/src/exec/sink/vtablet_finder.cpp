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
#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"
#include "storage/tablet_info.h"

namespace doris {

void AdaptiveRandomBucketState::init_partition(int32_t sender_id, int64_t partition_id,
                                               const std::vector<int64_t>& tablets,
                                               const std::vector<int32_t>& bucket_seqs,
                                               int32_t start_tablet_idx) {
    if (sender_id < 0 || partition_id < 0 || tablets.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    auto& partition_states = _sender_partition_states[sender_id];
    if (partition_states.contains(partition_id)) {
        return;
    }

    PartitionState state;
    state.partition_id = partition_id;
    state.tablets = tablets;
    state.bucket_seqs = bucket_seqs;
    if (start_tablet_idx >= 0 && start_tablet_idx < state.tablets.size()) {
        state.tablet_pos = start_tablet_idx;
    }
    state.current_tablet_id = state.tablets[state.tablet_pos];

    partition_states.emplace(partition_id, std::move(state));
    LOG(INFO) << "FIND_TABLET_RANDOM_BUCKET: load_id=" << _load_id << ", sender_id=" << sender_id
              << ", partition=" << partition_id << ", local tablet count=" << tablets.size()
              << ", start tablet=" << partition_states.at(partition_id).current_tablet_id;
}

int64_t AdaptiveRandomBucketState::current_tablet(int32_t sender_id, int64_t partition_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto sender_it = _sender_partition_states.find(sender_id);
    if (sender_it == _sender_partition_states.end()) {
        return -1;
    }
    auto it = sender_it->second.find(partition_id);
    if (it == sender_it->second.end()) {
        return -1;
    }
    return it->second.current_tablet_id;
}

void AdaptiveRandomBucketState::rotate_by_tablet(int32_t sender_id, int64_t partition_id,
                                                 int64_t tablet_id) {
    if (!config::enable_adaptive_random_bucket_load_bucket_rotation) {
        return;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    auto sender_it = _sender_partition_states.find(sender_id);
    if (sender_it == _sender_partition_states.end()) {
        return;
    }
    auto state_it = sender_it->second.find(partition_id);
    if (state_it == sender_it->second.end()) {
        return;
    }
    auto& state = state_it->second;
    if (state.current_tablet_id != tablet_id) {
        return;
    }
    int32_t next_pos = (state.tablet_pos + 1) % static_cast<int32_t>(state.tablets.size());
    LOG(INFO) << "FIND_TABLET_RANDOM_BUCKET: load_id=" << _load_id << ", sender_id=" << sender_id
              << ", partition=" << state.partition_id << " rotate tablet "
              << state.current_tablet_id << " -> " << state.tablets[next_pos]
              << " after tablet=" << tablet_id << " memtable flushed";
    state.tablet_pos = next_pos;
    state.current_tablet_id = state.tablets[next_pos];
}

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
        // Receiver-side random bucket mode only needs partition ids on sender side.
        // The receiver decides the concrete tablet from its local ordered tablet list.
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
