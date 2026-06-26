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

#include "load/channel/adaptive_random_bucket_state.h"

#include <glog/logging.h>

#include <utility>

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

} // namespace doris
