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

Status AdaptiveRandomBucketState::init_partition(int64_t partition_id,
                                                 const std::vector<int64_t>& tablets,
                                                 const std::vector<int32_t>& bucket_seqs,
                                                 int32_t start_tablet_idx) {
    if (partition_id < 0 || tablets.empty()) {
        return Status::OK();
    }
    if (tablets.size() != bucket_seqs.size()) {
        return Status::InternalError(
                "invalid adaptive random bucket tablet sequence, load_id={}, partition_id={}, "
                "tablet_count={}, bucket_seq_count={}",
                print_id(_load_id), partition_id, tablets.size(), bucket_seqs.size());
    }
    if (start_tablet_idx < 0 || static_cast<size_t>(start_tablet_idx) >= tablets.size()) {
        return Status::InternalError(
                "invalid adaptive random bucket start tablet index, load_id={}, partition_id={}, "
                "start_tablet_idx={}, tablet_count={}",
                print_id(_load_id), partition_id, start_tablet_idx, tablets.size());
    }
    std::lock_guard<std::mutex> lock(_mutex);
    auto existing = _partition_states.find(partition_id);
    if (existing != _partition_states.end()) {
        const auto& state = existing->second;
        if (state.tablets != tablets || state.bucket_seqs != bucket_seqs ||
            state.initial_tablet_pos != start_tablet_idx) {
            return Status::InternalError(
                    "inconsistent adaptive random bucket partition init, load_id={}, "
                    "partition_id={}, existing_tablet_count={}, new_tablet_count={}, "
                    "existing_bucket_seq_count={}, new_bucket_seq_count={}, "
                    "existing_start_idx={}, new_start_idx={}",
                    print_id(_load_id), partition_id, state.tablets.size(), tablets.size(),
                    state.bucket_seqs.size(), bucket_seqs.size(), state.initial_tablet_pos,
                    start_tablet_idx);
        }
        return Status::OK();
    }

    PartitionState state;
    state.partition_id = partition_id;
    state.tablets = tablets;
    state.bucket_seqs = bucket_seqs;
    state.initial_tablet_pos = start_tablet_idx;
    state.tablet_pos = start_tablet_idx;
    state.current_tablet_id = state.tablets[state.tablet_pos];

    _partition_states.emplace(partition_id, std::move(state));
    LOG(INFO) << "FIND_TABLET_RANDOM_BUCKET: load_id=" << _load_id << ", partition=" << partition_id
              << ", local tablet count=" << tablets.size()
              << ", start tablet=" << _partition_states.at(partition_id).current_tablet_id;
    return Status::OK();
}

int64_t AdaptiveRandomBucketState::current_tablet(int64_t partition_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _partition_states.find(partition_id);
    if (it == _partition_states.end()) {
        return -1;
    }
    return it->second.current_tablet_id;
}

void AdaptiveRandomBucketState::rotate_by_tablet(int64_t partition_id, int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto state_it = _partition_states.find(partition_id);
    if (state_it == _partition_states.end()) {
        return;
    }
    auto& state = state_it->second;
    if (state.current_tablet_id != tablet_id) {
        return;
    }
    int32_t next_pos = (state.tablet_pos + 1) % static_cast<int32_t>(state.tablets.size());
    LOG(INFO) << "FIND_TABLET_RANDOM_BUCKET: load_id=" << _load_id
              << ", partition=" << state.partition_id << " rotate tablet "
              << state.current_tablet_id << " -> " << state.tablets[next_pos]
              << " after tablet=" << tablet_id << " memtable flushed";
    state.tablet_pos = next_pos;
    state.current_tablet_id = state.tablets[next_pos];
}

} // namespace doris
