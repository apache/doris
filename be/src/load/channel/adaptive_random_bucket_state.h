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

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "util/uid_util.h"

namespace doris {

class AdaptiveRandomBucketState {
public:
    explicit AdaptiveRandomBucketState(UniqueId load_id) : _load_id(load_id) {}

    void init_partition(int32_t sender_id, int64_t partition_id,
                        const std::vector<int64_t>& tablets,
                        const std::vector<int32_t>& bucket_seqs, int32_t start_tablet_idx);
    int64_t current_tablet(int32_t sender_id, int64_t partition_id);
    void rotate_by_tablet(int32_t sender_id, int64_t partition_id, int64_t tablet_id);

private:
    struct PartitionState {
        int64_t partition_id = -1;
        std::vector<int64_t> tablets;
        std::vector<int32_t> bucket_seqs;
        int32_t tablet_pos = 0;
        int64_t current_tablet_id = -1;
    };

    std::mutex _mutex;
    UniqueId _load_id;
    std::unordered_map<int32_t, std::unordered_map<int64_t, PartitionState>>
            _sender_partition_states;
};

} // namespace doris
