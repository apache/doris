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
//
// A small shared helper for hierarchical spill partitioning (multi-level split).

#pragma once

#include <cstdint>

namespace doris::pipeline {

// Fixed 8-way fanout (3 bits per level), same as partitioned hash join spill.
// This keeps the hierarchy bounded and makes the path encoding compact and stable.
static constexpr uint32_t kSpillFanout = 8;
static constexpr uint32_t kSpillBitsPerLevel = 3;
static constexpr uint32_t kSpillMaxDepth = 6;

struct SpillPartitionId {
    uint32_t level = 0;
    uint32_t path = 0;

    // Pack (level,path) into a compact key for unordered_map lookup.
    // Assumes max depth is small; level is stored in high 8 bits.
    uint32_t key() const { return (level << 24) | path; }

    SpillPartitionId child(uint32_t child_index) const {
        return {.level = level + 1,
                .path = path | (child_index << ((level + 1) * kSpillBitsPerLevel))};
    }
};

inline uint32_t spill_partition_index(uint32_t hash, uint32_t level) {
    // Select 3 bits for the given level, yielding an index in [0, 7].
    return (hash >> (level * kSpillBitsPerLevel)) & (kSpillFanout - 1);
}

inline uint32_t base_partition_index(const SpillPartitionId& id) {
    return id.path & (kSpillFanout - 1);
}

template <typename PartitionMap>
inline SpillPartitionId find_leaf_partition_for_hash(uint32_t hash,
                                                     const PartitionMap& partitions) {
    // Follow split hierarchy so rows land in the final leaf partition.
    // If a partition was split, we keep descending until we reach a non-split partition
    // or hit the maximum depth.
    SpillPartitionId id {.level = 0, .path = spill_partition_index(hash, 0)};
    auto it = partitions.find(id.key());
    while (it != partitions.end() && it->second.is_split && id.level < kSpillMaxDepth) {
        const auto child_index = spill_partition_index(hash, id.level + 1);
        id = id.child(child_index);
        it = partitions.find(id.key());
    }
    return id;
}

} // namespace doris::pipeline
