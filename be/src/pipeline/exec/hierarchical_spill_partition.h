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
#include <string>

namespace doris::pipeline {

// Fixed 8-way fanout (3 bits per level).
// This keeps the hierarchy bounded and makes the path encoding compact and stable.
static constexpr uint32_t kSpillFanout = 8;
static constexpr uint32_t kSpillBitsPerLevel = 3;
static constexpr uint32_t kSpillMaxDepth = 6;

static_assert(kSpillMaxDepth <= 7,
              "Max depth must be <= 7 to fit level in high 8 bits of partition key");

// SpillPartitionId legend (8-way fanout, 3 bits per level):
//
//   level: tree depth, root partitions are level 0.
//   path : bit-packed child indices from low bits to high bits.
//
//   path bit layout:
//     [ ... | L2(3b) | L1(3b) | L0(3b) ]
//                              ^^^^^^^^
//                              base partition index (level 0)
//
//   example path:
//     level 0 -> child 5 (101)
//     level 1 -> child 2 (010)
//     level 2 -> child 7 (111)
//
//     path = 0b111_010_101
//              L2  L1  L0
//
//   key() packs into a single uint32:
//     key = (level << 24) | path
struct SpillPartitionId {
    // Depth in the split tree. Root partitions are at level 0.
    uint32_t level = 0;
    // Bit-packed child indices along the path (3 bits per level).
    // Lowest 3 bits represent the base partition index at level 0.
    uint32_t path = 0;

    // Pack (level,path) into a compact key for unordered_map lookup.
    // Assumes max depth is small; level is stored in high 8 bits.
    uint32_t key() const { return (level << 24) | path; }

    SpillPartitionId child(uint32_t child_index) const {
        const auto next_level = level + 1;
        return {.level = next_level,
                .path = path | (child_index << (next_level * kSpillBitsPerLevel))};
    }

    std::string to_string() const {
        return "partition(" + std::to_string(level) + ", " + std::to_string(path) + ")";
    }
};

struct SpillPartitionUtils {
    static inline constexpr uint32_t spill_partition_index(uint32_t hash, uint32_t level) {
        // Select 3 bits for the given level, yielding an index in [0, 7].
        return (hash >> (level * kSpillBitsPerLevel)) & (kSpillFanout - 1);
    }

    static inline constexpr uint32_t base_partition_index(const SpillPartitionId& id) {
        return id.path & (kSpillFanout - 1);
    }

    template <typename PartitionMap>
    static inline constexpr SpillPartitionId find_leaf_partition_for_hash(
            uint32_t hash, const PartitionMap& partitions) {
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
};

} // namespace doris::pipeline
