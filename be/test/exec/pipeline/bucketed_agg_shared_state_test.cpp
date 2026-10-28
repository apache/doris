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

#include <gtest/gtest.h>

#include <thread>

#include "exec/common/agg_utils.h"
#include "exec/pipeline/dependency.h"

namespace doris {

// Source instances merge different buckets concurrently, so each of them needs its own merge
// arena. The arenas are owned by the shared state and are independent of the sink count and
// of the number of buckets.
TEST(BucketedAggSharedStateTest, InitInstancesCreatesOneMergeArenaPerSource) {
    BucketedAggSharedState state;
    state.create_source_dependencies(3, 0, 0, "BUCKETED_AGG_SOURCE");
    ASSERT_TRUE(state.init_instances(2, [] { return Status::OK(); }).ok());

    ASSERT_EQ(state.source_merge_arenas.size(), 3);
    for (size_t i = 0; i < state.source_merge_arenas.size(); ++i) {
        ASSERT_NE(state.source_merge_arenas[i], nullptr);
        // No memory is reserved until a merge allocates from the arena.
        EXPECT_EQ(state.source_merge_arenas[i]->size(), 0);
        for (size_t j = 0; j < i; ++j) {
            EXPECT_NE(state.source_merge_arenas[i].get(), state.source_merge_arenas[j].get());
        }
    }
}

TEST(BucketedAggSharedStateTest, BucketRoutingDoesNotConstrainPhmapProbeOrFingerprint) {
    // At capacity 2^18 - 1, phmap uses bits [7, 24] for its initial probe.
    // Fixing a bucket must leave both halves of that index space available.
    // The old (hash >> 24) & 255 routing fails this invariant.
    for (int num_buckets : {1, 2, 4, 8, 16, 32, 64, 128, 256}) {
        for (int bucket = 0; bucket < num_buckets; ++bucket) {
            for (size_t slot : {0UL, (1UL << 17) - 1, 1UL << 17, (1UL << 18) - 1}) {
                for (size_t fingerprint : {0UL, 127UL}) {
                    size_t hash = (size_t(bucket) << 56) | (slot << 7) | fingerprint;
                    EXPECT_EQ(BucketedAggSharedState::bucket_index(hash, num_buckets), bucket);
                    EXPECT_EQ((hash >> 7) & ((1UL << 18) - 1), slot);
                }
            }
        }
    }
}

// GTest assertion macros inflate the complexity of this configuration matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(BucketedAggSharedStateTest, ConfiguredBucketsAndAutomaticSelection) {
    for (int sinks : {1, 4}) {
        for (int requested : {0, 1, 2, 4, 8, 16, 32, 64, 128, 256}) {
            BucketedAggSharedState state;
            state.create_source_dependencies(3, 0, 0, "BUCKETED_AGG_SOURCE");
            ASSERT_TRUE(state.init_instances(
                                     sinks, [] { return Status::OK(); }, requested)
                                .ok());
            int expected = requested == 0 ? 256 : requested;
            ASSERT_EQ(state.num_buckets, expected);
            ASSERT_EQ(state.per_instance_data.size(), sinks);
            for (const auto& instance : state.per_instance_data) {
                ASSERT_EQ(instance.bucket_agg_data.size(), expected);
            }
            for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
                EXPECT_EQ(state.bucket_states[b].merged_instances.size(), b < expected ? sinks : 0);
            }
            EXPECT_EQ(state.source_merge_arenas.size(), 3);
        }
    }
    BucketedAggSharedState serial_state;
    serial_state.create_source_dependencies(1, 0, 0, "BUCKETED_AGG_SOURCE");
    ASSERT_TRUE(serial_state
                        .init_instances(
                                1, [] { return Status::OK(); }, 0)
                        .ok());
    EXPECT_EQ(serial_state.num_buckets, 1);
    EXPECT_EQ(serial_state.per_instance_data[0].bucket_agg_data.size(), 1);
}

TEST(BucketedAggSharedStateTest, ConcurrentInitializationPublishesOneConfiguration) {
    BucketedAggSharedState state;
    state.create_source_dependencies(3, 0, 0, "BUCKETED_AGG_SOURCE");
    std::atomic<int> metadata_calls {0};
    auto initialize = [&] {
        EXPECT_TRUE(state.init_instances(
                                 4,
                                 [&] {
                                     ++metadata_calls;
                                     return Status::OK();
                                 },
                                 16)
                            .ok());
        EXPECT_EQ(state.num_buckets, 16);
        EXPECT_EQ(state.per_instance_data.size(), 4);
        EXPECT_EQ(state.per_instance_data[0].bucket_agg_data.size(), 16);
    };
    std::thread first(initialize);
    std::thread second(initialize);
    initialize();
    first.join();
    second.join();
    EXPECT_EQ(metadata_calls, 1);
}

TEST(BucketedAggSharedStateTest, InvalidConfigurationIsRejectedBeforeAllocation) {
    for (int requested : {-1, 3, 255, 257, 512}) {
        BucketedAggSharedState state;
        bool metadata_called = false;
        auto initialize = [&] {
            metadata_called = true;
            return Status::OK();
        };
        EXPECT_FALSE(state.init_instances(2, initialize, requested).ok());
        EXPECT_FALSE(state.init_instances(2, initialize, requested).ok());
        EXPECT_FALSE(metadata_called);
        EXPECT_TRUE(state.per_instance_data.empty());
    }
}

} // namespace doris
