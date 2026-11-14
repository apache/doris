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

} // namespace doris
