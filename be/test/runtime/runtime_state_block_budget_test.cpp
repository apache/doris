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

#include "common/config.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/block_budget.h"

namespace doris {

// ---------------------------------------------------------------------------
// RuntimeState::batch_size()
// ---------------------------------------------------------------------------

class RuntimeStateBatchSizeTest : public ::testing::Test {
protected:
    RuntimeState state;
};

TEST_F(RuntimeStateBatchSizeTest, DefaultWhenUnset) {
    EXPECT_EQ(state.batch_size(), 4062);
}

TEST_F(RuntimeStateBatchSizeTest, NormalValue) {
    state._query_options.__set_batch_size(4096);
    EXPECT_EQ(state.batch_size(), 4096);
}

TEST_F(RuntimeStateBatchSizeTest, ClampToMin) {
    state._query_options.__set_batch_size(0);
    EXPECT_EQ(state.batch_size(), 1);

    state._query_options.__set_batch_size(-100);
    EXPECT_EQ(state.batch_size(), 1);
}

TEST_F(RuntimeStateBatchSizeTest, ClampToMax) {
    state._query_options.__set_batch_size(100000);
    EXPECT_EQ(state.batch_size(), 65535);
}

TEST_F(RuntimeStateBatchSizeTest, ExactBoundaries) {
    state._query_options.__set_batch_size(1);
    EXPECT_EQ(state.batch_size(), 1);

    state._query_options.__set_batch_size(65535);
    EXPECT_EQ(state.batch_size(), 65535);
}

TEST_F(RuntimeStateBatchSizeTest, ConstructedBlockBudgetUsesBatchSizeRows) {
    state._query_options.__set_batch_size(4096);
    EXPECT_EQ(BlockBudget(state.batch_size(), state.preferred_block_size_bytes()).max_rows, 4096UL);
}

// ---------------------------------------------------------------------------
// RuntimeState::preferred_block_size_bytes()
// ---------------------------------------------------------------------------

class RuntimeStateAdaptiveBatchSizeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_enable_adaptive = config::enable_adaptive_batch_size;
        config::enable_adaptive_batch_size = true;
    }

    void TearDown() override { config::enable_adaptive_batch_size = _saved_enable_adaptive; }

    bool _saved_enable_adaptive = false;
};

class RuntimeStateBlockSizeBytesTest : public RuntimeStateAdaptiveBatchSizeTest {
protected:
    RuntimeState state;
};

TEST_F(RuntimeStateBlockSizeBytesTest, DefaultWhenUnset) {
    // Field not set → default 8MB.
    EXPECT_EQ(state.preferred_block_size_bytes(), 8388608UL);
}

TEST_F(RuntimeStateBlockSizeBytesTest, NormalValue) {
    state._query_options.__set_preferred_block_size_bytes(4194304L); // 4MB
    EXPECT_EQ(state.preferred_block_size_bytes(), 4194304UL);
}

TEST_F(RuntimeStateBlockSizeBytesTest, ZeroClampsToMin) {
    // FE rejects 0, but BE still clamps direct thrift / mixed-version inputs defensively.
    state._query_options.__set_preferred_block_size_bytes(0);
    EXPECT_EQ(state.preferred_block_size_bytes(), 1048576UL);
}

TEST_F(RuntimeStateBlockSizeBytesTest, ClampToMin) {
    // Non-zero values below 1MB should be clamped to 1MB.
    state._query_options.__set_preferred_block_size_bytes(50);
    EXPECT_EQ(state.preferred_block_size_bytes(), 1048576UL); // 1MB
}

TEST_F(RuntimeStateBlockSizeBytesTest, ClampToMax) {
    // Values above 512MB should be clamped to 512MB.
    state._query_options.__set_preferred_block_size_bytes(1073741824L); // 1GB
    EXPECT_EQ(state.preferred_block_size_bytes(), 536870912UL);         // 512MB
}

TEST_F(RuntimeStateBlockSizeBytesTest, ExactBoundaries) {
    state._query_options.__set_preferred_block_size_bytes(1048576L); // 1MB
    EXPECT_EQ(state.preferred_block_size_bytes(), 1048576UL);

    state._query_options.__set_preferred_block_size_bytes(536870912L); // 512MB
    EXPECT_EQ(state.preferred_block_size_bytes(), 536870912UL);
}

TEST_F(RuntimeStateBlockSizeBytesTest, DisabledWhenConfigOff) {
    config::enable_adaptive_batch_size = false;
    state._query_options.__set_preferred_block_size_bytes(8388608L);
    EXPECT_EQ(state.preferred_block_size_bytes(), 536870912UL);
    EXPECT_EQ(BlockBudget(state.batch_size(), state.preferred_block_size_bytes()).max_bytes,
              536870912UL);
}

// ---------------------------------------------------------------------------
// MockRuntimeState: verify the test override bypasses clamping
// ---------------------------------------------------------------------------

class MockRuntimeStateBlockBudgetTest : public RuntimeStateAdaptiveBatchSizeTest {
protected:
    MockRuntimeState state;
};

TEST_F(MockRuntimeStateBlockBudgetTest, PreferredBlockSizeBypassesClamping) {
    state._query_options.__set_preferred_block_size_bytes(50);
    EXPECT_EQ(state.preferred_block_size_bytes(), 50UL);
}

TEST_F(MockRuntimeStateBlockBudgetTest, PreferredBlockSizeDefaultFallback) {
    // When not set, falls back to base class default (8MB).
    EXPECT_EQ(state.preferred_block_size_bytes(), 8388608UL);
}

TEST_F(MockRuntimeStateBlockBudgetTest, BatchSizeOverride) {
    // MockRuntimeState returns _batch_size member directly.
    state._batch_size = 256;
    EXPECT_EQ(state.batch_size(), 256);
}

TEST_F(MockRuntimeStateBlockBudgetTest, ConfigOffStillDisablesAdaptiveBytes) {
    config::enable_adaptive_batch_size = false;
    state._query_options.__set_preferred_block_size_bytes(50);
    EXPECT_EQ(state.preferred_block_size_bytes(), 536870912UL);
}

} // namespace doris
