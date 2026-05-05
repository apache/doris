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

#include <gen_cpp/data.pb.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/exchange/vdata_stream_sender.h"
#include "exec/operator/exchange_sink_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// Tests for BlockSerializer::next_serialized_block, focusing on the byte-budget
// breakout path introduced by the adaptive batch size feature.
class BlockSerializerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_adaptive = config::enable_adaptive_batch_size;
        config::enable_adaptive_batch_size = true;
    }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }

    bool _saved_adaptive = false;
};

// With a tight byte budget, accumulating small blocks must trigger serialization
// once the merged block bytes reach the budget. Without the byte budget this would
// only fire on row-count overflow or eos.
TEST_F(BlockSerializerTest, ByteBudgetTriggersSerialize) {
    MockRuntimeState state;
    // Large row cap + tiny byte budget so the test isolates the byte-driven path.
    state._batch_size = 1 << 20;
    state._query_options.__set_batch_size(state._batch_size);
    state._query_options.__set_preferred_block_size_bytes(64);

    ExchangeSinkLocalState local_state(&state);
    BlockSerializer serializer(&local_state, /*is_local=*/true);

    PBlock pblock;
    bool serialized = false;

    // Push enough rows that the merged Int32 column exceeds 64 bytes.
    // 16 Int32 values == 64 bytes of column data; merging triggers exceeded().
    for (int i = 0; i < 32; ++i) {
        Block block = ColumnHelper::create_block<DataTypeInt32>({i});
        ASSERT_TRUE(serializer
                            .next_serialized_block(&block, &pblock, /*num_receivers=*/1,
                                                   &serialized, /*eos=*/false)
                            .ok());
        if (serialized) {
            break;
        }
    }
    EXPECT_TRUE(serialized) << "byte budget never triggered serialize";
    ASSERT_NE(serializer.get_block(), nullptr);
    EXPECT_GE(serializer.get_block()->bytes(), 64U);
}

// eos must always force serialize, regardless of budget state.
TEST_F(BlockSerializerTest, EosForcesSerialize) {
    MockRuntimeState state;
    state._batch_size = 1 << 20;
    state._query_options.__set_batch_size(state._batch_size);
    state._query_options.__set_preferred_block_size_bytes(1ULL << 30);

    ExchangeSinkLocalState local_state(&state);
    BlockSerializer serializer(&local_state, /*is_local=*/true);

    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    PBlock pblock;
    bool serialized = false;
    ASSERT_TRUE(
            serializer.next_serialized_block(&block, &pblock, 1, &serialized, /*eos=*/true).ok());
    EXPECT_TRUE(serialized);
}

// When neither budget nor eos triggers, serialization must not happen so the
// upstream caller continues to accumulate.
TEST_F(BlockSerializerTest, NoTriggerLeavesUnserialized) {
    MockRuntimeState state;
    state._batch_size = 1 << 20;
    state._query_options.__set_batch_size(state._batch_size);
    state._query_options.__set_preferred_block_size_bytes(1ULL << 30);

    ExchangeSinkLocalState local_state(&state);
    BlockSerializer serializer(&local_state, /*is_local=*/true);

    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    PBlock pblock;
    bool serialized = true; // sentinel
    ASSERT_TRUE(
            serializer.next_serialized_block(&block, &pblock, 1, &serialized, /*eos=*/false).ok());
    EXPECT_FALSE(serialized);
    ASSERT_NE(serializer.get_block(), nullptr);
    EXPECT_EQ(serializer.get_block()->rows(), 3U);
}

} // namespace doris
