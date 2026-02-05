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

#include "pipeline/shuffle/exchange_writer.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "gen_cpp/Types_types.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_data_stream_sender.h"
#include "testutil/mock/mock_runtime_state.h"

using doris::MockRuntimeState;
using doris::Status;
using doris::vectorized::Block;
using doris::vectorized::ColumnHelper;
using doris::vectorized::DataTypeInt32;
using doris::vectorized::Channel;
using doris::vectorized::MockChannel;
using doris::pipeline::ExchangeSinkLocalState;

namespace doris::pipeline {

// Helper: create channels that will never actually send rows (is_receiver_eof == true),
// so writer logic can be tested without exercising Channel::add_rows / BlockSerializer.
static std::shared_ptr<Channel> make_disabled_channel(ExchangeSinkLocalState* local_state) {
    TUniqueId id;
    id.hi = 0;
    id.lo = 0;
    auto ch = std::make_shared<MockChannel>(local_state, id, /*is_local=*/true);
    ch->set_receiver_eof(Status::EndOfFile("test eof"));
    return ch;
}

static std::vector<std::shared_ptr<Channel>> make_disabled_channels(
        ExchangeSinkLocalState* local_state, size_t n) {
    std::vector<std::shared_ptr<Channel>> channels;
    channels.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        channels.emplace_back(make_disabled_channel(local_state));
    }
    return channels;
}

TEST(TrivialExchangeWriterTest, BasicDistribution) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeTrivialWriter writer {local_state};

    const size_t channel_count = 2;
    auto channels = make_disabled_channels(&local_state, channel_count);

    // rows: [1,2,3,4,5], channel_ids: [0,1,0,1,1]
    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids = {0, 1, 0, 1, 1};
    const size_t rows = channel_ids.size();

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Expect histogram: [2,3]
    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    EXPECT_EQ(writer._channel_rows_histogram[0], 2U);
    EXPECT_EQ(writer._channel_rows_histogram[1], 3U);

    // Expect row index order: [0,2,1,3,4]
    ASSERT_EQ(writer._origin_row_idx.size(), rows);
    std::vector<uint32_t> got(rows);
    for (size_t i = 0; i < rows; ++i) {
        got[i] = writer._origin_row_idx[i];
    }
    std::vector<uint32_t> expected {0, 2, 1, 3, 4};
    EXPECT_EQ(got, expected);
}

TEST(TrivialExchangeWriterTest, AllRowsToSingleChannel) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeTrivialWriter writer {local_state};

    const size_t channel_count = 3;
    auto channels = make_disabled_channels(&local_state, channel_count);

    Block block = ColumnHelper::create_block<DataTypeInt32>({10, 20, 30, 40});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids = {2, 2, 2, 2};
    const size_t rows = channel_ids.size();

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    EXPECT_EQ(writer._channel_rows_histogram[0], 0U);
    EXPECT_EQ(writer._channel_rows_histogram[1], 0U);
    EXPECT_EQ(writer._channel_rows_histogram[2], 4U);

    ASSERT_EQ(writer._origin_row_idx.size(), rows);
    std::vector<uint32_t> got(rows);
    for (size_t i = 0; i < rows; ++i) {
        got[i] = writer._origin_row_idx[i];
    }
    std::vector<uint32_t> expected {0, 1, 2, 3};
    EXPECT_EQ(got, expected);
}

TEST(TrivialExchangeWriterTest, EmptyInput) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeTrivialWriter writer {local_state};

    const size_t channel_count = 4;
    auto channels = make_disabled_channels(&local_state, channel_count);

    Block block = ColumnHelper::create_block<DataTypeInt32>({});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids {};
    const size_t rows = 0;

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    for (size_t i = 0; i < channel_count; ++i) {
        EXPECT_EQ(writer._channel_rows_histogram[i], 0U);
    }
    EXPECT_EQ(writer._origin_row_idx.size(), 0U);
}

TEST(OlapExchangeWriterTest, NeedCheckSkipsInvalidChannelIds) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeOlapWriter writer {local_state};

    const size_t channel_count = 3;
    auto channels = make_disabled_channels(&local_state, channel_count);

    // channel_ids: [0, x, 2, x, 2]
    Block block = ColumnHelper::create_block<DataTypeInt32>({10, 20, 30, 40, 50});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids = {0, 10, 2, 10, 2};
    const size_t rows = channel_ids.size();

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false, 10);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Only valid ids(less than _partition_count) should be counted: hist = [1,0,2]
    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    EXPECT_EQ(writer._channel_rows_histogram[0], 1U);
    EXPECT_EQ(writer._channel_rows_histogram[1], 0U);
    EXPECT_EQ(writer._channel_rows_histogram[2], 2U);

    // row_idx should contain rows [0,2,4] grouped by channel
    ASSERT_EQ(writer._origin_row_idx.size(), 3U);
    std::vector<uint32_t> got(3);
    for (size_t i = 0; i < 3; ++i) {
        got[i] = writer._origin_row_idx[i];
    }
    std::vector<uint32_t> expected {0, 2, 4};
    EXPECT_EQ(got, expected);
}

TEST(OlapExchangeWriterTest, NoCheckUsesAllRows) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeOlapWriter writer {local_state};

    const size_t channel_count = 2;
    auto channels = make_disabled_channels(&local_state, channel_count);

    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids = {0, 1, 0};
    const size_t rows = channel_ids.size();

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false, 10);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    EXPECT_EQ(writer._channel_rows_histogram[0], 2U);
    EXPECT_EQ(writer._channel_rows_histogram[1], 1U);

    ASSERT_EQ(writer._origin_row_idx.size(), rows);
    std::vector<uint32_t> got(rows);
    for (size_t i = 0; i < rows; ++i) {
        got[i] = writer._origin_row_idx[i];
    }
    std::vector<uint32_t> expected {0, 2, 1};
    EXPECT_EQ(got, expected);
}

TEST(OlapExchangeWriterTest, EmptyInput) {
    MockRuntimeState state;
    ExchangeSinkLocalState local_state(&state);
    ExchangeOlapWriter writer {local_state};

    const size_t channel_count = 3;
    auto channels = make_disabled_channels(&local_state, channel_count);

    Block block = ColumnHelper::create_block<DataTypeInt32>({});
    std::vector<vectorized::PartitionerBase::HashValType> channel_ids {};
    const size_t rows = 0;

    Status st = writer._channel_add_rows(&state, channels, channel_count, channel_ids, rows, &block,
                                         /*eos=*/false, 1);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(writer._channel_rows_histogram.size(), channel_count);
    for (size_t i = 0; i < channel_count; ++i) {
        EXPECT_EQ(writer._channel_rows_histogram[i], 0U);
    }
    EXPECT_EQ(writer._origin_row_idx.size(), 0U);
}

} // namespace doris::pipeline
