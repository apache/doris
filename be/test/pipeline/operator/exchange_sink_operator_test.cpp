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

#include "pipeline/exec/exchange_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_data_stream_sender.h"
#include "testutil/mock/mock_descriptors.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

TUniqueId create_TUniqueId(int64_t hi, int64_t lo) {
    TUniqueId t {};
    t.hi = hi;
    t.lo = lo;
    return t;
}

TUniqueId fragment_instance_id = create_TUniqueId(2, 2);

using namespace vectorized;
struct MockExchangeLocalState : public ExchangeSinkLocalState {
    MockExchangeLocalState(ExchangeSinkOperatorX* parent, RuntimeState* state)
            : ExchangeSinkLocalState(parent, state) {}

    void _create_channels() override {
        for (auto c : mock_channel) {
            channels.push_back(c);
        }
    }
    std::vector<std::shared_ptr<MockChannel>> mock_channel;
};

struct MockExchangeSinkOperatorX : public ExchangeSinkOperatorX {
    MockExchangeSinkOperatorX(OperatorContext& ctx)
            : ExchangeSinkOperatorX(
                      &ctx.state,
                      MockRowDescriptor {{std::make_shared<DataTypeInt32>()}, &ctx.pool}, 0,
                      TDataStreamSink {}, {}, {}) {}

    void _init_sink_buffer() override {
        std::vector<InstanceLoId> ins_ids {fragment_instance_id.lo};
        _sink_buffer = _create_buffer(_state, ins_ids);
    }
};

struct ChannelInfo {
    bool is_local;
    TUniqueId fragment_instance_id;
};

auto create_exchange_sink(std::vector<ChannelInfo> channel_info) {
    std::shared_ptr<OperatorContext> ctx = std::make_shared<OperatorContext>();

    ctx->state._fragment_instance_id = fragment_instance_id;

    std::shared_ptr<MockExchangeSinkOperatorX> op =
            std::make_shared<MockExchangeSinkOperatorX>(*ctx);
    EXPECT_TRUE(op->prepare(&ctx->state));

    auto local_state = std::make_unique<MockExchangeLocalState>(op.get(), &ctx->state);

    std::vector<std::shared_ptr<MockChannel>> mock_channel;
    for (auto info : channel_info) {
        mock_channel.push_back(std::make_shared<MockChannel>(
                local_state.get(), info.fragment_instance_id, info.is_local));
    }
    local_state->mock_channel = mock_channel;

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = &ctx->profile,
                             .sender_id = 0,
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .tsink = TDataSink {}};
    EXPECT_TRUE(local_state->init(&ctx->state, info).ok());
    ctx->state.emplace_sink_local_state(0, std::move(local_state));

    // open local state
    auto* sink_local_state = ctx->state.get_sink_local_state();
    EXPECT_TRUE(sink_local_state->open(&ctx->state).ok());

    return std::make_tuple(op, ctx, mock_channel);
}

auto test_for_no_partitioned(std::vector<ChannelInfo> channel_info) {
    auto [op, ctx, mock_channel] = create_exchange_sink(channel_info);

    {
        //execute sink
        bool eos = true;
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        auto st = op->sink(&ctx->state, &block, eos);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        for (auto c : mock_channel) {
            auto block = c->get_block();
            EXPECT_TRUE(ColumnHelper::block_equal(
                    ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}), block));
        }
        EXPECT_EQ(mock_channel.size(), channel_info.size());
        for (int i = 0; i < mock_channel.size(); i++) {
            auto c = mock_channel[i];
            EXPECT_EQ(channel_info[i].is_local, c->is_local());
        }
    }
}

TEST(ExchangeSinkOperatorTest, test_remote_and_local) {
    test_for_no_partitioned({{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 2)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 3)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 4)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 5)}});
}

TEST(ExchangeSinkOperatorTest, test_all_local) {
    test_for_no_partitioned({{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 2)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 3)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 4)},
                             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 5)}});
}

TEST(ExchangeSinkOperatorTest, test_all_remote) {
    test_for_no_partitioned({{.is_local = false, .fragment_instance_id = create_TUniqueId(1, 1)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 2)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 3)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 4)},
                             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 5)}});
}

TEST(ExchangeSinkOperatorTest, test_some_api) {
    auto [op, ctx, mock_channel] = create_exchange_sink(
            {{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)},
             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 2)},
             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 3)},
             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 4)},
             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 5)}});

    auto* sink_local_state = ctx->state.get_sink_local_state();
    auto* exchange_sink_local_state = dynamic_cast<MockExchangeLocalState*>(sink_local_state);
    EXPECT_TRUE(exchange_sink_local_state != nullptr);

    exchange_sink_local_state->_working_channels_count = 5;

    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 5);

    EXPECT_FALSE(exchange_sink_local_state->_finish_dependency->ready());

    exchange_sink_local_state->on_channel_finished(1);
    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 4);

    exchange_sink_local_state->on_channel_finished(2);
    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 3);

    exchange_sink_local_state->on_channel_finished(3);
    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 2);

    exchange_sink_local_state->on_channel_finished(4);
    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 1);

    exchange_sink_local_state->on_channel_finished(5);
    EXPECT_EQ(exchange_sink_local_state->_working_channels_count, 0);

    EXPECT_TRUE(exchange_sink_local_state->_finish_dependency->ready());
}

} // namespace doris::pipeline
