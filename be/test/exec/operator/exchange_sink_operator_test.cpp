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

#include "exec/operator/exchange_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_data_stream_sender.h"
#include "testutil/mock/mock_descriptors.h"
namespace doris {

template <typename T>
class ScopedConfigValue {
public:
    ScopedConfigValue(T& target, T value) : _target(target), _old(target) { _target = value; }
    ~ScopedConfigValue() { _target = _old; }

private:
    T& _target;
    T _old;
};

TUniqueId create_TUniqueId(int64_t hi, int64_t lo) {
    TUniqueId t {};
    t.hi = hi;
    t.lo = lo;
    return t;
}

TUniqueId fragment_instance_id = create_TUniqueId(2, 2);

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
    MockExchangeSinkOperatorX(OperatorContext& ctx,
                              TPartitionType::type partition_type = TPartitionType::UNPARTITIONED)
            : ExchangeSinkOperatorX(
                      &ctx.state,
                      MockRowDescriptor {{std::make_shared<DataTypeInt32>()}, &ctx.pool}, 0,
                      TDataStreamSink {}, {}, {}) {
        _part_type = partition_type;
    }

    void _init_sink_buffer() override {
        std::vector<InstanceLoId> ins_ids {fragment_instance_id.lo};
        _sink_buffer = _create_buffer(_state, ins_ids);
    }
};

struct ChannelInfo {
    bool is_local;
    TUniqueId fragment_instance_id;
};

template <typename ChannelType = MockChannel>
auto create_exchange_sink(std::vector<ChannelInfo> channel_info,
                          TPartitionType::type partition_type = TPartitionType::UNPARTITIONED,
                          int batch_size = 4096) {
    std::shared_ptr<OperatorContext> ctx = std::make_shared<OperatorContext>();

    ctx->state._fragment_instance_id = fragment_instance_id;
    ctx->state._batch_size = batch_size;

    std::shared_ptr<MockExchangeSinkOperatorX> op =
            std::make_shared<MockExchangeSinkOperatorX>(*ctx, partition_type);
    EXPECT_TRUE(op->prepare(&ctx->state));

    auto local_state = std::make_unique<MockExchangeLocalState>(op.get(), &ctx->state);

    std::vector<std::shared_ptr<ChannelType>> mock_channel;
    for (auto info : channel_info) {
        mock_channel.push_back(std::make_shared<ChannelType>(
                local_state.get(), info.fragment_instance_id, info.is_local));
        local_state->mock_channel.push_back(mock_channel.back());
    }

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

// Retain the actual outgoing holders so tests can reuse the input before deserializing,
// and exercise the broadcast memory limiter while remote consumers have not drained.
struct RetainingBroadcastChannel : MockChannel {
    using MockChannel::MockChannel;

    Status send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& holder, bool eos) override {
        RETURN_IF_ERROR(send_status);
        holders.push_back(holder);
        end_of_stream.push_back(eos);
        return Status::OK();
    }

    std::vector<std::shared_ptr<BroadcastPBlockHolder>> holders;
    std::vector<bool> end_of_stream;
    Status send_status;
};

auto create_remote_broadcast_sink() {
    return create_exchange_sink<RetainingBroadcastChannel>(
            {{.is_local = false, .fragment_instance_id = create_TUniqueId(1, 1)},
             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 2)}},
            TPartitionType::UNPARTITIONED, 4);
}

void expect_broadcast_block(const Block& expected,
                            const std::shared_ptr<BroadcastPBlockHolder>& holder) {
    Block actual;
    size_t uncompressed_size = 0;
    int64_t uncompress_time = 0;
    auto status = actual.deserialize(*holder->get_block(), &uncompressed_size, &uncompress_time);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(expected.columns(), actual.columns());
    ASSERT_EQ(expected.rows(), actual.rows());
    for (size_t i = 0; i < expected.columns(); ++i) {
        const auto& expected_column = expected.get_by_position(i);
        const auto& actual_column = actual.get_by_position(i);
        EXPECT_EQ(expected_column.name, actual_column.name);
        EXPECT_TRUE(expected_column.type->equals(*actual_column.type));
        EXPECT_TRUE(ColumnHelper::column_equal(
                expected_column.column->convert_to_full_column_if_const(),
                actual_column.column->convert_to_full_column_if_const()));
    }
}

void expect_no_broadcast_accumulation(const ExchangeSinkLocalState& local) {
    EXPECT_EQ(local._serializer.get_block(), nullptr);
    EXPECT_EQ(local._serializer.mem_usage(), 0);
    EXPECT_EQ(local._merge_block_timer->value(), 0);
}

TEST(ExchangeSinkOperatorTest, remote_full_batches_skip_accumulation) {
    auto [op, ctx, channels] = create_remote_broadcast_sink();
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    auto input = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    expect_no_broadcast_accumulation(*local);
    EXPECT_EQ(local->_bytes_sent_counter->value(),
              channels[0]->holders[0]->get_block()->column_values().size() * channels.size());
    EXPECT_EQ(channels[0]->holders[0], channels[1]->holders[0]);
    EXPECT_FALSE(channels[0]->holders[0]->serialized_block().empty());

    // Clearing and reusing the producer's columns cannot affect the queued PBlock.
    input.clear_column_data();
    input = ColumnHelper::create_block<DataTypeInt32>({5, 6, 7, 8, 9});
    ASSERT_TRUE(op->sink(&ctx->state, &input, true).ok());
    input.clear_column_data();
    for (const auto& channel : channels) {
        ASSERT_EQ(channel->holders.size(), 2);
        expect_broadcast_block(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4}),
                               channel->holders[0]);
        expect_broadcast_block(ColumnHelper::create_block<DataTypeInt32>({5, 6, 7, 8, 9}),
                               channel->holders[1]);
        EXPECT_EQ(channel->end_of_stream, (std::vector<bool> {false, true}));
    }
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
    EXPECT_EQ(local->_memory_used_counter->current_value(), 0);
}

TEST(ExchangeSinkOperatorTest, remote_broadcast_preserves_pending_rows_and_empty_eos) {
    auto [op, ctx, channels] = create_remote_broadcast_sink();
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    Block input;
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
    input = ColumnHelper::create_block<DataTypeInt32>({1});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    input = ColumnHelper::create_block<DataTypeInt32>({2});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_TRUE(channels[0]->holders.empty());
    ASSERT_EQ(local->_serializer.get_block()->rows(), 2);

    input = ColumnHelper::create_block<DataTypeInt32>({3, 4, 5, 6});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    ASSERT_EQ(channels[0]->holders.size(), 1);
    expect_broadcast_block(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5, 6}),
                           channels[0]->holders[0]);
    auto* buffer = local->_serializer.get_block();
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(buffer->rows(), 0);
    auto merge_time = local->_merge_block_timer->value();

    input = ColumnHelper::create_block<DataTypeInt32>({7, 8, 9, 10});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_EQ(local->_serializer.get_block(), buffer);
    EXPECT_EQ(local->_merge_block_timer->value(), merge_time);
    expect_broadcast_block(input, channels[0]->holders[1]);

    input.clear_column_data();
    ASSERT_TRUE(op->sink(&ctx->state, &input, true).ok());
    ASSERT_EQ(channels[0]->holders.size(), 3);
    EXPECT_EQ(channels[0]->holders[2]->get_block()->ByteSizeLong(), 0);
    EXPECT_TRUE(channels[0]->end_of_stream.back());
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
    EXPECT_EQ(local->_memory_used_counter->current_value(), 0);
}

TEST(ExchangeSinkOperatorTest, remote_broadcast_complex_columns_match_merged_serialization) {
    for (const auto compression :
         {segment_v2::NO_COMPRESSION, segment_v2::LZ4, segment_v2::SNAPPY}) {
        auto [op, ctx, channels] = create_remote_broadcast_sink();
        op->_compression_type = compression;
        auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
        Block input;
        input.insert(ColumnHelper::create_column_with_name<DataTypeString>(
                {"", std::string(8192, 'a'), "third", "fourth"}));
        input.insert(ColumnHelper::create_nullable_column_with_name<DataTypeInt32>({1, 2, 3, 4},
                                                                                   {0, 1, 0, 1}));
        input.insert(
                {ColumnConst::create(ColumnHelper::create_column<DataTypeString>({"constant"}), 4),
                 std::make_shared<DataTypeString>(), "constant"});
        auto offsets = ColumnOffset64::create();
        offsets->get_data().assign({2, 2, 3, 5});
        input.insert({ColumnArray::create(ColumnHelper::create_nullable_column<DataTypeInt32>(
                                                  {1, 2, 3, 4, 5}, {0, 1, 0, 0, 0}),
                                          std::move(offsets)),
                      std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "array"});

        // Materialize the old accumulation path as the reference, including constant expansion.
        auto merged = MutableBlock::create_unique(input.clone_empty());
        ASSERT_TRUE(merged->merge(input).ok());
        auto expected = merged->to_block();
        ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
        EXPECT_EQ(local->_serializer.get_block(), nullptr);
        input.clear();
        for (const auto& channel : channels) {
            ASSERT_EQ(channel->holders.size(), 1);
            expect_broadcast_block(expected, channel->holders[0]);
        }
    }
}

TEST(ExchangeSinkOperatorTest, single_remote_full_batch_then_empty_eos) {
    auto [op, ctx, channels] = create_exchange_sink<RetainingBroadcastChannel>(
            {{.is_local = false, .fragment_instance_id = create_TUniqueId(1, 1)}},
            TPartitionType::UNPARTITIONED, 4);
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    auto input = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
    ASSERT_EQ(channels[0]->holders.size(), 1);
    expect_broadcast_block(input, channels[0]->holders[0]);
    EXPECT_TRUE(channels[0]->holders[0]->serialized_block().empty());

    input.clear();
    ASSERT_TRUE(op->sink(&ctx->state, &input, true).ok());
    ASSERT_EQ(channels[0]->holders.size(), 2);
    EXPECT_EQ(channels[0]->holders[1]->get_block()->ByteSizeLong(), 0);
    EXPECT_TRUE(channels[0]->end_of_stream.back());
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
}

TEST(ExchangeSinkOperatorTest, small_remote_batch_waits_for_eos_even_with_large_capacity) {
    auto [op, ctx, channels] = create_remote_broadcast_sink();
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    ctx->state.get_query_ctx()->set_low_memory_mode();
    auto input = ColumnHelper::create_block<DataTypeString>({std::string(8 * 1024 * 1024, 'x')});
    input.clear_column_data();
    {
        auto columns = input.mutate_columns_scoped();
        columns.mutable_columns()[0]->insert_data("one", 3);
    }
    ASSERT_GT(input.allocated_bytes(), 4 * 1024 * 1024);
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_TRUE(channels[0]->holders.empty());
    ASSERT_NE(local->_serializer.get_block(), nullptr);
    EXPECT_EQ(local->_serializer.get_block()->rows(), 1);
    input.clear_column_data();
    ASSERT_TRUE(op->sink(&ctx->state, &input, true).ok());
    ASSERT_EQ(channels[0]->holders.size(), 1);
    expect_broadcast_block(ColumnHelper::create_block<DataTypeString>({"one"}),
                           channels[0]->holders[0]);
    EXPECT_TRUE(channels[0]->end_of_stream[0]);
}

TEST(ExchangeSinkOperatorTest, mixed_broadcast_keeps_accumulation) {
    auto [op, ctx, channels] = create_exchange_sink<RetainingBroadcastChannel>(
            {{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)},
             {.is_local = false, .fragment_instance_id = create_TUniqueId(1, 2)}},
            TPartitionType::UNPARTITIONED, 4);
    auto input = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    EXPECT_GT(local->_merge_block_timer->value(), 0);
    EXPECT_TRUE(ColumnHelper::block_equal(input, channels[0]->get_block()));
    ASSERT_EQ(channels[1]->holders.size(), 1);
    expect_broadcast_block(input, channels[1]->holders[0]);
}

TEST(ExchangeSinkOperatorTest, remote_broadcast_low_memory_releases_reusable_buffer) {
    auto [op, ctx, channels] = create_remote_broadcast_sink();
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    for (int i = 0; i < 2; ++i) {
        auto small = ColumnHelper::create_block<DataTypeInt32>({1, 2});
        ASSERT_TRUE(op->sink(&ctx->state, &small, false).ok());
    }
    ASSERT_NE(local->_serializer.get_block(), nullptr);
    ASSERT_EQ(local->_serializer.get_block()->rows(), 0);
    ctx->state.get_query_ctx()->set_low_memory_mode();
    auto input = ColumnHelper::create_block<DataTypeInt32>({3, 4, 5, 6});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_EQ(local->_serializer.get_block(), nullptr);
    EXPECT_EQ(local->_memory_used_counter->current_value(), 0);
    ASSERT_EQ(channels[0]->holders.size(), 2);
    expect_broadcast_block(input, channels[0]->holders[1]);
}

TEST(ExchangeSinkOperatorTest, remote_broadcast_retains_backpressure_and_receiver_eof) {
    auto [op, ctx, channels] = create_remote_broadcast_sink();
    auto* local = static_cast<MockExchangeLocalState*>(ctx->state.get_sink_local_state());
    auto limiter = local->_broadcast_pb_mem_limiter;
    limiter->_total_queue_blocks_count_limit = 1;
    channels[0]->set_receiver_eof(Status::EndOfFile("receiver finished"));
    auto input = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4});
    ASSERT_TRUE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_TRUE(channels[0]->holders.empty());
    ASSERT_EQ(channels[1]->holders.size(), 1);
    EXPECT_EQ(limiter->_total_queue_blocks_count, 1);
    EXPECT_GT(limiter->_total_queue_buffer_size, 0);
    EXPECT_FALSE(limiter->_broadcast_dependency->ready());
    channels[1]->holders.clear();
    EXPECT_EQ(limiter->_total_queue_blocks_count, 0);
    EXPECT_EQ(limiter->_total_queue_buffer_size, 0);
    EXPECT_TRUE(limiter->_broadcast_dependency->ready());

    channels[1]->send_status = Status::InternalError("send failed");
    EXPECT_FALSE(op->sink(&ctx->state, &input, false).ok());
    EXPECT_EQ(limiter->_total_queue_blocks_count, 0);
    EXPECT_EQ(limiter->_total_queue_buffer_size, 0);
    EXPECT_TRUE(limiter->_broadcast_dependency->ready());
    channels[1]->set_receiver_eof(Status::EndOfFile("receiver finished"));
    EXPECT_TRUE(op->sink(&ctx->state, &input, true).is<ErrorCode::END_OF_FILE>());
}

auto test_for_no_partitioned(std::vector<ChannelInfo> channel_info) {
    auto [op, ctx, mock_channel] = create_exchange_sink(channel_info);

    {
        //execute sink
        bool eos = true;
        Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
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

TEST(ExchangeSinkOperatorTest, shared_writer_scaling_state_is_synchronized) {
    auto [op, ctx, mock_channel] = create_exchange_sink(
            {{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)}});
    constexpr size_t thread_count = 8;
    constexpr size_t updates_per_thread = 100;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back([&] {
            for (size_t update = 0; update < updates_per_thread; ++update) {
                op->update_writer_scaling_for_test(1, 1);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto [data_processed, writer_count] = op->writer_scaling_state_for_test();
    EXPECT_EQ(data_processed, thread_count * updates_per_thread);
    EXPECT_EQ(writer_count, 1);
}

TEST(ExchangeSinkOperatorTest, all_local_external_sink_counts_bytes_before_move) {
    ScopedConfigValue<int64_t> threshold_guard(
            config::table_sink_non_partition_write_scaling_data_processed_threshold, 1);
    auto [op, ctx, mock_channel] = create_exchange_sink(
            {{.is_local = true, .fragment_instance_id = create_TUniqueId(1, 1)},
             {.is_local = true, .fragment_instance_id = create_TUniqueId(1, 2)}},
            TPartitionType::EXTERNAL_TABLE_SINK_UNPARTITIONED);
    Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    const auto block_bytes = block.bytes();

    auto st = op->sink(&ctx->state, &block, false);

    ASSERT_TRUE(st.ok()) << st.msg();
    auto [data_processed, writer_count] = op->writer_scaling_state_for_test();
    EXPECT_EQ(data_processed, block_bytes);
    EXPECT_EQ(writer_count, 2);
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

} // namespace doris
