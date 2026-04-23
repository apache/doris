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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "exec/common/memory.h"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/thrift_builder.h"
#include "exprs/vslot_ref.h"

namespace doris {

class LocalExchangerTest : public testing::Test {
public:
    LocalExchangerTest() = default;
    ~LocalExchangerTest() override = default;
    void SetUp() override {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                TRuntimeFilterParamsBuilder().build());
        _runtime_state = RuntimeState::create_unique(_query_id, _fragment_id, _query_options,
                                                     _query_ctx->query_globals,
                                                     ExecEnv::GetInstance(), _query_ctx.get());
    }
    void TearDown() override {}

private:
    std::unique_ptr<RuntimeState> _runtime_state;
    TUniqueId _query_id;
    int _fragment_id = 0;
    TQueryOptions _query_options;
    std::shared_ptr<QueryContext> _query_ctx;

    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
};

TEST_F(LocalExchangerTest, ShuffleExchanger) {
    int num_sink = 4;
    int num_sources = 4;
    int num_partitions = 4;
    int free_block_limit = 0;
    std::map<int, int> shuffle_idx_to_instance_idx;
    for (int i = 0; i < num_partitions; i++) {
        shuffle_idx_to_instance_idx[i] = i;
    }

    const auto expect_block_bytes = 128;
    const auto num_blocks = 2;
    config::local_exchange_buffer_mem_limit =
            (num_partitions - 1) * num_blocks * expect_block_bytes;

    std::vector<std::pair<std::vector<uint32_t>, int>> hash_vals_and_value;
    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_partitions);
    shared_state->exchanger = ShuffleExchanger::create_unique(num_sink, num_sources, num_partitions,
                                                              free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (ShuffleExchanger*)shared_state->exchanger.get();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_partitioner =
                std::make_unique<Crc32HashPartitioner<ShuffleChannelIds>>(

                        num_partitions);
        auto texpr =
                TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .set_slot_ref(TSlotRefBuilder(0, 0).build())
                        .build();
        auto slot = doris::VSlotRef::create_shared(texpr);
        slot->_column_id = 0;
        ((Crc32HashPartitioner<ShuffleChannelIds>*)_sink_local_states[i]->_partitioner.get())
                ->_partition_expr_ctxs.push_back(std::make_shared<doris::VExprContext>(slot));
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }

    {
        // Enqueue 2 blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_partitions; i++) {
            hash_vals_and_value.emplace_back(std::vector<uint32_t> {}, i);
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(hash_vals_and_value.back().second, 10);

                auto pre_size = hash_vals_and_value.back().first.size();
                hash_vals_and_value.back().first.resize(pre_size + 10);
                std::fill(hash_vals_and_value.back().first.begin() + pre_size,
                          hash_vals_and_value.back().first.end(), 0);
                int_col0->update_crcs_with_value(hash_vals_and_value.back().first.data() + pre_size,
                                                 PrimitiveType::TYPE_INT,
                                                 cast_set<uint32_t>(int_col0->size()), 0, nullptr);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                      .partitioner = _sink_local_states[i]->_partitioner.get(),
                                      .local_state = _sink_local_states[i].get(),
                                      .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                                      .ins_idx = _sink_local_states[i]->_channel_id};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_partitions - 1);
            }
        }
    }

    {
        int64_t mem_usage = 0;
        for (const auto& it : hash_vals_and_value) {
            auto channel_id = it.first.back() % num_partitions;
            EXPECT_GT(shared_state->mem_counters[channel_id]->value(), 0);
            mem_usage += shared_state->mem_counters[channel_id]->value();
            EXPECT_EQ(_local_states[channel_id]->_dependency->ready(), true);
        }
        EXPECT_EQ(shared_state->mem_usage, mem_usage);
        // Dequeue from data queue and accumulate rows if rows is smaller than batch_size.
        for (const auto& it : hash_vals_and_value) {
            bool eos = false;
            auto channel_id = it.first.back() % num_partitions;
            Block block;
            EXPECT_EQ(exchanger->get_block(
                              _runtime_state.get(), &block, &eos,
                              {nullptr, nullptr, _local_states[channel_id]->_copy_data_timer},
                              {cast_set<int>(_local_states[channel_id]->_channel_id),
                               _local_states[channel_id].get()}),
                      Status::OK());
            EXPECT_EQ(block.rows(), 20);
            EXPECT_EQ(eos, false);
            EXPECT_EQ(_local_states[channel_id]->_dependency->ready(), false);
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < num_partitions; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(hash_vals_and_value[i].second, 10);

            auto pre_size = hash_vals_and_value[i].first.size();
            hash_vals_and_value[i].first.resize(pre_size + 10);
            std::fill(hash_vals_and_value[i].first.begin() + pre_size,
                      hash_vals_and_value[i].first.end(), 0);
            int_col0->update_crcs_with_value(hash_vals_and_value[i].first.data() + pre_size,
                                             PrimitiveType::TYPE_INT,
                                             cast_set<uint32_t>(int_col0->size()), 0, nullptr);
            EXPECT_EQ(hash_vals_and_value[i].first.front(), hash_vals_and_value[i].first.back());
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (const auto& it : hash_vals_and_value) {
            bool eos = false;
            auto channel_id = it.first.back() % num_partitions;
            EXPECT_EQ(_local_states[channel_id]->_dependency->ready(), true);
            Block block;
            EXPECT_EQ(exchanger->get_block(
                              _runtime_state.get(), &block, &eos,
                              {nullptr, nullptr, _local_states[channel_id]->_copy_data_timer},
                              {cast_set<int>(_local_states[channel_id]->_channel_id),
                               _local_states[channel_id].get()}),
                      Status::OK());
            EXPECT_EQ(block.rows(), 10);
            EXPECT_EQ(eos, false);
            EXPECT_EQ(_local_states[channel_id]->_dependency->ready(), false);
        }
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, false);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        Block block;
        EXPECT_EQ(exchanger->get_block(
                          _runtime_state.get(), &block, &eos,
                          {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                          {cast_set<int>(_local_states[i]->_channel_id), _local_states[i].get()}),
                  Status::OK());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->sub_running_source_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, true);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }

    {
        // After exchanger closed, data will never push into data queue again.
        hash_vals_and_value.clear();
        for (size_t i = 0; i < num_partitions; i++) {
            hash_vals_and_value.emplace_back(std::vector<uint32_t> {}, i);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(hash_vals_and_value.back().second, 10);

            auto pre_size = hash_vals_and_value.back().first.size();
            hash_vals_and_value.back().first.resize(pre_size + 10);
            std::fill(hash_vals_and_value.back().first.begin() + pre_size,
                      hash_vals_and_value.back().first.end(), 0);
            int_col0->update_crcs_with_value(hash_vals_and_value.back().first.data() + pre_size,
                                             PrimitiveType::TYPE_INT,
                                             cast_set<uint32_t>(int_col0->size()), 0, nullptr);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }
}

TEST_F(LocalExchangerTest, PassthroughExchanger) {
    int num_sink = 4;
    int num_sources = 4;
    int free_block_limit = 1;

    const auto expect_block_bytes = 128;
    const auto num_blocks = num_sources + 1;
    config::local_exchange_buffer_mem_limit = (num_sources - 1) * num_blocks * expect_block_bytes;

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (PassthroughExchanger*)shared_state->exchanger.get();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }

    {
        // Enqueue `num_blocks` blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                      .partitioner = _sink_local_states[i]->_partitioner.get(),
                                      .local_state = _sink_local_states[i].get(),
                                      .shuffle_idx_to_instance_idx = nullptr,
                                      .ins_idx = _sink_local_states[i]->_channel_id};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_sources - 1);
                EXPECT_EQ(_sink_local_states[i]->_channel_id, i + 1 + j);
            }
        }
    }

    {
        int64_t mem_usage = 0;
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_GT(shared_state->mem_counters[i]->value(), 0);
            mem_usage += shared_state->mem_counters[i]->value();
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
        }
        EXPECT_EQ(shared_state->mem_usage, mem_usage);
        // Dequeue from data queue and accumulate rows if rows is smaller than batch_size.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j <= num_blocks; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == num_blocks ? 0 : 10);
                EXPECT_EQ(eos, false);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != num_blocks);
            }
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < num_sink; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i + 1 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == 1 ? 0 : 10);
                EXPECT_FALSE(eos);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != 1);
            }
        }
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, false);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        Block block;
        EXPECT_EQ(exchanger->get_block(
                          _runtime_state.get(), &block, &eos,
                          {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                          {cast_set<int>(_local_states[i]->_channel_id), _local_states[i].get()}),
                  Status::OK());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->sub_running_source_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, true);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }

    {
        // After exchanger closed, data will never push into data queue again.
        for (size_t i = 0; i < num_sink; i++) {
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i + 2 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }
}

TEST_F(LocalExchangerTest, PassToOneExchanger) {
    int num_sink = 4;
    int num_sources = 4;
    int free_block_limit = 0;

    const auto expect_block_bytes = 128;
    const auto num_blocks = 2;
    config::local_exchange_buffer_mem_limit = (num_sources - 1) * num_blocks * expect_block_bytes;

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassToOneExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (PassToOneExchanger*)shared_state->exchanger.get();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }

    {
        // Enqueue `num_blocks` blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                      .partitioner = _sink_local_states[i]->_partitioner.get(),
                                      .local_state = _sink_local_states[i].get(),
                                      .shuffle_idx_to_instance_idx = nullptr,
                                      .ins_idx = _sink_local_states[i]->_channel_id};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_sources - 1);
                EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
            }
        }
        for (size_t i = 1; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }

    {
        int64_t mem_usage = 0;
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(shared_state->mem_counters[i]->value(),
                      i == 0 ? expect_block_bytes * num_blocks * num_sink : 0);
            mem_usage += shared_state->mem_counters[i]->value();
            if (i == 0) {
                EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            }
        }
        EXPECT_EQ(shared_state->mem_usage, mem_usage);
        // Dequeue from data queue and accumulate rows if rows is smaller than batch_size.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j <= (i == 0 ? num_blocks * num_sink : 0); j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), i == 0 && j < num_blocks * num_sink ? 10 : 0);
                EXPECT_EQ(eos, i != 0);
                if (i == 0) {
                    EXPECT_EQ(_local_states[i]->_dependency->ready(), j != num_blocks * num_sink);
                }
            }
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < 1; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < 1; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == 1 ? 0 : 10);
                EXPECT_FALSE(eos);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != 1);
            }
        }
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, false);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < 1; i++) {
        bool eos = false;
        Block block;
        EXPECT_EQ(exchanger->get_block(
                          _runtime_state.get(), &block, &eos,
                          {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                          {cast_set<int>(_local_states[i]->_channel_id), _local_states[i].get()}),
                  Status::OK());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->sub_running_source_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, true);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }

    {
        // After exchanger closed, data will never push into data queue again.
        for (size_t i = 0; i < num_sink; i++) {
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }
}

TEST_F(LocalExchangerTest, BroadcastExchanger) {
    int num_sink = 4;
    int num_sources = 4;
    int free_block_limit = 0;

    const auto expect_block_bytes = 128;
    const auto num_blocks = 2;
    config::local_exchange_buffer_mem_limit = (num_sources - 1) * num_blocks * expect_block_bytes;

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            BroadcastExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (BroadcastExchanger*)shared_state->exchanger.get();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }

    {
        // Enqueue `num_blocks` blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                      .partitioner = _sink_local_states[i]->_partitioner.get(),
                                      .local_state = _sink_local_states[i].get(),
                                      .shuffle_idx_to_instance_idx = nullptr,
                                      .ins_idx = _sink_local_states[i]->_channel_id};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_sources - 1);
                EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
            }
        }
    }

    {
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(shared_state->mem_counters[i]->value(),
                      expect_block_bytes * num_blocks * num_sources);
            EXPECT_EQ(shared_state->mem_usage, shared_state->mem_counters[i]->value());
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
        }

        // Dequeue from data queue and accumulate rows if rows is smaller than batch_size.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j <= num_blocks * num_sources; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == num_blocks * num_sources ? 0 : 10);
                EXPECT_FALSE(eos);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != num_blocks * num_sources);
            }
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < num_sink; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= num_sources; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == num_sources ? 0 : 10);
                EXPECT_FALSE(eos);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != num_sources);
            }
        }
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, false);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        Block block;
        EXPECT_EQ(exchanger->get_block(
                          _runtime_state.get(), &block, &eos,
                          {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                          {cast_set<int>(_local_states[i]->_channel_id), _local_states[i].get()}),
                  Status::OK());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->sub_running_source_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, true);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }

    {
        // After exchanger closed, data will never push into data queue again.
        for (size_t i = 0; i < num_sink; i++) {
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }
}

TEST_F(LocalExchangerTest, AdaptivePassthroughExchanger) {
    int num_sink = 4;
    int num_sources = 4;
    int free_block_limit = 0;

    const auto expect_block_bytes = 128;
    const auto num_blocks = num_sources;
    const auto num_rows_per_block = num_sources * 3;
    // Each sink call adds one BlockWrapper of expect_block_bytes to the shared mem usage
    // (both the shuffle path used for the first num_sources calls and the passthrough path
    // used afterwards). With 16 calls total, mem grows monotonically by 128 bytes per call.
    // We want the sink dependency to remain ready while i < num_sources - 1 (mem reaches
    // 1536 after i=2) and to block once any sink call from i=num_sources-1 starts (mem
    // would jump past the limit to 1664).
    config::local_exchange_buffer_mem_limit =
            (num_sources - 1) * num_blocks * expect_block_bytes;

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            AdaptivePassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (AdaptivePassthroughExchanger*)shared_state->exchanger.get();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_ins_idx = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }

    EXPECT_EQ(exchanger->_is_pass_through, false);
    {
        // Enqueue `num_blocks` blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_sources; i++) {
            for (size_t j = 0; j < num_blocks; j++) {
                EXPECT_EQ(exchanger->_is_pass_through, i * num_blocks + j >= num_sources);
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(i, num_rows_per_block);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                      .partitioner = _sink_local_states[i]->_partitioner.get(),
                                      .local_state = _sink_local_states[i].get(),
                                      .shuffle_idx_to_instance_idx = nullptr,
                                      .ins_idx = static_cast<int>(i)};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_sources - 1)
                        << i << " " << j << " " << shared_state->mem_usage << " " << shared_state->buffer_mem_limit;
                EXPECT_EQ(_sink_local_states[i]->_channel_id,
                          i * num_blocks + j >= num_sources ? i + 1 + j : i);
            }
        }
    }

    {
        // Dequeue from data queue and accumulate rows if rows is smaller than batch_size.
        for (size_t i = 0; i < num_sources; i++) {
            // First `num_sources` blocks are splited by rows into all channels and the others are passthrough.
            for (size_t j = 0; j <= 2 * num_blocks - 1; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(eos, false);
            }
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < num_sink; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, num_rows_per_block);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i == 0 ? i + 1 : i + 1 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(), j == 1 ? 0 : num_rows_per_block);
                EXPECT_FALSE(eos);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != 1);
            }
        }
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, false);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        Block block;
        EXPECT_EQ(exchanger->get_block(
                          _runtime_state.get(), &block, &eos,
                          {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                          {cast_set<int>(_local_states[i]->_channel_id), _local_states[i].get()}),
                  Status::OK());
        EXPECT_EQ(block.rows(), 0);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->sub_running_source_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        EXPECT_EQ(exchanger->_data_queue[i].eos, true);
        EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
    }

    {
        // After exchanger closed, data will never push into data queue again.
        for (size_t i = 0; i < num_sink; i++) {
            Block in_block;
            DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            auto int_col0 = ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            SinkInfo sink_info = {.channel_id = &_sink_local_states[i]->_channel_id,
                                  .partitioner = _sink_local_states[i]->_partitioner.get(),
                                  .local_state = _sink_local_states[i].get(),
                                  .shuffle_idx_to_instance_idx = nullptr,
                                  .ins_idx = _sink_local_states[i]->_channel_id};
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i == 0 ? i + 2 : i + 2 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSinkOperatorXInit) {
    // Test init with HASH_SHUFFLE type
    {
        std::vector<TExpr> texprs;
        TExpr texpr;
        texpr.nodes.push_back(
                TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .set_slot_ref(TSlotRefBuilder(0, 0).build())
                        .build());
        texprs.push_back(texpr);

        std::map<int, int> shuffle_idx_to_instance_idx;
        for (int i = 0; i < 4; i++) {
            shuffle_idx_to_instance_idx[i] = i;
        }

        LocalExchangeSinkOperatorX op(0, 0, 4, texprs, shuffle_idx_to_instance_idx);

        // Test init with HASH_SHUFFLE and global shuffle
        std::map<int, int> global_shuffle_map;
        for (int i = 0; i < 4; i++) {
            global_shuffle_map[i] = (i + 1) % 4;
        }
        auto status = op.init(_runtime_state.get(), ExchangeType::HASH_SHUFFLE, 0, true,
                              global_shuffle_map);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._type, ExchangeType::HASH_SHUFFLE);
        EXPECT_TRUE(op._use_global_shuffle);
        EXPECT_NE(op._partitioner, nullptr);
    }

    // Test init with HASH_SHUFFLE without global shuffle
    {
        std::vector<TExpr> texprs;
        TExpr texpr;
        texpr.nodes.push_back(
                TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .set_slot_ref(TSlotRefBuilder(0, 0).build())
                        .build());
        texprs.push_back(texpr);

        std::map<int, int> shuffle_idx_to_instance_idx;
        LocalExchangeSinkOperatorX op(0, 0, 4, texprs, shuffle_idx_to_instance_idx);

        auto status = op.init(_runtime_state.get(), ExchangeType::HASH_SHUFFLE, 0, false, {});
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(op._use_global_shuffle);
        // When not using global shuffle, shuffle_idx_to_instance_idx should be identity mapping
        for (int i = 0; i < 4; i++) {
            EXPECT_EQ(op._shuffle_idx_to_instance_idx[i], i);
        }
    }

    // Test init with BUCKET_HASH_SHUFFLE type
    {
        std::vector<TExpr> texprs;
        TExpr texpr;
        texpr.nodes.push_back(
                TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .set_slot_ref(TSlotRefBuilder(0, 0).build())
                        .build());
        texprs.push_back(texpr);

        std::map<int, int> shuffle_idx_to_instance_idx;
        LocalExchangeSinkOperatorX op(0, 0, 4, texprs, shuffle_idx_to_instance_idx);

        auto status =
                op.init(_runtime_state.get(), ExchangeType::BUCKET_HASH_SHUFFLE, 8, false, {});
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._type, ExchangeType::BUCKET_HASH_SHUFFLE);
        EXPECT_NE(op._partitioner, nullptr);
    }

    // Test init with PASSTHROUGH type (no partitioner needed)
    {
        std::vector<TExpr> texprs;
        std::map<int, int> shuffle_idx_to_instance_idx;
        LocalExchangeSinkOperatorX op(0, 0, 4, texprs, shuffle_idx_to_instance_idx);

        auto status = op.init(_runtime_state.get(), ExchangeType::PASSTHROUGH, 0, false, {});
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._type, ExchangeType::PASSTHROUGH);
        EXPECT_EQ(op._partitioner, nullptr);
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSinkOperatorXSink) {
    int num_sink = 2;
    int num_sources = 2;
    int num_partitions = 2;
    int free_block_limit = 0;

    std::map<int, int> shuffle_idx_to_instance_idx;
    for (int i = 0; i < num_partitions; i++) {
        shuffle_idx_to_instance_idx[i] = i;
    }

    std::vector<TExpr> texprs;
    TExpr texpr;
    texpr.nodes.push_back(
            TExprNodeBuilder(TExprNodeType::SLOT_REF,
                             TTypeDescBuilder()
                                     .set_types(TTypeNodeBuilder()
                                                        .set_type(TTypeNodeType::SCALAR)
                                                        .set_scalar_type(TPrimitiveType::INT)
                                                        .build())
                                     .build(),
                             0)
                    .set_slot_ref(TSlotRefBuilder(0, 0).build())
                    .build());
    texprs.push_back(texpr);

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> source_local_states;
    sink_local_states.resize(num_sink);
    source_local_states.resize(num_sources);

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_partitions);
    shared_state->exchanger = ShuffleExchanger::create_unique(num_sink, num_sources, num_partitions,
                                                              free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    LocalExchangeSinkOperatorX op(0, 0, num_partitions, texprs, shuffle_idx_to_instance_idx);
    EXPECT_TRUE(op.init(_runtime_state.get(), ExchangeType::HASH_SHUFFLE, 0, false, {}).ok());

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(&op, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_partitioner =
                std::make_unique<Crc32HashPartitioner<ShuffleChannelIds>>(num_partitions);
        auto slot_texpr =
                TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .set_slot_ref(TSlotRefBuilder(0, 0).build())
                        .build();
        auto slot = doris::VSlotRef::create_shared(slot_texpr);
        slot->_column_id = 0;
        ((Crc32HashPartitioner<ShuffleChannelIds>*)sink_local_states[i]->_partitioner.get())
                ->_partition_expr_ctxs.push_back(std::make_shared<doris::VExprContext>(slot));
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_ins_idx = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        source_local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        source_local_states[i]->_exchanger = shared_state->exchanger.get();
        source_local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        source_local_states[i]->_copy_data_timer = copy_data_timer;
        source_local_states[i]->_channel_id = i;
        source_local_states[i]->_shared_state = shared_state.get();
        source_local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        source_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = source_local_states[i]->_memory_used_counter;
    }

    // Test sink with data block
    {
        Block in_block;
        DataTypePtr int_type = std::make_shared<DataTypeInt32>();
        auto int_col0 = ColumnInt32::create();
        int_col0->insert_many_vals(0, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});

        SinkInfo sink_info = {.channel_id = &sink_local_states[0]->_channel_id,
                              .partitioner = sink_local_states[0]->_partitioner.get(),
                              .local_state = sink_local_states[0].get(),
                              .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                              .ins_idx = sink_local_states[0]->_channel_id};

        auto* exchanger = (ShuffleExchanger*)shared_state->exchanger.get();
        auto status = exchanger->sink(_runtime_state.get(), &in_block, false,
                                      {sink_local_states[0]->_compute_hash_value_timer,
                                       sink_local_states[0]->_distribute_timer, nullptr},
                                      sink_info);
        EXPECT_TRUE(status.ok());
    }

    // Test sink with empty block
    {
        Block empty_block;
        SinkInfo sink_info = {.channel_id = &sink_local_states[0]->_channel_id,
                              .partitioner = sink_local_states[0]->_partitioner.get(),
                              .local_state = sink_local_states[0].get(),
                              .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                              .ins_idx = sink_local_states[0]->_channel_id};

        auto* exchanger = (ShuffleExchanger*)shared_state->exchanger.get();
        auto status = exchanger->sink(_runtime_state.get(), &empty_block, false,
                                      {sink_local_states[0]->_compute_hash_value_timer,
                                       sink_local_states[0]->_distribute_timer, nullptr},
                                      sink_info);
        EXPECT_TRUE(status.ok());
    }

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* exchanger = (ShuffleExchanger*)shared_state->exchanger.get();
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSinkOperatorXSetLowMemoryMode) {
    int num_sink = 2;
    int num_sources = 2;
    int free_block_limit = 10;

    std::vector<TExpr> texprs;
    std::map<int, int> shuffle_idx_to_instance_idx;

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    sink_local_states.resize(num_sink);

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    LocalExchangeSinkOperatorX op(0, 0, num_sources, texprs, shuffle_idx_to_instance_idx);
    EXPECT_TRUE(op.init(_runtime_state.get(), ExchangeType::PASSTHROUGH, 0, false, {}).ok());

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(&op, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Verify initial free_block_limit
    EXPECT_EQ(shared_state->exchanger->_free_block_limit, free_block_limit);

    // Call set_low_memory_mode through exchanger
    shared_state->exchanger->set_low_memory_mode();

    // Verify free_block_limit is set to 0 in low memory mode
    EXPECT_EQ(shared_state->exchanger->_free_block_limit, 0);

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* exchanger = (PassthroughExchanger*)shared_state->exchanger.get();
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, TestShuffleExchangerWrongMap) {
    int num_sink = 1;
    int num_sources = 4;
    int num_partitions = 4;
    int free_block_limit = 0;
    std::map<int, int> shuffle_idx_to_instance_idx;
    for (int i = 0; i < num_partitions; i++) {
        shuffle_idx_to_instance_idx[i] = i;
    }
    // Wrong map lost (0 -> 0) mapping
    std::map<int, int> wrong_shuffle_idx_to_instance_idx;
    for (int i = 1; i < num_partitions; i++) {
        wrong_shuffle_idx_to_instance_idx[i] = i;
    }

    std::vector<std::pair<std::vector<uint32_t>, int>> hash_vals_and_value;
    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> _sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> _local_states;
    _sink_local_states.resize(num_sink);
    _local_states.resize(num_sources);
    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_partitions);
    shared_state->exchanger = ShuffleExchanger::create_unique(num_sink, num_sources, num_partitions,
                                                              free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    auto* exchanger = (ShuffleExchanger*)shared_state->exchanger.get();
    auto texpr = TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                  TTypeDescBuilder()
                                          .set_types(TTypeNodeBuilder()
                                                             .set_type(TTypeNodeType::SCALAR)
                                                             .set_scalar_type(TPrimitiveType::INT)
                                                             .build())
                                          .build(),
                                  0)
                         .set_slot_ref(TSlotRefBuilder(0, 0).build())
                         .build();
    std::vector<TExpr> texprs;
    texprs.emplace_back();
    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_partitioner =
                std::make_unique<Crc32HashPartitioner<ShuffleChannelIds>>(

                        num_partitions);
        auto slot = doris::VSlotRef::create_shared(texpr);
        slot->_column_id = 0;
        ((Crc32HashPartitioner<ShuffleChannelIds>*)_sink_local_states[i]->_partitioner.get())
                ->_partition_expr_ctxs.push_back(std::make_shared<doris::VExprContext>(slot));
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        _local_states[i]->_exchanger = shared_state->exchanger.get();
        _local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        _local_states[i]->_copy_data_timer = copy_data_timer;
        _local_states[i]->_channel_id = i;
        _local_states[i]->_shared_state = shared_state.get();
        _local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        _local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = _local_states[i]->_memory_used_counter;
    }
    const auto num_blocks = 1;
    {
        for (size_t i = 0; i < num_partitions; i++) {
            hash_vals_and_value.emplace_back(std::vector<uint32_t> {}, i);
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(hash_vals_and_value.back().second, 10);

                auto pre_size = hash_vals_and_value.back().first.size();
                hash_vals_and_value.back().first.resize(pre_size + 10);
                std::fill(hash_vals_and_value.back().first.begin() + pre_size,
                          hash_vals_and_value.back().first.end(), 0);
                int_col0->update_crcs_with_value(hash_vals_and_value.back().first.data() + pre_size,
                                                 PrimitiveType::TYPE_INT,
                                                 cast_set<uint32_t>(int_col0->size()), 0, nullptr);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            }
        }
    }
    {
        // Enqueue 2 blocks with 10 rows for each data queue.
        for (size_t i = 0; i < num_partitions; i++) {
            hash_vals_and_value.emplace_back(std::vector<uint32_t> {}, i);
            for (size_t j = 0; j < num_blocks; j++) {
                Block in_block;
                DataTypePtr int_type = std::make_shared<DataTypeInt32>();
                auto int_col0 = ColumnInt32::create();
                int_col0->insert_many_vals(hash_vals_and_value[i].second, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                bool in_eos = false;
                SinkInfo sink_info = {.channel_id = &_sink_local_states[0]->_channel_id,
                                      .partitioner = _sink_local_states[0]->_partitioner.get(),
                                      .local_state = _sink_local_states[0].get(),
                                      .shuffle_idx_to_instance_idx = &shuffle_idx_to_instance_idx,
                                      .ins_idx = _sink_local_states[0]->_channel_id};
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[0]->_compute_hash_value_timer,
                                           _sink_local_states[0]->_distribute_timer, nullptr},
                                          sink_info),
                          Status::OK());
            }
        }
    }
    {
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 1);
        }
    }

    {
        LocalExchangeSinkOperatorX op(texprs, wrong_shuffle_idx_to_instance_idx);
        _sink_local_states[0]->_parent = &op;
        EXPECT_EQ(hash_vals_and_value[0].first.front() % num_partitions, 0);
        Block in_block;
        DataTypePtr int_type = std::make_shared<DataTypeInt32>();
        auto int_col0 = ColumnInt32::create();
        int_col0->insert_many_vals(hash_vals_and_value[0].second, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
        bool in_eos = false;
        SinkInfo sink_info = {.channel_id = &_sink_local_states[0]->_channel_id,
                              .partitioner = _sink_local_states[0]->_partitioner.get(),
                              .local_state = _sink_local_states[0].get(),
                              .shuffle_idx_to_instance_idx = &wrong_shuffle_idx_to_instance_idx,
                              .ins_idx = _sink_local_states[0]->_channel_id};
        EXPECT_TRUE(exchanger
                            ->sink(_runtime_state.get(), &in_block, in_eos,
                                   {_sink_local_states[0]->_compute_hash_value_timer,
                                    _sink_local_states[0]->_distribute_timer, nullptr},
                                   sink_info)
                            .is<ErrorCode::INTERNAL_ERROR>());
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSharedStateBasic) {
    int num_instances = 4;

    // Test constructor
    auto shared_state = LocalExchangeSharedState::create_shared(num_instances);
    EXPECT_EQ(shared_state->source_deps.size(), num_instances);
    EXPECT_EQ(shared_state->mem_counters.size(), num_instances);
    EXPECT_EQ(shared_state->mem_usage, 0);
    EXPECT_EQ(shared_state->buffer_mem_limit, config::local_exchange_buffer_mem_limit);

    // Initialize exchanger
    shared_state->exchanger = PassthroughExchanger::create_unique(num_instances, num_instances, 0);

    // Create dependencies
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_instances, 0, 0, "TEST");

    // Initialize mem_counters
    auto profile = std::make_shared<RuntimeProfile>("");
    for (int i = 0; i < num_instances; i++) {
        shared_state->mem_counters[i] = profile->AddHighWaterMarkCounter(
                "MemUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Test add_mem_usage and sub_mem_usage
    shared_state->add_mem_usage(0, 1024);
    EXPECT_EQ(shared_state->mem_counters[0]->current_value(), 1024);

    shared_state->sub_mem_usage(0, 512);
    EXPECT_EQ(shared_state->mem_counters[0]->current_value(), 512);

    // Test set_ready_to_read
    shared_state->source_deps[0]->block();
    EXPECT_FALSE(shared_state->source_deps[0]->ready());
    shared_state->set_ready_to_read(0);
    EXPECT_TRUE(shared_state->source_deps[0]->ready());

    // Cleanup
    for (int i = 0; i < num_instances; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (int i = 0; i < num_instances; i++) {
        shared_state->exchanger->close({.channel_id = i, .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSharedStateMemUsage) {
    int num_instances = 2;

    auto shared_state = LocalExchangeSharedState::create_shared(num_instances);
    shared_state->exchanger = PassthroughExchanger::create_unique(num_instances, num_instances, 0);

    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_instances, 0, 0, "TEST");

    auto profile = std::make_shared<RuntimeProfile>("");
    for (int i = 0; i < num_instances; i++) {
        shared_state->mem_counters[i] = profile->AddHighWaterMarkCounter(
                "MemUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Set a small buffer limit for testing
    shared_state->buffer_mem_limit = 1000;

    // Test update_total_mem_usage - should block when exceeding limit
    EXPECT_TRUE(sink_dep->ready());
    shared_state->update_total_mem_usage(500, 0);
    EXPECT_EQ(shared_state->mem_usage, 500);
    EXPECT_TRUE(sink_dep->ready());

    shared_state->update_total_mem_usage(600, 0);
    EXPECT_EQ(shared_state->mem_usage, 1100);
    EXPECT_FALSE(sink_dep->ready());

    // Test update_total_mem_usage -- should unblock when below limit
    shared_state->update_total_mem_usage(-200, 0);
    EXPECT_EQ(shared_state->mem_usage, 900);
    EXPECT_TRUE(sink_dep->ready());

    // Cleanup
    for (int i = 0; i < num_instances; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (int i = 0; i < num_instances; i++) {
        shared_state->exchanger->close({.channel_id = i, .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSharedStateSubOperators) {
    int num_instances = 2;

    auto shared_state = LocalExchangeSharedState::create_shared(num_instances);
    shared_state->exchanger = PassthroughExchanger::create_unique(num_instances, num_instances, 0);

    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_instances, 0, 0, "TEST");

    auto profile = std::make_shared<RuntimeProfile>("");
    for (int i = 0; i < num_instances; i++) {
        shared_state->mem_counters[i] = profile->AddHighWaterMarkCounter(
                "MemUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Verify initial running operators count
    EXPECT_EQ(shared_state->exchanger->_running_sink_operators, num_instances);
    EXPECT_EQ(shared_state->exchanger->_running_source_operators, num_instances);

    // Test sub_running_sink_operators
    shared_state->sub_running_sink_operators(0);
    EXPECT_EQ(shared_state->exchanger->_running_sink_operators, num_instances - 1);

    // When last sink operator is removed, dependencies should be set to always ready
    shared_state->sub_running_sink_operators(0);
    EXPECT_EQ(shared_state->exchanger->_running_sink_operators, 0);

    // Test sub_running_source_operators
    shared_state->exchanger->close({.channel_id = 0, .local_state = nullptr});
    shared_state->sub_running_source_operators();
    EXPECT_EQ(shared_state->exchanger->_running_source_operators, num_instances - 1);

    shared_state->exchanger->close({.channel_id = 1, .local_state = nullptr});
    shared_state->sub_running_source_operators();
    EXPECT_EQ(shared_state->exchanger->_running_source_operators, 0);
}

TEST_F(LocalExchangerTest, MemShareArbitratorBasic) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    int64_t query_mem_limit = 1024 * 1024 * 1024; // 1GB
    double max_ratio = 0.5;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, max_ratio);

    // Register operators
    arb->register_operator();
    arb->register_operator();

    // Test debug_string
    auto debug_str = arb->debug_string();
    EXPECT_TRUE(debug_str.find("query_mem_limit") != std::string::npos);
}

TEST_F(LocalExchangerTest, MemLimiterBasic) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    int64_t query_mem_limit = 1024 * 1024 * 1024; // 1GB
    int64_t parallelism = 4;
    bool serial_operator = false;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.5);
    arb->register_operator();

    auto mem_limiter =
            MemLimiter::create_shared(query_id, parallelism, serial_operator, query_mem_limit, arb);

    // Test init
    auto init_result = mem_limiter->init();
    EXPECT_EQ(init_result, 0); // First init returns 0

    // Test reestimated_block_mem_bytes
    mem_limiter->reestimated_block_mem_bytes(32 * 1024 * 1024); // 32MB

    // Test update_running_tasks_count
    EXPECT_EQ(mem_limiter->update_running_tasks_count(1), 1);
    EXPECT_EQ(mem_limiter->update_running_tasks_count(1), 2);
    EXPECT_EQ(mem_limiter->update_running_tasks_count(-1), 1);

    // Test debug_string
    auto debug_str = mem_limiter->debug_string();
    EXPECT_TRUE(debug_str.find("parallelism") != std::string::npos);
    EXPECT_TRUE(debug_str.find("running_tasks_count") != std::string::npos);

    // Test close
    mem_limiter->close();
}

TEST_F(LocalExchangerTest, MemLimiterSerialOperator) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    int64_t parallelism = 4;
    bool serial_operator = true;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.5);
    arb->register_operator();

    auto mem_limiter =
            MemLimiter::create_shared(query_id, parallelism, serial_operator, query_mem_limit, arb);

    mem_limiter->init();
    mem_limiter->reestimated_block_mem_bytes(64 * 1024 * 1024);

    // For serial operator, all slots should go to instance 0
    auto debug_str = mem_limiter->debug_string();
    EXPECT_TRUE(debug_str.find("serial_operator: 1") != std::string::npos ||
                debug_str.find("serial_operator: true") != std::string::npos);

    mem_limiter->close();
}

TEST_F(LocalExchangerTest, AdaptiveTaskProcessorBasic) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 3;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    int64_t parallelism = 4;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.5);
    arb->register_operator();

    auto mem_limiter =
            MemLimiter::create_shared(query_id, parallelism, false, query_mem_limit, arb);
    mem_limiter->init();
    mem_limiter->reestimated_block_mem_bytes(64 * 1024 * 1024);

    int32_t max_concurrency = 8;
    int32_t min_concurrency = 1;

    auto processor =
            AdaptiveTaskProcessor::create_unique(max_concurrency, min_concurrency, mem_limiter);

    // Test available_task_slots
    int slots = processor->available_task_slots(0);
    EXPECT_GE(slots, min_concurrency);
    EXPECT_LE(slots, max_concurrency);

    // Test debug_string
    auto debug_str = processor->debug_string();
    EXPECT_TRUE(debug_str.find("max_concurrency") != std::string::npos);
    EXPECT_TRUE(debug_str.find("min_concurrency") != std::string::npos);

    mem_limiter->close();
}

TEST_F(LocalExchangerTest, AdaptiveTaskProcessorAdjustParallelism) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 4;
    int64_t query_mem_limit = 1024 * 1024 * 1024;
    int64_t parallelism = 4;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.5);
    arb->register_operator();

    auto mem_limiter =
            MemLimiter::create_shared(query_id, parallelism, false, query_mem_limit, arb);
    mem_limiter->init();
    mem_limiter->reestimated_block_mem_bytes(64 * 1024 * 1024);

    int32_t max_concurrency = 8;
    int32_t min_concurrency = 1;

    auto processor =
            AdaptiveTaskProcessor::create_unique(max_concurrency, min_concurrency, mem_limiter);

    // Create dependencies for testing
    auto dep1 = std::make_shared<Dependency>(0, 0, "TEST_DEP_1", true);
    auto dep2 = std::make_shared<Dependency>(1, 0, "TEST_DEP_2", true);

    std::mutex le_lock;
    std::unique_lock<std::mutex> lc(le_lock);
    // Test adjust_parallelism - block when running > expected
    processor->adjust_parallelism(5, 2, dep1, lc);
    // dep1 should be blocked
    EXPECT_FALSE(dep1->ready());

    processor->adjust_parallelism(5, 2, dep2, lc);
    // dep2 should also be blocked
    EXPECT_FALSE(dep2->ready());

    // Test adjust_parallelism - unblock when running <= expected
    processor->adjust_parallelism(2, 5, nullptr, lc);
    // Dependencies should be unblocked (LIFO order)
    EXPECT_TRUE(dep2->ready());
    EXPECT_TRUE(dep1->ready());

    mem_limiter->close();
}

TEST_F(LocalExchangerTest, LocalExchangeSharedStateWithAdaptiveProcessor) {
    int num_instances = 4;

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 5;
    int64_t query_mem_limit = 1024 * 1024 * 1024;

    auto arb = MemShareArbitrator::create_shared(query_id, query_mem_limit, 0.5);
    arb->register_operator();

    auto mem_limiter =
            MemLimiter::create_shared(query_id, num_instances, false, query_mem_limit, arb);
    mem_limiter->init();
    mem_limiter->reestimated_block_mem_bytes(64 * 1024 * 1024);

    auto shared_state = LocalExchangeSharedState::create_shared(num_instances);
    shared_state->exchanger = PassthroughExchanger::create_unique(num_instances, num_instances, 0);

    // Create sink dependencies for each instance
    for (int i = 0; i < num_instances; i++) {
        auto sink_dep = std::make_shared<Dependency>(
                i, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY_" + std::to_string(i), true);
        sink_dep->set_shared_state(shared_state.get());
        shared_state->sink_deps.push_back(sink_dep);
    }
    shared_state->create_source_dependencies(num_instances, 0, 0, "TEST");

    // Set up adaptive processor
    shared_state->adaptive_processor =
            AdaptiveTaskProcessor::create_shared(num_instances, 1, mem_limiter);

    auto profile = std::make_shared<RuntimeProfile>("");
    for (int i = 0; i < num_instances; i++) {
        shared_state->mem_counters[i] = profile->AddHighWaterMarkCounter(
                "MemUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Test update_total_mem_usage with adaptive processor
    // This should use adaptive_processor->adjust_parallelism instead of simple mem_usage check
    shared_state->update_total_mem_usage(1024, 0);
    shared_state->update_total_mem_usage(1024, 1);

    // Test update_total_mem_usage with adaptive processor
    shared_state->update_total_mem_usage(-512, 0);
    shared_state->update_total_mem_usage(-512, 1);

    // Cleanup
    for (int i = 0; i < num_instances; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (int i = 0; i < num_instances; i++) {
        shared_state->exchanger->close({.channel_id = i, .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }

    mem_limiter->close();
}

TEST_F(LocalExchangerTest, LocalExchangeSourceOperatorXInit) {
    // Test init with different exchange types
    {
        LocalExchangeSourceOperatorX op;
        auto status = op.init(ExchangeType::PASSTHROUGH);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._exchange_type, ExchangeType::PASSTHROUGH);
        EXPECT_TRUE(op._op_name.find("PASSTHROUGH") != std::string::npos);
    }

    {
        LocalExchangeSourceOperatorX op;
        auto status = op.init(ExchangeType::HASH_SHUFFLE);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._exchange_type, ExchangeType::HASH_SHUFFLE);
        EXPECT_TRUE(op._op_name.find("HASH_SHUFFLE") != std::string::npos);
    }

    {
        LocalExchangeSourceOperatorX op;
        auto status = op.init(ExchangeType::BROADCAST);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._exchange_type, ExchangeType::BROADCAST);
        EXPECT_TRUE(op._op_name.find("BROADCAST") != std::string::npos);
    }

    {
        LocalExchangeSourceOperatorX op;
        auto status = op.init(ExchangeType::PASS_TO_ONE);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._exchange_type, ExchangeType::PASS_TO_ONE);
        EXPECT_TRUE(op._op_name.find("PASS_TO_ONE") != std::string::npos);
    }

    {
        LocalExchangeSourceOperatorX op;
        auto status = op.init(ExchangeType::ADAPTIVE_PASSTHROUGH);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(op._exchange_type, ExchangeType::ADAPTIVE_PASSTHROUGH);
        EXPECT_TRUE(op._op_name.find("ADAPTIVE_PASSTHROUGH") != std::string::npos);
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSourceOperatorXIsSource) {
    LocalExchangeSourceOperatorX op;
    EXPECT_TRUE(op.init(ExchangeType::PASSTHROUGH).ok());
    EXPECT_TRUE(op.is_source());
}

TEST_F(LocalExchangerTest, LocalExchangeSourceLocalStateDependenciesPassToOne) {
    int num_sink = 2;
    int num_sources = 2;
    int free_block_limit = 0;

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassToOneExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> source_local_states;
    source_local_states.resize(num_sources);

    for (size_t i = 0; i < num_sources; i++) {
        source_local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        source_local_states[i]->_exchanger = shared_state->exchanger.get();
        source_local_states[i]->_channel_id = i;
        source_local_states[i]->_shared_state = shared_state.get();
        source_local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        source_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = source_local_states[i]->_memory_used_counter;
    }

    // For PASS_TO_ONE, channel_id 0 should have dependencies, others should not
    // Note: dependencies() calls Base::dependencies() which requires proper setup
    // Here we just verify the exchanger type is correct
    EXPECT_EQ(shared_state->exchanger->get_type(), ExchangeType::PASS_TO_ONE);

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSinkLocalStateDebugString) {
    int num_sink = 2;
    int num_sources = 2;
    int num_partitions = 2;
    int free_block_limit = 0;

    std::map<int, int> shuffle_idx_to_instance_idx;
    for (int i = 0; i < num_partitions; i++) {
        shuffle_idx_to_instance_idx[i] = i;
    }

    std::vector<TExpr> texprs;
    TExpr texpr;
    texpr.nodes.push_back(
            TExprNodeBuilder(TExprNodeType::SLOT_REF,
                             TTypeDescBuilder()
                                     .set_types(TTypeNodeBuilder()
                                                        .set_type(TTypeNodeType::SCALAR)
                                                        .set_scalar_type(TPrimitiveType::INT)
                                                        .build())
                                     .build(),
                             0)
                    .set_slot_ref(TSlotRefBuilder(0, 0).build())
                    .build());
    texprs.push_back(texpr);

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_partitions);
    shared_state->exchanger = ShuffleExchanger::create_unique(num_sink, num_sources, num_partitions,
                                                              free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    LocalExchangeSinkOperatorX op(0, 0, num_partitions, texprs, shuffle_idx_to_instance_idx);
    EXPECT_TRUE(op.init(_runtime_state.get(), ExchangeType::HASH_SHUFFLE, 0, false, {}).ok());

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    sink_local_states.resize(num_sink);

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(&op, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    // Test debug_string
    auto debug_str = sink_local_states[0]->debug_string(0);
    EXPECT_TRUE(debug_str.find("_use_global_shuffle") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_channel_id") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_num_partitions") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_num_senders") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_num_sources") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_running_sink_operators") != std::string::npos);
    EXPECT_TRUE(debug_str.find("_running_source_operators") != std::string::npos);

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        shared_state->exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSourceGetBlock) {
    int num_sink = 2;
    int num_sources = 2;
    int free_block_limit = 0;

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> source_local_states;
    sink_local_states.resize(num_sink);
    source_local_states.resize(num_sources);

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        source_local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        source_local_states[i]->_exchanger = shared_state->exchanger.get();
        source_local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        source_local_states[i]->_copy_data_timer = copy_data_timer;
        source_local_states[i]->_channel_id = i;
        source_local_states[i]->_shared_state = shared_state.get();
        source_local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        source_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = source_local_states[i]->_memory_used_counter;
    }

    auto* exchanger = (PassthroughExchanger*)shared_state->exchanger.get();

    // Sink a block
    {
        Block in_block;
        DataTypePtr int_type = std::make_shared<DataTypeInt32>();
        auto int_col0 = ColumnInt32::create();
        int_col0->insert_many_vals(42, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});

        SinkInfo sink_info = {.channel_id = &sink_local_states[0]->_channel_id,
                              .partitioner = nullptr,
                              .local_state = sink_local_states[0].get(),
                              .shuffle_idx_to_instance_idx = nullptr,
                              .ins_idx = sink_local_states[0]->_channel_id};

        auto status = exchanger->sink(_runtime_state.get(), &in_block, false,
                                      {sink_local_states[0]->_compute_hash_value_timer,
                                       sink_local_states[0]->_distribute_timer, nullptr},
                                      sink_info);
        EXPECT_TRUE(status.ok());
    }

    // Get block from source
    {
        bool eos = false;
        Block out_block;
        auto status = exchanger->get_block(
                _runtime_state.get(), &out_block, &eos,
                {nullptr, nullptr, source_local_states[0]->_copy_data_timer},
                {cast_set<int>(source_local_states[0]->_channel_id), source_local_states[0].get()});
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(out_block.rows(), 10);
        EXPECT_FALSE(eos);
    }

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeSinkLocalStateChannelIdUpdate) {
    int num_sink = 2;
    int num_sources = 4;
    int free_block_limit = 0;

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    sink_local_states.resize(num_sink);

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    for (size_t i = 0; i < num_sources; i++) {
        shared_state->mem_counters[i] = profile->AddHighWaterMarkCounter(
                "MemUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    auto* exchanger = (PassthroughExchanger*)shared_state->exchanger.get();

    // Verify channel_id is updated in round-robin fashion for passthrough exchanger
    int initial_channel_id = sink_local_states[0]->_channel_id;
    EXPECT_EQ(initial_channel_id, 0);

    // Sink multiple blocks and verify channel_id changes
    for (int j = 0; j < 4; j++) {
        Block in_block;
        DataTypePtr int_type = std::make_shared<DataTypeInt32>();
        auto int_col0 = ColumnInt32::create();
        int_col0->insert_many_vals(j, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});

        SinkInfo sink_info = {.channel_id = &sink_local_states[0]->_channel_id,
                              .partitioner = nullptr,
                              .local_state = sink_local_states[0].get(),
                              .shuffle_idx_to_instance_idx = nullptr,
                              .ins_idx = sink_local_states[0]->_channel_id};

        auto status = exchanger->sink(_runtime_state.get(), &in_block, false,
                                      {sink_local_states[0]->_compute_hash_value_timer,
                                       sink_local_states[0]->_distribute_timer, nullptr},
                                      sink_info);
        EXPECT_TRUE(status.ok());
        // Channel ID should increment (round-robin)
        EXPECT_EQ(sink_local_states[0]->_channel_id % num_sources, (j + 1) % num_sources);
    }

    // Cleanup
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}

TEST_F(LocalExchangerTest, LocalExchangeEndToEndWithEos) {
    int num_sink = 2;
    int num_sources = 2;
    int free_block_limit = 0;

    auto profile = std::make_shared<RuntimeProfile>("");
    auto shared_state = LocalExchangeSharedState::create_shared(num_sources);
    shared_state->exchanger =
            PassthroughExchanger::create_unique(num_sink, num_sources, free_block_limit);
    auto sink_dep = std::make_shared<Dependency>(0, 0, "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
    shared_state->create_source_dependencies(num_sources, 0, 0, "TEST");

    std::vector<std::unique_ptr<LocalExchangeSinkLocalState>> sink_local_states;
    std::vector<std::unique_ptr<LocalExchangeSourceLocalState>> source_local_states;
    sink_local_states.resize(num_sink);
    source_local_states.resize(num_sources);

    for (size_t i = 0; i < num_sink; i++) {
        auto* compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto* distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        sink_local_states[i] = std::make_unique<LocalExchangeSinkLocalState>(nullptr, nullptr);
        sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        sink_local_states[i]->_distribute_timer = distribute_timer;
        sink_local_states[i]->_channel_id = i;
        sink_local_states[i]->_shared_state = shared_state.get();
        sink_local_states[i]->_dependency = sink_dep.get();
        sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }

    for (size_t i = 0; i < num_sources; i++) {
        auto* get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto* copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        source_local_states[i] = std::make_unique<LocalExchangeSourceLocalState>(nullptr, nullptr);
        source_local_states[i]->_exchanger = shared_state->exchanger.get();
        source_local_states[i]->_get_block_failed_counter = get_block_failed_counter;
        source_local_states[i]->_copy_data_timer = copy_data_timer;
        source_local_states[i]->_channel_id = i;
        source_local_states[i]->_shared_state = shared_state.get();
        source_local_states[i]->_dependency = shared_state->get_dep_by_channel_id(i).front().get();
        source_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "MemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
        shared_state->mem_counters[i] = source_local_states[i]->_memory_used_counter;
    }

    auto* exchanger = (PassthroughExchanger*)shared_state->exchanger.get();

    // Sink blocks from all sinks
    for (size_t i = 0; i < num_sink; i++) {
        Block in_block;
        DataTypePtr int_type = std::make_shared<DataTypeInt32>();
        auto int_col0 = ColumnInt32::create();
        int_col0->insert_many_vals(i, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});

        SinkInfo sink_info = {.channel_id = &sink_local_states[i]->_channel_id,
                              .partitioner = nullptr,
                              .local_state = sink_local_states[i].get(),
                              .shuffle_idx_to_instance_idx = nullptr,
                              .ins_idx = sink_local_states[i]->_channel_id};

        auto status = exchanger->sink(_runtime_state.get(), &in_block, false,
                                      {sink_local_states[i]->_compute_hash_value_timer,
                                       sink_local_states[i]->_distribute_timer, nullptr},
                                      sink_info);
        EXPECT_TRUE(status.ok());
    }

    // Close all sinks (simulating EOS)
    for (size_t i = 0; i < num_sink; i++) {
        shared_state->sub_running_sink_operators(0);
    }

    // Get blocks from sources - should eventually get EOS
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        Block out_block;

        // First get the data block
        auto status = exchanger->get_block(
                _runtime_state.get(), &out_block, &eos,
                {nullptr, nullptr, source_local_states[i]->_copy_data_timer},
                {cast_set<int>(source_local_states[i]->_channel_id), source_local_states[i].get()});
        EXPECT_TRUE(status.ok());

        // Get again to check for EOS
        Block empty_block;
        status = exchanger->get_block(
                _runtime_state.get(), &empty_block, &eos,
                {nullptr, nullptr, source_local_states[i]->_copy_data_timer},
                {cast_set<int>(source_local_states[i]->_channel_id), source_local_states[i].get()});
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(eos);
    }

    // Cleanup
    for (size_t i = 0; i < num_sources; i++) {
        exchanger->close({.channel_id = cast_set<int>(i), .local_state = nullptr});
        shared_state->sub_running_source_operators();
    }
}
} // namespace doris
