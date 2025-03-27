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

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/local_exchange/local_exchange_source_operator.h"
#include "thrift_builder.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::pipeline {

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
    TUniqueId _query_id = TUniqueId();
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
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_partitioner.reset(
                new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                        num_partitions));
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
        auto slot = doris::vectorized::VSlotRef::create_shared(texpr);
        slot->_column_id = 0;
        ((vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>*)_sink_local_states[i]
                 ->_partitioner.get())
                ->_partition_expr_ctxs.push_back(
                        std::make_shared<doris::vectorized::VExprContext>(slot));
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
            hash_vals_and_value.push_back({std::vector<uint32_t> {}, i});
            for (size_t j = 0; j < num_blocks; j++) {
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
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
                EXPECT_EQ(exchanger->sink(
                                  _runtime_state.get(), &in_block, in_eos,
                                  {_sink_local_states[i]->_compute_hash_value_timer,
                                   _sink_local_states[i]->_distribute_timer, nullptr},
                                  {&_sink_local_states[i]->_channel_id,
                                   _sink_local_states[i]->_partitioner.get(),
                                   _sink_local_states[i].get(), &shuffle_idx_to_instance_idx}),
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
            vectorized::Block block;
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
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
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), &shuffle_idx_to_instance_idx}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (const auto& it : hash_vals_and_value) {
            bool eos = false;
            auto channel_id = it.first.back() % num_partitions;
            EXPECT_EQ(_local_states[channel_id]->_dependency->ready(), true);
            vectorized::Block block;
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
        shared_state->sub_running_sink_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        vectorized::Block block;
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
        exchanger->close({cast_set<int>(i), nullptr});
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
            hash_vals_and_value.push_back({std::vector<uint32_t> {}, i});
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
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
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), &shuffle_idx_to_instance_idx}),
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
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
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
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          {&_sink_local_states[i]->_channel_id,
                                           _sink_local_states[i]->_partitioner.get(),
                                           _sink_local_states[i].get(), nullptr}),
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
                vectorized::Block block;
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i + 1 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                vectorized::Block block;
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
        shared_state->sub_running_sink_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        vectorized::Block block;
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
        exchanger->close({cast_set<int>(i), nullptr});
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
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
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
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
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          {&_sink_local_states[i]->_channel_id,
                                           _sink_local_states[i]->_partitioner.get(),
                                           _sink_local_states[i].get(), nullptr}),
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
                vectorized::Block block;
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < 1; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                vectorized::Block block;
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
        shared_state->sub_running_sink_operators();
    }
    for (size_t i = 0; i < 1; i++) {
        bool eos = false;
        vectorized::Block block;
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
        exchanger->close({cast_set<int>(i), nullptr});
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
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
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
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
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
                int_col0->insert_many_vals(i, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          {&_sink_local_states[i]->_channel_id,
                                           _sink_local_states[i]->_partitioner.get(),
                                           _sink_local_states[i].get(), nullptr}),
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
                vectorized::Block block;
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= num_sources; j++) {
                bool eos = false;
                vectorized::Block block;
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
        shared_state->sub_running_sink_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        vectorized::Block block;
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
        exchanger->close({cast_set<int>(i), nullptr});
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
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
    const auto splited_block_bytes = 64;
    const auto num_blocks = num_sources;
    const auto num_rows_per_block = num_sources * 3;
    config::local_exchange_buffer_mem_limit = splited_block_bytes * num_sources * num_blocks +
                                              (num_sources - 2) * num_blocks * expect_block_bytes;

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
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
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
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
                int_col0->insert_many_vals(i, num_rows_per_block);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                EXPECT_EQ(expect_block_bytes, in_block.allocated_bytes());
                bool in_eos = false;
                EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                          {_sink_local_states[i]->_compute_hash_value_timer,
                                           _sink_local_states[i]->_distribute_timer, nullptr},
                                          {&_sink_local_states[i]->_channel_id,
                                           _sink_local_states[i]->_partitioner.get(),
                                           _sink_local_states[i].get(), nullptr}),
                          Status::OK());
                EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), i < num_sources - 1)
                        << i << " " << j << " " << shared_state->mem_usage;
                EXPECT_EQ(_sink_local_states[i]->_channel_id,
                          i * num_blocks + j >= num_sources ? i + 1 + j : i);
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
            // First `num_sources` blocks are splited by rows into all channels and the others are passthrough.
            for (size_t j = 0; j <= 2 * num_blocks - 1; j++) {
                bool eos = false;
                vectorized::Block block;
                EXPECT_EQ(
                        exchanger->get_block(_runtime_state.get(), &block, &eos,
                                             {nullptr, nullptr, _local_states[i]->_copy_data_timer},
                                             {cast_set<int>(_local_states[i]->_channel_id),
                                              _local_states[i].get()}),
                        Status::OK());
                EXPECT_EQ(block.rows(),
                          j < num_blocks ? num_rows_per_block / num_sources
                                         : (j == 2 * num_blocks - 1 ? 0 : num_rows_per_block))
                        << j;
                EXPECT_EQ(eos, false);
                EXPECT_EQ(_local_states[i]->_dependency->ready(), j != 2 * num_blocks - 1) << j;
            }
        }
        EXPECT_EQ(shared_state->mem_usage, 0);
    }
    {
        // Add new block and source dependency will be ready again.
        for (size_t i = 0; i < num_sink; i++) {
            EXPECT_EQ(_sink_local_states[i]->_dependency->ready(), true);
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, num_rows_per_block);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i == 0 ? i + 1 : i + 1 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(_local_states[i]->_dependency->ready(), true);
            for (size_t j = 0; j <= 1; j++) {
                bool eos = false;
                vectorized::Block block;
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
        shared_state->sub_running_sink_operators();
    }
    for (size_t i = 0; i < num_sources; i++) {
        bool eos = false;
        vectorized::Block block;
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
        exchanger->close({cast_set<int>(i), nullptr});
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
            vectorized::Block in_block;
            vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
            auto int_col0 = vectorized::ColumnInt32::create();
            int_col0->insert_many_vals(i, 10);
            in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
            bool in_eos = false;
            EXPECT_EQ(exchanger->sink(_runtime_state.get(), &in_block, in_eos,
                                      {_sink_local_states[i]->_compute_hash_value_timer,
                                       _sink_local_states[i]->_distribute_timer, nullptr},
                                      {&_sink_local_states[i]->_channel_id,
                                       _sink_local_states[i]->_partitioner.get(),
                                       _sink_local_states[i].get(), nullptr}),
                      Status::OK());
            EXPECT_EQ(_sink_local_states[i]->_channel_id, i == 0 ? i + 2 : i + 2 + num_blocks);
        }
        for (size_t i = 0; i < num_sources; i++) {
            EXPECT_EQ(exchanger->_data_queue[i].eos, true);
            EXPECT_EQ(exchanger->_data_queue[i].data_queue.size_approx(), 0);
        }
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
    texprs.push_back(TExpr {});
    for (size_t i = 0; i < num_sink; i++) {
        auto compute_hash_value_timer =
                ADD_TIMER(profile, "ComputeHashValueTime" + std::to_string(i));
        auto distribute_timer = ADD_TIMER(profile, "distribute_timer" + std::to_string(i));
        _sink_local_states[i].reset(new LocalExchangeSinkLocalState(nullptr, nullptr));
        _sink_local_states[i]->_exchanger = shared_state->exchanger.get();
        _sink_local_states[i]->_compute_hash_value_timer = compute_hash_value_timer;
        _sink_local_states[i]->_distribute_timer = distribute_timer;
        _sink_local_states[i]->_partitioner.reset(
                new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                        num_partitions));
        auto slot = doris::vectorized::VSlotRef::create_shared(texpr);
        slot->_column_id = 0;
        ((vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>*)_sink_local_states[i]
                 ->_partitioner.get())
                ->_partition_expr_ctxs.push_back(
                        std::make_shared<doris::vectorized::VExprContext>(slot));
        _sink_local_states[i]->_channel_id = i;
        _sink_local_states[i]->_shared_state = shared_state.get();
        _sink_local_states[i]->_dependency = sink_dep.get();
        _sink_local_states[i]->_memory_used_counter = profile->AddHighWaterMarkCounter(
                "SinkMemoryUsage" + std::to_string(i), TUnit::BYTES, "", 1);
    }
    for (size_t i = 0; i < num_sources; i++) {
        auto get_block_failed_counter =
                ADD_TIMER(profile, "_get_block_failed_counter" + std::to_string(i));
        auto copy_data_timer = ADD_TIMER(profile, "_copy_data_timer" + std::to_string(i));
        _local_states[i].reset(new LocalExchangeSourceLocalState(nullptr, nullptr));
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
            hash_vals_and_value.push_back({std::vector<uint32_t> {}, i});
            for (size_t j = 0; j < num_blocks; j++) {
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
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
            hash_vals_and_value.push_back({std::vector<uint32_t> {}, i});
            for (size_t j = 0; j < num_blocks; j++) {
                vectorized::Block in_block;
                vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
                auto int_col0 = vectorized::ColumnInt32::create();
                int_col0->insert_many_vals(hash_vals_and_value[i].second, 10);
                in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
                bool in_eos = false;
                EXPECT_EQ(exchanger->sink(
                                  _runtime_state.get(), &in_block, in_eos,
                                  {_sink_local_states[0]->_compute_hash_value_timer,
                                   _sink_local_states[0]->_distribute_timer, nullptr},
                                  {&_sink_local_states[0]->_channel_id,
                                   _sink_local_states[0]->_partitioner.get(),
                                   _sink_local_states[0].get(), &shuffle_idx_to_instance_idx}),
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
        vectorized::Block in_block;
        vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();
        auto int_col0 = vectorized::ColumnInt32::create();
        int_col0->insert_many_vals(hash_vals_and_value[0].second, 10);
        in_block.insert({std::move(int_col0), int_type, "test_int_col0"});
        bool in_eos = false;
        EXPECT_TRUE(
                exchanger
                        ->sink(_runtime_state.get(), &in_block, in_eos,
                               {_sink_local_states[0]->_compute_hash_value_timer,
                                _sink_local_states[0]->_distribute_timer, nullptr},
                               {&_sink_local_states[0]->_channel_id,
                                _sink_local_states[0]->_partitioner.get(),
                                _sink_local_states[0].get(), &wrong_shuffle_idx_to_instance_idx})
                        .is<ErrorCode::INTERNAL_ERROR>());
    }
}
} // namespace doris::pipeline
