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

#include "exec/runtime_filter/runtime_filter_producer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <string>

#include "core/data_type/data_type_number.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_merger.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "testutil/column_helper.h"

namespace doris {

class RuntimeFilterProducerTest : public RuntimeFilterTest {};

TEST_F(RuntimeFilterProducerTest, basic) {
    std::shared_ptr<RuntimeFilterProducer> producer;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
}

TEST_F(RuntimeFilterProducerTest, no_sync_filter_size) {
    {
        std::shared_ptr<RuntimeFilterProducer> producer;
        auto desc = TRuntimeFilterDescBuilder()
                            .set_build_bf_by_runtime_size(true)
                            .set_is_broadcast_join(true)
                            .build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
        ASSERT_EQ(producer->_need_sync_filter_size, false);
        ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
    }
    {
        std::shared_ptr<RuntimeFilterProducer> producer;
        auto desc = TRuntimeFilterDescBuilder()
                            .set_build_bf_by_runtime_size(false)
                            .set_is_broadcast_join(false)
                            .build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
        ASSERT_EQ(producer->_need_sync_filter_size, false);
        ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
    }
}

TEST_F(RuntimeFilterProducerTest, sync_filter_size) {
    std::shared_ptr<RuntimeFilterProducer> producer;
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto mocked_dependency =
            std::make_shared<CountedFinishDependency>(0, 0, "MOCKED_FINISH_DEPENDENCY");
    producer->latch_dependency(mocked_dependency);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 100));
    // local mode, single rf get size directly into WAITING_FOR_DATA
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
}

TEST_F(RuntimeFilterProducerTest, sync_filter_size_local_no_merge) {
    std::shared_ptr<RuntimeFilterProducer> producer;
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto mocked_dependency =
            std::make_shared<CountedFinishDependency>(0, 0, "MOCKED_FINISH_DEPENDENCY");
    producer->latch_dependency(mocked_dependency);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 100));
    // local mode, single rf get size directly into WAITING_FOR_DATA
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
}

TEST_F(RuntimeFilterProducerTest, sync_filter_size_local_merge) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 0, &consumer));

    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "");

    producer->latch_dependency(dependency);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 123));
    // global mode, need waitting synced size
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SYNCED_SIZE);
    ASSERT_FALSE(dependency->ready());

    producer2->latch_dependency(dependency);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->send_size(_runtime_states[1].get(), 1));
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
    ASSERT_EQ(producer->_synced_size, 124);
    ASSERT_TRUE(dependency->ready());
}

TEST_F(RuntimeFilterProducerTest, set_disable) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 0, &consumer));

    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "");

    producer->latch_dependency(dependency);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 123));
    // global mode, need waitting synced size
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SYNCED_SIZE);

    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::READY_TO_PUBLISH);
    ASSERT_EQ(producer->_wrapper->_state, RuntimeFilterWrapper::State::DISABLED);

    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    ASSERT_EQ(producer2->_rf_state, RuntimeFilterProducer::State::READY_TO_PUBLISH);
    ASSERT_EQ(producer2->_wrapper->_state, RuntimeFilterWrapper::State::DISABLED);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));
    ASSERT_EQ(consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(consumer->_wrapper->_state, RuntimeFilterWrapper::State::DISABLED);
}

// A runtime filter whose consumers are partly in local RF mgr and partly in global RF mgr.
TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_local_consumer_gets_private_wrapper) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> local_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_consumer_runtime_filter(desc, false, 0, &local_consumer));
    std::shared_ptr<RuntimeFilterConsumer> local_consumer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, false, 0, &local_consumer2));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer2->insert(ColumnHelper::create_column<DataTypeInt32>({6}), 0));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    ASSERT_EQ(local_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::NOT_READY);

    // The local consumer builds its bucket prune cache before the merger receives producer2.
    std::vector<RuntimeFilterExprPtr> exprs;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(local_consumer->acquire_expr(exprs));
    ASSERT_EQ(exprs.size(), 1);
    auto hashes = exprs[0]->get_bucket_prune_hashes(std::make_shared<DataTypeInt32>());
    ASSERT_EQ(hashes->size(), 1);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(local_consumer2->_rf_state, RuntimeFilterConsumer::State::READY);

    int32_t five = 5;
    int32_t six = 6;
    auto merged_set = merge_consumer->_wrapper->hybrid_set();
    ASSERT_EQ(merged_set->size(), 2);
    ASSERT_TRUE(merged_set->find(&five));
    ASSERT_TRUE(merged_set->find(&six));

    auto local_set = local_consumer->_wrapper->hybrid_set();
    ASSERT_EQ(local_set->size(), 1);
    ASSERT_TRUE(local_set->find(&five));
    ASSERT_FALSE(local_set->find(&six));
    auto local_set2 = local_consumer2->_wrapper->hybrid_set();
    ASSERT_EQ(local_set2->size(), 1);
    ASSERT_TRUE(local_set2->find(&six));
    ASSERT_FALSE(local_set2->find(&five));

    ASSERT_NE(local_consumer->_wrapper, merge_consumer->_wrapper);
    ASSERT_NE(local_consumer2->_wrapper, merge_consumer->_wrapper);
    ASSERT_NE(local_consumer->_wrapper, local_consumer2->_wrapper);
    ASSERT_NE(local_set, merged_set);
    ASSERT_NE(local_set2, merged_set);
    ASSERT_NE(local_consumer->_wrapper->bloom_filter_func(),
              merge_consumer->_wrapper->bloom_filter_func());

    ASSERT_EQ(exprs[0]->get_bucket_prune_hashes(std::make_shared<DataTypeInt32>()), hashes);
    ASSERT_EQ(hashes->size(), 1);

    std::vector<RuntimeFilterExprPtr> merged_exprs;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merge_consumer->acquire_expr(merged_exprs));
    ASSERT_EQ(merged_exprs.size(), 1);
    ASSERT_EQ(merged_exprs[0]->get_bucket_prune_hashes(std::make_shared<DataTypeInt32>())->size(),
              2);
}

TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_merge_overflow_keeps_local_filter) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .set_type(TRuntimeFilterType::IN)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    // Only the first instance has a consumer in local RF mgr.
    std::shared_ptr<RuntimeFilterConsumer> local_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_consumer_runtime_filter(desc, false, 0, &local_consumer));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    // Each producer is within runtime_filter_max_in_num, but the merged one is not.
    std::vector<int32_t> data(600);
    std::iota(data.begin(), data.end(), 0);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(data.size()));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>(data), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    std::iota(data.begin(), data.end(), 600);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->init(data.size()));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer2->insert(ColumnHelper::create_column<DataTypeInt32>(data), 0));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));

    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(merge_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);

    ASSERT_EQ(local_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(local_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::READY);
    auto local_set = local_consumer->_wrapper->hybrid_set();
    ASSERT_EQ(local_set->size(), 600);
    int32_t value = 0;
    ASSERT_TRUE(local_set->find(&value));
    value = 599;
    ASSERT_TRUE(local_set->find(&value));
    value = 600;
    ASSERT_FALSE(local_set->find(&value));
}

// Broadcast join producers which share one hash table also share one wrapper.
TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_shared_wrapper) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(true)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> local_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_consumer_runtime_filter(desc, false, 0, &local_consumer));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer2->set_wrapper(producer->wrapper());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    std::vector<RuntimeFilterExprPtr> exprs;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(local_consumer->acquire_expr(exprs));
    ASSERT_EQ(exprs.size(), 1);
    ASSERT_EQ(exprs[0]->get_bucket_prune_hashes(std::make_shared<DataTypeInt32>())->size(), 1);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    auto merged_set = merge_consumer->_wrapper->hybrid_set();
    ASSERT_EQ(merged_set->size(), 1);
    int32_t five = 5;
    ASSERT_TRUE(merged_set->find(&five));
    ASSERT_NE(local_consumer->_wrapper, merge_consumer->_wrapper);
}

// An early terminated instance of a broadcast join publishes its own disabled wrapper, which must
// not disable the shared wrapper in place: the local consumers of other instances still use it.
TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_shared_wrapper_with_disabled_instance) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(true)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    auto terminated_mgr = std::make_unique<RuntimeFilterMgr>(false);
    auto terminated_state =
            RuntimeState::create_unique(TUniqueId(), 0, _query_options, _query_ctx->query_globals,
                                        ExecEnv::GetInstance(), _query_ctx.get());
    terminated_state->set_runtime_filter_mgr(terminated_mgr.get());
    terminated_state->set_desc_tbl(&_tbl);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
    std::shared_ptr<RuntimeFilterProducer> terminated_producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            terminated_state->register_producer_runtime_filter(desc, &terminated_producer));

    std::shared_ptr<RuntimeFilterConsumer> local_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_consumer_runtime_filter(desc, false, 0, &local_consumer));
    std::shared_ptr<RuntimeFilterConsumer> local_consumer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, false, 0, &local_consumer2));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    auto shared_wrapper = producer->wrapper();
    producer2->set_wrapper(shared_wrapper);
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    terminated_producer->set_wrapper_state_and_ready_to_publish(
            RuntimeFilterWrapper::State::DISABLED, "skip all rf process");

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(terminated_producer->publish(terminated_state.get(), true));
    int32_t five = 5;
    ASSERT_EQ(shared_wrapper->get_state(), RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(shared_wrapper->hybrid_set()->size(), 1);
    ASSERT_TRUE(shared_wrapper->hybrid_set()->find(&five));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));
    ASSERT_EQ(shared_wrapper->get_state(), RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(shared_wrapper->hybrid_set()->size(), 1);
    for (const auto& consumer : {local_consumer, local_consumer2}) {
        ASSERT_EQ(consumer->_rf_state, RuntimeFilterConsumer::State::READY);
        ASSERT_EQ(consumer->_wrapper, shared_wrapper);
        ASSERT_EQ(consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::READY);
        ASSERT_EQ(consumer->_wrapper->hybrid_set()->size(), 1);
        ASSERT_TRUE(consumer->_wrapper->hybrid_set()->find(&five));
    }
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_NE(merge_consumer->_wrapper, shared_wrapper);
    ASSERT_EQ(merge_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    ASSERT_NE(merge_consumer->_wrapper->_reason.status().msg().find("skip all rf process"),
              std::string::npos);
}

TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_disabled) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> local_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_consumer_runtime_filter(desc, false, 0, &local_consumer));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    // Producers are disabled before the filters are built.
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));

    ASSERT_EQ(local_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(local_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(merge_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    ASSERT_NE(local_consumer->_wrapper, merge_consumer->_wrapper);
}

// The merger never takes over a producer's wrapper, it merges into its own copy.
TEST_F(RuntimeFilterProducerTest, publish_local_merge_targets_merger_owns_private_wrapper) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    std::shared_ptr<RuntimeFilterConsumer> consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 0, &consumer));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer2->insert(ColumnHelper::create_column<DataTypeInt32>({6}), 0));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    auto wrapper = producer->wrapper();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    std::shared_ptr<LocalMergeContext> context;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _query_ctx->runtime_filter_mgr()->get_local_merge_context(desc.filter_id, 0, &context));
    ASSERT_NE(context->merger->_wrapper, wrapper);
    ASSERT_NE(context->merger->_wrapper->hybrid_set(), wrapper->hybrid_set());
    ASSERT_EQ(context->merger->_wrapper->hybrid_set()->size(), 1);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));
    ASSERT_EQ(consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(consumer->_wrapper, context->merger->_wrapper);
    ASSERT_EQ(consumer->_wrapper->hybrid_set()->size(), 2);
    int32_t five = 5;
    int32_t six = 6;
    ASSERT_EQ(wrapper->hybrid_set()->size(), 1);
    ASSERT_TRUE(wrapper->hybrid_set()->find(&five));
    ASSERT_FALSE(wrapper->hybrid_set()->find(&six));
}

// A non-broadcast filter with only remote targets never hands its wrapper to a plain local
// consumer: that only happens in the `!_has_remote_target` branch of `publish()`. Once such a
// producer's wrapper reaches the local merger, no other reader remains, so the merger may take
// it over directly instead of cloning it.
TEST_F(RuntimeFilterProducerTest, publish_remote_target_merge_takes_ownership) {
    auto desc = TRuntimeFilterDescBuilder().set_mode(false).set_is_broadcast_join(false).build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    auto wrapper = producer->wrapper();
    // Only the first of the two expected producers has published, so the merger is not ready
    // yet and publish() never reaches `_send_to_remote_targets()`.
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    ASSERT_EQ(producer->_wrapper, nullptr);

    std::shared_ptr<LocalMergeContext> context;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _query_ctx->runtime_filter_mgr()->get_local_merge_context(desc.filter_id, 0, &context));
    // The merger adopted the producer's wrapper directly instead of cloning it.
    ASSERT_EQ(context->merger->_wrapper, wrapper);
    ASSERT_EQ(wrapper->hybrid_set()->size(), 1);
    int32_t five = 5;
    ASSERT_TRUE(wrapper->hybrid_set()->find(&five));
}

// An IN_OR_BLOOM merger which is still an IN filter takes the bloom filter of a producer which
// already changed to a bloom filter. It must take a copy, the producer's own local consumers are
// probing the original.
TEST_F(RuntimeFilterProducerTest, publish_mixed_targets_in_or_bloom_merge_copies_bloom_filter) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));

    // Only the second instance has a consumer in local RF mgr.
    std::shared_ptr<RuntimeFilterConsumer> local_consumer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, false, 0, &local_consumer2));
    std::shared_ptr<RuntimeFilterConsumer> merge_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_consumer_runtime_filter(desc, true, 1, &merge_consumer));

    // The first producer stays an IN filter, the second one exceeds runtime_filter_max_in_num
    // and changes to a bloom filter.
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(producer->wrapper()->get_real_type(), RuntimeFilterType::IN_FILTER);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->init(2000));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer2->insert(ColumnHelper::create_column<DataTypeInt32>({6, 7}), 0));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    auto bloom_wrapper = producer2->wrapper();
    ASSERT_EQ(bloom_wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    char* bloom_data = nullptr;
    int bloom_len = 0;
    bloom_wrapper->bloom_filter_func()->get_data(&bloom_data, &bloom_len);
    ASSERT_GT(bloom_len, 0);
    const std::string snapshot(bloom_data, bloom_len);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->publish(_runtime_states[1].get(), true));

    ASSERT_EQ(local_consumer2->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(local_consumer2->_wrapper, bloom_wrapper);
    ASSERT_EQ(merge_consumer->_rf_state, RuntimeFilterConsumer::State::READY);
    ASSERT_EQ(merge_consumer->_wrapper->get_state(), RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(merge_consumer->_wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    ASSERT_NE(merge_consumer->_wrapper->bloom_filter_func(), bloom_wrapper->bloom_filter_func());

    // The merged filter contains the values of both producers.
    std::vector<uint8_t> found(3);
    merge_consumer->_wrapper->bloom_filter_func()->find_fixed_len(
            ColumnHelper::create_column<DataTypeInt32>({5, 6, 7}), found.data());
    ASSERT_TRUE(std::all_of(found.begin(), found.end(), [](uint8_t i) -> bool { return i; }));

    // The bloom filter of the second producer is not written by the merge.
    char* merged_data = nullptr;
    int merged_len = 0;
    merge_consumer->_wrapper->bloom_filter_func()->get_data(&merged_data, &merged_len);
    ASSERT_NE(merged_data, bloom_data);
    ASSERT_EQ(bloom_len, merged_len);
    ASSERT_EQ(snapshot, std::string(bloom_data, bloom_len));
    ASSERT_NE(snapshot, std::string(merged_data, merged_len));
}

TEST_F(RuntimeFilterProducerTest, publish_release_wrapper) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(false)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));

    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->publish(_runtime_states[0].get(), true));
    ASSERT_EQ(producer->_wrapper, nullptr);
}

} // namespace doris