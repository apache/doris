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

#include "runtime_filter/runtime_filter_producer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "runtime_filter/runtime_filter_test_utils.h"

namespace doris {

class RuntimeFilterProducerTest : public RuntimeFilterTest {};

TEST_F(RuntimeFilterProducerTest, basic) {
    std::shared_ptr<RuntimeFilterProducer> producer;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
}

TEST_F(RuntimeFilterProducerTest, no_sync_filter_size) {
    {
        std::shared_ptr<RuntimeFilterProducer> producer;
        auto desc = TRuntimeFilterDescBuilder()
                            .set_build_bf_by_runtime_size(true)
                            .set_is_broadcast_join(true)
                            .build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
                RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
        ASSERT_EQ(producer->_need_sync_filter_size, false);
        ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
    }
    {
        std::shared_ptr<RuntimeFilterProducer> producer;
        auto desc = TRuntimeFilterDescBuilder()
                            .set_build_bf_by_runtime_size(false)
                            .set_is_broadcast_join(false)
                            .build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
                RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
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
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 100, nullptr));
    // local mode, single rf get size directly into WAITING_FOR_DATA
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
}

TEST_F(RuntimeFilterProducerTest, sync_filter_size_local_no_merge) {
    std::shared_ptr<RuntimeFilterProducer> producer;
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->send_size(_runtime_states[0].get(), 100, nullptr));
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
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer, &_profile));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2, &_profile));

    std::shared_ptr<RuntimeFilterConsumer> consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_runtime_states[1]->register_consumer_runtime_filter(
            desc, true, 0, &consumer, &_profile));

    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto dependency = std::make_shared<pipeline::CountedFinishDependency>(0, 0, "");

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->send_size(_runtime_states[0].get(), 123, dependency));
    // global mode, need waitting synced size
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SYNCED_SIZE);
    ASSERT_FALSE(dependency->ready());

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->send_size(_runtime_states[1].get(), 1, dependency));
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_DATA);
    ASSERT_EQ(producer->_synced_size, 124);
    ASSERT_TRUE(dependency->ready());
}

TEST_F(RuntimeFilterProducerTest, set_ignore_or_disable) {
    auto desc = TRuntimeFilterDescBuilder()
                        .set_build_bf_by_runtime_size(true)
                        .set_is_broadcast_join(false)
                        .add_planId_to_target_expr(0)
                        .build();

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer, &_profile));
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2, &_profile));

    std::shared_ptr<RuntimeFilterConsumer> consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_runtime_states[1]->register_consumer_runtime_filter(
            desc, true, 0, &consumer, &_profile));

    ASSERT_EQ(producer->_need_sync_filter_size, true);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SEND_SIZE);

    auto dependency = std::make_shared<pipeline::CountedFinishDependency>(0, 0, "");

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->send_size(_runtime_states[0].get(), 123, dependency));
    // global mode, need waitting synced size
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SYNCED_SIZE);

    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::IGNORED);
    ASSERT_EQ(producer->_rf_state, RuntimeFilterProducer::State::READY_TO_PUBLISH);
    ASSERT_EQ(producer->_wrapper->_state, RuntimeFilterWrapper::State::IGNORED);

    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED);
    ASSERT_EQ(producer2->_rf_state, RuntimeFilterProducer::State::READY_TO_PUBLISH);
    ASSERT_EQ(producer2->_wrapper->_state, RuntimeFilterWrapper::State::DISABLED);
}

} // namespace doris
