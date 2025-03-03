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

#include "runtime_filter/runtime_filter_consumer.h"

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "runtime_filter/runtime_filter_producer.h"
#include "runtime_filter/runtime_filter_test_utils.h"

namespace doris {

class RuntimeFilterConsumerTest : public RuntimeFilterTest {
public:
    void test_signal_aquire(TRuntimeFilterType::type type) {
        std::shared_ptr<RuntimeFilterConsumer> consumer;
        auto desc = TRuntimeFilterDescBuilder().set_type(type).add_planId_to_target_expr(0).build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterConsumer::create(RuntimeFilterParamsContext::create(_query_ctx.get()),
                                              &desc, 0, &consumer, &_profile));

        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
                RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(123));
        producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

        consumer->signal(producer.get());

        try {
            consumer->signal(producer.get());
            ASSERT_TRUE(false);
        } catch (const Exception& e) {
            ASSERT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        }

        std::vector<vectorized::VRuntimeFilterPtr> push_exprs;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(consumer->acquire_expr(push_exprs));
        ASSERT_NE(push_exprs.size(), 0);
        ASSERT_TRUE(consumer->is_applied());
    }
};

TEST_F(RuntimeFilterConsumerTest, basic) {
    std::shared_ptr<RuntimeFilterConsumer> consumer;
    auto desc = TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterConsumer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, 0, &consumer, &_profile));

    std::shared_ptr<RuntimeFilterConsumer> registed_consumer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_runtime_states[1]->register_consumer_runtime_filter(
            desc, true, 0, &registed_consumer, &_profile));
}

TEST_F(RuntimeFilterConsumerTest, signal_aquire_in_or_bloom) {
    test_signal_aquire(TRuntimeFilterType::IN_OR_BLOOM);
}

TEST_F(RuntimeFilterConsumerTest, signal_aquire_bloom) {
    test_signal_aquire(TRuntimeFilterType::BLOOM);
}

TEST_F(RuntimeFilterConsumerTest, signal_aquire_in) {
    test_signal_aquire(TRuntimeFilterType::IN);
}

TEST_F(RuntimeFilterConsumerTest, signal_aquire_min_max) {
    test_signal_aquire(TRuntimeFilterType::MIN_MAX);
}

TEST_F(RuntimeFilterConsumerTest, timeout_aquire) {
    std::shared_ptr<RuntimeFilterConsumer> consumer;
    auto desc = TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterConsumer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, 0, &consumer, &_profile));

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &producer, &_profile));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);

    std::vector<vectorized::VRuntimeFilterPtr> push_exprs;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(consumer->acquire_expr(push_exprs));
    ASSERT_EQ(push_exprs.size(), 0);
    ASSERT_FALSE(consumer->is_applied());
    ASSERT_EQ(consumer->_rf_state, RuntimeFilterConsumer::State::TIMEOUT);

    consumer->signal(producer.get());
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(consumer->acquire_expr(push_exprs));
    ASSERT_EQ(push_exprs.size(), 1);
    ASSERT_TRUE(consumer->is_applied());
}

} // namespace doris
