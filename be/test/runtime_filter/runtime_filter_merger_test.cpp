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

#include "runtime_filter/runtime_filter_merger.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "runtime_filter/runtime_filter_producer.h"
#include "runtime_filter/runtime_filter_test_utils.h"

namespace doris {

class RuntimeFilterMergerTest : public RuntimeFilterTest {
public:
    void test_merge_from(RuntimeFilterWrapper::State first_product_state,
                         RuntimeFilterWrapper::State first_expected_state,
                         RuntimeFilterWrapper::State second_product_state,
                         RuntimeFilterWrapper::State second_expected_state) {
        std::shared_ptr<RuntimeFilterMerger> merger;
        auto desc = TRuntimeFilterDescBuilder().build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(
                RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &merger, &_profile));
        merger->set_expected_producer_num(2);
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::IGNORED);

        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer, &_profile));
        producer->set_wrapper_state_and_ready_to_publish(first_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get()));
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, first_expected_state);

        std::shared_ptr<RuntimeFilterProducer> producer2;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[1]->register_producer_runtime_filter(desc, &producer2, &_profile));
        producer2->set_wrapper_state_and_ready_to_publish(second_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer2.get()));
        ASSERT_TRUE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, second_expected_state);
    }

    void test_serialize(RuntimeFilterWrapper::State state) {
        std::shared_ptr<RuntimeFilterMerger> merger;
        auto desc = TRuntimeFilterDescBuilder().build();
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(
                RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &merger, &_profile));
        merger->set_expected_producer_num(1);
        ASSERT_FALSE(merger->ready());

        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer, &_profile));
        producer->set_wrapper_state_and_ready_to_publish(state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get()));
        ASSERT_TRUE(merger->ready());

        PMergeFilterRequest request;
        void* data = nullptr;
        int len = 0;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->serialize(&request, &data, &len));

        std::shared_ptr<RuntimeFilterMerger> deserialized_merger;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterMerger::create(RuntimeFilterParamsContext::create(_query_ctx.get()),
                                            &desc, &deserialized_merger, &_profile));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(deserialized_merger->assign(request, nullptr));
        ASSERT_EQ(merger->_wrapper->_state, state);
    }
};

TEST_F(RuntimeFilterMergerTest, basic) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, add_rf_size) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &merger, &_profile));
    merger->set_expected_producer_num(2);

    ASSERT_FALSE(merger->add_rf_size(123));
    ASSERT_TRUE(merger->add_rf_size(1));
    ASSERT_EQ(merger->get_received_sum_size(), 124);
    ASSERT_FALSE(merger->ready());

    try {
        ASSERT_TRUE(merger->add_rf_size(1));
        ASSERT_TRUE(false);
    } catch (const Exception& e) {
        ASSERT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
    }
}

TEST_F(RuntimeFilterMergerTest, invalid_merge) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(
            RuntimeFilterParamsContext::create(_query_ctx.get()), &desc, &merger, &_profile));
    merger->set_expected_producer_num(1);
    ASSERT_FALSE(merger->ready());
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::IGNORED);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer, &_profile));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get())); // ready wrapper
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::READY);

    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2, &_profile));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    auto st = merger->merge_from(producer2.get());
    ASSERT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(RuntimeFilterMergerTest, merge_from_ready_and_ignored) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::IGNORED, RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, merge_from_ignored_and_ready) {
    test_merge_from(RuntimeFilterWrapper::State::IGNORED, RuntimeFilterWrapper::State::IGNORED,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, merge_from_ready_and_disabled) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, merge_from_disabled_and_ready) {
    test_merge_from(RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, merge_from_disabled_and_ignored) {
    test_merge_from(RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED,
                    RuntimeFilterWrapper::State::IGNORED, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, merge_from_ignored_and_disabled) {
    test_merge_from(RuntimeFilterWrapper::State::IGNORED, RuntimeFilterWrapper::State::IGNORED,
                    RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, serialize_ready) {
    test_serialize(RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, serialize_disabled) {
    test_serialize(RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, serialize_ignored) {
    test_serialize(RuntimeFilterWrapper::State::IGNORED);
}

} // namespace doris
