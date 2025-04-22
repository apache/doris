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
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
        merger->set_expected_producer_num(2);
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::UNINITED);

        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
        producer->set_wrapper_state_and_ready_to_publish(first_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get()));
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, first_expected_state);

        std::shared_ptr<RuntimeFilterProducer> producer2;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
        producer2->set_wrapper_state_and_ready_to_publish(second_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer2.get()));
        ASSERT_TRUE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, second_expected_state);
    }

    void test_serialize(RuntimeFilterWrapper::State state,
                        TRuntimeFilterDesc desc = TRuntimeFilterDescBuilder()
                                                          .set_type(TRuntimeFilterType::IN_OR_BLOOM)
                                                          .build()) {
        std::shared_ptr<RuntimeFilterMerger> merger;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
        merger->set_expected_producer_num(1);
        ASSERT_FALSE(merger->ready());

        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(123));
        producer->set_wrapper_state_and_ready_to_publish(state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get()));
        ASSERT_TRUE(merger->ready());

        PMergeFilterRequest request;
        void* data = nullptr;
        int len = 0;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->serialize(&request, &data, &len));

        std::shared_ptr<RuntimeFilterProducer> deserialized_producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                RuntimeFilterProducer::create(_query_ctx.get(), &desc, &deserialized_producer));
        butil::IOBuf buf;
        buf.append(data, len);
        butil::IOBufAsZeroCopyInputStream stream(buf);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(deserialized_producer->assign(request, &stream));
        ASSERT_EQ(deserialized_producer->_wrapper->_state, state);
    }
};

TEST_F(RuntimeFilterMergerTest, basic) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, add_rf_size) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
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
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
    merger->set_expected_producer_num(1);
    ASSERT_FALSE(merger->ready());
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::UNINITED);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get())); // ready wrapper
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::READY);

    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    auto st = merger->merge_from(producer2.get());
    ASSERT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(RuntimeFilterMergerTest, merge_from_ready_and_disabled) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, merge_from_disabled_and_ready) {
    test_merge_from(RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::DISABLED,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, serialize_ready) {
    test_serialize(RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, serialize_disabled) {
    test_serialize(RuntimeFilterWrapper::State::DISABLED);
}

TEST_F(RuntimeFilterMergerTest, serialize_bloom) {
    test_serialize(RuntimeFilterWrapper::State::READY,
                   TRuntimeFilterDescBuilder().set_type(TRuntimeFilterType::BLOOM).build());
}

TEST_F(RuntimeFilterMergerTest, serialize_min_max) {
    test_serialize(RuntimeFilterWrapper::State::READY,
                   TRuntimeFilterDescBuilder().set_type(TRuntimeFilterType::MIN_MAX).build());
}

TEST_F(RuntimeFilterMergerTest, serialize_in) {
    test_serialize(RuntimeFilterWrapper::State::READY,
                   TRuntimeFilterDescBuilder().set_type(TRuntimeFilterType::IN).build());
}

TEST_F(RuntimeFilterMergerTest, serialize_min_only) {
    auto desc = TRuntimeFilterDescBuilder().set_type(TRuntimeFilterType::MIN_MAX).build();
    desc.__set_min_max_type(TMinMaxRuntimeFilterType::MIN);
    test_serialize(RuntimeFilterWrapper::State::READY, desc);
}

TEST_F(RuntimeFilterMergerTest, serialize_max_only) {
    auto desc = TRuntimeFilterDescBuilder().set_type(TRuntimeFilterType::MIN_MAX).build();
    desc.__set_min_max_type(TMinMaxRuntimeFilterType::MAX);
    test_serialize(RuntimeFilterWrapper::State::READY, desc);
}

} // namespace doris
