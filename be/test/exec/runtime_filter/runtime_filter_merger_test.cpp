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

#include "exec/runtime_filter/runtime_filter_merger.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "core/data_type/data_type_number.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exprs/hybrid_set.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/column_helper.h"

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
        merger->increase_expected_producer_num(2);
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::UNINITED);

        bool ready = false;
        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
        producer->set_wrapper_state_and_ready_to_publish(first_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get(), &ready));
        ASSERT_FALSE(ready);
        ASSERT_FALSE(merger->ready());
        ASSERT_EQ(merger->_wrapper->_state, first_expected_state);

        std::shared_ptr<RuntimeFilterProducer> producer2;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
        producer2->set_wrapper_state_and_ready_to_publish(second_product_state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer2.get(), &ready));
        ASSERT_TRUE(ready);
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
        merger->increase_expected_producer_num(1);
        ASSERT_FALSE(merger->ready());

        bool ready = false;
        std::shared_ptr<RuntimeFilterProducer> producer;
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(123));
        producer->set_wrapper_state_and_ready_to_publish(state);
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get(), &ready));
        ASSERT_TRUE(ready);
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

// The merger merges into a private copy of the first wrapper and never writes a producer's
// wrapper, which may be shared with consumers in local RF mgr or with other producers.
TEST_F(RuntimeFilterMergerTest, merge_from_never_writes_producer_wrapper) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
    merger->increase_expected_producer_num(3);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    auto wrapper = producer->wrapper();
    auto hashes = wrapper->get_or_compute_bucket_prune_hashes(std::make_shared<DataTypeInt32>());
    ASSERT_EQ(hashes->size(), 1);

    bool ready = false;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get(), &ready));
    ASSERT_FALSE(ready);
    ASSERT_NE(merger->_wrapper, wrapper);
    ASSERT_NE(merger->_wrapper->hybrid_set(), wrapper->hybrid_set());
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(merger->_wrapper->hybrid_set()->size(), 1);
    ASSERT_FALSE(merger->_wrapper->_bucket_prune_hashes_started.load());

    // The same wrapper published by another producer of a broadcast join changes nothing.
    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
    producer2->set_wrapper(wrapper);
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer2.get(), &ready));
    ASSERT_FALSE(ready);
    ASSERT_EQ(merger->_wrapper->hybrid_set()->size(), 1);

    // A disabled producer (e.g. of an early terminated instance) disables the merger's copy only.
    auto terminated_mgr = std::make_unique<RuntimeFilterMgr>(false);
    auto terminated_state =
            RuntimeState::create_unique(TUniqueId(), 0, _query_options, _query_ctx->query_globals,
                                        ExecEnv::GetInstance(), _query_ctx.get());
    terminated_state->set_runtime_filter_mgr(terminated_mgr.get());
    terminated_state->set_desc_tbl(&_tbl);
    std::shared_ptr<RuntimeFilterProducer> producer3;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            terminated_state->register_producer_runtime_filter(desc, &producer3));
    producer3->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED,
                                                      "skip all rf process");
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer3.get(), &ready));
    ASSERT_TRUE(ready);
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::DISABLED);
    ASSERT_NE(merger->_wrapper->_reason.status().msg().find("skip all rf process"),
              std::string::npos);

    int32_t five = 5;
    ASSERT_EQ(wrapper->_state, RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(wrapper->hybrid_set()->size(), 1);
    ASSERT_TRUE(wrapper->hybrid_set()->find(&five));
    ASSERT_EQ(wrapper->get_or_compute_bucket_prune_hashes(std::make_shared<DataTypeInt32>()),
              hashes);
}

// A deserialized RPC input is short lived; the merger must keep a private copy after it dies.
TEST_F(RuntimeFilterMergerTest, merge_from_rpc_input_uses_private_copy) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
    merger->increase_expected_producer_num(2);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    PMergeFilterRequest request;
    void* data = nullptr;
    int len = 0;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->serialize(&request, &data, &len));
    butil::IOBuf buf;
    buf.append(data, len);
    butil::IOBufAsZeroCopyInputStream stream(buf);
    std::shared_ptr<RuntimeFilterProducer> rpc_input;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &rpc_input));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(rpc_input->assign(request, &stream));
    auto wrapper = rpc_input->wrapper();

    bool ready = false;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(rpc_input.get(), &ready));
    ASSERT_FALSE(ready);
    ASSERT_NE(merger->_wrapper, wrapper);
    ASSERT_NE(merger->_wrapper->hybrid_set(), wrapper->hybrid_set());
    auto merged_wrapper = merger->_wrapper;
    rpc_input.reset();

    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            producer2->insert(ColumnHelper::create_column<DataTypeInt32>({6}), 0));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    request.Clear();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer2->serialize(&request, &data, &len));
    butil::IOBuf buf2;
    buf2.append(data, len);
    butil::IOBufAsZeroCopyInputStream stream2(buf2);
    std::shared_ptr<RuntimeFilterProducer> rpc_input2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &rpc_input2));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(rpc_input2->assign(request, &stream2));
    auto wrapper2 = rpc_input2->wrapper();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(rpc_input2.get(), &ready));
    ASSERT_TRUE(ready);
    ASSERT_EQ(merger->_wrapper, merged_wrapper);
    ASSERT_EQ(merger->_wrapper->hybrid_set()->size(), 2);
    int32_t five = 5;
    int32_t six = 6;
    ASSERT_TRUE(merged_wrapper->hybrid_set()->find(&five));
    ASSERT_TRUE(merged_wrapper->hybrid_set()->find(&six));
    ASSERT_EQ(wrapper->hybrid_set()->size(), 1);
    ASSERT_TRUE(wrapper->hybrid_set()->find(&five));
    ASSERT_FALSE(wrapper->hybrid_set()->find(&six));
    ASSERT_EQ(wrapper2->hybrid_set()->size(), 1);
    ASSERT_FALSE(wrapper2->hybrid_set()->find(&five));
}

TEST_F(RuntimeFilterMergerTest, failed_rpc_clone_does_not_publish_partial_filter) {
    auto options = _query_options;
    options.__set_runtime_filter_max_in_num(1024);
    options.__set_ignore_runtime_filter_error(true);
    options.__set_enable_reserve_memory(false);
    TNetworkAddress fe_address;
    fe_address.hostname = LOCALHOST;
    fe_address.port = DUMMY_PORT;
    auto query_ctx = QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), options, fe_address,
                                          true, fe_address, QuerySource::INTERNAL_FRONTEND);
    ASSERT_TRUE(query_ctx->ignore_runtime_filter_error());

    auto desc = TRuntimeFilterDescBuilder()
                        .set_type(TRuntimeFilterType::IN_OR_BLOOM)
                        .set_build_bf_by_runtime_size(false)
                        .set_bloom_filter_size_bytes(4096)
                        .set_mode(false)
                        .build();
    std::shared_ptr<RuntimeFilterMerger> merger;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(query_ctx.get(), &desc, &merger));
    merger->increase_expected_producer_num(2);

    std::shared_ptr<RuntimeFilterProducer> bloom_source;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(query_ctx.get(), &desc, &bloom_source));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(bloom_source->init(2000));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            bloom_source->insert(ColumnHelper::create_column<DataTypeInt32>({5}), 0));
    bloom_source->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    ASSERT_EQ(bloom_source->wrapper()->get_real_type(), RuntimeFilterType::BLOOM_FILTER);

    PMergeFilterRequest request;
    void* data = nullptr;
    int len = 0;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(bloom_source->serialize(&request, &data, &len));
    butil::IOBuf buf;
    buf.append(data, len);
    butil::IOBufAsZeroCopyInputStream stream(buf);
    std::shared_ptr<RuntimeFilterProducer> rpc_bloom;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(query_ctx.get(), &desc, &rpc_bloom));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(rpc_bloom->assign(request, &stream));
    ASSERT_EQ(rpc_bloom->wrapper()->get_real_type(), RuntimeFilterType::BLOOM_FILTER);

    bool ready = false;
    auto limited_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                            "FailedRuntimeFilterClone", 2048);
    {
        SwitchThreadMemTrackerLimiter switch_tracker(limited_tracker);
        auto status = merger->merge_from(rpc_bloom.get(), &ready);
        ASSERT_EQ(status.code(), ErrorCode::MEM_LIMIT_EXCEEDED) << status.to_string();
    }
    rpc_bloom.reset();
    ASSERT_FALSE(ready);
    ASSERT_FALSE(merger->ready());
    ASSERT_EQ(merger->_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

    std::shared_ptr<RuntimeFilterProducer> in_source;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(query_ctx.get(), &desc, &in_source));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(in_source->init(1));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            in_source->insert(ColumnHelper::create_column<DataTypeInt32>({6}), 0));
    in_source->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    request.Clear();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(in_source->serialize(&request, &data, &len));
    butil::IOBuf empty_buf;
    butil::IOBufAsZeroCopyInputStream empty_stream(empty_buf);
    std::shared_ptr<RuntimeFilterProducer> rpc_in;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(query_ctx.get(), &desc, &rpc_in));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(rpc_in->assign(request, &empty_stream));
    ASSERT_EQ(rpc_in->wrapper()->get_real_type(), RuntimeFilterType::IN_FILTER);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(rpc_in.get(), &ready));
    ASSERT_FALSE(ready);
    ASSERT_FALSE(merger->ready());
    ASSERT_EQ(merger->_wrapper->hybrid_set()->size(), 1);
    ASSERT_NE(merger->debug_string().find("received_producer_num: 1"), std::string::npos);
}

TEST_F(RuntimeFilterMergerTest, basic) {
    test_merge_from(RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY,
                    RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::READY);
}

TEST_F(RuntimeFilterMergerTest, add_rf_size) {
    std::shared_ptr<RuntimeFilterMerger> merger;
    auto desc = TRuntimeFilterDescBuilder().build();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterMerger::create(_query_ctx.get(), &desc, &merger));
    merger->increase_expected_producer_num(2);

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
    merger->increase_expected_producer_num(1);
    ASSERT_FALSE(merger->ready());
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::UNINITED);

    bool ready = false;
    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[0]->register_producer_runtime_filter(desc, &producer));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(merger->merge_from(producer.get(), &ready));
    ASSERT_TRUE(ready);
    ASSERT_TRUE(merger->ready());
    ASSERT_EQ(merger->_wrapper->_state, RuntimeFilterWrapper::State::READY);

    std::shared_ptr<RuntimeFilterProducer> producer2;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            _runtime_states[1]->register_producer_runtime_filter(desc, &producer2));
    producer2->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    auto st = merger->merge_from(producer2.get(), &ready);
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
