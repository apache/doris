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

#include "exec/runtime_filter/sync_size_callback.h"

#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"

namespace doris {

class SyncSizeCallbackTest : public RuntimeFilterTest {
protected:
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        _dependency = std::make_shared<CountedFinishDependency>(0, 0, "TEST_DEP");
        _dependency->add();
        _wrapper = std::make_shared<RuntimeFilterWrapper>(
                PrimitiveType::TYPE_INT, RuntimeFilterType::BLOOM_FILTER, /*filter_id=*/0,
                RuntimeFilterWrapper::State::UNINITED);
    }

    std::shared_ptr<QueryContext> make_query_ctx(bool ignore_rf_error) {
        auto opts = TQueryOptionsBuilder().build();
        opts.__set_ignore_runtime_filter_error(ignore_rf_error);
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        return QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), opts, fe_address, true,
                                    fe_address, QuerySource::INTERNAL_FRONTEND);
    }

    std::shared_ptr<SyncSizeCallback> make_callback(
            std::weak_ptr<QueryContext> ctx = {}, std::shared_ptr<Dependency> dep = nullptr,
            std::shared_ptr<RuntimeFilterWrapper> wrapper = nullptr,
            std::vector<std::weak_ptr<RuntimeFilterProducer>> producers = {}) {
        return SyncSizeCallback::create_shared(dep ? dep : _dependency,
                                               wrapper ? wrapper : _wrapper, std::move(ctx),
                                               std::move(producers));
    }

    // A producer of a local merge with two producers, which sent its size and now waits for the
    // synced size. Only it latches `dependency`, so the counter is 1.
    std::shared_ptr<RuntimeFilterProducer> make_waiting_producer(
            const std::shared_ptr<CountedFinishDependency>& dependency) {
        auto desc = TRuntimeFilterDescBuilder()
                            .set_build_bf_by_runtime_size(true)
                            .set_is_broadcast_join(false)
                            .add_planId_to_target_expr(0)
                            .build();
        std::shared_ptr<RuntimeFilterProducer> producer;
        EXPECT_TRUE(_runtime_states[0]->register_producer_runtime_filter(desc, &producer).ok());
        std::shared_ptr<RuntimeFilterProducer> producer2;
        EXPECT_TRUE(_runtime_states[1]->register_producer_runtime_filter(desc, &producer2).ok());
        std::shared_ptr<RuntimeFilterConsumer> consumer;
        EXPECT_TRUE(_runtime_states[1]
                            ->register_consumer_runtime_filter(desc, true, 0, &consumer)
                            .ok());
        producer->latch_dependency(dependency);
        EXPECT_TRUE(producer->send_size(_runtime_states[0].get(), 123).ok());
        EXPECT_EQ(producer->_rf_state, RuntimeFilterProducer::State::WAITING_FOR_SYNCED_SIZE);
        EXPECT_EQ(dependency->_counter, 1U);
        return producer;
    }

    std::shared_ptr<CountedFinishDependency> _dependency;
    std::shared_ptr<RuntimeFilterWrapper> _wrapper;
};

// ==================== cntl_->Failed() path ====================

TEST_F(SyncSizeCallbackTest, rpc_fail_cancels_query_when_ignore_rf_error_false) {
    auto ctx = make_query_ctx(false);
    auto callback = make_callback(ctx);
    callback->cntl_->SetFailed("injected failure");

    callback->call();

    // Query should be cancelled
    ASSERT_TRUE(ctx->is_cancelled());
    // Wrapper should be disabled
    ASSERT_EQ(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should have been subbed (counter back to 0 -> ready)
    ASSERT_TRUE(_dependency->ready());
}

TEST_F(SyncSizeCallbackTest, rpc_fail_does_not_cancel_query_when_ignore_rf_error_true) {
    auto ctx = make_query_ctx(true);
    auto callback = make_callback(ctx);
    callback->cntl_->SetFailed("injected failure");

    callback->call();

    // Query should NOT be cancelled
    ASSERT_FALSE(ctx->is_cancelled());
    // Wrapper should still be disabled
    ASSERT_EQ(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should have been subbed
    ASSERT_TRUE(_dependency->ready());
}

// ==================== response error status path ====================

TEST_F(SyncSizeCallbackTest, response_error_cancels_query_when_ignore_rf_error_false) {
    auto ctx = make_query_ctx(false);
    auto callback = make_callback(ctx);
    // Set a non-OK status in the response
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::INTERNAL_ERROR);
    status_pb->add_error_msgs("injected response error");

    callback->call();

    // Query should be cancelled
    ASSERT_TRUE(ctx->is_cancelled());
    // Wrapper should be disabled
    ASSERT_EQ(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should have been subbed
    ASSERT_TRUE(_dependency->ready());
}

TEST_F(SyncSizeCallbackTest, response_error_does_not_cancel_query_when_ignore_rf_error_true) {
    auto ctx = make_query_ctx(true);
    auto callback = make_callback(ctx);
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::INTERNAL_ERROR);
    status_pb->add_error_msgs("injected response error");

    callback->call();

    // Query should NOT be cancelled
    ASSERT_FALSE(ctx->is_cancelled());
    // Wrapper should still be disabled
    ASSERT_EQ(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should have been subbed
    ASSERT_TRUE(_dependency->ready());
}

// ==================== success path ====================

TEST_F(SyncSizeCallbackTest, success_path_no_cancel_no_sub) {
    auto ctx = make_query_ctx(false);
    auto callback = make_callback(ctx);
    // Default: cntl_ not failed, response status OK (status_code defaults to 0 = OK)

    callback->call();

    // Query should NOT be cancelled
    ASSERT_FALSE(ctx->is_cancelled());
    // Wrapper should NOT be disabled
    ASSERT_NE(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should NOT have been subbed (still blocked)
    ASSERT_FALSE(_dependency->ready());
}

// ==================== merged producers ====================

// A failed sync releases every producer whose size is merged into the request, through the
// producer, and not the legacy `dependency` of the callback.
TEST_F(SyncSizeCallbackTest, rpc_fail_releases_merged_producers) {
    auto ctx = make_query_ctx(true);
    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "MERGED_DEP");
    auto producer = make_waiting_producer(dependency);
    auto callback = make_callback(ctx, nullptr, nullptr, {producer});
    callback->cntl_->SetFailed("injected failure");

    callback->call();

    ASSERT_FALSE(ctx->is_cancelled());
    ASSERT_EQ(dependency->_counter, 0U);
    ASSERT_TRUE(dependency->ready());
    ASSERT_EQ(producer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    ASSERT_FALSE(_dependency->ready());
}

TEST_F(SyncSizeCallbackTest, response_error_releases_merged_producers) {
    auto ctx = make_query_ctx(true);
    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "MERGED_DEP");
    auto producer = make_waiting_producer(dependency);
    auto callback = make_callback(ctx, nullptr, nullptr, {producer});
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::INTERNAL_ERROR);
    status_pb->add_error_msgs("injected response error");

    callback->call();

    ASSERT_EQ(dependency->_counter, 0U);
    ASSERT_EQ(producer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
}

// The merge node may still sync a producer released on an error response, e.g. when it failed to
// sync another BE. Neither that nor a second release subs the dependency again, which would throw
// on underflow.
TEST_F(SyncSizeCallbackTest, released_producer_ignores_later_sync) {
    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "MERGED_DEP");
    auto producer = make_waiting_producer(dependency);

    producer->release_on_sync_size_error("injected failure");
    ASSERT_EQ(dependency->_counter, 0U);

    ASSERT_NO_THROW(producer->set_synced_size(124));
    ASSERT_NO_THROW(producer->release_on_sync_size_error("released twice"));
    ASSERT_EQ(dependency->_counter, 0U);
    ASSERT_EQ(producer->_synced_size, -1);
    ASSERT_EQ(producer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
}

// A producer synced before the error arrives keeps its synced size and filter.
TEST_F(SyncSizeCallbackTest, synced_producer_ignores_later_release) {
    auto dependency = std::make_shared<CountedFinishDependency>(0, 0, "MERGED_DEP");
    auto producer = make_waiting_producer(dependency);

    producer->set_synced_size(124);
    ASSERT_EQ(dependency->_counter, 0U);

    ASSERT_NO_THROW(producer->release_on_sync_size_error("late failure"));
    ASSERT_EQ(dependency->_counter, 0U);
    ASSERT_EQ(producer->_synced_size, 124);
    ASSERT_NE(producer->_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
}

// ==================== expired weak_ptr paths ====================

TEST_F(SyncSizeCallbackTest, rpc_fail_with_expired_query_context_no_crash) {
    // Create a temporary QueryContext that will be destroyed
    std::weak_ptr<QueryContext> expired_ctx;
    {
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        auto tmp_ctx =
                QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), _query_options,
                                     fe_address, true, fe_address, QuerySource::INTERNAL_FRONTEND);
        expired_ctx = tmp_ctx;
    }
    // expired_ctx is now expired

    auto callback = make_callback(expired_ctx);
    callback->cntl_->SetFailed("injected failure");

    // Should not crash
    callback->call();

    // Wrapper should be disabled
    ASSERT_EQ(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    // Dependency should have been subbed
    ASSERT_TRUE(_dependency->ready());
}

TEST_F(SyncSizeCallbackTest, rpc_fail_with_expired_wrapper_no_crash) {
    auto ctx = make_query_ctx(false);
    auto tmp_wrapper = std::make_shared<RuntimeFilterWrapper>(
            PrimitiveType::TYPE_INT, RuntimeFilterType::BLOOM_FILTER, /*filter_id=*/1,
            RuntimeFilterWrapper::State::UNINITED);
    auto callback = make_callback(ctx, _dependency, tmp_wrapper);
    // Destroy the wrapper before calling
    tmp_wrapper.reset();

    callback->cntl_->SetFailed("injected failure");

    // Should not crash even though wrapper is expired
    callback->call();

    // Query should be cancelled (ignore_runtime_filter_error defaults to false)
    ASSERT_TRUE(ctx->is_cancelled());
    // Dependency should have been subbed
    ASSERT_TRUE(_dependency->ready());
}

TEST_F(SyncSizeCallbackTest, auto_release_closure_requires_external_callback_owner) {
    auto ctx = make_query_ctx(false);
    auto request = std::make_shared<PSendFilterSizeRequest>();
    auto* closure = new AutoReleaseClosure<PSendFilterSizeRequest, SyncSizeCallback>(
            request, SyncSizeCallback::create_shared(_dependency, _wrapper, ctx));
    closure->cntl_->SetFailed("injected failure");

    closure->Run();

    ASSERT_FALSE(ctx->is_cancelled());
    ASSERT_NE(_wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
    ASSERT_FALSE(_dependency->ready());
}

// =====================================================================
// HandleErrorBrpcCallback error handling tests
// =====================================================================

class HandleErrorBrpcCallbackTest : public RuntimeFilterTest {
protected:
    std::shared_ptr<QueryContext> make_query_ctx() {
        auto opts = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        return QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), opts, fe_address, true,
                                    fe_address, QuerySource::INTERNAL_FRONTEND);
    }
};

class WeakMergeFilterCallback : public DummyBrpcCallback<PMergeFilterResponse> {
public:
    explicit WeakMergeFilterCallback(std::weak_ptr<QueryContext> context = {})
            : _context(std::move(context)) {}

    void call() override {
        if (this->cntl_->Failed()) {
            if (auto ctx = _context.lock()) {
                ctx->cancel(Status::NetworkError("RPC meet failed: {}", this->cntl_->ErrorText()));
            }
        }
    }

private:
    std::weak_ptr<QueryContext> _context;
};

TEST_F(HandleErrorBrpcCallbackTest, rpc_fail_cancels_query_when_context_provided) {
    auto ctx = make_query_ctx();
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(ctx);
    callback->cntl_->SetFailed("injected failure");

    callback->call();

    ASSERT_TRUE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, rpc_fail_no_cancel_when_no_context) {
    // Simulates ignore_runtime_filter_error=true pattern: pass empty weak_ptr
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared();
    callback->cntl_->SetFailed("injected failure");

    callback->call();
    // No crash, no cancel (no context to cancel)
}

TEST_F(HandleErrorBrpcCallbackTest, response_error_cancels_query_when_context_provided) {
    auto ctx = make_query_ctx();
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(ctx);
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::INTERNAL_ERROR);
    status_pb->add_error_msgs("injected error");

    callback->call();

    ASSERT_TRUE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, response_error_no_cancel_when_no_context) {
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared();
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::INTERNAL_ERROR);
    status_pb->add_error_msgs("injected error");

    callback->call();
    // No crash, no cancel
}

TEST_F(HandleErrorBrpcCallbackTest, success_path_no_cancel) {
    auto ctx = make_query_ctx();
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(ctx);

    callback->call();

    ASSERT_FALSE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, expired_query_context_no_crash) {
    std::weak_ptr<QueryContext> expired_ctx;
    {
        auto tmp_ctx = make_query_ctx();
        expired_ctx = tmp_ctx;
    }

    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(expired_ctx);
    callback->cntl_->SetFailed("injected failure");

    callback->call();
    // Should not crash; expired context
}

TEST_F(HandleErrorBrpcCallbackTest, end_of_file_status_does_not_cancel) {
    auto ctx = make_query_ctx();
    auto callback = HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(ctx);
    auto* status_pb = callback->response_->mutable_status();
    status_pb->set_status_code(TStatusCode::END_OF_FILE);

    callback->call();

    ASSERT_FALSE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, auto_release_closure_requires_external_callback_owner) {
    auto ctx = make_query_ctx();
    auto request = std::make_shared<PMergeFilterRequest>();
    auto* closure = new AutoReleaseClosure<PMergeFilterRequest,
                                           HandleErrorBrpcCallback<PMergeFilterResponse>>(
            request, HandleErrorBrpcCallback<PMergeFilterResponse>::create_shared(ctx));
    closure->cntl_->SetFailed("injected failure");

    closure->Run();

    ASSERT_FALSE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, weak_auto_release_closure_requires_external_callback_owner) {
    auto ctx = make_query_ctx();
    auto request = std::make_shared<PMergeFilterRequest>();
    auto* closure = new AutoReleaseClosure<PMergeFilterRequest, WeakMergeFilterCallback>(
            request, std::make_shared<WeakMergeFilterCallback>(ctx));
    closure->cntl_->SetFailed("injected failure");

    closure->Run();

    ASSERT_FALSE(ctx->is_cancelled());
}

TEST_F(HandleErrorBrpcCallbackTest, weak_auto_release_closure_runs_with_external_callback_owner) {
    auto ctx = make_query_ctx();
    auto request = std::make_shared<PMergeFilterRequest>();
    auto callback = std::make_shared<WeakMergeFilterCallback>(ctx);
    auto* closure =
            new AutoReleaseClosure<PMergeFilterRequest, WeakMergeFilterCallback>(request, callback);
    closure->cntl_->SetFailed("injected failure");

    closure->Run();

    ASSERT_TRUE(ctx->is_cancelled());
}

// ==================== CountedFinishDependency::sub() underflow guard ====================

TEST(CountedFinishDependencyTest, sub_with_zero_counter_throws) {
    auto dep = std::make_shared<CountedFinishDependency>(0, 0, "TEST_DEP");
    // Counter is 0 from construction; sub() must not silently underflow.
    EXPECT_THROW(dep->sub(), Exception);
}

TEST(CountedFinishDependencyTest, sub_after_balanced_add_sub_throws) {
    auto dep = std::make_shared<CountedFinishDependency>(0, 0, "TEST_DEP");
    dep->add();
    dep->sub(); // counter back to 0, dependency now ready
    // A stray extra sub() must throw rather than wrap to UINT32_MAX (which would
    // hang the query forever).
    EXPECT_THROW(dep->sub(), Exception);
}

TEST(CountedFinishDependencyTest, balanced_add_sub_sets_ready) {
    auto dep = std::make_shared<CountedFinishDependency>(0, 0, "TEST_DEP");
    dep->add(3);
    EXPECT_FALSE(dep->ready());
    dep->sub();
    dep->sub();
    EXPECT_FALSE(dep->ready());
    dep->sub();
    EXPECT_TRUE(dep->ready());
}

} // namespace doris
