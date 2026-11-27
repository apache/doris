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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "io/fs/stream_load_pipe.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/defer_op.h"

namespace doris {

TEST(GracefulShutdownTest, CountsLoadUntilLastOwnerAndRollbackFinish) {
    const auto baseline = StreamLoadContext::num_active_contexts();
    ExecEnv env;
    class RollbackExecutor : public StreamLoadExecutor {
    public:
        RollbackExecutor(ExecEnv* env, size_t baseline)
                : StreamLoadExecutor(env), _baseline(baseline) {}
        void rollback_txn(StreamLoadContext* ctx) override {
            // The load must still be counted during its final transaction RPC.
            EXPECT_EQ(StreamLoadContext::num_active_contexts(), _baseline + 1);
            rolled_back = true;
        }
        bool rolled_back = false;

    private:
        size_t _baseline;
    };
    auto executor = std::make_unique<RollbackExecutor>(&env, baseline);
    auto* observer = executor.get();
    env.set_stream_load_executor(std::move(executor));
    auto context = StreamLoadContext::create_shared(&env);
    context->need_rollback = true;
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline + 1);
    auto retained = context;
    context.reset();
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline + 1);
    EXPECT_FALSE(observer->rolled_back);
    retained.reset();
    EXPECT_TRUE(observer->rolled_back);
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline);
    env.clear_stream_load_executor();
}

TEST(GracefulShutdownTest, FailedTransactionalSetupReleasesContextBeforeFragmentCreation) {
    const auto baseline = StreamLoadContext::num_active_contexts();
    ExecEnv env;
    FragmentMgr fragments(&env);
    env._fragment_mgr = &fragments;
    env.set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
    class SetupFailureExecutor : public StreamLoadExecutor {
    public:
        SetupFailureExecutor(ExecEnv* env, size_t baseline)
                : StreamLoadExecutor(env), _baseline(baseline) {}

        Status execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx,
                                     const TPipelineFragmentParamsList& parent) override {
            EXPECT_EQ(_exec_env->new_load_stream_mgr()->get(ctx->id).get(), ctx.get());
            context = ctx;
            pipe = ctx->pipe;
            // Exercise the real early _get_or_create_query_ctx error. The BE_TEST executor
            // normally bypasses FragmentMgr and just fulfills the load status promise.
            return _exec_env->fragment_mgr()->exec_plan_fragment(
                    ctx->put_result.pipeline_params, QuerySource::STREAM_LOAD,
                    [this](RuntimeState*, Status*) { callback_invoked = true; }, parent);
        }

        void rollback_txn(StreamLoadContext* ctx) override {
            EXPECT_EQ(_exec_env->new_load_stream_mgr()->get(ctx->id), nullptr);
            EXPECT_TRUE(ctx->body_sink->cancelled());
            EXPECT_FALSE(ctx->status.ok());
            EXPECT_EQ(StreamLoadContext::num_active_contexts(), _baseline + 1);
            ++rollbacks;
        }

        std::weak_ptr<StreamLoadContext> context;
        std::shared_ptr<io::StreamLoadPipe> pipe;
        bool callback_invoked = false;
        int rollbacks = 0;

    private:
        size_t _baseline;
    };
    auto executor = std::make_unique<SetupFailureExecutor>(&env, baseline);
    auto* observer = executor.get();
    env.set_stream_load_executor(std::move(executor));
    TPipelineFragmentParams params;
    params.__set_query_id(UniqueId(101, 202).to_thrift());
    params.__set_is_simplified_param(true);
    params.txn_conf.__set_need_txn(true);
    params.txn_conf.__set_txn_id(123);
    Defer cleanup {[&] {
        // Also clean up safely on the old, leaking implementation when an expectation fails.
        auto retained = env.new_load_stream_mgr()->get(params.query_id);
        env.new_load_stream_mgr()->remove(params.query_id);
        retained.reset();
        fragments.stop();
        env._fragment_mgr = nullptr;
        env.clear_new_load_stream_mgr();
        env.clear_stream_load_executor();
    }};

    const auto status = fragments.exec_plan_fragment(params, QuerySource::INTERNAL_FRONTEND, {});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Failed to get query fragments context"), std::string::npos);
    EXPECT_FALSE(observer->callback_invoked);
    EXPECT_EQ(fragments._pipeline_map.num_items(), 0);
    EXPECT_EQ(env.new_load_stream_mgr()->get(params.query_id), nullptr);
    EXPECT_EQ(observer->rollbacks, 1);
    ASSERT_NE(observer->pipe, nullptr);
    EXPECT_TRUE(observer->pipe->cancelled());
    EXPECT_TRUE(observer->context.expired());
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline);
}

} // namespace doris
