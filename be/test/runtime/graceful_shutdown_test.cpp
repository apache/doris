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

#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/exec_env.h"

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

} // namespace doris
