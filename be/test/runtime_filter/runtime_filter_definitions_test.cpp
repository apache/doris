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

#include "runtime_filter/runtime_filter_definitions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "pipeline/thrift_builder.h"
#include "runtime/query_context.h"

namespace doris {

class RuntimeFilterDefinitionTest : public testing::Test {
public:
    RuntimeFilterDefinitionTest() = default;
    ~RuntimeFilterDefinitionTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterDefinitionTest, TestErrorPath) {
    RuntimeState state;
    auto query_options = TQueryOptionsBuilder().build();
    auto fe_address = TNetworkAddress();
    fe_address.hostname = BackendOptions::get_localhost();
    fe_address.port = config::brpc_port;
    auto ctx = QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options, fe_address,
                                    true, fe_address, QuerySource::INTERNAL_FRONTEND);
    state._query_ctx = ctx.get();
    {
        auto* res = RuntimeFilterParamsContext::create(&state);
        EXPECT_NE(res->get_query_ctx(), nullptr);
        EXPECT_NE(res->get_runtime_state(), nullptr);
        EXPECT_NE(res->global_runtime_filter_mgr(), nullptr);
        EXPECT_NE(res->local_runtime_filter_mgr(), nullptr);
    }
    {
        auto* res = RuntimeFilterParamsContext::create(ctx.get());
        EXPECT_NE(res->get_query_ctx(), nullptr);
        EXPECT_EQ(res->get_runtime_state(), nullptr);
        EXPECT_NE(res->global_runtime_filter_mgr(), nullptr);
        bool ex = false;
        try {
            [[maybe_unused]] auto* local = res->local_runtime_filter_mgr();
        } catch (std::exception) {
            ex = true;
        }
        EXPECT_TRUE(ex);
        res->set_state(&state);
        EXPECT_NE(res->get_runtime_state(), nullptr);
        EXPECT_NE(res->local_runtime_filter_mgr(), nullptr);
    }
}

} // namespace doris
