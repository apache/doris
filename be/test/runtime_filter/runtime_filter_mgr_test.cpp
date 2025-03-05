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

#include "runtime_filter/runtime_filter_mgr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "pipeline/thrift_builder.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter_producer.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class RuntimeFilterMgrTest : public testing::Test {
public:
    RuntimeFilterMgrTest() = default;
    ~RuntimeFilterMgrTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterMgrTest, TestGlobalMgr) {
    auto filter_id = 0;
    std::shared_ptr<RuntimeFilterMgr> global_runtime_filter_mgr;
    std::shared_ptr<RuntimeFilterMgr> local_runtime_filter_mgr;
    std::shared_ptr<QueryContext> ctx;
    RuntimeState state;
    auto profile = std::make_shared<RuntimeProfile>("Test");
    auto desc = TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build();
    {
        // Create
        auto query_options = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = BackendOptions::get_localhost();
        fe_address.port = config::brpc_port;
        ctx = QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options, fe_address,
                                   true, fe_address, QuerySource::INTERNAL_FRONTEND);
        state._query_ctx = ctx.get();

        global_runtime_filter_mgr = std::make_shared<RuntimeFilterMgr>(
                TUniqueId(), RuntimeFilterParamsContext::create(ctx.get()),
                ctx->query_mem_tracker(), true);
        local_runtime_filter_mgr = std::make_shared<RuntimeFilterMgr>(
                TUniqueId(), RuntimeFilterParamsContext::create(&state), ctx->query_mem_tracker(),
                false);
    }

    {
        // Get / Register consumer
        EXPECT_TRUE(global_runtime_filter_mgr->get_consume_filters(filter_id).empty());
        std::shared_ptr<RuntimeFilterConsumer> consumer_filter;
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->register_consumer_filter(desc, 0, &consumer_filter, profile.get())
                            .ok());
        EXPECT_FALSE(global_runtime_filter_mgr->get_consume_filters(filter_id).empty());
    }

    {
        // Get / Register producer

        std::shared_ptr<RuntimeFilterProducer> producer_filter;
        // producer_filter should not be nullptr
        EXPECT_FALSE(global_runtime_filter_mgr
                             ->register_local_merger_producer_filter(desc, producer_filter,
                                                                     profile.get())
                             .ok());
        // local merge filter should not be registered in local mgr
        EXPECT_FALSE(local_runtime_filter_mgr
                             ->register_local_merger_producer_filter(desc, producer_filter,
                                                                     profile.get())
                             .ok());
        // producer should not registered in global mgr
        EXPECT_FALSE(global_runtime_filter_mgr
                             ->register_producer_filter(desc, &producer_filter, profile.get())
                             .ok());
        EXPECT_EQ(producer_filter, nullptr);
        // Register in local mgr
        EXPECT_TRUE(local_runtime_filter_mgr
                            ->register_producer_filter(desc, &producer_filter, profile.get())
                            .ok());
        EXPECT_NE(producer_filter, nullptr);
        // Register in local mgr twice
        EXPECT_FALSE(local_runtime_filter_mgr
                             ->register_producer_filter(desc, &producer_filter, profile.get())
                             .ok());
        EXPECT_NE(producer_filter, nullptr);

        LocalMergeContext* local_merge_filters = nullptr;
        EXPECT_FALSE(global_runtime_filter_mgr
                             ->get_local_merge_producer_filters(filter_id, &local_merge_filters)
                             .ok());
        EXPECT_FALSE(local_runtime_filter_mgr
                             ->get_local_merge_producer_filters(filter_id, &local_merge_filters)
                             .ok());
        // Register local merge filter
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->register_local_merger_producer_filter(desc, producer_filter,
                                                                    profile.get())
                            .ok());
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->get_local_merge_producer_filters(filter_id, &local_merge_filters)
                            .ok());
        EXPECT_NE(local_merge_filters, nullptr);
        EXPECT_EQ(local_merge_filters->producers.size(), 1);
        local_merge_filters->producers.front()->_rf_state =
                RuntimeFilterProducer::State ::WAITING_FOR_SYNCED_SIZE;
    }
    {
        TNetworkAddress addr;
        EXPECT_FALSE(global_runtime_filter_mgr->get_merge_addr(&addr).ok());

        TRuntimeFilterParams param;
        TNetworkAddress new_addr;
        param.__set_runtime_filter_merge_addr(new_addr);
        EXPECT_TRUE(global_runtime_filter_mgr->set_runtime_filter_params(param));
        EXPECT_FALSE(global_runtime_filter_mgr->set_runtime_filter_params(param));
        EXPECT_TRUE(global_runtime_filter_mgr->get_merge_addr(&addr).ok());
    }
    {
        PSyncFilterSizeRequest request;
        request.set_filter_id(filter_id);
        request.set_filter_size(16);
        EXPECT_TRUE(global_runtime_filter_mgr->sync_filter_size(&request).ok());
    }
}

} // namespace doris
