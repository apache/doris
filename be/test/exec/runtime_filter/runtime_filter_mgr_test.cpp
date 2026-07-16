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

#include "exec/runtime_filter/runtime_filter_mgr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "exec/pipeline/thrift_builder.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "runtime/query_context.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

RuntimeFilterPublishTarget make_publish_target(int index) {
    RuntimeFilterPublishTarget target;
    target.addr.set_hostname("host" + std::to_string(index));
    target.addr.set_port(9000 + index);
    target.fragment_ids.push_back(index);
    return target;
}

std::vector<int32_t> flatten_fragment_ids(
        const std::vector<std::vector<RuntimeFilterPublishTarget>>& slices) {
    std::vector<int32_t> fragment_ids;
    for (const auto& slice : slices) {
        for (const auto& target : slice) {
            fragment_ids.insert(fragment_ids.end(), target.fragment_ids.begin(),
                                target.fragment_ids.end());
        }
    }
    return fragment_ids;
}

} // namespace

class RuntimeFilterMgrTest : public testing::Test {
public:
    RuntimeFilterMgrTest() = default;
    ~RuntimeFilterMgrTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterMgrTest, TestRuntimeFilterMgr) {
    auto filter_id = 0;
    std::shared_ptr<RuntimeFilterMgr> global_runtime_filter_mgr;
    std::shared_ptr<RuntimeFilterMgr> local_runtime_filter_mgr;
    std::shared_ptr<QueryContext> ctx;
    RuntimeState state;
    auto profile = std::make_shared<RuntimeProfile>("Test");
    auto desc = TRuntimeFilterDescBuilder()
                        .add_planId_to_target_expr(0)
                        .set_build_bf_by_runtime_size(true)
                        .build();
    {
        // Create
        auto query_options = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = BackendOptions::get_localhost();
        fe_address.port = config::brpc_port;
        ctx = QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options, fe_address,
                                   true, fe_address, QuerySource::INTERNAL_FRONTEND);
        state._query_ctx = ctx.get();

        global_runtime_filter_mgr = std::make_shared<RuntimeFilterMgr>(true);
        local_runtime_filter_mgr = std::make_shared<RuntimeFilterMgr>(false);
    }

    {
        // Get / Register consumer
        EXPECT_TRUE(global_runtime_filter_mgr->get_consume_filters(filter_id).empty());
        std::shared_ptr<RuntimeFilterConsumer> consumer_filter;
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->register_consumer_filter(&state, desc, 0, &consumer_filter)
                            .ok());
        EXPECT_FALSE(global_runtime_filter_mgr->get_consume_filters(filter_id).empty());
    }

    {
        // Get / Register producer

        std::shared_ptr<RuntimeFilterProducer> producer_filter;
        // producer_filter should not be nullptr
        EXPECT_FALSE(
                global_runtime_filter_mgr
                        ->register_local_merge_producer_filter(ctx.get(), desc, producer_filter)
                        .ok());
        // local merge filter should not be registered in local mgr
        EXPECT_FALSE(
                local_runtime_filter_mgr
                        ->register_local_merge_producer_filter(ctx.get(), desc, producer_filter)
                        .ok());
        // producer should not registered in global mgr
        EXPECT_FALSE(global_runtime_filter_mgr
                             ->register_producer_filter(ctx.get(), desc, &producer_filter)
                             .ok());
        EXPECT_EQ(producer_filter, nullptr);
        // Register in local mgr
        EXPECT_TRUE(local_runtime_filter_mgr
                            ->register_producer_filter(ctx.get(), desc, &producer_filter)
                            .ok());
        auto mocked_dependency =
                std::make_shared<CountedFinishDependency>(0, 0, "MOCKED_FINISH_DEPENDENCY");
        producer_filter->latch_dependency(mocked_dependency);
        EXPECT_NE(producer_filter, nullptr);
        // Register in local mgr twice
        EXPECT_FALSE(local_runtime_filter_mgr
                             ->register_producer_filter(ctx.get(), desc, &producer_filter)
                             .ok());
        EXPECT_NE(producer_filter, nullptr);

        std::shared_ptr<LocalMergeContext> context;
        // filter_id not yet registered: global mgr returns OK with nullptr
        // (graceful skip for recursive CTE stage reset).
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->get_local_merge_context(filter_id, producer_filter->stage(), &context)
                            .ok());
        EXPECT_EQ(context, nullptr);
        // local mgr always returns error (not supported)
        EXPECT_FALSE(
                local_runtime_filter_mgr
                        ->get_local_merge_context(filter_id, producer_filter->stage(), &context)
                        .ok());
        // Register local merge filter
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->register_local_merge_producer_filter(ctx.get(), desc, producer_filter)
                            .ok());
        EXPECT_TRUE(global_runtime_filter_mgr
                            ->get_local_merge_context(filter_id, producer_filter->stage(), &context)
                            .ok());
        EXPECT_NE(context, nullptr);
        EXPECT_NE(context->merger, nullptr);
        EXPECT_EQ(context->producers.size(), 1);
        context->producers.front()->_rf_state =
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

TEST_F(RuntimeFilterMgrTest, TestRuntimeFilterMergeControllerEntity) {
    int rid = 1;
    UniqueId query_id;
    std::shared_ptr<QueryContext> ctx;
    std::shared_ptr<RuntimeFilterMergeControllerEntity> entity;
    auto profile = std::make_shared<RuntimeProfile>("Test");
    RuntimeState state;
    {
        // Create
        auto query_options = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = BackendOptions::get_localhost();
        fe_address.port = config::brpc_port;
        ctx = QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options, fe_address,
                                   true, fe_address, QuerySource::INTERNAL_FRONTEND);
        entity = std::make_shared<RuntimeFilterMergeControllerEntity>();
    }
    {
        // Init
        TRuntimeFilterParams param =
                TRuntimeFilterParamsBuilder()
                        .add_rid_to_runtime_filter(
                                rid,
                                TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build())
                        .add_rid_to_target_paramv2(rid, {TRuntimeFilterTargetParamsV2()})
                        .build();
        EXPECT_FALSE(entity->init(ctx, param).ok());

        param = TRuntimeFilterParamsBuilder()
                        .add_rid_to_runtime_filter(
                                rid,
                                TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build())
                        .add_runtime_filter_builder_num(rid, 1)
                        .add_rid_to_target_paramv2(rid, {TRuntimeFilterTargetParamsV2()})
                        .build();
        EXPECT_TRUE(entity->init(ctx, param).ok());
    }
}

TEST_F(RuntimeFilterMgrTest, SplitRuntimeFilterPublishTargets) {
    std::vector<RuntimeFilterPublishTarget> targets;
    for (int i = 0; i < 10; ++i) {
        targets.push_back(make_publish_target(i));
    }

    auto slices = split_runtime_filter_publish_targets(targets, 3);
    ASSERT_EQ(slices.size(), 3);
    EXPECT_EQ(slices[0].size(), 4);
    EXPECT_EQ(slices[1].size(), 3);
    EXPECT_EQ(slices[2].size(), 3);
    EXPECT_EQ(flatten_fragment_ids(slices), (std::vector<int32_t> {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    slices = split_runtime_filter_publish_targets(targets, 20);
    ASSERT_EQ(slices.size(), targets.size());
    for (const auto& slice : slices) {
        EXPECT_EQ(slice.size(), 1);
    }

    slices = split_runtime_filter_publish_targets(targets, 1);
    ASSERT_EQ(slices.size(), 1);
    EXPECT_EQ(slices[0].size(), targets.size());
}

TEST_F(RuntimeFilterMgrTest, CalculateTreePublishFanout) {
    constexpr int64_t MB = 1024L * 1024L;
    constexpr int64_t max_send_bytes = 256L * MB;
    constexpr size_t target_count = 48;

    EXPECT_EQ(calculate_tree_publish_fanout(1L * MB, target_count, max_send_bytes), 0);
    EXPECT_EQ(calculate_tree_publish_fanout(4L * MB, target_count, max_send_bytes), 0);
    EXPECT_EQ(calculate_tree_publish_fanout(8L * MB, target_count, max_send_bytes), 32);
    EXPECT_EQ(calculate_tree_publish_fanout(16L * MB, target_count, max_send_bytes), 16);
    EXPECT_EQ(calculate_tree_publish_fanout(32L * MB, target_count, max_send_bytes), 8);
    EXPECT_EQ(calculate_tree_publish_fanout(64L * MB, target_count, max_send_bytes), 4);
    EXPECT_EQ(calculate_tree_publish_fanout(128L * MB, target_count, max_send_bytes), 2);

    EXPECT_EQ(calculate_tree_publish_fanout(4L * MB, target_count, 0), 0);
    EXPECT_EQ(calculate_tree_publish_fanout(256L * MB, target_count, max_send_bytes), 1);
    EXPECT_EQ(calculate_tree_publish_fanout(1L * MB, 1024, max_send_bytes), 256);
}

TEST_F(RuntimeFilterMgrTest, BuildRuntimeFilterPublishTasks) {
    PPublishFilterRequestV2 base_request;
    base_request.set_filter_id(10);
    base_request.mutable_query_id()->set_hi(1);
    base_request.mutable_query_id()->set_lo(2);
    base_request.set_filter_type(PFilterType::BLOOM_FILTER);
    base_request.set_tree_publish_fanout(2);
    base_request.set_publish_rpc_timeout_ms(3000);
    base_request.add_fragment_ids(999);
    auto* stale_forward_target = base_request.add_forward_targets();
    stale_forward_target->mutable_target_addr()->set_hostname("stale");
    stale_forward_target->mutable_target_addr()->set_port(1);
    stale_forward_target->add_fragment_ids(999);

    std::vector<RuntimeFilterPublishTarget> targets;
    for (int i = 0; i < 5; ++i) {
        targets.push_back(make_publish_target(i));
    }

    auto tasks = build_runtime_filter_publish_tasks(base_request, targets, 2);
    ASSERT_EQ(tasks.size(), 2);

    EXPECT_EQ(tasks[0].receiver.addr.hostname(), "host0");
    EXPECT_EQ(tasks[0].request.fragment_ids_size(), 1);
    EXPECT_EQ(tasks[0].request.fragment_ids(0), 0);
    ASSERT_EQ(tasks[0].request.forward_targets_size(), 2);
    EXPECT_EQ(tasks[0].request.forward_targets(0).target_addr().hostname(), "host1");
    EXPECT_EQ(tasks[0].request.forward_targets(0).fragment_ids(0), 1);
    EXPECT_EQ(tasks[0].request.forward_targets(1).target_addr().hostname(), "host2");
    EXPECT_EQ(tasks[0].request.forward_targets(1).fragment_ids(0), 2);

    EXPECT_EQ(tasks[1].receiver.addr.hostname(), "host3");
    EXPECT_EQ(tasks[1].request.fragment_ids_size(), 1);
    EXPECT_EQ(tasks[1].request.fragment_ids(0), 3);
    ASSERT_EQ(tasks[1].request.forward_targets_size(), 1);
    EXPECT_EQ(tasks[1].request.forward_targets(0).target_addr().hostname(), "host4");
    EXPECT_EQ(tasks[1].request.forward_targets(0).fragment_ids(0), 4);

    EXPECT_EQ(tasks[0].request.filter_id(), base_request.filter_id());
    EXPECT_EQ(tasks[0].request.tree_publish_fanout(), 2);
    EXPECT_EQ(tasks[0].request.publish_rpc_timeout_ms(), 3000);
}

} // namespace doris
