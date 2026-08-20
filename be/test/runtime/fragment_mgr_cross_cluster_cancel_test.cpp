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

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/frontend_info.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "storage/id_manager.h"
#include "util/defer_op.h"

namespace doris {

class FragmentMgrCrossClusterCancelTest : public testing::Test {
public:
    void SetUp() override {
        // Make frontends list deterministic for this ExecEnv instance.
        _exec_env.update_frontends({});

        _exec_env._workload_group_manager = new WorkloadGroupMgr();
        // Ensure there is a "normal" workload group, otherwise WorkloadGroupMgr::get_group() will throw.
        WorkloadGroupInfo normal_wg_info {.id = 1, .name = "normal"};
        _exec_env._workload_group_manager->get_or_create_workload_group(normal_wg_info);

        _exec_env._fragment_mgr = new FragmentMgr(&_exec_env);
    }

    void TearDown() override {
        if (_exec_env._fragment_mgr != nullptr) {
            _exec_env._fragment_mgr->stop();
        }
        delete _exec_env._fragment_mgr;
        _exec_env._fragment_mgr = nullptr;
        delete _exec_env._workload_group_manager;
        _exec_env._workload_group_manager = nullptr;
        _exec_env.update_frontends({});
    }

protected:
    static TDescriptorTable _make_min_desc_tbl() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(true)
                                       .column_name("c1")
                                       .column_pos(1)
                                       .build());
        tuple_builder.build(&dtb);
        return dtb.desc_tbl();
    }

    static TQueryOptions _make_min_query_options(int64_t fe_process_uuid) {
        TQueryOptions query_options;
        query_options.__set_query_type(TQueryType::SELECT);
        query_options.__set_execution_timeout(60);
        query_options.__set_query_timeout(60);
        query_options.__set_mem_limit(64L * 1024 * 1024);
        query_options.__set_fe_process_uuid(fe_process_uuid);
        return query_options;
    }

    ExecEnv _exec_env;
};

TEST_F(FragmentMgrCrossClusterCancelTest,
       MarkQuerySourceAsExternalFrontendWhenCoordinatorNotLocal) {
    auto* fragment_mgr = _exec_env.fragment_mgr();
    ASSERT_NE(fragment_mgr, nullptr);

    TUniqueId query_id;
    query_id.__set_hi(1);
    query_id.__set_lo(2);

    TNetworkAddress coord;
    coord.hostname = "fe-a";
    coord.port = 9030;

    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(false);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 123));
    params.__set_desc_tbl(_make_min_desc_tbl());

    std::shared_ptr<QueryContext> query_ctx;
    TPipelineFragmentParamsList parent;
    auto st = fragment_mgr->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                     query_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(query_ctx, nullptr);
    EXPECT_EQ(query_ctx->get_query_source(), QuerySource::EXTERNAL_FRONTEND);
}

TEST_F(FragmentMgrCrossClusterCancelTest, CancelWorkerInvalidQueryDetectionSkipsExternalFrontend) {
    auto* fragment_mgr = _exec_env.fragment_mgr();
    ASSERT_NE(fragment_mgr, nullptr);

    TUniqueId query_id;
    query_id.__set_hi(3);
    query_id.__set_lo(4);

    TNetworkAddress coord;
    coord.hostname = "fe-b";
    coord.port = 9030;

    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(false);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 456));
    params.__set_desc_tbl(_make_min_desc_tbl());

    std::shared_ptr<QueryContext> query_ctx;
    TPipelineFragmentParamsList parent;
    auto st = fragment_mgr->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                     query_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(query_ctx, nullptr);
    ASSERT_EQ(query_ctx->get_query_source(), QuerySource::EXTERNAL_FRONTEND);

    std::vector<TUniqueId> queries_lost_coordinator;
    std::vector<TUniqueId> queries_pipeline_task_leak;
    std::map<int64_t, std::unordered_set<TUniqueId>> running_queries_on_all_fes;
    std::map<TNetworkAddress, FrontendInfo> running_fes;
    timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 0;

    fragment_mgr->_collect_invalid_queries(queries_lost_coordinator, queries_pipeline_task_leak,
                                           running_queries_on_all_fes, running_fes, ts);
    EXPECT_TRUE(queries_lost_coordinator.empty());
    EXPECT_TRUE(queries_pipeline_task_leak.empty());
}

TEST(FragmentMgrDelayDeleteMapTest, ClearShouldNotAbortWhenReleasingLastQueryContextRef) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = new FragmentMgr(exec_env);

    TUniqueId query_id;
    query_id.__set_hi(101);
    query_id.__set_lo(202);

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_execution_timeout(60);
    query_options.__set_mem_limit(64L * 1024 * 1024);

    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;

    auto query_ctx =
            QueryContext::create(query_id, exec_env, query_options, fe_addr,
                                 /*is_nereids*/ true, fe_addr, QuerySource::INTERNAL_FRONTEND);
    exec_env->_fragment_mgr->_retain_query_context_for_runtime_filter(query_id, query_ctx);
    query_ctx.reset();

    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.clear();
    exec_env->_fragment_mgr->stop();
    delete exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = previous_fragment_mgr;
}

TEST(FragmentMgrDelayDeleteMapTest, ReleaseQueryContextAfterAllRuntimeFiltersPublished) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = new FragmentMgr(exec_env);

    TUniqueId query_id;
    query_id.__set_hi(203);
    query_id.__set_lo(304);

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::LOAD);
    query_options.__set_execution_timeout(60);
    query_options.__set_mem_limit(64L * 1024 * 1024);

    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;

    auto query_ctx =
            QueryContext::create(query_id, exec_env, query_options, fe_addr,
                                 /*is_nereids*/ true, fe_addr, QuerySource::INTERNAL_FRONTEND);
    auto handler = std::make_shared<RuntimeFilterMergeControllerEntity>();
    TRuntimeFilterParams runtime_filter_params;
    ASSERT_TRUE(handler->init(query_ctx, runtime_filter_params).ok());
    constexpr int filter_id = 1;
    {
        LockGuard guard(handler->_filter_map_mutex);
        auto [filter, inserted] = handler->_filter_map.try_emplace(filter_id);
        ASSERT_TRUE(inserted);
    }
    query_ctx->set_merge_controller_handler(handler);

    std::shared_ptr<QueryContext> existing_query_ctx;
    ASSERT_TRUE(exec_env->_fragment_mgr->_query_ctx_map
                        .apply_if_not_exists(query_id, existing_query_ctx,
                                             [&](auto& map) {
                                                 map.insert({query_id, query_ctx});
                                                 return Status::OK();
                                             })
                        .ok());
    exec_env->_fragment_mgr->_retain_query_context_for_runtime_filter(query_id, query_ctx);

    PMergeFilterRequest request;
    request.mutable_query_id()->set_hi(query_id.hi);
    request.mutable_query_id()->set_lo(query_id.lo);
    request.set_filter_id(filter_id);
    request.set_stage(1);

    ASSERT_TRUE(exec_env->_fragment_mgr->merge_filter(&request, nullptr).ok());
    EXPECT_EQ(exec_env->_fragment_mgr->_query_ctx_map_delay_delete.num_items(), 1);

    auto stats = exec_env->_fragment_mgr->get_query_ctx_map_delay_delete_stats();
    ASSERT_EQ(stats.contexts.size(), 1);
    EXPECT_EQ(stats.contexts[0].query_id, query_id);
    auto dump = exec_env->_fragment_mgr->dump_query_ctx_map_delay_delete();
    EXPECT_NE(dump.find(print_id(query_id)), std::string::npos);
    EXPECT_NE(dump.find("state=waiting_runtime_filters"), std::string::npos);

    {
        LockGuard guard(handler->_filter_map_mutex);
        handler->_filter_map.at(filter_id).done = true;
    }
    ASSERT_TRUE(exec_env->_fragment_mgr->merge_filter(&request, nullptr).ok());
    EXPECT_EQ(exec_env->_fragment_mgr->_query_ctx_map_delay_delete.num_items(), 0);

    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;
    query_ctx.reset();
    EXPECT_TRUE(weak_query_ctx.expired());

    exec_env->_fragment_mgr->stop();
    delete exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = previous_fragment_mgr;
}

TEST(FragmentMgrDelayDeleteMapTest, CancellationReleasesQueryContext) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = new FragmentMgr(exec_env);

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::LOAD);
    query_options.__set_execution_timeout(60);
    query_options.__set_mem_limit(64L * 1024 * 1024);

    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;

    TUniqueId cancelled_query_id;
    cancelled_query_id.__set_hi(607);
    cancelled_query_id.__set_lo(708);
    auto cancelled_query_ctx =
            QueryContext::create(cancelled_query_id, exec_env, query_options, fe_addr,
                                 /*is_nereids*/ true, fe_addr, QuerySource::INTERNAL_FRONTEND);
    exec_env->_fragment_mgr->_retain_query_context_for_runtime_filter(cancelled_query_id,
                                                                      cancelled_query_ctx);
    std::weak_ptr<QueryContext> weak_query_ctx = cancelled_query_ctx;
    cancelled_query_ctx->cancel(Status::Cancelled("unit test"));
    EXPECT_EQ(exec_env->_fragment_mgr->_query_ctx_map_delay_delete.num_items(), 0);
    EXPECT_TRUE(exec_env->_fragment_mgr->get_query_ctx_map_delay_delete_stats().contexts.empty());
    EXPECT_FALSE(weak_query_ctx.expired());
    cancelled_query_ctx.reset();
    EXPECT_TRUE(weak_query_ctx.expired());

    exec_env->_fragment_mgr->stop();
    delete exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = previous_fragment_mgr;
}

TEST(FragmentMgrRerunnableParamsTest, StopReleasesLastQueryContextRefOutsideLock) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    auto* fragment_mgr = new FragmentMgr(exec_env);
    exec_env->_fragment_mgr = fragment_mgr;

    TUniqueId query_id;
    query_id.__set_hi(303);
    query_id.__set_lo(404);

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_execution_timeout(60);
    query_options.__set_mem_limit(64L * 1024 * 1024);

    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;

    auto query_ctx =
            QueryContext::create(query_id, exec_env, query_options, fe_addr,
                                 /*is_nereids*/ true, fe_addr, QuerySource::INTERNAL_FRONTEND);
    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;
    {
        std::lock_guard<std::mutex> lock(fragment_mgr->_rerunnable_params_lock);
        fragment_mgr->_rerunnable_params_map[{query_id, 1}].query_ctx = query_ctx;
    }
    query_ctx.reset();

    EXPECT_FALSE(weak_query_ctx.expired());
    fragment_mgr->stop();
    EXPECT_TRUE(weak_query_ctx.expired());

    exec_env->_fragment_mgr = previous_fragment_mgr;
    delete fragment_mgr;
}

} // namespace doris
