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

#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/pipeline/thrift_builder.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/frontend_info.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "storage/id_manager.h"
#include "util/debug_points.h"
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
        if (_exec_env._fragment_mgr != nullptr && !_fragment_mgr_stopped) {
            _exec_env._fragment_mgr->stop();
        }
        delete _exec_env._fragment_mgr;
        _exec_env._fragment_mgr = nullptr;
        delete _exec_env._workload_group_manager;
        _exec_env._workload_group_manager = nullptr;
        _exec_env.update_frontends({});
    }

protected:
    void stop_fragment_mgr_background() {
        // These synthetic ownership tests drive cleanup themselves. Stop the eager
        // cancel worker before publishing a partially initialized test context.
        _exec_env.fragment_mgr()->stop();
        _fragment_mgr_stopped = true;
    }

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
    bool _fragment_mgr_stopped = false;
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

TEST_F(FragmentMgrCrossClusterCancelTest, LocalOnlyRfMergeOwnerReleasedAfterLastFragmentOwner) {
    stop_fragment_mgr_background();
    auto* fragment_mgr = _exec_env.fragment_mgr();

    TUniqueId query_id;
    query_id.__set_hi(707);
    query_id.__set_lo(808);

    TNetworkAddress coord;
    coord.hostname = "fe-local-rf";
    coord.port = 9030;

    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(true);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 0));
    params.__set_desc_tbl(_make_min_desc_tbl());

    TRuntimeFilterParams rf_params =
            TRuntimeFilterParamsBuilder()
                    .add_rid_to_runtime_filter(
                            1, TRuntimeFilterDescBuilder(1).add_planId_to_target_expr(0).build())
                    .add_runtime_filter_builder_num(1, 1)
                    .build();
    rf_params.__set_rf_merge_context_single_use(true);
    TRuntimeFilterInfo rf_info;
    rf_info.__set_runtime_filter_params(rf_params);
    TPipelineFragmentParamsList parent;
    parent.__set_runtime_filter_info(rf_info);

    std::shared_ptr<QueryContext> query_ctx;
    auto st = fragment_mgr->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                     query_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(query_ctx, nullptr);
    ASSERT_NE(query_ctx->get_merge_controller_handler(), nullptr);
    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;

    // The first fragment may finish while a later simplified fragment is still being prepared.
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());

    auto controller = query_ctx->get_merge_controller_handler();
    controller->mark_initial_fragments_prepared();
    controller.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_NE(fragment_mgr->get_query_ctx(query_id), nullptr);
    query_ctx.reset();
    EXPECT_TRUE(weak_query_ctx.expired());
}

TEST_F(FragmentMgrCrossClusterCancelTest, MissingSingleUseContractKeepsMergeOwnerForReset) {
    stop_fragment_mgr_background();
    auto* fragment_mgr = _exec_env.fragment_mgr();
    TUniqueId query_id;
    query_id.__set_hi(709);
    query_id.__set_lo(810);
    TNetworkAddress coord;
    coord.hostname = "fe-legacy-rf";
    coord.port = 9030;
    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(true);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 0));
    params.__set_desc_tbl(_make_min_desc_tbl());
    TRuntimeFilterParams rf_params =
            TRuntimeFilterParamsBuilder()
                    .add_rid_to_runtime_filter(
                            1, TRuntimeFilterDescBuilder(1).add_planId_to_target_expr(0).build())
                    .add_runtime_filter_builder_num(1, 1)
                    .build();
    TRuntimeFilterInfo rf_info;
    rf_info.__set_runtime_filter_params(rf_params);
    TPipelineFragmentParamsList parent;
    parent.__set_runtime_filter_info(rf_info);
    std::shared_ptr<QueryContext> query_ctx;
    ASSERT_TRUE(fragment_mgr
                        ->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                   query_ctx)
                        .ok());
    query_ctx->get_merge_controller_handler()->mark_initial_fragments_prepared();
    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;
    query_ctx.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());

    google::protobuf::RepeatedField<int32_t> empty_ids;
    EXPECT_TRUE(fragment_mgr->reset_global_rf(query_id, empty_ids).ok());
    google::protobuf::RepeatedField<int32_t> filter_ids;
    filter_ids.Add(1);
    EXPECT_TRUE(fragment_mgr->reset_global_rf(query_id, filter_ids).ok());
    rf_params.__set_rf_merge_context_single_use(false);
    RuntimeFilterMergeControllerEntity explicit_false_controller;
    auto live_query_ctx = fragment_mgr->get_query_ctx(query_id);
    ASSERT_TRUE(explicit_false_controller.init(live_query_ctx, rf_params).ok());
    explicit_false_controller.mark_initial_fragments_prepared();
    EXPECT_FALSE(explicit_false_controller.can_release_query_context());
    live_query_ctx.reset();
    fragment_mgr->remove_query_context(query_id);
    EXPECT_TRUE(weak_query_ctx.expired());
}

TEST_F(FragmentMgrCrossClusterCancelTest, RemoteRfRequiresSizeDispatchAndCallbackCompletion) {
    stop_fragment_mgr_background();
    auto* fragment_mgr = _exec_env.fragment_mgr();
    TUniqueId query_id;
    query_id.__set_hi(711);
    query_id.__set_lo(812);
    TNetworkAddress coord;
    coord.hostname = "fe-remote-rf";
    coord.port = 9030;
    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(true);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 0));
    params.__set_desc_tbl(_make_min_desc_tbl());
    TRuntimeFilterParams rf_params =
            TRuntimeFilterParamsBuilder()
                    .add_rid_to_runtime_filter(1, TRuntimeFilterDescBuilder(1)
                                                          .add_planId_to_target_expr(0)
                                                          .set_mode(false)
                                                          .set_build_bf_by_runtime_size(true)
                                                          .build())
                    .add_runtime_filter_builder_num(1, 1)
                    .add_rid_to_runtime_filter(
                            2, TRuntimeFilterDescBuilder(2).add_planId_to_target_expr(0).build())
                    .add_runtime_filter_builder_num(2, 1)
                    .build();
    rf_params.__set_rf_merge_context_single_use(true);
    TRuntimeFilterInfo rf_info;
    rf_info.__set_runtime_filter_params(rf_params);
    TPipelineFragmentParamsList parent;
    parent.__set_runtime_filter_info(rf_info);
    std::shared_ptr<QueryContext> query_ctx;
    ASSERT_TRUE(fragment_mgr
                        ->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                   query_ctx)
                        .ok());
    auto controller = query_ctx->get_merge_controller_handler();
    controller->mark_initial_fragments_prepared();
    std::shared_ptr<GlobalMergeContext::Completion> completion;
    {
        SharedLockGuard guard(controller->_filter_map_mutex);
        completion = controller->_filter_map.at(1).completion;
    }
    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;

    // Dispatch begins only after all producers have merged. Its earlier 'done' flag is
    // not sufficient: the size phase and all async callbacks must also have finished.
    query_ctx.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());
    completion->publish_dispatch_finished.store(true);
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());
    completion->size_dispatch_finished.store(true);
    completion->pending_callbacks.store(1);
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());
    completion->pending_callbacks.store(0);
    controller.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_TRUE(weak_query_ctx.expired());
}

TEST_F(FragmentMgrCrossClusterCancelTest, IgnoredRfDispatchFailureReleasesQueryContext) {
    stop_fragment_mgr_background();
    auto* fragment_mgr = _exec_env.fragment_mgr();
    TUniqueId query_id;
    query_id.__set_hi(713);
    query_id.__set_lo(814);
    TNetworkAddress coord;
    coord.hostname = "fe-ignored-rf-error";
    coord.port = 9030;
    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_is_simplified_param(false);
    params.__set_coord(coord);
    params.__set_is_nereids(true);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    auto query_options = _make_min_query_options(/*fe_process_uuid*/ 0);
    query_options.__set_ignore_runtime_filter_error(true);
    params.__set_query_options(query_options);
    params.__set_desc_tbl(_make_min_desc_tbl());
    TRuntimeFilterParams rf_params =
            TRuntimeFilterParamsBuilder()
                    .add_rid_to_runtime_filter(1, TRuntimeFilterDescBuilder(1)
                                                          .add_planId_to_target_expr(0)
                                                          .set_mode(false)
                                                          .set_build_bf_by_runtime_size(true)
                                                          .build())
                    .add_runtime_filter_builder_num(1, 1)
                    .build();
    rf_params.__set_rf_merge_context_single_use(true);
    TRuntimeFilterInfo rf_info;
    rf_info.__set_runtime_filter_params(rf_params);
    TPipelineFragmentParamsList parent;
    parent.__set_runtime_filter_info(rf_info);
    std::shared_ptr<QueryContext> query_ctx;
    ASSERT_TRUE(fragment_mgr
                        ->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                   query_ctx)
                        .ok());
    auto controller = query_ctx->get_merge_controller_handler();
    controller->mark_initial_fragments_prepared();
    std::shared_ptr<GlobalMergeContext::Completion> completion;
    {
        SharedLockGuard guard(controller->_filter_map_mutex);
        completion = controller->_filter_map.at(1).completion;
    }
    ASSERT_TRUE(completion->ignore_runtime_filter_error);
    completion->finish_dispatch(completion->publish_dispatch_finished, false);
    completion->pending_callbacks.store(1);
    auto callback = make_merge_size_dispatch_callback({}, completion);
    callback->cntl_->SetFailed("injected ignored size RPC failure");
    completion->finish_dispatch(completion->size_dispatch_finished, false);
    std::weak_ptr<QueryContext> weak_query_ctx = query_ctx;
    query_ctx.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_FALSE(weak_query_ctx.expired());

    callback->call();
    EXPECT_FALSE(completion->failed.load());
    EXPECT_EQ(completion->pending_callbacks.load(), 0);
    controller.reset();
    fragment_mgr->_release_finished_rf_query_contexts();
    EXPECT_TRUE(weak_query_ctx.expired());
}

TEST(FragmentMgrDelayDeleteMapTest, ClearShouldNotAbortWhenReleasingLastQueryContextRef) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = new FragmentMgr(exec_env);
    exec_env->_fragment_mgr->stop();

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
    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.insert(query_id, query_ctx);
    query_ctx.reset();

    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.clear();
    delete exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = previous_fragment_mgr;
}

TEST(FragmentMgrDelayDeleteMapTest, EraseReleasesLastQueryContextRefOutsideLock) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_fragment_mgr = exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = new FragmentMgr(exec_env);
    exec_env->_fragment_mgr->stop();
    TUniqueId query_id;
    query_id.__set_hi(103);
    query_id.__set_lo(204);
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
    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.insert(query_id, query_ctx);
    query_ctx.reset();

    EXPECT_TRUE(exec_env->_fragment_mgr->_query_ctx_map_delay_delete.erase(query_id));
    EXPECT_TRUE(weak_query_ctx.expired());
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

TEST_F(FragmentMgrCrossClusterCancelTest, RebuildRestoresFragmentExecutingCount) {
    auto* fragment_mgr = _exec_env.fragment_mgr();
    const bool previous_enable_debug_points = config::enable_debug_points;
    constexpr auto debug_point_name = "fault_inject::PipelineFragmentContext::prepare.skip";
    config::enable_debug_points = true;
    DebugPoints::instance()->add(debug_point_name);

    TUniqueId query_id;
    query_id.__set_hi(505);
    query_id.__set_lo(606);
    constexpr int fragment_id = 1;
    Defer cleanup([&] {
        DebugPoints::instance()->remove(debug_point_name);
        config::enable_debug_points = previous_enable_debug_points;
        fragment_mgr->remove_pipeline_context({query_id, fragment_id});
        fragment_mgr->remove_query_context(query_id);
    });

    TPipelineFragmentParams params;
    params.__set_query_id(query_id);
    params.__set_fragment_id(fragment_id);
    params.__set_need_notify_close(true);
    params.__set_is_simplified_param(false);
    TNetworkAddress coord;
    coord.hostname = "fe-rebuild";
    coord.port = 9030;
    params.__set_coord(coord);
    params.__set_is_nereids(true);
    params.__set_current_connect_fe(coord);
    params.__set_fragment_num_on_host(1);
    params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 789));
    params.__set_desc_tbl(_make_min_desc_tbl());

    std::shared_ptr<QueryContext> query_ctx;
    TPipelineFragmentParamsList parent;
    ASSERT_TRUE(fragment_mgr
                        ->_get_or_create_query_ctx(params, parent, QuerySource::INTERNAL_FRONTEND,
                                                   query_ctx)
                        .ok());
    ASSERT_NE(query_ctx, nullptr);
    {
        std::lock_guard lock(fragment_mgr->_rerunnable_params_lock);
        auto& info = fragment_mgr->_rerunnable_params_map[{query_id, fragment_id}];
        info.params = params;
        info.query_ctx = query_ctx;
    }

    const auto count_before_rebuild = get_fragment_executing_count();
    auto st =
            fragment_mgr->rerun_fragment({}, query_id, fragment_id, PRerunFragmentParams::REBUILD);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(fragment_mgr->_pipeline_map.num_items(), 1);
    EXPECT_EQ(get_fragment_executing_count(), count_before_rebuild + 1);

    fragment_mgr->remove_pipeline_context({query_id, fragment_id});
    EXPECT_EQ(get_fragment_executing_count(), count_before_rebuild);
}

TEST(FragmentMgrExternalScanTest, SelectedColumnsFollowOutputExpressionOrder) {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_INT)
                              .nullable(true)
                              .column_name("k1")
                              .column_pos(0)
                              .build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(TYPE_BIGINT)
                              .nullable(true)
                              .column_name("v2")
                              .column_pos(1)
                              .build());
    tuple_builder.build(&desc_tbl_builder);

    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&pool, desc_tbl_builder.desc_tbl(), &desc_tbl).ok());

    auto make_slot_ref_expr = [](TSlotId slot_id, TPrimitiveType::type type) {
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(slot_id);
        slot_ref.__set_tuple_id(0);

        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_type(TSlotDescriptorBuilder().get_common_type(type));
        node.__set_num_children(0);
        node.__set_slot_ref(slot_ref);

        TExpr expr;
        expr.nodes.emplace_back(std::move(node));
        return expr;
    };

    TPlanFragment plan_fragment;
    plan_fragment.__set_output_exprs({make_slot_ref_expr(1, TPrimitiveType::BIGINT),
                                      make_slot_ref_expr(0, TPrimitiveType::INT)});

    std::vector<TScanColumnDesc> selected_columns;
    ASSERT_TRUE(FragmentMgr::_build_external_scan_selected_columns(plan_fragment, *desc_tbl,
                                                                   &selected_columns)
                        .ok());
    ASSERT_EQ(selected_columns.size(), 2);
    EXPECT_EQ(selected_columns[0].name, "v2");
    EXPECT_EQ(selected_columns[0].type, TPrimitiveType::BIGINT);
    EXPECT_EQ(selected_columns[1].name, "k1");
    EXPECT_EQ(selected_columns[1].type, TPrimitiveType::INT);
}

} // namespace doris
