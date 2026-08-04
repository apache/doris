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

#include <chrono>
#include <future>

#include "cpp/sync_point.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/query_context.h"
#include "runtime/workload_group/workload_group_manager.h"
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

// Regression test for the lock scope in FragmentMgr::dump_pipeline_tasks().
//
// Pipeline contexts are stored in a sharded map. Diagnostic formatting may wait for
// fragment-local locks, so it must happen after the corresponding map shard lock is released.
// Otherwise, a slow diagnostic dump can block fragment lookup, insertion, and removal on the
// same shard and eventually cause unrelated fragment RPCs to time out.
//
// The test reproduces that ordering deterministically:
// 1. Insert one pipeline context into the map.
// 2. Start dump_pipeline_tasks() and pause it after the context snapshot has been collected,
//    immediately before formatting begins.
// 3. While formatting is paused, look up the same key so the lookup targets the same shard.
// 4. Verify that the lookup completes without waiting for diagnostic formatting to resume.
//
// If formatting is moved back inside ConcurrentContextMap::apply(), the pause in step 2 occurs
// while the shard lock is held and the lookup in step 3 cannot complete within the timeout.
TEST_F(FragmentMgrCrossClusterCancelTest, DumpPipelineTasksReleasesMapLockBeforeFormatting) {
    TUniqueId query_id;
    query_id.__set_hi(5);
    query_id.__set_lo(6);
    TNetworkAddress fe_address;
    fe_address.hostname = "127.0.0.1";
    fe_address.port = 9030;
    auto query_ctx =
            QueryContext::create(query_id, &_exec_env, _make_min_query_options(789), fe_address,
                                 true, fe_address, QuerySource::INTERNAL_FRONTEND);

    TPipelineFragmentParams params;
    params.__set_fragment_id(7);
    auto context = std::make_shared<PipelineFragmentContext>(
            query_id, params, query_ctx, &_exec_env, [](RuntimeState*, Status*) {});
    const auto key = std::make_pair(query_id, 7);
    // The dump and the concurrent lookup below use this exact key, guaranteeing that they
    // access the same map shard.
    _exec_env.fragment_mgr()->_pipeline_map.insert(key, context);

    // Pause the dump at the boundary under test: all context shared_ptrs have been copied and
    // the map lock should have been released, but no potentially blocking formatting has run.
    std::promise<void> snapshot_reached_promise;
    auto snapshot_reached = snapshot_reached_promise.get_future();
    std::promise<void> release_snapshot_promise;
    auto release_snapshot = release_snapshot_promise.get_future();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "FragmentMgr::dump_pipeline_tasks.before_format_context",
            [&](auto&&) {
                snapshot_reached_promise.set_value();
                release_snapshot.wait();
            },
            &guard);
    sync_point->enable_processing();
    Defer disable_sync_point {[&]() { sync_point->disable_processing(); }};

    auto dump_result = std::async(
            std::launch::async, [&]() { return _exec_env.fragment_mgr()->dump_pipeline_tasks(); });
    EXPECT_EQ(snapshot_reached.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    // A same-shard lookup must remain available even though diagnostic formatting is paused.
    auto find_result = std::async(std::launch::async, [&]() {
        return _exec_env.fragment_mgr()->_pipeline_map.find(key);
    });
    const auto find_status = find_result.wait_for(std::chrono::seconds(1));

    // Always unblock the dump before checking expectations so a failed lookup assertion cannot
    // leave the asynchronous dump waiting on the test and make the test process hang.
    release_snapshot_promise.set_value();

    EXPECT_EQ(find_status, std::future_status::ready);
    EXPECT_EQ(find_result.get(), context);
    EXPECT_EQ(dump_result.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_NE(dump_result.get().find("QueryId: 5-6"), std::string::npos);
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
    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.insert(query_id, query_ctx);
    query_ctx.reset();

    exec_env->_fragment_mgr->_query_ctx_map_delay_delete.clear();
    exec_env->_fragment_mgr->stop();
    delete exec_env->_fragment_mgr;
    exec_env->_fragment_mgr = previous_fragment_mgr;
}

} // namespace doris
