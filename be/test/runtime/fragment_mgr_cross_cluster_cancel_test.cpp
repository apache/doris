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

#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/config.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/workload_group/workload_group_manager.h"

namespace doris {

class FragmentMgrCrossClusterCancelTest : public testing::Test {
public:
    void SetUp() override {
        _origin_cancel_worker_interval_seconds =
                config::fragment_mgr_cancel_worker_interval_seconds;
        // Make cancel_worker run quickly in UT, and restore it in TearDown.
        config::fragment_mgr_cancel_worker_interval_seconds = 1;

        // Make frontends list deterministic for both this ExecEnv instance and global ExecEnv.
        _origin_global_frontends = ExecEnv::GetInstance()->get_frontends();
        ExecEnv::GetInstance()->update_frontends({});
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

        ExecEnv::GetInstance()->update_frontends(_origin_global_frontends);
        config::fragment_mgr_cancel_worker_interval_seconds =
                _origin_cancel_worker_interval_seconds;
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
    int32_t _origin_cancel_worker_interval_seconds = 0;
    std::vector<TFrontendInfo> _origin_global_frontends;
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

TEST_F(FragmentMgrCrossClusterCancelTest, CancelWorkerSkipsExternalFrontendQuery) {
    auto* fragment_mgr = _exec_env.fragment_mgr();
    ASSERT_NE(fragment_mgr, nullptr);

    // Make global running frontends non-empty so cancel_worker executes the invalid-query check path.
    // NOTE: cancel_worker uses ExecEnv::GetInstance()->get_running_frontends().
    TFrontendInfo global_fe;
    global_fe.coordinator_address.hostname = "fe-global";
    global_fe.coordinator_address.port = 9030;
    global_fe.process_uuid = 777;
    ExecEnv::GetInstance()->update_frontends({global_fe});

    // Make local running frontends contain `coord_internal` so this query remains INTERNAL_FRONTEND.
    // NOTE: _get_or_create_query_ctx uses `_exec_env->get_running_frontends()`.
    TNetworkAddress coord_internal;
    coord_internal.hostname = "fe-local";
    coord_internal.port = 9030;

    TFrontendInfo local_fe;
    local_fe.coordinator_address = coord_internal;
    local_fe.process_uuid = 999;
    _exec_env.update_frontends({local_fe});

    // Create an INTERNAL_FRONTEND query (should be cancelled by cancel_worker when coordinator not found).
    TUniqueId internal_query_id;
    internal_query_id.__set_hi(3);
    internal_query_id.__set_lo(4);

    TPipelineFragmentParams internal_params;
    internal_params.__set_query_id(internal_query_id);
    internal_params.__set_is_simplified_param(false);
    internal_params.__set_coord(coord_internal);
    internal_params.__set_is_nereids(false);
    internal_params.__set_current_connect_fe(coord_internal);
    internal_params.__set_fragment_num_on_host(1);
    internal_params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 111));
    internal_params.__set_desc_tbl(_make_min_desc_tbl());

    std::shared_ptr<QueryContext> internal_query_ctx;
    TPipelineFragmentParamsList parent;
    auto st = fragment_mgr->_get_or_create_query_ctx(
            internal_params, parent, QuerySource::INTERNAL_FRONTEND, internal_query_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(internal_query_ctx, nullptr);
    ASSERT_EQ(internal_query_ctx->get_query_source(), QuerySource::INTERNAL_FRONTEND);

    // Create a cross-cluster query (coordinator not in local frontends), it should be marked as EXTERNAL_FRONTEND.
    TUniqueId external_query_id;
    external_query_id.__set_hi(5);
    external_query_id.__set_lo(6);

    TNetworkAddress coord_external;
    coord_external.hostname = "fe-remote";
    coord_external.port = 9030;

    TPipelineFragmentParams external_params;
    external_params.__set_query_id(external_query_id);
    external_params.__set_is_simplified_param(false);
    external_params.__set_coord(coord_external);
    external_params.__set_is_nereids(false);
    external_params.__set_current_connect_fe(coord_external);
    external_params.__set_fragment_num_on_host(1);
    external_params.__set_query_options(_make_min_query_options(/*fe_process_uuid*/ 222));
    external_params.__set_desc_tbl(_make_min_desc_tbl());

    std::shared_ptr<QueryContext> external_query_ctx;
    st = fragment_mgr->_get_or_create_query_ctx(external_params, parent,
                                                QuerySource::INTERNAL_FRONTEND, external_query_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(external_query_ctx, nullptr);
    ASSERT_EQ(external_query_ctx->get_query_source(), QuerySource::EXTERNAL_FRONTEND);

    // Wait for background cancel_worker to cancel the INTERNAL_FRONTEND query,
    // and keep the EXTERNAL_FRONTEND query alive.
    // NOTE: In BE_TEST, FragmentMgr::remove_query_context() does not erase `_query_ctx_map`,
    // so we validate by `is_cancelled()` instead of expecting get_query_ctx() == nullptr.
    constexpr int kMaxWaitMs = 5000;
    int waited_ms = 0;
    while (waited_ms < kMaxWaitMs) {
        if (internal_query_ctx->is_cancelled()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        waited_ms += 100;
    }

    EXPECT_TRUE(internal_query_ctx->is_cancelled());
    EXPECT_FALSE(external_query_ctx->is_cancelled());
    EXPECT_EQ(external_query_ctx->get_query_source(), QuerySource::EXTERNAL_FRONTEND);
}

} // namespace doris
