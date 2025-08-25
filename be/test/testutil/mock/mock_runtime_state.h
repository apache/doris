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

#pragma once
#include "mock_query_context.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_descriptors.h"

namespace doris {

class MockContext : public TaskExecutionContext {};

class MockFragmentManager : public FragmentMgr {
public:
    MockFragmentManager(Status& status_, ExecEnv* exec_env)
            : FragmentMgr(exec_env), status(status_) {}
    void cancel_query(const TUniqueId query_id, const Status reason) override { status = reason; }

private:
    Status& status;
};

class MockRuntimeState : public RuntimeState {
public:
    MockRuntimeState() {
        set_task_execution_context(_mock_context);
        _query_ctx_uptr->set_mock_llm_resource();
        _query_ctx = _query_ctx_uptr.get();

        _mock_desc_tbl = std::make_unique<MockDescriptorTbl1>();
        set_desc_tbl(_mock_desc_tbl.get());
    }
    MockRuntimeState(const TQueryGlobals& query_globals) : RuntimeState(query_globals) {
        _mock_desc_tbl = std::make_unique<MockDescriptorTbl1>();
        set_desc_tbl(_mock_desc_tbl.get());
    }
    MockRuntimeState(const TUniqueId& query_id, int32_t fragment_id,
                     const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                     ExecEnv* exec_env, QueryContext* ctx)
            : RuntimeState(query_id, fragment_id, query_options, query_globals, exec_env, ctx) {}

    int batch_size() const override { return batsh_size; }

    bool enable_shared_exchange_sink_buffer() const override {
        return _enable_shared_exchange_sink_buffer;
    }

    bool enable_share_hash_table_for_broadcast_join() const override {
        return _enable_share_hash_table_for_broadcast_join;
    }

    void set_enable_spill(bool enable) { _query_options.__set_enable_spill(enable); }

    void set_enable_strict_cast(bool enable) { _query_options.__set_enable_strict_cast(enable); }

    bool enable_local_exchange() const override { return true; }
    WorkloadGroupPtr workload_group() override { return _workload_group; }

    // default batch size
    int batsh_size = 4096;
    bool _enable_shared_exchange_sink_buffer = true;
    bool _enable_share_hash_table_for_broadcast_join = true;
    std::shared_ptr<MockContext> _mock_context = std::make_shared<MockContext>();
    std::shared_ptr<MockQueryContext> _query_ctx_uptr = MockQueryContext::create();
    WorkloadGroupPtr _workload_group = nullptr;
    std::unique_ptr<MockDescriptorTbl1> _mock_desc_tbl;
};

} // namespace doris
