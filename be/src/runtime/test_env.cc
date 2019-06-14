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

#include "runtime/test_env.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"

using boost::shared_ptr;

namespace doris {

boost::scoped_ptr<MetricRegistry> TestEnv::_s_static_metrics;

TestEnv::TestEnv() {
    if (_s_static_metrics == NULL) {
        _s_static_metrics.reset(new MetricRegistry("test_env"));
        // DorisMetrics::create_metrics(_s_static_metrics.get());
    }
    _exec_env.reset(new ExecEnv());
    // _exec_env->init_for_tests();
    _io_mgr_tracker.reset(new MemTracker(-1));
    _block_mgr_parent_tracker.reset(new MemTracker(-1));
    _exec_env->disk_io_mgr()->init(_io_mgr_tracker.get());
    init_metrics();
    _tmp_file_mgr.reset(new TmpFileMgr());
    _tmp_file_mgr->init(_metrics.get());
}

void TestEnv::init_metrics() {
    _metrics.reset(new MetricRegistry("test_env"));
}

void TestEnv::init_tmp_file_mgr(const std::vector<std::string>& tmp_dirs,
        bool one_dir_per_device) {
    // Need to recreate metrics to avoid error when registering metric twice.
    init_metrics();
    _tmp_file_mgr.reset(new TmpFileMgr());
    _tmp_file_mgr->init_custom(tmp_dirs, one_dir_per_device, _metrics.get());
}

TestEnv::~TestEnv() {
    // Queries must be torn down first since they are dependent on global state.
    tear_down_query_states();
    _block_mgr_parent_tracker.reset();
    _exec_env.reset();
    _io_mgr_tracker.reset();
    _tmp_file_mgr.reset();
    _metrics.reset();
}

RuntimeState* TestEnv::create_runtime_state(int64_t query_id) {
    TExecPlanFragmentParams plan_params = TExecPlanFragmentParams();
    plan_params.params.query_id.hi = 0;
    plan_params.params.query_id.lo = query_id;
    return new RuntimeState(plan_params, TQueryOptions(), "", _exec_env.get());
}

Status TestEnv::create_query_state(int64_t query_id, int max_buffers, int block_size,
        RuntimeState** runtime_state) {
    *runtime_state = create_runtime_state(query_id);
    if (*runtime_state == NULL) {
        return Status::InternalError("Unexpected error creating RuntimeState");
    }

    shared_ptr<BufferedBlockMgr2> mgr;
    RETURN_IF_ERROR(BufferedBlockMgr2::create(
                *runtime_state, _block_mgr_parent_tracker.get(),
                (*runtime_state)->runtime_profile(), _tmp_file_mgr.get(),
                calculate_mem_tracker(max_buffers, block_size), block_size, &mgr));
    (*runtime_state)->set_block_mgr2(mgr);
    // (*runtime_state)->_block_mgr = mgr;

    _query_states.push_back(shared_ptr<RuntimeState>(*runtime_state));
    return Status::OK();
}

Status TestEnv::create_query_states(int64_t start_query_id, int num_mgrs,
        int buffers_per_mgr, int block_size,
        std::vector<RuntimeState*>* runtime_states) {
    for (int i = 0; i < num_mgrs; ++i) {
        RuntimeState* runtime_state = NULL;
        RETURN_IF_ERROR(create_query_state(start_query_id + i, buffers_per_mgr, block_size,
                    &runtime_state));
        runtime_states->push_back(runtime_state);
    }
    return Status::OK();
}

void TestEnv::tear_down_query_states() {
    _query_states.clear();
}

int64_t TestEnv::calculate_mem_tracker(int max_buffers, int block_size) {
    DCHECK_GE(max_buffers, -1);
    if (max_buffers == -1) {
        return -1;
    }
    return max_buffers * static_cast<int64_t>(block_size);
}

} // end namespace doris
