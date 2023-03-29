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

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <memory>

#include "olap/storage_engine.h"
#include "runtime/result_queue_mgr.h"
#include "util/disk_info.h"

namespace doris {

TestEnv::TestEnv() {
    // Some code will use ExecEnv::GetInstance(), so init the global ExecEnv singleton
    _exec_env = ExecEnv::GetInstance();
    _exec_env->_result_queue_mgr = new ResultQueueMgr();
    // TODO may need rpc support, etc.
}

TestEnv::~TestEnv() {
    SAFE_DELETE(_exec_env->_result_queue_mgr);

    if (_engine == StorageEngine::_s_instance) {
        // the engine instance is created by this test env
        StorageEngine::_s_instance = nullptr;
    }
    SAFE_DELETE(_engine);
}

RuntimeState* TestEnv::create_runtime_state(int64_t query_id) {
    TExecPlanFragmentParams plan_params = TExecPlanFragmentParams();
    plan_params.params.query_id.hi = 0;
    plan_params.params.query_id.lo = query_id;
    return new RuntimeState(plan_params.params, TQueryOptions(), TQueryGlobals(), _exec_env);
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

void TestEnv::init_storage_engine(bool need_open, const std::vector<std::string>& paths) {
    if (StorageEngine::_s_instance) {
        LOG(INFO) << "Engine instance already exists";
        return;
    }
    // init and open storage engine
    doris::EngineOptions options;
    for (const auto& path : paths) {
        options.store_paths.emplace_back(path, -1);
    }
    options.backend_uid = UniqueId::gen_uid();
    config::tablet_map_shard_size = 1;
    config::txn_map_shard_size = 1;
    config::txn_shard_size = 1;

    // This engine will be the singleton instance, cuz StorageEngine::_s_instance is nullptr now.
    Status st;
    if (need_open) {
        st = StorageEngine::open(options, &_engine);
    } else {
        _engine = new StorageEngine(options);
    }
    EXPECT_TRUE(st.ok());
    _exec_env->set_storage_engine(_engine);
}

} // end namespace doris
