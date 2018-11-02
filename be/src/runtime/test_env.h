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

#ifndef DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H
#define DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H

#include "runtime/buffered_block_mgr2.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {

// Helper testing class that creates an environment with a buffered-block-mgr similar
// to the one Impala's runtime is using.
class TestEnv {
public:
    TestEnv();
    ~TestEnv();

    // Reinitialize tmp_file_mgr with custom configuration. Only valid to call before
    // query states have been created.
    void init_tmp_file_mgr(const std::vector<std::string>& tmp_dirs, bool one_dir_per_device);

    // Create a RuntimeState for a query with a new block manager. The RuntimeState is
    // owned by the TestEnv.
    Status create_query_state(int64_t query_id, int max_buffers, int block_size,
            RuntimeState** runtime_state);

    // Create multiple separate RuntimeStates with associated block managers, e.g. as if
    // multiple queries were executing. The RuntimeStates are owned by TestEnv.
    Status create_query_states(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
            int block_size, std::vector<RuntimeState*>* runtime_states);

    // Destroy all RuntimeStates and block managers created by this TestEnv.
    void tear_down_query_states();

    // Calculate memory limit accounting for overflow and negative values.
    // If max_buffers is -1, no memory limit will apply.
    int64_t calculate_mem_tracker(int max_buffers, int block_size);

    ExecEnv* exec_env() {
        return _exec_env.get();
    }
    MemTracker* block_mgr_parent_tracker() {
        return _block_mgr_parent_tracker.get();
    }
    MemTracker* io_mgr_tracker() {
        return _io_mgr_tracker.get();
    }
    MetricRegistry* metrics() {
        return _metrics.get();
    }
    TmpFileMgr* tmp_file_mgr() {
        return _tmp_file_mgr.get();
    }

private:

    // Recreate global metric groups.
    void init_metrics();

    // Create a new RuntimeState sharing global environment.
    RuntimeState* create_runtime_state(int64_t query_id);

    // Global state for test environment.
    static boost::scoped_ptr<MetricRegistry> _s_static_metrics;
    boost::scoped_ptr<ExecEnv> _exec_env;
    boost::scoped_ptr<MemTracker> _block_mgr_parent_tracker;
    boost::scoped_ptr<MemTracker> _io_mgr_tracker;
    boost::scoped_ptr<MetricRegistry> _metrics;
    boost::scoped_ptr<TmpFileMgr> _tmp_file_mgr;

    // Per-query states with associated block managers.
    std::vector<boost::shared_ptr<RuntimeState> > _query_states;
};

} // end namespace doris

#endif // DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H
