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

#include <sys/stat.h>

#include <memory>

#include "olap/storage_engine.h"
#include "runtime/fragment_mgr.h"
#include "runtime/initial_reservations.h"
#include "runtime/result_queue_mgr.h"
#include "util/disk_info.h"
#include "util/priority_thread_pool.hpp"

namespace doris {

TestEnv::TestEnv()
        : _block_mgr_parent_tracker(MemTracker::create_tracker(-1, "BufferedBlockMgr2")) {
    // Some code will use ExecEnv::GetInstance(), so init the global ExecEnv singleton
    _exec_env = ExecEnv::GetInstance();
    _exec_env->_thread_mgr = new ThreadResourceMgr(2);
    _exec_env->_buffer_reservation = new ReservationTracker();
    _exec_env->_task_pool_mem_tracker_registry.reset(new MemTrackerTaskPool());
    _exec_env->_disk_io_mgr = new DiskIoMgr(1, 1, 1, 10);
    _exec_env->disk_io_mgr()->init(-1);
    _exec_env->_scan_thread_pool = new PriorityThreadPool(1, 16);
    _exec_env->_result_queue_mgr = new ResultQueueMgr();
    // TODO may need rpc support, etc.
}

void TestEnv::init_tmp_file_mgr(const std::vector<std::string>& tmp_dirs, bool one_dir_per_device) {
    _tmp_file_mgr = std::make_shared<TmpFileMgr>();
    _exec_env->_tmp_file_mgr = _tmp_file_mgr.get();

    DiskInfo::init();
    // will use DiskInfo::num_disks(), DiskInfo should be initialized before
    auto st = _tmp_file_mgr->init_custom(tmp_dirs, one_dir_per_device);
    DCHECK(st.ok()) << st.get_error_msg();
}

void TestEnv::init_buffer_pool(int64_t min_page_len, int64_t capacity, int64_t clean_pages_limit) {
    _exec_env->_buffer_pool = new BufferPool(min_page_len, capacity, clean_pages_limit);
}

TestEnv::~TestEnv() {
    SAFE_DELETE(_exec_env->_result_queue_mgr);
    SAFE_DELETE(_exec_env->_buffer_pool);
    SAFE_DELETE(_exec_env->_scan_thread_pool);
    SAFE_DELETE(_exec_env->_disk_io_mgr);
    SAFE_DELETE(_exec_env->_buffer_reservation);
    SAFE_DELETE(_exec_env->_thread_mgr);

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

Status TestEnv::create_query_state(int64_t query_id, int max_buffers, int block_size,
                                   RuntimeState** runtime_state) {
    *runtime_state = create_runtime_state(query_id);
    if (*runtime_state == nullptr) {
        return Status::InternalError("Unexpected error creating RuntimeState");
    }

    std::shared_ptr<BufferedBlockMgr2> mgr;
    RETURN_IF_ERROR(BufferedBlockMgr2::create(
            *runtime_state, (*runtime_state)->runtime_profile(), _tmp_file_mgr.get(),
            calculate_mem_tracker(max_buffers, block_size), block_size, &mgr));
    (*runtime_state)->set_block_mgr2(mgr);
    // (*runtime_state)->_block_mgr = mgr;

    _query_states.push_back(std::shared_ptr<RuntimeState>(*runtime_state));
    return Status::OK();
}

Status TestEnv::create_query_states(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
                                    int block_size, std::vector<RuntimeState*>* runtime_states) {
    for (int i = 0; i < num_mgrs; ++i) {
        RuntimeState* runtime_state = nullptr;
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
    DCHECK(st.ok()) << st.get_error_msg();
    _exec_env->set_storage_engine(_engine);
}

} // end namespace doris
