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

#include "runtime/memory/thread_mem_tracker_mgr.h"

#include <gen_cpp/types.pb.h>

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace doris {

class AsyncCancelQueryTask : public Runnable {
    ENABLE_FACTORY_CREATOR(AsyncCancelQueryTask);

public:
    AsyncCancelQueryTask(TUniqueId query_id, const std::string& exceed_msg)
            : _query_id(query_id), _exceed_msg(exceed_msg) {}
    ~AsyncCancelQueryTask() override = default;
    void run() override {
        ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                _query_id, Status::MemoryLimitExceeded(_exceed_msg));
    }

private:
    TUniqueId _query_id;
    const std::string _exceed_msg;
};

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    CHECK(init());
    flush_untracked_mem();
    _reserved_mem_stack.push_back(_reserved_mem);
    if (_reserved_mem != 0) {
        // _untracked_mem temporary store bytes that not synchronized to process reserved memory,
        // but bytes have been subtracted from thread _reserved_mem.
        doris::GlobalMemoryArbitrator::release_process_reserved_memory(_untracked_mem);
        _reserved_mem = 0;
        _untracked_mem = 0;
    }
    _limiter_tracker = mem_tracker;
    _limiter_tracker_raw = mem_tracker.get();
}

void ThreadMemTrackerMgr::detach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& old_mem_tracker) {
    CHECK(init());
    flush_untracked_mem();
    release_reserved();
    DCHECK(!_reserved_mem_stack.empty());
    _reserved_mem = _reserved_mem_stack.back();
    _reserved_mem_stack.pop_back();
    _limiter_tracker = old_mem_tracker;
    _limiter_tracker_raw = old_mem_tracker.get();
}

void ThreadMemTrackerMgr::cancel_query(const std::string& exceed_msg) {
    if (is_attach_query() && !_is_query_cancelled) {
        Status submit_st = ExecEnv::GetInstance()->lazy_release_obj_pool()->submit(
                AsyncCancelQueryTask::create_shared(_query_id, exceed_msg));
        if (submit_st.ok()) {
            // Use this flag to avoid the cancel request submit to pool many times, because even we cancel the query
            // successfully, but the application may not use if (state.iscancelled) to exist quickly. And it may try to
            // allocate memory and may failed again and the pool will be full.
            _is_query_cancelled = true;
        } else {
            LOG(WARNING) << "Failed to submit cancel query task to pool, query_id "
                         << print_id(_query_id) << ", error st " << submit_st;
        }
    }
}

} // namespace doris
