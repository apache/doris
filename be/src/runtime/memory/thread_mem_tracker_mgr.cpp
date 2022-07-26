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

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_task_pool.h"
#include "service/backend_options.h"

namespace doris {

void ThreadMemTrackerMgr::attach_limiter_tracker(const std::string& cancel_msg,
                                                 const std::string& task_id,
                                                 const TUniqueId& fragment_instance_id,
                                                 MemTrackerLimiter* mem_tracker) {
    DCHECK(mem_tracker);
    _task_id = task_id;
    _fragment_instance_id = fragment_instance_id;
    _exceed_cb.cancel_msg = cancel_msg;
    _limiter_tracker = mem_tracker;
}

void ThreadMemTrackerMgr::detach_limiter_tracker() {
    flush_untracked_mem<false>();
    _task_id = "";
    _fragment_instance_id = TUniqueId();
    _exceed_cb.cancel_msg = "";
    _limiter_tracker = ExecEnv::GetInstance()->process_mem_tracker();
}

void ThreadMemTrackerMgr::exceeded_cancel_task(const std::string& cancel_details) {
    if (_fragment_instance_id != TUniqueId()) {
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                cancel_details);
    }
}

void ThreadMemTrackerMgr::exceeded(int64_t mem_usage, Status try_consume_st) {
    if (_exceed_cb.cb_func != nullptr) {
        _exceed_cb.cb_func();
    }
    if (is_attach_task()) {
        if (_exceed_cb.cancel_task) {
            auto st = _limiter_tracker->mem_limit_exceeded(
                    nullptr,
                    fmt::format("Task mem limit exceeded and cancel it, msg:{}",
                                _exceed_cb.cancel_msg),
                    mem_usage, try_consume_st);
            exceeded_cancel_task(st.to_string());
            _exceed_cb.cancel_task = false; // Make sure it will only be canceled once
        }
    }
}
} // namespace doris
