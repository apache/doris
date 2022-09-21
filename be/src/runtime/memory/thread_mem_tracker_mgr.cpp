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

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::string& task_id, const TUniqueId& fragment_instance_id,
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    flush_untracked_mem<false>();
    _task_id_stack.push_back(task_id);
    _fragment_instance_id_stack.push_back(fragment_instance_id);
    _limiter_tracker_stack.push_back(mem_tracker);
    _limiter_tracker_raw = mem_tracker.get();
}

void ThreadMemTrackerMgr::detach_limiter_tracker() {
    DCHECK(!_limiter_tracker_stack.empty());
    flush_untracked_mem<false>();
    _task_id_stack.pop_back();
    _fragment_instance_id_stack.pop_back();
    _limiter_tracker_stack.pop_back();
    _limiter_tracker_raw = _limiter_tracker_stack.back().get();
}

void ThreadMemTrackerMgr::exceeded_cancel_task(const std::string& cancel_details) {
    if (_fragment_instance_id_stack.back() != TUniqueId()) {
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id_stack.back(), PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                cancel_details);
    }
}

void ThreadMemTrackerMgr::exceeded(const std::string& failed_msg) {
    if (_cb_func != nullptr) {
        _cb_func();
    }
    if (is_attach_query()) {
        auto cancel_msg = _limiter_tracker_raw->mem_limit_exceeded(
                fmt::format("exec node:<{}>", last_consumer_tracker()),
                _limiter_tracker_raw->parent().get(), failed_msg);
        exceeded_cancel_task(cancel_msg);
        _check_limit = false; // Make sure it will only be canceled once
    }
}

} // namespace doris
