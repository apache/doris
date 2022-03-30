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

#include "runtime/thread_mem_tracker_mgr.h"

#include "runtime/mem_tracker_task_pool.h"
#include "service/backend_options.h"

namespace doris {

void ThreadMemTrackerMgr::attach_task(const std::string& cancel_msg, const std::string& task_id,
                                      const TUniqueId& fragment_instance_id,
                                      const std::shared_ptr<MemTracker>& mem_tracker) {
    _task_id = task_id;
    _fragment_instance_id = fragment_instance_id;
    _consume_err_cb.cancel_msg = cancel_msg;
    if (mem_tracker == nullptr) {
#ifdef BE_TEST
        if (ExecEnv::GetInstance()->task_pool_mem_tracker_registry() == nullptr) {
            return;
        }
#endif
        _temp_task_mem_tracker =
                ExecEnv::GetInstance()->task_pool_mem_tracker_registry()->get_task_mem_tracker(
                        task_id);
        update_tracker(_temp_task_mem_tracker);
    } else {
        update_tracker(mem_tracker);
    }
}

void ThreadMemTrackerMgr::detach_task() {
    _task_id = "";
    _fragment_instance_id = TUniqueId();
    _consume_err_cb.init();
    clear_untracked_mems();
    _tracker_id = "process";
    // The following memory changes for the two map operations of _untracked_mems and _mem_trackers
    // will be re-recorded in _untracked_mem.
    _untracked_mems.clear();
    _untracked_mems["process"] = 0;
    _mem_trackers.clear();
    _mem_trackers["process"] = MemTracker::get_process_tracker();
}

void ThreadMemTrackerMgr::exceeded_cancel_task(const std::string& cancel_details) {
    _temp_task_mem_tracker =
            ExecEnv::GetInstance()->task_pool_mem_tracker_registry()->get_task_mem_tracker(
                    _task_id);
    DCHECK(_temp_task_mem_tracker);
    if (_fragment_instance_id != TUniqueId()) {
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                cancel_details);
        _fragment_instance_id = TUniqueId(); // Make sure it will only be canceled once
    }
}

void ThreadMemTrackerMgr::exceeded(int64_t mem_usage, Status st) {
    auto rst = _mem_trackers[_tracker_id]->mem_limit_exceeded(
            nullptr, "In TCMalloc Hook, " + _consume_err_cb.cancel_msg, mem_usage, st);
    if (_consume_err_cb.cb_func != nullptr) {
        _consume_err_cb.cb_func();
    }
    if (is_attach_task()) {
        if (_consume_err_cb.cancel_task == true) {
            exceeded_cancel_task(rst.to_string());
        } else {
            // TODO(zxy) Need other processing, or log (not too often).
        }
    } else {
        // TODO(zxy) Need other processing, or log (not too often).
    }
}
} // namespace doris
