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

#include <chrono>
#include <thread>

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "service/backend_options.h"

namespace doris {

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
        const TUniqueId& fragment_instance_id) {
    DCHECK(mem_tracker);
    flush_untracked_mem<false, true>();
    _fragment_instance_id = fragment_instance_id;
    _limiter_tracker = mem_tracker;
    _limiter_tracker_raw = mem_tracker.get();
    _check_limit = true;
}

void ThreadMemTrackerMgr::detach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& old_mem_tracker) {
    flush_untracked_mem<false, true>();
    _fragment_instance_id = TUniqueId();
    _limiter_tracker = old_mem_tracker;
    _limiter_tracker_raw = old_mem_tracker.get();
}

void ThreadMemTrackerMgr::cancel_fragment() {
    ExecEnv::GetInstance()->fragment_mgr()->cancel(_fragment_instance_id,
                                                   PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                                                   _exceed_mem_limit_msg);
    _check_limit = false; // Make sure it will only be canceled once
}

void ThreadMemTrackerMgr::exceeded(int64_t size) {
    if (_cb_func != nullptr) {
        _cb_func();
    }
    _limiter_tracker_raw->print_log_usage(_exceed_mem_limit_msg);

    if (is_attach_query()) {
        if (_is_process_exceed && _wait_gc) {
            int64_t wait_milliseconds = config::thread_wait_gc_max_milliseconds;
            while (wait_milliseconds > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Check every 100 ms.
                if (!MemTrackerLimiter::sys_mem_exceed_limit_check(size)) {
                    MemInfo::refresh_interval_memory_growth += size;
                    return; // Process memory is sufficient, no cancel query.
                }
                wait_milliseconds -= 100;
            }
        }
        cancel_fragment();
    }
}

} // namespace doris
