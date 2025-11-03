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

namespace doris {
#include "common/compile_check_begin.h"

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    CHECK(init());
    flush_untracked_mem();
    _last_attach_snapshots_stack.push_back(
            {_limiter_tracker_sptr, _wg_wptr, _reserved_mem, _consumer_tracker_stack});
    if (_reserved_mem != 0) {
        // _untracked_mem temporary store bytes that not synchronized to process reserved memory,
        // but bytes have been subtracted from thread _reserved_mem.
        doris::GlobalMemoryArbitrator::shrink_process_reserved(_untracked_mem);
        _limiter_tracker_sptr->shrink_reserved(_untracked_mem);
        _reserved_mem = 0;
        _untracked_mem = 0;
    }
    _consumer_tracker_stack.clear();
    _limiter_tracker_sptr = mem_tracker;
    _limiter_tracker = _limiter_tracker_sptr.get();
}

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
        const std::weak_ptr<WorkloadGroup>& wg_wptr) {
    attach_limiter_tracker(mem_tracker);
    _wg_wptr = wg_wptr;
}

void ThreadMemTrackerMgr::detach_limiter_tracker() {
    CHECK(init());
    flush_untracked_mem();
    shrink_reserved();
    DCHECK(!_last_attach_snapshots_stack.empty());
    _limiter_tracker_sptr = _last_attach_snapshots_stack.back().limiter_tracker;
    _limiter_tracker = _limiter_tracker_sptr.get();
    _wg_wptr = _last_attach_snapshots_stack.back().wg_wptr;
    _reserved_mem = _last_attach_snapshots_stack.back().reserved_mem;
    _consumer_tracker_stack = _last_attach_snapshots_stack.back().consumer_tracker_stack;
    _last_attach_snapshots_stack.pop_back();
}

#include "common/compile_check_end.h"
} // namespace doris
