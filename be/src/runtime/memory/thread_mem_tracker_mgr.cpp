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

#include "common/exception.h"
#include "common/signal_handler.h"
#include "runtime/exec_env.h"

namespace doris {

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    if (UNLIKELY(!mem_tracker)) {
        // Without this guard, _limiter_tracker silently becomes null and any
        // later allocation on this thread would dereference it inside the
        // allocator's memory check path, surfacing as a generic NPE far from
        // the real call site that fed the null tracker. The query id is
        // read from signal-handler thread-local storage; it does not depend
        // on any object that may already be torn down. Note: do not call
        // print_debug_string() here — it itself dereferences _limiter_tracker
        // (via make_profile_str()), which on a fresh ThreadMemTrackerMgr is
        // still null and would crash inside the format call before the
        // intended FatalError is thrown.
        throw Exception(Status::FatalError(
                "ThreadMemTrackerMgr::attach_limiter_tracker called with null mem_tracker. "
                "previous limiter label={}, snapshot_stack_depth={}, "
                "query_id={:x}-{:x}",
                _limiter_tracker ? _limiter_tracker->label() : "<null>",
                _last_attach_snapshots_stack.size(), doris::signal::query_id_hi,
                doris::signal::query_id_lo));
    }
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
    if (UNLIKELY(_last_attach_snapshots_stack.empty())) {
        // detach_limiter_tracker() is invoked from RAII destructors that are
        // noexcept; throwing would call std::terminate without a useful
        // message. A LOG(FATAL) gives a controlled abort with a flushed
        // message and a glog-captured stack trace.
        LOG(FATAL) << "ThreadMemTrackerMgr::detach_limiter_tracker called with empty snapshot "
                      "stack. current limiter label="
                   << (_limiter_tracker ? _limiter_tracker->label() : "<null>")
                   << ", query_id=" << std::hex << doris::signal::query_id_hi << "-"
                   << doris::signal::query_id_lo << std::dec;
    }
    DCHECK(!_last_attach_snapshots_stack.empty());
    if (UNLIKELY(!_last_attach_snapshots_stack.back().limiter_tracker)) {
        // Same noexcept-destructor rationale as above.
        LOG(FATAL) << "ThreadMemTrackerMgr::detach_limiter_tracker restored null limiter from "
                      "snapshot. stack_depth_before_pop="
                   << _last_attach_snapshots_stack.size() << ", query_id=" << std::hex
                   << doris::signal::query_id_hi << "-" << doris::signal::query_id_lo << std::dec;
    }
    _limiter_tracker_sptr = _last_attach_snapshots_stack.back().limiter_tracker;
    _limiter_tracker = _limiter_tracker_sptr.get();
    _wg_wptr = _last_attach_snapshots_stack.back().wg_wptr;
    _reserved_mem = _last_attach_snapshots_stack.back().reserved_mem;
    _consumer_tracker_stack = _last_attach_snapshots_stack.back().consumer_tracker_stack;
    _last_attach_snapshots_stack.pop_back();
}

} // namespace doris
