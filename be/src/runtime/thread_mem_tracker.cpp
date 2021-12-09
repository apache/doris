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

#include "runtime/thread_mem_tracker.h"

#include "service/backend_options.h"

namespace doris {

void ThreadMemTracker::attach_query(const std::string& query_id,
                                    const TUniqueId& fragment_instance_id) {
#ifdef BE_TEST
    if (ExecEnv::GetInstance()->query_mem_tracker_registry() == nullptr) {
        return;
    }
#endif
    update_query_mem_tracker(
            ExecEnv::GetInstance()->query_mem_tracker_registry()->get_query_mem_tracker(query_id));
    _query_id = query_id;
    _fragment_instance_id = fragment_instance_id;
}

void ThreadMemTracker::detach_query() {
    update_query_mem_tracker(std::weak_ptr<MemTracker>());
    _query_id = "";
    _fragment_instance_id = TUniqueId();
}

void ThreadMemTracker::update_query_mem_tracker(std::weak_ptr<MemTracker> mem_tracker) {
    if (_untracked_mem != 0) {
        consume();
        _untracked_mem = 0;
    }
    _query_mem_tracker = mem_tracker;
}

void ThreadMemTracker::query_mem_limit_exceeded(int64_t mem_usage) {
    if (_fragment_instance_id != TUniqueId() && ExecEnv::GetInstance()->initialized() &&
        ExecEnv::GetInstance()->fragment_mgr()->is_canceling(_fragment_instance_id).ok()) {
        std::string detail = "Query Memory exceed limit in TCMalloc Hook New.";
        auto st = _query_mem_tracker.lock()->MemLimitExceeded(nullptr, detail, mem_usage);

        detail +=
                " Query Memory exceed limit in TCMalloc Hook New, Backend: {}, Query: {}, "
                "Fragment: {}, Used: {}, Limit: {}. You can change the limit by session variable "
                "exec_mem_limit.";
        fmt::format(detail, BackendOptions::get_localhost(), _query_id,
                    print_id(_fragment_instance_id),
                    std::to_string(_query_mem_tracker.lock()->consumption()),
                    std::to_string(_query_mem_tracker.lock()->limit()));
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED, detail);
        _fragment_instance_id = TUniqueId(); // Make sure it will only be canceled once
    }
}

void ThreadMemTracker::global_mem_limit_exceeded(int64_t mem_usage) {
    if (time(nullptr) - global_exceeded_interval > 60) {
        std::string detail = "Global Memory exceed limit in TCMalloc Hook New.";
        auto st = _global_hook_tracker->MemLimitExceeded(nullptr, detail, mem_usage);
        global_exceeded_interval = time(nullptr);
    }
}

void ThreadMemTracker::consume() {
    // Query_mem_tracker and global_hook_tracker are counted separately,
    // in order to ensure that the process memory counted by global_hook_tracker is accurate enough.
    //
    // Otherwise, if query_mem_tracker is the child of global_hook_tracker and global_hook_tracker
    // is the default tracker, it may be the same block of memory. Consume is called in query_mem_tracker,
    // and release is called in global_hook_tracker, which is repeatedly released after ~query_mem_tracker.
    if (!_query_mem_tracker.expired()) {
        if (_stop_query_mem_tracker == false) {
            _stop_query_mem_tracker = true;
            if (!_query_mem_tracker.lock()->TryConsume(_untracked_mem)) {
                query_mem_limit_exceeded(_untracked_mem);
            }
            _stop_query_mem_tracker = false;
        }
    }

    // The first time GetGlobalHookTracker is called after the main thread starts, == nullptr
    if (_global_hook_tracker != nullptr) {
        if (_stop_global_mem_tracker == false) {
            _stop_global_mem_tracker = true;
            if (!_global_hook_tracker->TryConsume(_untracked_mem)) {
                // Currently, _global_hook_tracker is only used for real-time observation to verify
                // the accuracy of MemTracker statistics. Therefore, when the _global_hook_tracker
                // TryConsume fails, the process is not expected to terminate. To ensure the accuracy
                // of real-time statistics, continue to complete the Consume.
                _global_hook_tracker->Consume(_untracked_mem);
                global_mem_limit_exceeded(_untracked_mem);
            }
            _stop_global_mem_tracker = false;
        }
    }
}

void ThreadMemTracker::try_consume(int64_t size) {
    if (_stop_mem_tracker == true) {
        return;
    }
    _untracked_mem += size;
    // When some threads `0 < _untracked_mem < _tracker_consume_cache_size`
    // and some threads `_untracked_mem <= -_tracker_consume_cache_size` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    if (_untracked_mem >= _tracker_consume_cache_size ||
        _untracked_mem <= -_tracker_consume_cache_size) {
        consume();
        _untracked_mem = 0;
    }
}

} // namespace doris
