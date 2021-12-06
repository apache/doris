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

#pragma once

#include "runtime/thread_context.h"

#include <fmt/format.h>

namespace doris {

void ThreadContext::update_query_mem_tracker(const std::string& query_id) {
#ifdef BE_TEST
    if (ExecEnv::GetInstance()->query_mem_tracker_registry() == nullptr) {
        return;
    }
#endif
    update_query_mem_tracker(
            ExecEnv::GetInstance()->query_mem_tracker_registry()->get_query_mem_tracker(query_id));
}

void ThreadContext::update_query_mem_tracker(
        std::weak_ptr<MemTracker> mem_tracker = std::weak_ptr<MemTracker>()) {
    if (_untracked_mem != 0) {
        consume();
        _untracked_mem = 0;
    }
    _query_mem_tracker = mem_tracker;
}

void ThreadContext::query_mem_limit_exceeded(int64_t mem_usage) {
    if (_task_id != "" && _fragment_instance_id != TUniqueId() &&
        ExecEnv::GetInstance()->initialized() &&
        ExecEnv::GetInstance()->fragment_mgr()->is_canceling(_fragment_instance_id).ok()) {
        auto st = _query_mem_tracker.lock()->MemLimitExceeded(
                nullptr, "Query Memory exceed limit in TCMalloc Hook New.", mem_usage);

        std::string detail =
                "Query Memory exceed limit in TCMalloc Hook New, Backend: {}, Fragment: {}, Used: "
                "{}, Limit: {}. You can change the limit by session variable exec_mem_limit.";
        fmt::format(detail, BackendOptions::get_localhost(), print_id(_fragment_instance_id),
                    std::to_string(_query_mem_tracker.lock()->consumption()),
                    std::to_string(_query_mem_tracker.lock()->limit()));
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED, detail);
        _fragment_instance_id = TUniqueId(); // Make sure it will only be canceled once
    }
}

void ThreadContext::global_mem_limit_exceeded(int64_t mem_usage) {
    std::string detail = "Global Memory exceed limit in TCMalloc Hook New.";
    auto st = _query_mem_tracker.lock()->MemLimitExceeded(nullptr, detail, mem_usage);
}

// Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
// such as calling LOG/iostream/sstream/stringstream/etc. related methods,
// must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
void ThreadContext::consume() {
    // Query_mem_tracker and global_hook_tracker are counted separately,
    // in order to ensure that the process memory counted by global_hook_tracker is accurate enough.
    //
    // Otherwise, if query_mem_tracker is the child of global_hook_tracker and global_hook_tracker
    // is the default tracker, it may be the same block of memory. Consume is called in query_mem_tracker,
    // and release is called in global_hook_tracker, which is repeatedly released after ~query_mem_tracker.
    if (!_query_mem_tracker.expired()) {
        if (_query_mem_consuming == false) {
            _query_mem_consuming = true;
            if (!_query_mem_tracker.lock()->TryConsume(_missed_query_tracker_mem +
                                                       _untracked_mem)) {
                query_mem_limit_exceeded(_missed_query_tracker_mem + _untracked_mem);
                _missed_query_tracker_mem += _untracked_mem;
            } else {
                _missed_query_tracker_mem = 0;
            }
            _query_mem_consuming = false;
        } else {
            _missed_query_tracker_mem += _untracked_mem;
        }
    }

    // The first time GetGlobalHookTracker is called after the main thread starts, == nullptr
    if (_global_hook_tracker != nullptr) {
        if (_global_mem_consuming == false) {
            _global_mem_consuming = true;
            if (!_global_hook_tracker->TryConsume(_missed_global_tracker_mem + _untracked_mem)) {
                global_mem_limit_exceeded(_missed_global_tracker_mem + _untracked_mem);
                _missed_global_tracker_mem += _untracked_mem;
            } else {
                _missed_global_tracker_mem = 0;
            }
            _global_mem_consuming = false;
        } else {
            _missed_global_tracker_mem += _untracked_mem;
        }
    } else {
        _missed_global_tracker_mem += _untracked_mem;
    }
}

void ThreadContext::try_consume(int64_t size) {
    if (_stop_mem_tracker == true) {
        return;
    }
    _untracked_mem += size;
    // When some threads `0 <_untracked_mem <_untracked_mem_limit`
    // and some threads `_untracked_mem <= -_untracked_mem_limit` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    if (_untracked_mem >= _untracked_mem_limit || _untracked_mem <= -_untracked_mem_limit) {
        consume();
        _untracked_mem = 0;
    }
}

} // namespace doris
