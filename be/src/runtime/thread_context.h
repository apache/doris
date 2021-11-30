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

#include <string>

#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"

namespace doris {

class TheadContext {
public:
    TheadContext()
            : _thread_id(std::this_thread::get_id()),
              _global_hook_tracker(MemTracker::GetGlobalHookTracker()) {}
    ~TheadContext() { update_query_mem_tracker(); }

    void attach_query(const TUniqueId& query_id,
                      const TUniqueId& fragment_instance_id = TUniqueId()) {
        _query_id = query_id;
        _fragment_instance_id = fragment_instance_id;
#ifdef BE_TEST
        if (ExecEnv::GetInstance()->query_mem_tracker_registry() == nullptr) {
            return;
        }
#endif
        update_query_mem_tracker(
                ExecEnv::GetInstance()->query_mem_tracker_registry()->GetQueryMemTracker(
                        print_id(query_id)));
    }

    void unattach_query() {
        _query_id = TUniqueId();
        _fragment_instance_id = TUniqueId();
        update_query_mem_tracker();
    }

    void update_query_mem_tracker(
            std::weak_ptr<MemTracker> mem_tracker = std::weak_ptr<MemTracker>()) {
        if (_untracked_mem != 0) {
            consume();
            _untracked_mem = 0;
        }
        _query_mem_tracker = mem_tracker;
    }

    void query_mem_limit_exceeded(int64_t mem_usage) {
        if (_query_id != TUniqueId() && _fragment_instance_id != TUniqueId() &&
            ExecEnv::GetInstance()->is_init() &&
            ExecEnv::GetInstance()->fragment_mgr()->is_canceling(_fragment_instance_id).ok()) {
            std::string detail = "Query Memory exceed limit in TCMalloc Hook New.";
            auto st = _query_mem_tracker.lock()->MemLimitExceeded(nullptr, detail, mem_usage);
            detail += ", Backend: " + BackendOptions::get_localhost() +
                      ", Fragment: " + print_id(_fragment_instance_id) +
                      ", Used: " + std::to_string(_query_mem_tracker.lock()->consumption()) +
                      ", Limit: " + std::to_string(_query_mem_tracker.lock()->limit()) +
                      ". You can change the limit by session variable exec_mem_limit.";
            ExecEnv::GetInstance()->fragment_mgr()->cancel(
                    _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_EXCEED_LIMIT, detail);
            _fragment_instance_id = TUniqueId(); // Make sure it will only be canceled once
        }
    }

    void global_mem_limit_exceeded(int64_t mem_usage) {
        std::string detail = "Global Memory exceed limit in TCMalloc Hook New.";
        auto st = _query_mem_tracker.lock()->MemLimitExceeded(nullptr, detail, mem_usage);
    }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void consume() {
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
                if (!_global_hook_tracker->TryConsume(_missed_global_tracker_mem +
                                                      _untracked_mem)) {
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

    void try_consume(int64_t size) {
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

    void consume_mem(int64_t size) { try_consume(size); }

    void release_mem(int64_t size) { try_consume(-size); }

    void stop_mem_tracker() {
        _stop_mem_tracker = true;
    }

    const TUniqueId& query_id() { return _query_id; }
    const std::thread::id& thread_id() { return _thread_id; }

private:
    std::thread::id _thread_id;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    std::weak_ptr<MemTracker> _query_mem_tracker;
    std::shared_ptr<MemTracker> _global_hook_tracker = nullptr;

    // The memory size that is not tracker is used to control batch trackers,
    // avoid frequent consume/release.
    int64_t _untracked_mem = 0;
    int64_t _untracked_mem_limit = config::untracked_mem_limit;

    // Memory size of tracker failure after mem limit exceeded,
    // expect to be successfully consumed later.
    int64_t _missed_query_tracker_mem = 0;
    int64_t _missed_global_tracker_mem = 0;

    // When memory is being consumed, avoid entering infinite recursion.
    bool _query_mem_consuming = false;
    bool _global_mem_consuming = false;

    // In some cases, we want to turn off memory statistics.
    // For example, when ~GlobalHookTracker, TCMalloc delete hook
    // release GlobalHookTracker will crash.
    bool _stop_mem_tracker = false;
};

inline thread_local TheadContext thread_local_ctx;
} // namespace doris
