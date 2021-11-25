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

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace doris {

class TheadContext {
public:
    TheadContext()
            : _thread_id(std::this_thread::get_id()),
              _global_hook_tracker(MemTracker::GetGlobalHookTracker()) {}
    ~TheadContext() { update_query_mem_tracker(); }

    void attach_query(const doris::TUniqueId& query_id) {
        _query_id = doris::print_id(query_id);
        update_query_mem_tracker(ExecEnv::GetInstance()->query_mem_trackers()->RegisterQueryMemTracker(
                doris::print_id(query_id)));
    }

    void unattach_query() {
        _query_id = "";
        update_query_mem_tracker();
    }

    void update_query_mem_tracker(
            std::weak_ptr<MemTracker> mem_tracker = std::weak_ptr<MemTracker>()) {
        if (_untracked_mem != 0 && !_query_mem_tracker.expired()) {
            if (!_query_mem_tracker.lock()->TryConsume(_untracked_mem)) {
                return; // add call back
            }
            _untracked_mem = 0;
        }
        _query_mem_tracker = mem_tracker;
    }

    void consume(int64_t size) {
        _untracked_mem += size;
        if (_untracked_mem >= _untracked_mem_limit || _untracked_mem <= -_untracked_mem_limit) {
            // TODO(zxy): _untracked_mem <0 means that there is the same block of memory,
            // tracker A calls consume, and tracker B calls release. This will make the memory
            // statistics inaccurate and should be avoided as much as possible. 
            // This DCHECK should be turned on in the future.
            // DCHECK(_untracked_mem >= 0); 

            // There is no default tracker to avoid repeated releases of MemTacker.
            // When the consume is called on the child MemTracker,
            // after the release is called on the parent MemTracker, 
            // the child ~MemTracker will cause repeated releases.
            if (!_query_mem_tracker.expired()) {
                if (!_query_mem_tracker.lock()->TryConsume(_untracked_mem)) {
                    return; // add call back
                }
            }
            if (!_global_hook_tracker->TryConsume(_untracked_mem)) {
                return; // add call back
            }
            _untracked_mem = 0;
        }
    }

    void consume_mem(int64_t size) { consume(size); }

    void release_mem(int64_t size) { consume(-size); }

    const std::string& query_id() { return _query_id; }
    const std::thread::id& thread_id() { return _thread_id; }

private:
    std::thread::id _thread_id;
    std::string _query_id;
    std::weak_ptr<MemTracker> _query_mem_tracker;
    std::shared_ptr<MemTracker> _global_hook_tracker = nullptr;
    int64_t _untracked_mem = 0;
    int64_t _untracked_mem_limit = 1 * 1024 * 1024;
};

inline thread_local TheadContext thread_local_ctx;
} // namespace doris
