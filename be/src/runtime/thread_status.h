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

class ThreadStatus {
public:
    ThreadStatus() : _thread_id(std::this_thread::get_id()) {}
    ~ThreadStatus() { update_mem_tracker(nullptr); }

    void attach_query(const doris::TUniqueId& query_id) {
        _query_id = doris::print_id(query_id);
        update_mem_tracker(ExecEnv::GetInstance()->query_mem_trackers()->RegisterQueryMemTracker(
                doris::print_id(query_id)));
    }

    void update_mem_tracker(std::shared_ptr<MemTracker> mem_tracker) {
        if (_untracked_mem != 0 && _mem_tracker != nullptr) {
            if (!_mem_tracker->TryConsume(_untracked_mem)) {
                return; // add call back
            }
            _untracked_mem = 0;
        }
        _mem_tracker = mem_tracker;
    }

    void consume(int64_t size) {
        if (_mem_tracker == nullptr && ExecEnv::GetInstance()->is_init()) {
            _mem_tracker = ExecEnv::GetInstance()->hook_process_mem_tracker();
        }
        _untracked_mem += size;
        if (_mem_tracker != nullptr && (_untracked_mem >= _s_untracked_mem_limit ||
                                        _untracked_mem <= -_s_untracked_mem_limit)) {
            if (!_mem_tracker->TryConsume(_untracked_mem)) {
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
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    int64_t _untracked_mem = 0;
    int64_t _s_untracked_mem_limit = 1 * 1024 * 1024;
};

inline thread_local ThreadStatus current_thread;
} // namespace doris
