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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

namespace doris {

// Memory Hook is counted in the memory tracker of the current thread.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() {}

    ~ThreadMemTrackerMgr() {
        // if _init == false, exec env is not initialized when init(). and never consumed mem tracker once.
        if (_init) flush_untracked_mem();
    }

    bool init();

    // After attach, the current thread Memory Hook starts to consume/release task mem_tracker
    void attach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                                const TUniqueId& fragment_instance_id);
    void detach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& old_mem_tracker =
                                        ExecEnv::GetInstance()->orphan_mem_tracker());

    // Must be fast enough! Thread update_tracker may be called very frequently.
    bool push_consumer_tracker(MemTracker* mem_tracker);
    void pop_consumer_tracker();
    std::string last_consumer_tracker() {
        return _consumer_tracker_stack.empty() ? "" : _consumer_tracker_stack.back()->label();
    }

    void start_count_scope_mem() {
        _scope_mem = 0;
        _count_scope_mem = true;
    }

    int64_t stop_count_scope_mem() {
        flush_untracked_mem();
        _count_scope_mem = false;
        return _scope_mem;
    }

    // Note that, If call the memory allocation operation in Memory Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    // Returns whether the memory exceeds limit, and will consume mem trcker no matter whether the limit is exceeded.
    void consume(int64_t size, bool large_memory_check = false);
    void flush_untracked_mem();

    bool is_attach_query() { return _fragment_instance_id != TUniqueId(); }

    std::shared_ptr<MemTrackerLimiter> limiter_mem_tracker() {
        CHECK(init());
        return _limiter_tracker;
    }
    MemTrackerLimiter* limiter_mem_tracker_raw() {
        CHECK(init());
        return _limiter_tracker_raw;
    }

    void disable_wait_gc() { _wait_gc = false; }
    bool wait_gc() { return _wait_gc; }
    void cancel_instance(const std::string& exceed_msg);

    std::string print_debug_string() {
        fmt::memory_buffer consumer_tracker_buf;
        for (const auto& v : _consumer_tracker_stack) {
            fmt::format_to(consumer_tracker_buf, "{}, ", MemTracker::log_usage(v->make_snapshot()));
        }
        return fmt::format(
                "ThreadMemTrackerMgr debug, _untracked_mem:{}, "
                "_limiter_tracker:<{}>, _consumer_tracker_stack:<{}>",
                std::to_string(_untracked_mem), _limiter_tracker_raw->log_usage(),
                fmt::to_string(consumer_tracker_buf));
    }

private:
    // is false: ExecEnv::ready() = false when thread local is initialized
    bool _init = false;
    // Cache untracked mem.
    int64_t _untracked_mem = 0;
    int64_t old_untracked_mem = 0;

    bool _count_scope_mem = false;
    int64_t _scope_mem = 0;

    std::string _failed_consume_msg = std::string();
    // If true, the Allocator will wait for the GC to free memory if it finds that the memory exceed limit.
    // A thread of query/load will only wait once during execution.
    bool _wait_gc = false;

    std::shared_ptr<MemTrackerLimiter> _limiter_tracker;
    MemTrackerLimiter* _limiter_tracker_raw = nullptr;
    std::vector<MemTracker*> _consumer_tracker_stack;

    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
    TUniqueId _fragment_instance_id = TUniqueId();
};

inline bool ThreadMemTrackerMgr::init() {
    // 1. Initialize in the thread context when the thread starts
    // 2. ExecEnv not initialized when thread start, initialized in limiter_mem_tracker().
    if (_init) return true;
    if (ExecEnv::GetInstance()->orphan_mem_tracker() != nullptr) {
        _limiter_tracker = ExecEnv::GetInstance()->orphan_mem_tracker();
        _limiter_tracker_raw = ExecEnv::GetInstance()->orphan_mem_tracker_raw();
        _wait_gc = true;
        _init = true;
        return true;
    }
    return false;
}

inline bool ThreadMemTrackerMgr::push_consumer_tracker(MemTracker* tracker) {
    DCHECK(tracker) << print_debug_string();
    if (std::count(_consumer_tracker_stack.begin(), _consumer_tracker_stack.end(), tracker)) {
        return false;
    }
    _consumer_tracker_stack.push_back(tracker);
    tracker->release(_untracked_mem);
    return true;
}

inline void ThreadMemTrackerMgr::pop_consumer_tracker() {
    DCHECK(!_consumer_tracker_stack.empty());
    _consumer_tracker_stack.back()->consume(_untracked_mem);
    _consumer_tracker_stack.pop_back();
}

inline void ThreadMemTrackerMgr::consume(int64_t size, bool large_memory_check) {
    _untracked_mem += size;
    if (!ExecEnv::ready()) {
        return;
    }
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    // After the jemalloc hook is loaded, before ExecEnv init, _limiter_tracker=nullptr.
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume) {
        flush_untracked_mem();
    }

    if (large_memory_check && doris::config::large_memory_check_bytes > 0 &&
        size > doris::config::large_memory_check_bytes) {
        _stop_consume = true;
        LOG(WARNING) << fmt::format(
                "malloc or new large memory: {}, {}, this is just a warning, not prevent memory "
                "alloc, stacktrace:\n{}",
                size,
                is_attach_query() ? "in query or load: " + print_id(_fragment_instance_id)
                                  : "not in query or load",
                get_stack_trace());
        _stop_consume = false;
    }
}

inline void ThreadMemTrackerMgr::flush_untracked_mem() {
    // Temporary memory may be allocated during the consumption of the mem tracker, which will lead to entering
    // the Memory Hook again, so suspend consumption to avoid falling into an infinite loop.
    if (!init()) return;
    _stop_consume = true;
    DCHECK(_limiter_tracker_raw);

    old_untracked_mem = _untracked_mem;
    if (_count_scope_mem) _scope_mem += _untracked_mem;
    _limiter_tracker_raw->consume(old_untracked_mem);
    for (auto tracker : _consumer_tracker_stack) {
        tracker->consume(old_untracked_mem);
    }
    _untracked_mem -= old_untracked_mem;
    _stop_consume = false;
}

} // namespace doris
