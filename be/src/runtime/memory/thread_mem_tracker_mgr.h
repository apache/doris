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

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>

#include "gutil/macros.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

using ExceedCallBack = void (*)();

// Memory Hook is counted in the memory tracker of the current thread.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() {}

    ~ThreadMemTrackerMgr() {
        // if _init == false, exec env is not initialized when init(). and never consumed mem tracker once.
        if (_init) flush_untracked_mem<false, true>();
    }

    // only for memory hook
    static void consume_no_attach(int64_t size) {
        if (ExecEnv::GetInstance()->initialized()) {
            ExecEnv::GetInstance()->orphan_mem_tracker()->consume(size);
        }
    }

    void init();

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
        flush_untracked_mem<false, true>();
        _count_scope_mem = false;
        return _scope_mem;
    }

    void set_exceed_call_back(ExceedCallBack cb_func) { _cb_func = cb_func; }

    // Note that, If call the memory allocation operation in Memory Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    // Returns whether the memory exceeds limit, and will consume mem trcker no matter whether the limit is exceeded.
    void consume(int64_t size);
    // If the memory exceeds the limit, return false, and will not consume mem tracker.
    bool try_consume(int64_t size);

    // Force is equal to false. When the memory exceeds the limit,this alloc will be terminated and false
    // will be returned.
    // Force is equal to true, even if the memory is found to be overrun, continue to consume mem tracker,
    // because this time alloc will still actually allocate memory, and always return true.
    template <bool CheckLimit, bool Force>
    bool flush_untracked_mem();

    bool is_attach_query() { return _fragment_instance_id != TUniqueId(); }

    std::shared_ptr<MemTrackerLimiter> limiter_mem_tracker() {
        if (!_init) init(); // ExecEnv not initialized when thread is created.
        return _limiter_tracker;
    }
    MemTrackerLimiter* limiter_mem_tracker_raw() {
        if (!_init) init();
        return _limiter_tracker_raw;
    }

    bool check_limit() { return _check_limit; }
    void set_check_limit(bool check_limit) { _check_limit = check_limit; }
    std::string exceed_mem_limit_msg() { return _exceed_mem_limit_msg; }
    void clear_exceed_mem_limit_msg() { _exceed_mem_limit_msg = ""; }

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
    void cancel_fragment();
    void exceeded();

    void save_exceed_mem_limit_msg() {
        _exceed_mem_limit_msg = _limiter_tracker_raw->mem_limit_exceeded(
                fmt::format("execute:<{}>", last_consumer_tracker()), _failed_consume_msg);
    }

private:
    // is false: ExecEnv::GetInstance()->initialized() = false when thread local is initialized
    bool _init = false;
    // Cache untracked mem.
    int64_t _untracked_mem = 0;
    int64_t old_untracked_mem = 0;

    bool _count_scope_mem = false;
    int64_t _scope_mem = 0;

    std::string _failed_consume_msg = std::string();
    std::string _exceed_mem_limit_msg = std::string();

    std::shared_ptr<MemTrackerLimiter> _limiter_tracker;
    MemTrackerLimiter* _limiter_tracker_raw = nullptr;
    std::vector<MemTracker*> _consumer_tracker_stack;

    // If true, call memtracker try_consume, otherwise call consume.
    bool _check_limit = false;
    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
    TUniqueId _fragment_instance_id = TUniqueId();
    ExceedCallBack _cb_func = nullptr;
};

inline void ThreadMemTrackerMgr::init() {
    DCHECK(_limiter_tracker == nullptr);
    _limiter_tracker = ExecEnv::GetInstance()->orphan_mem_tracker();
    _limiter_tracker_raw = ExecEnv::GetInstance()->orphan_mem_tracker_raw();
    _check_limit = true;
    _init = true;
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

inline void ThreadMemTrackerMgr::consume(int64_t size) {
    _untracked_mem += size;
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    // After the jemalloc hook is loaded, before ExecEnv init, _limiter_tracker=nullptr.
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume && ExecEnv::GetInstance()->initialized()) {
        if (_check_limit) {
            flush_untracked_mem<true, true>();
        } else {
            flush_untracked_mem<false, true>();
        }
    }
}

inline bool ThreadMemTrackerMgr::try_consume(int64_t size) {
    _untracked_mem += size;
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume && ExecEnv::GetInstance()->initialized()) {
        if (_check_limit) {
            return flush_untracked_mem<true, false>();
        } else {
            return flush_untracked_mem<false, true>();
        }
    }
    return true;
}

template <bool CheckLimit, bool Force>
inline bool ThreadMemTrackerMgr::flush_untracked_mem() {
    // Temporary memory may be allocated during the consumption of the mem tracker, which will lead to entering
    // the Memory Hook again, so suspend consumption to avoid falling into an infinite loop.
    _stop_consume = true;
    if (!_init) init(); // ExecEnv not initialized when thread is created.
    DCHECK(_limiter_tracker_raw);
    old_untracked_mem = _untracked_mem;
    if (_count_scope_mem) _scope_mem += _untracked_mem;
    if (CheckLimit) {
        if (!_limiter_tracker_raw->try_consume(old_untracked_mem, _failed_consume_msg)) {
            if (Force) _limiter_tracker_raw->consume(old_untracked_mem);
            save_exceed_mem_limit_msg();
            exceeded();
            if (!Force) return false;
        }
    } else {
        _limiter_tracker_raw->consume(old_untracked_mem);
    }
    for (auto tracker : _consumer_tracker_stack) {
        tracker->consume(old_untracked_mem);
    }
    _untracked_mem -= old_untracked_mem;
    _stop_consume = false;
    return true;
}

} // namespace doris
