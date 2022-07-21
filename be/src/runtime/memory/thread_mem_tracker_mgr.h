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
#include <parallel_hashmap/phmap.h>

#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

using ExceedCallBack = void (*)();
struct MemExceedCallBackInfo {
    std::string cancel_msg;
    bool cancel_task; // Whether to cancel the task when the current tracker exceeds the limit.
    ExceedCallBack cb_func;

    MemExceedCallBackInfo() { init(); }

    MemExceedCallBackInfo(const std::string& cancel_msg, bool cancel_task, ExceedCallBack cb_func)
            : cancel_msg(cancel_msg), cancel_task(cancel_task), cb_func(cb_func) {}

    void init() {
        cancel_msg = "";
        cancel_task = true;
        cb_func = nullptr;
    }
};

// TCMalloc new/delete Hook is counted in the memory_tracker of the current thread.
//
// In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior. Therefore, when alloc for some large memory.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() {}

    ~ThreadMemTrackerMgr() {
        flush_untracked_mem<false>();
        _exceed_cb.init();
        DCHECK(_consumer_tracker_stack.empty());
    }

    // only for tcmalloc hook
    static void consume_no_attach(int64_t size) {
        ExecEnv::GetInstance()->process_mem_tracker()->consume(size);
    }

    // After thread initialization, calling `init` again must call `clear_untracked_mems` first
    // to avoid memory tracking loss.
    void init();

    // After attach, the current thread TCMalloc Hook starts to consume/release task mem_tracker
    void attach_limiter_tracker(const std::string& cancel_msg, const std::string& task_id,
                                const TUniqueId& fragment_instance_id,
                                MemTrackerLimiter* mem_tracker);

    void detach_limiter_tracker();

    // Must be fast enough! Thread update_tracker may be called very frequently.
    // So for performance, add tracker as early as possible, and then call update_tracker<Existed>.
    void push_consumer_tracker(MemTracker* mem_tracker);
    void pop_consumer_tracker();

    MemExceedCallBackInfo update_exceed_call_back(const std::string& cancel_msg, bool cancel_task,
                                                  ExceedCallBack cb_func) {
        _temp_exceed_cb = _exceed_cb;
        _exceed_cb.cancel_msg = cancel_msg;
        _exceed_cb.cancel_task = cancel_task;
        _exceed_cb.cb_func = cb_func;
        return _temp_exceed_cb;
    }

    void update_exceed_call_back(const MemExceedCallBackInfo& exceed_cb) { _exceed_cb = exceed_cb; }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void consume(int64_t size);

    void transfer_to(int64_t size, MemTrackerLimiter* mem_tracker) {
        consume(-size);
        mem_tracker->consume(size);
    }
    void transfer_from(int64_t size, MemTrackerLimiter* mem_tracker) {
        mem_tracker->release(size);
        consume(size);
    }

    template <bool CheckLimit>
    void flush_untracked_mem();

    bool is_attach_task() { return _task_id != ""; }

    MemTrackerLimiter* limiter_mem_tracker() { return _limiter_tracker; }

    void set_check_limit(bool check_limit) { _check_limit = check_limit; }

    std::string print_debug_string() {
        fmt::memory_buffer consumer_tracker_buf;
        for (const auto& v : _consumer_tracker_stack) {
            fmt::format_to(consumer_tracker_buf, "{}, ", v->log_usage());
        }
        return fmt::format(
                "ThreadMemTrackerMgr debug, _untracked_mem:{}, _task_id:{}, "
                "_limiter_tracker:<{}>, _consumer_tracker_stack:<{}>",
                std::to_string(_untracked_mem), _task_id, _limiter_tracker->log_usage(1),
                fmt::to_string(consumer_tracker_buf));
    }

private:
    // If tryConsume fails due to task mem tracker exceeding the limit, the task must be canceled
    void exceeded_cancel_task(const std::string& cancel_details);

    void exceeded(int64_t mem_usage, Status try_consume_st);

private:
    // Cache untracked mem, only update to _untracked_mems when switching mem tracker.
    // Frequent calls to unordered_map _untracked_mems[] in consume will degrade performance.
    int64_t _untracked_mem = 0;

    MemTrackerLimiter* _limiter_tracker;
    std::vector<MemTracker*> _consumer_tracker_stack;

    // If true, call memtracker try_consume, otherwise call consume.
    bool _check_limit = false;
    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
    MemExceedCallBackInfo _exceed_cb;
    MemExceedCallBackInfo _temp_exceed_cb;
};

inline void ThreadMemTrackerMgr::init() {
    DCHECK(_consumer_tracker_stack.empty());
    _task_id = "";
    _exceed_cb.init();
    _limiter_tracker = ExecEnv::GetInstance()->process_mem_tracker();
    _check_limit = true;
}

inline void ThreadMemTrackerMgr::push_consumer_tracker(MemTracker* tracker) {
    DCHECK(tracker) << print_debug_string();
    DCHECK(!std::count(_consumer_tracker_stack.begin(), _consumer_tracker_stack.end(), tracker))
            << print_debug_string();
    _consumer_tracker_stack.push_back(tracker);
    tracker->release(_untracked_mem);
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
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume) {
        if (_check_limit) {
            flush_untracked_mem<true>();
        } else {
            flush_untracked_mem<false>();
        }
    }
}

template <bool CheckLimit>
inline void ThreadMemTrackerMgr::flush_untracked_mem() {
    // Temporary memory may be allocated during the consumption of the mem tracker, which will lead to entering
    // the TCMalloc Hook again, so suspend consumption to avoid falling into an infinite loop.
    _stop_consume = true;
    DCHECK(_limiter_tracker);
    if (CheckLimit) {
        Status st = _limiter_tracker->try_consume(_untracked_mem);
        if (!st) {
            // The memory has been allocated, so when TryConsume fails, need to continue to complete
            // the consume to ensure the accuracy of the statistics.
            _limiter_tracker->consume(_untracked_mem);
            exceeded(_untracked_mem, st);
        }
    } else {
        _limiter_tracker->consume(_untracked_mem);
    }
    for (auto tracker : _consumer_tracker_stack) {
        tracker->consume(_untracked_mem);
    }
    _untracked_mem = 0;
    _stop_consume = false;
}

} // namespace doris
