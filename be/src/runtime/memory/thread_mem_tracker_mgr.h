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
#include <service/brpc_conflict.h>

#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
// After brpc_conflict.h
#include <bthread/bthread.h>

namespace doris {

extern bthread_key_t btls_key;
static const bthread_key_t EMPTY_BTLS_KEY = {0, 0};

using ExceedCallBack = void (*)();

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
        // if _init == false, exec env is not initialized when init(). and never consumed mem tracker once.
        if (_init) {
            flush_untracked_mem<false>();
            if (bthread_self() == 0) {
                DCHECK(_consumer_tracker_stack.empty());
                DCHECK(_limiter_tracker_stack.size() == 1)
                        << ", limiter_tracker_stack.size(): " << _limiter_tracker_stack.size();
            }
        }
    }

    // only for tcmalloc hook
    static void consume_no_attach(int64_t size) {
        if (ExecEnv::GetInstance()->initialized()) {
            ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume(size);
        }
    }

    // After thread initialization, calling `init` again must call `clear_untracked_mems` first
    // to avoid memory tracking loss.
    void init();
    void init_impl();
    void clear();

    // After attach, the current thread TCMalloc Hook starts to consume/release task mem_tracker
    void attach_limiter_tracker(const std::string& task_id, const TUniqueId& fragment_instance_id,
                                const std::shared_ptr<MemTrackerLimiter>& mem_tracker);
    void detach_limiter_tracker();
    // Usually there are only two layers, the first is the default trackerOrphan;
    // the second is the query tracker or bthread tracker.
    int64_t get_attach_layers() { return _limiter_tracker_stack.size(); }

    // Must be fast enough! Thread update_tracker may be called very frequently.
    // So for performance, add tracker as early as possible, and then call update_tracker<Existed>.
    void push_consumer_tracker(MemTracker* mem_tracker);
    void pop_consumer_tracker();
    std::string last_consumer_tracker() {
        return _consumer_tracker_stack.empty() ? "" : _consumer_tracker_stack[-1]->label();
    }

    void set_exceed_call_back(ExceedCallBack cb_func) { _cb_func = cb_func; }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void consume(int64_t size);

    template <bool CheckLimit>
    void flush_untracked_mem();

    bool is_attach_query() { return _fragment_instance_id_stack.back() != TUniqueId(); }

    std::shared_ptr<MemTrackerLimiter> limiter_mem_tracker() {
        if (!_init) init();
        return _limiter_tracker_stack.back();
    }
    MemTrackerLimiter* limiter_mem_tracker_raw() {
        if (!_init) init();
        return _limiter_tracker_raw;
    }

    bool check_limit() { return _check_limit; }
    void set_check_limit(bool check_limit) { _check_limit = check_limit; }
    void set_check_attach(bool check_attach) { _check_attach = check_attach; }

    std::string print_debug_string() {
        fmt::memory_buffer consumer_tracker_buf;
        for (const auto& v : _consumer_tracker_stack) {
            fmt::format_to(consumer_tracker_buf, "{}, ",
                           MemTracker::log_usage(v->make_snapshot(0)));
        }
        return fmt::format(
                "ThreadMemTrackerMgr debug, _untracked_mem:{}, _task_id:{}, "
                "_limiter_tracker:<{}>, _consumer_tracker_stack:<{}>",
                std::to_string(_untracked_mem), _task_id_stack.back(),
                _limiter_tracker_raw->log_usage(1), fmt::to_string(consumer_tracker_buf));
    }

private:
    // If tryConsume fails due to task mem tracker exceeding the limit, the task must be canceled
    void exceeded_cancel_task(const std::string& cancel_details);

    void exceeded(const std::string& failed_msg);

private:
    // is false: ExecEnv::GetInstance()->initialized() = false when thread local is initialized
    bool _init = false;
    // Cache untracked mem, only update to _untracked_mems when switching mem tracker.
    // Frequent calls to unordered_map _untracked_mems[] in consume will degrade performance.
    int64_t _untracked_mem = 0;
    int64_t old_untracked_mem = 0;
    std::string failed_msg = std::string();

    // _limiter_tracker_stack[0] = orphan_mem_tracker
    std::vector<std::shared_ptr<MemTrackerLimiter>> _limiter_tracker_stack;
    MemTrackerLimiter* _limiter_tracker_raw = nullptr;
    std::vector<MemTracker*> _consumer_tracker_stack;

    // If true, call memtracker try_consume, otherwise call consume.
    bool _check_limit = false;
    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
    bool _check_attach = true;
    std::vector<std::string> _task_id_stack;
    std::vector<TUniqueId> _fragment_instance_id_stack;
    ExceedCallBack _cb_func = nullptr;
};

inline void ThreadMemTrackerMgr::init() {
    DCHECK(_limiter_tracker_stack.size() == 0);
    DCHECK(_limiter_tracker_raw == nullptr);
    DCHECK(_consumer_tracker_stack.empty());
    init_impl();
}

inline void ThreadMemTrackerMgr::init_impl() {
    _limiter_tracker_stack.push_back(ExecEnv::GetInstance()->orphan_mem_tracker());
    _limiter_tracker_raw = ExecEnv::GetInstance()->orphan_mem_tracker_raw();
    _task_id_stack.push_back("");
    _fragment_instance_id_stack.push_back(TUniqueId());
    _check_limit = true;
    _init = true;
}

inline void ThreadMemTrackerMgr::clear() {
    flush_untracked_mem<false>();
    std::vector<std::shared_ptr<MemTrackerLimiter>>().swap(_limiter_tracker_stack);
    std::vector<MemTracker*>().swap(_consumer_tracker_stack);
    std::vector<std::string>().swap(_task_id_stack);
    std::vector<TUniqueId>().swap(_fragment_instance_id_stack);
    init_impl();
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
    // After the jemalloc hook is loaded, before ExecEnv init, _limiter_tracker=nullptr.
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume && ExecEnv::GetInstance()->initialized()) {
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
    if (!_init) init();
    DCHECK(_limiter_tracker_raw);
    old_untracked_mem = _untracked_mem;
    if (CheckLimit) {
#ifndef BE_TEST
        // When all threads are started, `attach_limiter_tracker` is expected to be called to bind the limiter tracker.
        // If _check_attach is true and it is not in the brpc server (the protobuf will be operated when bthread is started),
        // it will check whether the tracker label is equal to the default "Process" when flushing.
        // If you do not want this check, set_check_attach=true
        // TODO(zxy) The current p0 test cannot guarantee that all threads are checked,
        // so disable it and try to open it when memory tracking is not on time.
        // DCHECK(!_check_attach || btls_key != EMPTY_BTLS_KEY ||
        //        _limiter_tracker_raw->label() != "Process");
#endif
        if (!_limiter_tracker_raw->try_consume(old_untracked_mem, failed_msg)) {
            // The memory has been allocated, so when TryConsume fails, need to continue to complete
            // the consume to ensure the accuracy of the statistics.
            _limiter_tracker_raw->consume(old_untracked_mem);
            exceeded(failed_msg);
        }
    } else {
        _limiter_tracker_raw->consume(old_untracked_mem);
    }
    for (auto tracker : _consumer_tracker_stack) {
        tracker->consume(old_untracked_mem);
    }
    _untracked_mem -= old_untracked_mem;
    _stop_consume = false;
}

} // namespace doris
