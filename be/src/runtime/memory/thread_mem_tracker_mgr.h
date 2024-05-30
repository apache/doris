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
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

namespace doris {

constexpr size_t SYNC_PROC_RESERVED_INTERVAL_BYTES = (1ULL << 20); // 1M

// Memory Hook is counted in the memory tracker of the current thread.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() = default;

    ~ThreadMemTrackerMgr() {
        // if _init == false, exec env is not initialized when init(). and never consumed mem tracker once.
        if (_init) {
            DCHECK(_reserved_mem == 0);
            flush_untracked_mem();
        }
    }

    bool init();

    // After attach, the current thread Memory Hook starts to consume/release task mem_tracker
    void attach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& mem_tracker);
    void detach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& old_mem_tracker =
                                        ExecEnv::GetInstance()->orphan_mem_tracker());

    // Must be fast enough! Thread update_tracker may be called very frequently.
    bool push_consumer_tracker(MemTracker* mem_tracker);
    void pop_consumer_tracker();
    std::string last_consumer_tracker() {
        return _consumer_tracker_stack.empty() ? "" : _consumer_tracker_stack.back()->label();
    }

    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }

    TUniqueId query_id() { return _query_id; }

    void start_count_scope_mem() {
        CHECK(init());
        _scope_mem = _reserved_mem; // consume in advance
        _count_scope_mem = true;
    }

    int64_t stop_count_scope_mem() {
        flush_untracked_mem();
        _count_scope_mem = false;
        return _scope_mem - _reserved_mem;
    }

    // Note that, If call the memory allocation operation in Memory Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    // Returns whether the memory exceeds limit, and will consume mem trcker no matter whether the limit is exceeded.
    void consume(int64_t size, int skip_large_memory_check = 0);
    void flush_untracked_mem();

    bool try_reserve(int64_t size);
    void release_reserved();

    bool is_attach_query() { return _query_id != TUniqueId(); }

    bool is_query_cancelled() const { return _is_query_cancelled; }

    void reset_query_cancelled_flag(bool new_val) { _is_query_cancelled = new_val; }

    std::shared_ptr<MemTrackerLimiter> limiter_mem_tracker() {
        CHECK(init());
        return _limiter_tracker;
    }
    MemTrackerLimiter* limiter_mem_tracker_raw() {
        CHECK(init());
        return _limiter_tracker_raw;
    }

    void enable_wait_gc() { _wait_gc = true; }
    void disable_wait_gc() { _wait_gc = false; }
    [[nodiscard]] bool wait_gc() const { return _wait_gc; }
    void cancel_query(const std::string& exceed_msg);

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

    int64_t untracked_mem() const { return _untracked_mem; }
    int64_t reserved_mem() const { return _reserved_mem; }

private:
    // is false: ExecEnv::ready() = false when thread local is initialized
    bool _init = false;
    // Cache untracked mem.
    int64_t _untracked_mem = 0;
    int64_t _old_untracked_mem = 0;

    int64_t _reserved_mem = 0;
    // SCOPED_ATTACH_TASK cannot be nested, but SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER can continue to be used,
    // so `attach_limiter_tracker` may be nested.
    std::vector<int64_t> _reserved_mem_stack;

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
    TUniqueId _query_id = TUniqueId();
    bool _is_query_cancelled = false;
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
    tracker->consume(_reserved_mem); // consume in advance
    return true;
}

inline void ThreadMemTrackerMgr::pop_consumer_tracker() {
    DCHECK(!_consumer_tracker_stack.empty());
    flush_untracked_mem();
    _consumer_tracker_stack.back()->consume(_untracked_mem);
    _consumer_tracker_stack.back()->release(_reserved_mem);
    _consumer_tracker_stack.pop_back();
}

inline void ThreadMemTrackerMgr::consume(int64_t size, int skip_large_memory_check) {
    if (_reserved_mem != 0) {
        if (_reserved_mem > size) {
            // only need to subtract _reserved_mem, no need to consume MemTracker,
            // every time _reserved_mem is minus the sum of size >= SYNC_PROC_RESERVED_INTERVAL_BYTES,
            // subtract size from process global reserved memory,
            // because this part of the reserved memory has already been used by BE process.
            _reserved_mem -= size;
            // temporary store bytes that not synchronized to process reserved memory.
            _untracked_mem += size;
            // If _untracked_mem > 0, reserved memory that has been used, if _untracked_mem greater than
            // SYNC_PROC_RESERVED_INTERVAL_BYTES, release process reserved memory.
            // If _untracked_mem < 0, used reserved memory is returned, will increase reserved memory,
            // if _untracked_mem less than -SYNC_PROC_RESERVED_INTERVAL_BYTES, increase process reserved memory.
            if (std::abs(_untracked_mem) >= SYNC_PROC_RESERVED_INTERVAL_BYTES) {
                doris::GlobalMemoryArbitrator::release_process_reserved_memory(_untracked_mem);
                _untracked_mem = 0;
            }
            return;
        } else {
            // _reserved_mem <= size, reserved memory used done,
            // the remaining _reserved_mem is subtracted from this memory consumed,
            // and reset _reserved_mem to 0, and subtract the remaining _reserved_mem from
            // process global reserved memory, this means that all reserved memory has been used by BE process.
            size -= _reserved_mem;
            doris::GlobalMemoryArbitrator::release_process_reserved_memory(_reserved_mem +
                                                                           _untracked_mem);
            _reserved_mem = 0;
            _untracked_mem = 0;
        }
    }
    // store bytes that not consumed by thread mem tracker.
    _untracked_mem += size;
    DCHECK(_reserved_mem == 0);
    if (!_init && !ExecEnv::ready()) {
        return;
    }
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    // After the jemalloc hook is loaded, before ExecEnv init, _limiter_tracker=nullptr.
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes && !_stop_consume) {
        flush_untracked_mem();
    }

    if (skip_large_memory_check == 0 && doris::config::large_memory_check_bytes > 0 &&
        size > doris::config::large_memory_check_bytes) {
        _stop_consume = true;
        LOG(WARNING) << fmt::format(
                "malloc or new large memory: {}, {}, this is just a warning, not prevent memory "
                "alloc, stacktrace:\n{}",
                size,
                is_attach_query() ? "in query or load: " + print_id(_query_id)
                                  : "not in query or load",
                get_stack_trace());
        _stop_consume = false;
    }
}

inline void ThreadMemTrackerMgr::flush_untracked_mem() {
    // if during reserve memory, _untracked_mem temporary store bytes that not synchronized
    // to process reserved memory, but bytes have been subtracted from thread _reserved_mem.
    // so not need flush untracked_mem to consume mem tracker.
    if (_reserved_mem != 0) {
        return;
    }
    // Temporary memory may be allocated during the consumption of the mem tracker, which will lead to entering
    // the Memory Hook again, so suspend consumption to avoid falling into an infinite loop.
    if (_untracked_mem == 0 || !init()) {
        return;
    }
    _stop_consume = true;
    DCHECK(_limiter_tracker_raw);

    _old_untracked_mem = _untracked_mem;
    if (_count_scope_mem) {
        _scope_mem += _untracked_mem;
    }
    _limiter_tracker_raw->consume(_old_untracked_mem);
    for (auto* tracker : _consumer_tracker_stack) {
        tracker->consume(_old_untracked_mem);
    }
    _untracked_mem -= _old_untracked_mem;
    _stop_consume = false;
}

inline bool ThreadMemTrackerMgr::try_reserve(int64_t size) {
    DCHECK(_limiter_tracker_raw);
    DCHECK(size >= 0);
    CHECK(init());
    // if _reserved_mem not equal to 0, repeat reserve,
    // _untracked_mem store bytes that not synchronized to process reserved memory.
    flush_untracked_mem();
    if (!_limiter_tracker_raw->try_consume(size)) {
        return false;
    }
    if (!doris::GlobalMemoryArbitrator::try_reserve_process_memory(size)) {
        _limiter_tracker_raw->release(size); // rollback
        return false;
    }
    if (_count_scope_mem) {
        _scope_mem += size;
    }
    for (auto* tracker : _consumer_tracker_stack) {
        tracker->consume(size);
    }
    _reserved_mem += size;
    return true;
}

inline void ThreadMemTrackerMgr::release_reserved() {
    if (_reserved_mem != 0) {
        doris::GlobalMemoryArbitrator::release_process_reserved_memory(_reserved_mem +
                                                                       _untracked_mem);
        _limiter_tracker_raw->release(_reserved_mem);
        if (_count_scope_mem) {
            _scope_mem -= _reserved_mem;
        }
        for (auto* tracker : _consumer_tracker_stack) {
            tracker->release(_reserved_mem);
        }
        _untracked_mem = 0;
        _reserved_mem = 0;
    }
}

} // namespace doris
