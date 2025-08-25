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

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/be_mock_util.h"
#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/stack_util.h"

namespace doris {
#include "common/compile_check_begin.h"

constexpr size_t SYNC_PROC_RESERVED_INTERVAL_BYTES = (1ULL << 20); // 1M
static std::string MEMORY_ORPHAN_CHECK_MSG =
        "The ThreadContext of the current thread not attach a valid MemoryTracker. after the "
        "thread is started, the ResourceContext in SCOPED_ATTACH_TASK macro should contain a valid "
        "MemoryTracker, or a valid MemoryTracker should be passed in later using "
        "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER macro.";

// Memory Hook is counted in the memory tracker of the current thread.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() = default;

    MOCK_FUNCTION ~ThreadMemTrackerMgr() {
        // if _init == false, exec env is not initialized when init(). and never consumed mem tracker once.
        if (_init) {
            DCHECK(_reserved_mem == 0);
            flush_untracked_mem();
        }
    }

    bool init();

    // After attach, the current thread Memory Hook starts to consume/release task mem_tracker
    void attach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& mem_tracker);
    void attach_limiter_tracker(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                                const std::weak_ptr<WorkloadGroup>& wg_wptr);
    void detach_limiter_tracker();

    // Must be fast enough! Thread update_tracker may be called very frequently.
    bool push_consumer_tracker(MemTracker* mem_tracker);
    void pop_consumer_tracker();
    std::string last_consumer_tracker_label() {
        return _consumer_tracker_stack.empty() ? "" : _consumer_tracker_stack.back()->label();
    }

    // Note that, If call the memory allocation operation in Memory Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    // Returns whether the memory exceeds limit, and will consume mem trcker no matter whether the limit is exceeded.
    void consume(int64_t size);
    void flush_untracked_mem();

    enum class TryReserveChecker {
        NONE = 0,
        CHECK_TASK = 1,
        CHECK_WORKLOAD_GROUP = 2,
        CHECK_TASK_AND_WORKLOAD_GROUP = 3,
        CHECK_PROCESS = 4,
        CHECK_TASK_AND_PROCESS = 5,
        CHECK_WORKLOAD_GROUP_AND_PROCESS = 6,
        CHECK_TASK_AND_WORKLOAD_GROUP_AND_PROCESS = 7,
    };

    // if only_check_process_memory == true, still reserve query, wg, process memory, only check process memory.
    MOCK_FUNCTION doris::Status try_reserve(
            int64_t size, TryReserveChecker checker =
                                  TryReserveChecker::CHECK_TASK_AND_WORKLOAD_GROUP_AND_PROCESS);

    void shrink_reserved();

    MemTrackerLimiter* limiter_mem_tracker() {
        CHECK(init());
        return _limiter_tracker;
    }

    // Prefer use `limiter_mem_tracker`, which is faster than `limiter_mem_tracker_sptr`.
    // when multiple threads hold the same `std::shared_ptr` at the same time,
    // modifying the `std::shared_ptr` reference count will be expensive when there is high concurrency.
    std::shared_ptr<MemTrackerLimiter> limiter_mem_tracker_sptr() {
        CHECK(init());
        return _limiter_tracker_sptr;
    }

    void enable_wait_gc() { _wait_gc = true; }
    void disable_wait_gc() { _wait_gc = false; }
    [[nodiscard]] bool wait_gc() const { return _wait_gc; }

    std::string print_debug_string() {
        fmt::memory_buffer consumer_tracker_buf;
        for (const auto& v : _consumer_tracker_stack) {
            fmt::format_to(consumer_tracker_buf, "{}, ", v->log_usage());
        }
        return fmt::format(
                "ThreadMemTrackerMgr debug, _untracked_mem:{}, "
                "_limiter_tracker:<{}>, _consumer_tracker_stack:<{}>",
                std::to_string(_untracked_mem), _limiter_tracker->make_profile_str(),
                fmt::to_string(consumer_tracker_buf));
    }

    int64_t untracked_mem() const { return _untracked_mem; }
    int64_t reserved_mem() const { return _reserved_mem; }

    int skip_memory_check = 0;
    int skip_large_memory_check = 0;

    void memory_orphan_check() {
#ifndef BE_TEST
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check ||
               limiter_mem_tracker()->label() != "Orphan")
                << doris::MEMORY_ORPHAN_CHECK_MSG;
#endif
    }

private:
    struct LastAttachSnapshot {
        std::shared_ptr<MemTrackerLimiter> limiter_tracker {nullptr};
        std::weak_ptr<WorkloadGroup> wg_wptr;
        int64_t reserved_mem = 0;
        std::vector<MemTracker*> consumer_tracker_stack;
    };

    // is false: ExecEnv::ready() = false when thread local is initialized
    bool _init = false;
    // Cache untracked mem.
    int64_t _untracked_mem = 0;
    int64_t _old_untracked_mem = 0;

    int64_t _reserved_mem = 0;

    // SCOPED_ATTACH_TASK cannot be nested, but SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER can continue to be used,
    // so `attach_limiter_tracker` may be nested.
    std::vector<LastAttachSnapshot> _last_attach_snapshots_stack;

    std::string _failed_consume_msg;
    // If true, the Allocator will wait for the GC to free memory if it finds that the memory exceed limit.
    // A thread of query/load will only wait once during execution.
    bool _wait_gc = false;

    std::shared_ptr<MemTrackerLimiter> _limiter_tracker_sptr {nullptr};
    MemTrackerLimiter* _limiter_tracker {nullptr};
    std::vector<MemTracker*> _consumer_tracker_stack;
    std::weak_ptr<WorkloadGroup> _wg_wptr;

    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
};

inline bool ThreadMemTrackerMgr::init() {
    // 1. Initialize in the thread context when the thread starts
    // 2. ExecEnv not initialized when thread start, initialized in mem_tracker().
    if (_init) {
        return true;
    }
    if (ExecEnv::GetInstance()->orphan_mem_tracker() != nullptr) {
        _limiter_tracker_sptr = ExecEnv::GetInstance()->orphan_mem_tracker();
        _limiter_tracker = _limiter_tracker_sptr.get();
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
    return true;
}

inline void ThreadMemTrackerMgr::pop_consumer_tracker() {
    DCHECK(!_consumer_tracker_stack.empty());
    _consumer_tracker_stack.pop_back();
}

inline void ThreadMemTrackerMgr::consume(int64_t size) {
    memory_orphan_check();
    // `consumer_tracker` not support reserve memory and not require use `_untracked_mem` to batch consume,
    // because `consumer_tracker` will not be bound by many threads, so there is no performance problem.
    for (auto* tracker : _consumer_tracker_stack) {
        tracker->consume(size);
    }

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
                doris::GlobalMemoryArbitrator::shrink_process_reserved(_untracked_mem);
                _limiter_tracker->shrink_reserved(_untracked_mem);
                _untracked_mem = 0;
            }
            return;
        } else {
            // _reserved_mem <= size, reserved memory used done,
            // the remaining _reserved_mem is subtracted from this memory consumed,
            // and reset _reserved_mem to 0, and subtract the remaining _reserved_mem from
            // process global reserved memory, this means that all reserved memory has been used by BE process.
            size -= _reserved_mem;
            doris::GlobalMemoryArbitrator::shrink_process_reserved(_reserved_mem + _untracked_mem);
            _limiter_tracker->shrink_reserved(_reserved_mem + _untracked_mem);
            _reserved_mem = 0;
            _untracked_mem = 0;
        }
    }
    // store bytes that not consumed by thread mem tracker.
    _untracked_mem += size;
    DCHECK(_reserved_mem == 0);

    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    // After the jemalloc hook is loaded, before ExecEnv init, _limiter_tracker=nullptr.
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes && !_stop_consume) {
        if (!_init && !ExecEnv::ready()) {
            return;
        }
        flush_untracked_mem();
        // If size is large, then _untracked_mem must be larger than config::mem_tracker_consume_min_size_bytes.
        if (skip_large_memory_check == 0) {
            if (doris::config::stacktrace_in_alloc_large_memory_bytes > 0 &&
                size > doris::config::stacktrace_in_alloc_large_memory_bytes) {
                _stop_consume = true;
                LOG(WARNING) << fmt::format(
                        "alloc large memory: {}, consume tracker: {}, this is just a warning, not "
                        "prevent memory alloc, "
                        "stacktrace:\n{}",
                        size, _limiter_tracker->label(), get_stack_trace());
                _stop_consume = false;
            }
            if (doris::config::crash_in_alloc_large_memory_bytes > 0 &&
                size > doris::config::crash_in_alloc_large_memory_bytes) {
                throw Exception(
                        Status::FatalError("alloc large memory: {}, consume tracker: {}, crash "
                                           "generate core dumpsto help analyze, "
                                           "stacktrace:\n{}",
                                           size, _limiter_tracker->label(), get_stack_trace()));
            }
        }
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
    DCHECK(_limiter_tracker);

    _old_untracked_mem = _untracked_mem;
    _limiter_tracker->consume(_old_untracked_mem);
    _untracked_mem -= _old_untracked_mem;
    _stop_consume = false;
}

inline doris::Status ThreadMemTrackerMgr::try_reserve(int64_t size, TryReserveChecker checker) {
    DCHECK(size >= 0);
    CHECK(init());
    DCHECK(_limiter_tracker);
    memory_orphan_check();
    // if _reserved_mem not equal to 0, repeat reserve,
    // _untracked_mem store bytes that not synchronized to process reserved memory.
    flush_untracked_mem();
    auto wg_ptr = _wg_wptr.lock();

    bool task_limit_checker = static_cast<int>(checker) & 1;
    bool workload_group_limit_checker = static_cast<int>(checker) & 2;
    bool process_limit_checker = static_cast<int>(checker) & 4;

    if (task_limit_checker) {
        if (!_limiter_tracker->try_reserve(size)) {
            auto err_msg = fmt::format(
                    "reserve memory failed, size: {}, because query memory exceeded, memory "
                    "tracker: {}, "
                    "consumption: {}, limit: {}, peak: {}",
                    PrettyPrinter::print_bytes(size), _limiter_tracker->label(),
                    PrettyPrinter::print_bytes(_limiter_tracker->consumption()),
                    PrettyPrinter::print_bytes(_limiter_tracker->limit()),
                    PrettyPrinter::print_bytes(_limiter_tracker->peak_consumption()));
            return doris::Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(err_msg);
        }
    } else {
        _limiter_tracker->reserve(size);
    }

    if (wg_ptr) {
        if (workload_group_limit_checker) {
            if (!wg_ptr->try_add_wg_refresh_interval_memory_growth(size)) {
                auto err_msg = fmt::format(
                        "reserve memory failed, size: {}, because workload group memory exceeded, "
                        "workload group: {}",
                        PrettyPrinter::print_bytes(size), wg_ptr->memory_debug_string());
                _limiter_tracker->release(size);         // rollback
                _limiter_tracker->shrink_reserved(size); // rollback
                return doris::Status::Error<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>(err_msg);
            }
        } else {
            wg_ptr->add_wg_refresh_interval_memory_growth(size);
        }
    }

    if (process_limit_checker) {
        if (!doris::GlobalMemoryArbitrator::try_reserve_process_memory(size)) {
            auto err_msg = fmt::format(
                    "reserve memory failed, size: {}, because proccess memory exceeded, {}",
                    PrettyPrinter::print_bytes(size),
                    GlobalMemoryArbitrator::process_mem_log_str());
            _limiter_tracker->release(size);         // rollback
            _limiter_tracker->shrink_reserved(size); // rollback
            if (wg_ptr) {
                wg_ptr->sub_wg_refresh_interval_memory_growth(size); // rollback
            }
            return doris::Status::Error<ErrorCode::PROCESS_MEMORY_EXCEEDED>(err_msg);
        }
    } else {
        doris::GlobalMemoryArbitrator::reserve_process_memory(size);
    }

    _reserved_mem += size;
    DCHECK(_reserved_mem >= 0);
    return doris::Status::OK();
}

inline void ThreadMemTrackerMgr::shrink_reserved() {
    if (_reserved_mem != 0) {
        memory_orphan_check();
        doris::GlobalMemoryArbitrator::shrink_process_reserved(_reserved_mem + _untracked_mem);
        _limiter_tracker->shrink_reserved(_reserved_mem + _untracked_mem);
        _limiter_tracker->release(_reserved_mem);
        auto wg_ptr = _wg_wptr.lock();
        if (wg_ptr) {
            wg_ptr->sub_wg_refresh_interval_memory_growth(_reserved_mem);
        }
        _untracked_mem = 0;
        _reserved_mem = 0;
    }
}

#include "common/compile_check_end.h"
} // namespace doris
