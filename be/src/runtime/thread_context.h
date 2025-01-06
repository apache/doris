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

#include <memory>
#include <string>

#include "common/logging.h"
#include "runtime/thread_context_impl.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "util/defer_op.h" // IWYU pragma: keep

// Used to tracking query/load/compaction/e.g. execution thread memory usage.
// This series of methods saves some information to the thread local context of the current worker thread,
// including MemTracker, QueryID, etc. Use CONSUME_THREAD_MEM_TRACKER/RELEASE_THREAD_MEM_TRACKER in the code segment where
// the macro is located to record the memory into MemTracker.
// Not use it in rpc done.run(), because bthread_setspecific may have errors when UBSAN compiles.
#if defined(USE_MEM_TRACKER) && !defined(BE_TEST)
// Attach to query/load/compaction/e.g. when thread starts.
// This will save some info about a working thread in the thread context.
// Looking forward to tracking memory during thread execution into MemTrackerLimiter.
#define SCOPED_ATTACH_TASK(arg1) auto VARNAME_LINENUM(attach_task) = AttachTask(arg1)

// Switch MemTrackerLimiter for count memory during thread execution.
// Used after SCOPED_ATTACH_TASK, in order to count the memory into another
// MemTrackerLimiter instead of the MemTrackerLimiter added by the attach task.
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(arg1) \
    auto VARNAME_LINENUM(switch_mem_tracker) = doris::SwitchThreadMemTrackerLimiter(arg1)

// Looking forward to tracking memory during thread execution into MemTracker.
// Usually used to record query more detailed memory, including ExecNode operators.
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumer(mem_tracker)

#define DEFER_RELEASE_RESERVED() \
    Defer VARNAME_LINENUM(defer) {[&]() { doris::thread_context()->thread_mem_tracker_mgr->release_reserved(); }};
#else
// thread context need to be initialized, required by Allocator and elsewhere.
#define SCOPED_ATTACH_TASK(arg1, ...) \
    auto VARNAME_LINENUM(scoped_tls_at) = doris::ScopedInitThreadContext()
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(arg1) \
    auto VARNAME_LINENUM(scoped_tls_stmtl) = doris::ScopedInitThreadContext()
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(scoped_tls_cmt) = doris::ScopedInitThreadContext()
#define DEFER_RELEASE_RESERVED() (void)0
#endif

#if defined(USE_MEM_TRACKER) && !defined(BE_TEST)
// Count a code segment memory
// Usage example:
//      int64_t peak_mem = 0;
//      {
//          SCOPED_PEAK_MEM(&peak_mem);
//          xxxx
//      }
//      LOG(INFO) << *peak_mem;
#define SCOPED_PEAK_MEM(peak_mem) \
    auto VARNAME_LINENUM(scope_peak_mem) = doris::ScopedPeakMem(peak_mem)

// Count a code segment memory (memory malloc - memory free) to MemTracker.
// Compared to count `scope_mem`, MemTracker is easier to observe from the outside and is thread-safe.
// Usage example: std::unique_ptr<MemTracker> tracker = std::make_unique<MemTracker>("first_tracker");
//                { SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(_mem_tracker.get()); xxx; xxx; }
#define SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumerByHook(mem_tracker)
#else
#define SCOPED_PEAK_MEM() auto VARNAME_LINENUM(scoped_tls_pm) = doris::ScopedInitThreadContext()
#define SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(mem_tracker) \
    auto VARNAME_LINENUM(scoped_tls_cmtbh) = doris::ScopedInitThreadContext()
#endif

#define SCOPED_SKIP_MEMORY_CHECK() \
    auto VARNAME_LINENUM(scope_skip_memory_check) = doris::ScopeSkipMemoryCheck()

#define SKIP_LARGE_MEMORY_CHECK(...)                                       \
    do {                                                                   \
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();      \
        doris::thread_context()->thread_mem_tracker_mgr->skip_large_memory_check++;                \
        DEFER({                                                            \
            doris::thread_context()->thread_mem_tracker_mgr->skip_large_memory_check--;            \
            doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); \
        });                                                                \
        __VA_ARGS__;                                                       \
    } while (0)

#define LIMIT_LOCAL_SCAN_IO(data_dir, bytes_read)                   \
    std::shared_ptr<IOThrottle> iot = nullptr;                      \
    auto* t_ctx = doris::thread_context(true);                      \
    if (t_ctx) {                                                    \
        iot = t_ctx->resource_ctx->workload_group_context()->workload_group()->get_local_scan_io_throttle(data_dir); \
    }                                                               \
    if (iot) {                                                      \
        iot->acquire(-1);                                           \
    }                                                               \
    Defer defer {                                                   \
        [&]() {                                                     \
            if (iot) {                                              \
                iot->update_next_io_time(*bytes_read);              \
                    iot = t_ctx->resource_ctx->workload_group_context()->workload_group()->update_local_scan_io(data_dir, *bytes_read); \
            }                                                       \
        }                                                           \
    }

#define LIMIT_REMOTE_SCAN_IO(bytes_read)                   \
    std::shared_ptr<IOThrottle> iot = nullptr;             \
    auto* t_ctx = doris::thread_context(true);             \
    if (t_ctx) {                                           \
            iot = t_ctx->resource_ctx->workload_group_context()->workload_group()->get_remote_scan_io_throttle(data_dir); \
    }                                                      \
    if (iot) {                                             \
        iot->acquire(-1);                                  \
    }                                                      \
    Defer defer {                                          \
        [&]() {                                            \
            if (iot) {                                     \
                iot->update_next_io_time(*bytes_read);     \
                    iot = t_ctx->resource_ctx->workload_group_context()->workload_group()->update_remote_scan_io(*bytes_read); \
            }                                              \
        }                                                  \
    }

namespace doris {

class MemTracker;
class RuntimeState;
class QueryThreadContext;
class WorkloadGroup;

// use mem hook to consume thread mem tracker.
inline thread_local bool use_mem_hook = false;

class ScopedPeakMem {
public:
    explicit ScopedPeakMem(int64* peak_mem) : _peak_mem(peak_mem), _mem_tracker("ScopedPeakMem") {
        ThreadLocalHandle::create_thread_local_if_not_exits();
        thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(&_mem_tracker);
    }

    ~ScopedPeakMem() {
        thread_context()->thread_mem_tracker_mgr->pop_consumer_tracker();
        *_peak_mem += _mem_tracker.peak_consumption();
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }

private:
    int64* _peak_mem;
    MemTracker _mem_tracker;
};

// only hold thread context in scope.
class ScopedInitThreadContext {
public:
    explicit ScopedInitThreadContext() { ThreadLocalHandle::create_thread_local_if_not_exits(); }

    ~ScopedInitThreadContext() { ThreadLocalHandle::del_thread_local_if_count_is_zero(); }
};

class AttachTask {
public:
    explicit AttachTask(const std::shared_ptr<ResourceContext>& rc);

    ~AttachTask();
};

class SwitchThreadMemTrackerLimiter {
public:
    explicit SwitchThreadMemTrackerLimiter(const std::shared_ptr<doris::MemTrackerLimiter>& mem_tracker);
    explicit SwitchThreadMemTrackerLimiter(ResourceContext* rc);

    ~SwitchThreadMemTrackerLimiter();

private:
    std::shared_ptr<doris::MemTrackerLimiter> _old_mem_tracker {nullptr};
};

class AddThreadMemTrackerConsumer {
public:
    // The owner and user of MemTracker are in the same thread, and the raw pointer is faster.
    // If mem_tracker is nullptr, do nothing.
    explicit AddThreadMemTrackerConsumer(MemTracker* mem_tracker);

    // The owner and user of MemTracker are in different threads. If mem_tracker is nullptr, do nothing.
    explicit AddThreadMemTrackerConsumer(const std::shared_ptr<MemTracker>& mem_tracker);

    ~AddThreadMemTrackerConsumer();

private:
    std::shared_ptr<MemTracker> _mem_tracker; // Avoid mem_tracker being released midway.
    bool _need_pop = false;
};

class AddThreadMemTrackerConsumerByHook {
public:
    explicit AddThreadMemTrackerConsumerByHook(const std::shared_ptr<MemTracker>& mem_tracker);
    ~AddThreadMemTrackerConsumerByHook();

private:
    std::shared_ptr<MemTracker> _mem_tracker;
};

class ScopeSkipMemoryCheck {
public:
    explicit ScopeSkipMemoryCheck() {
        ThreadLocalHandle::create_thread_local_if_not_exits();
        doris::thread_context()->thread_mem_tracker_mgr->skip_memory_check++;
    }

    ~ScopeSkipMemoryCheck() {
        doris::thread_context()->thread_mem_tracker_mgr->skip_memory_check--;
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }
};

// Basic macros for mem tracker, usually do not need to be modified and used.
#if defined(USE_MEM_TRACKER) && !defined(BE_TEST)
// used to fix the tracking accuracy of caches.
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker)                                        \
    do {                                                                                     \
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check || \
           doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label() != "Orphan")  \
            << doris::memory_orphan_check_msg;                                                              \
        doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->transfer_to( \
                size, tracker);                                                              \
    } while (0)

#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker)                                        \
    do {                                                                                       \
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check || \
           doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label() != "Orphan")  \
            << doris::memory_orphan_check_msg;                                                                \
        tracker->transfer_to(                                                                  \
                size, doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()); \
    } while (0)

// Mem Hook to consume thread mem tracker
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size)           \
    do {                                                   \
        if (doris::use_mem_hook) {                         \
            doris::thread_context()->thread_mem_tracker_mgr->consume(size); \
        }                                                  \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK(size) CONSUME_THREAD_MEM_TRACKER_BY_HOOK(-size)
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)           \
    do {                                                                   \
        if (doris::use_mem_hook) {                                         \
            doris::thread_context()->thread_mem_tracker_mgr->consume(size_fn(__VA_ARGS__)); \
        }                                                                  \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)            \
    do {                                                                    \
        if (doris::use_mem_hook) {                                          \
            doris::thread_context()->thread_mem_tracker_mgr->consume(-size_fn(__VA_ARGS__)); \
        }                                                                   \
    } while (0)

// if use mem hook, avoid repeated consume.
// must call create_thread_local_if_not_exits() before use thread_context().
#define CONSUME_THREAD_MEM_TRACKER(size)                                                       \
    do {                                                                                       \
        if (size == 0 || doris::use_mem_hook) {                                                \
            break;                                                                             \
        }                                                                                      \
        if (doris::pthread_context_ptr_init) {                                                 \
            DCHECK(bthread_self() == 0);                                                       \
            doris::thread_context_ptr->thread_mem_tracker_mgr->consume(size);                                   \
        } else if (bthread_self() != 0) {                                                      \
            static_cast<doris::ThreadContext*>(bthread_getspecific(doris::btls_key))           \
                    ->thread_mem_tracker_mgr->consume(size);                                                    \
        } else if (doris::ExecEnv::ready()) {                                                  \
            DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check)          \
                << doris::memory_orphan_check_msg;                                             \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker()->consume_no_update_peak(size); \
        }                                                                                      \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER(size) CONSUME_THREAD_MEM_TRACKER(-size)

#else
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) (void)0
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK(size) (void)0
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...) (void)0
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...) (void)0
#define CONSUME_THREAD_MEM_TRACKER(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER(size) (void)0
#endif
} // namespace doris
