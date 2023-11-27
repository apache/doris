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
#include <bthread/types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <thread>

#include "common/exception.h"
#include "common/logging.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "util/defer_op.h" // IWYU pragma: keep

// Used to tracking query/load/compaction/e.g. execution thread memory usage.
// This series of methods saves some information to the thread local context of the current worker thread,
// including MemTracker, QueryID, etc. Use CONSUME_THREAD_MEM_TRACKER/RELEASE_THREAD_MEM_TRACKER in the code segment where
// the macro is located to record the memory into MemTracker.
// Not use it in rpc done.run(), because bthread_setspecific may have errors when UBSAN compiles.
#if defined(USE_MEM_TRACKER) && !defined(UNDEFINED_BEHAVIOR_SANITIZER) && !defined(BE_TEST)
// Attach to query/load/compaction/e.g. when thread starts.
// This will save some info about a working thread in the thread context.
// Looking forward to tracking memory during thread execution into MemTrackerLimiter.
#define SCOPED_ATTACH_TASK(arg1) auto VARNAME_LINENUM(attach_task) = AttachTask(arg1)
#define SCOPED_ATTACH_TASK_WITH_ID(arg1, arg2, arg3) \
    auto VARNAME_LINENUM(attach_task) = AttachTask(arg1, arg2, arg3)

// Switch MemTrackerLimiter for count memory during thread execution.
// Used after SCOPED_ATTACH_TASK, in order to count the memory into another
// MemTrackerLimiter instead of the MemTrackerLimiter added by the attach task.
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) \
    auto VARNAME_LINENUM(switch_mem_tracker) = SwitchThreadMemTrackerLimiter(mem_tracker_limiter)

// Looking forward to tracking memory during thread execution into MemTracker.
// Usually used to record query more detailed memory, including ExecNode operators.
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumer(mem_tracker)
#else
#define SCOPED_ATTACH_TASK(arg1, ...) (void)0
#define SCOPED_ATTACH_TASK_WITH_ID(arg1, arg2, arg3) (void)0
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) (void)0
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) (void)0
#endif

// Used to tracking the memory usage of the specified code segment use by mem hook.
#if defined(USE_MEM_TRACKER)
// Count a code segment memory (memory malloc - memory free) to int64_t
// Usage example: int64_t scope_mem = 0; { SCOPED_MEM_COUNT(&scope_mem); xxx; xxx; }
#define SCOPED_MEM_COUNT_BY_HOOK(scope_mem) \
    auto VARNAME_LINENUM(scope_mem_count) = doris::ScopeMemCountByHook(scope_mem)

// Count a code segment memory (memory malloc - memory free) to MemTracker.
// Compared to count `scope_mem`, MemTracker is easier to observe from the outside and is thread-safe.
// Usage example: std::unique_ptr<MemTracker> tracker = std::make_unique<MemTracker>("first_tracker");
//                { SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(_mem_tracker.get()); xxx; xxx; }
#define SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumerByHook(mem_tracker)
#else
#define SCOPED_MEM_COUNT_BY_HOOK(scope_mem) (void)0
#define SCOPED_CONSUME_MEM_TRACKER_BY_HOOK(mem_tracker) (void)0
#endif

#define SKIP_MEMORY_CHECK(...)                                             \
    do {                                                                   \
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();      \
        doris::thread_context()->skip_memory_check++;                      \
        DEFER({                                                            \
            doris::thread_context()->skip_memory_check--;                  \
            doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); \
        });                                                                \
        __VA_ARGS__;                                                       \
    } while (0)

#define SKIP_LARGE_MEMORY_CHECK(...)                                       \
    do {                                                                   \
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();      \
        doris::thread_context()->skip_large_memory_check++;                \
        DEFER({                                                            \
            doris::thread_context()->skip_large_memory_check--;            \
            doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); \
        });                                                                \
        __VA_ARGS__;                                                       \
    } while (0)

namespace doris {

class ThreadContext;
class MemTracker;
class RuntimeState;

extern bthread_key_t btls_key;

// Is true after ThreadContext construction.
inline thread_local bool pthread_context_ptr_init = false;
inline thread_local constinit ThreadContext* thread_context_ptr = nullptr;
// use mem hook to consume thread mem tracker.
inline thread_local bool use_mem_hook = false;

// The thread context saves some info about a working thread.
// 2 required info:
//   1. thread_id:   Current thread id, Auto generated.
//   2. type(abolished):        The type is a enum value indicating which type of task current thread is running.
//                   For example: QUERY, LOAD, COMPACTION, ...
//   3. task id:     A unique id to identify this task. maybe query id, load job id, etc.
//   4. ThreadMemTrackerMgr
//
// There may be other optional info to be added later.
class ThreadContext {
public:
    ThreadContext() { thread_mem_tracker_mgr = std::make_unique<ThreadMemTrackerMgr>(); }

    ~ThreadContext() = default;

    void attach_task(const TUniqueId& task_id, const TUniqueId& fragment_instance_id,
                     const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
#ifndef BE_TEST
        // will only attach_task at the beginning of the thread function, there should be no duplicate attach_task.
        DCHECK(mem_tracker);
        // Orphan is thread default tracker.
        DCHECK(thread_mem_tracker()->label() == "Orphan")
                << ", attach mem tracker label: " << mem_tracker->label();
#endif
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, fragment_instance_id);
    }

    void detach_task() {
        _task_id = TUniqueId();
        _fragment_instance_id = TUniqueId();
        thread_mem_tracker_mgr->detach_limiter_tracker();
    }

    [[nodiscard]] const TUniqueId& task_id() const { return _task_id; }
    [[nodiscard]] const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    static std::string get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }

    // After thread_mem_tracker_mgr is initialized, the current thread Hook starts to
    // consume/release mem_tracker.
    // Note that the use of shared_ptr will cause a crash. The guess is that there is an
    // intermediate state during the copy construction of shared_ptr. Shared_ptr is not equal
    // to nullptr, but the object it points to is not initialized. At this time, when the memory
    // is released somewhere, the hook is triggered to cause the crash.
    std::unique_ptr<ThreadMemTrackerMgr> thread_mem_tracker_mgr;
    [[nodiscard]] MemTrackerLimiter* thread_mem_tracker() const {
        return thread_mem_tracker_mgr->limiter_mem_tracker_raw();
    }

    void consume_memory(const int64_t size) const {
        thread_mem_tracker_mgr->consume(size, skip_large_memory_check);
    }

    int thread_local_handle_count = 0;
    int skip_memory_check = 0;
    int skip_large_memory_check = 0;

private:
    TUniqueId _task_id;
    TUniqueId _fragment_instance_id;
};

class ThreadLocalHandle {
public:
    static void create_thread_local_if_not_exits() {
        if (bthread_self() == 0) {
            if (!pthread_context_ptr_init) {
                thread_context_ptr = new ThreadContext();
                pthread_context_ptr_init = true;
            }
            DCHECK(thread_context_ptr != nullptr);
            thread_context_ptr->thread_local_handle_count++;
        } else {
            // Avoid calling bthread_getspecific frequently to get bthread local.
            // Very frequent bthread_getspecific will slow, but create_thread_local_if_not_exits is not expected to be much.
            // Cache the pointer of bthread local in pthead local.
            auto* bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
            if (bthread_context == nullptr) {
                // If bthread_context == nullptr:
                // 1. First call to bthread_getspecific (and before any bthread_setspecific) returns NULL
                // 2. There are not enough reusable btls in btls pool.
                // else if bthread_context != nullptr:
                // 1. A new bthread starts, but get a reuses btls.
                bthread_context = new ThreadContext;
                // The brpc server should respond as quickly as possible.
                bthread_context->thread_mem_tracker_mgr->disable_wait_gc();
                // set the data so that next time bthread_getspecific in the thread returns the data.
                CHECK(0 == bthread_setspecific(btls_key, bthread_context) || k_doris_exit);
            }
            DCHECK(bthread_context != nullptr);
            bthread_context->thread_local_handle_count++;
        }
    }

    // `create_thread_local_if_not_exits` and `del_thread_local_if_count_is_zero` should be used in pairs,
    // `del_thread_local_if_count_is_zero` should only be called if `create_thread_local_if_not_exits` returns true
    static void del_thread_local_if_count_is_zero() {
        if (pthread_context_ptr_init) {
            // in pthread
            thread_context_ptr->thread_local_handle_count--;
            if (thread_context_ptr->thread_local_handle_count == 0) {
                pthread_context_ptr_init = false;
                delete doris::thread_context_ptr;
                thread_context_ptr = nullptr;
            }
        } else if (bthread_self() != 0) {
            // in bthread
            auto* bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
            DCHECK(bthread_context != nullptr);
            bthread_context->thread_local_handle_count--;
        } else {
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    }
};

[[maybe_unused]] static bool is_thread_context_init() {
    if (pthread_context_ptr_init) {
        // in pthread
        DCHECK(bthread_self() == 0);
        DCHECK(thread_context_ptr != nullptr);
        return true;
    }
    if (bthread_self() != 0) {
        // in bthread
        return static_cast<ThreadContext*>(bthread_getspecific(btls_key)) != nullptr;
    }
    return false;
}

// must call create_thread_local_if_not_exits() before use thread_context().
static ThreadContext* thread_context() {
    if (pthread_context_ptr_init) {
        // in pthread
        DCHECK(bthread_self() == 0);
        DCHECK(thread_context_ptr != nullptr);
        return thread_context_ptr;
    }
    if (bthread_self() != 0) {
        // in bthread
        // bthread switching pthread may be very frequent, remember not to use lock or other time-consuming operations.
        auto* bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
        DCHECK(bthread_context != nullptr);
        return bthread_context;
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

class ScopeMemCountByHook {
public:
    explicit ScopeMemCountByHook(int64_t* scope_mem) {
        ThreadLocalHandle::create_thread_local_if_not_exits();
        use_mem_hook = true;
        _scope_mem = scope_mem;
        thread_context()->thread_mem_tracker_mgr->start_count_scope_mem();
    }

    ~ScopeMemCountByHook() {
        *_scope_mem += thread_context()->thread_mem_tracker_mgr->stop_count_scope_mem();
        use_mem_hook = false;
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }

private:
    int64_t* _scope_mem = nullptr;
};

class AttachTask {
public:
    explicit AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                        const TUniqueId& task_id = TUniqueId(),
                        const TUniqueId& fragment_instance_id = TUniqueId());

    explicit AttachTask(RuntimeState* runtime_state);

    ~AttachTask();
};

class SwitchThreadMemTrackerLimiter {
public:
    explicit SwitchThreadMemTrackerLimiter(const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
        ThreadLocalHandle::create_thread_local_if_not_exits();
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, TUniqueId());
    }

    ~SwitchThreadMemTrackerLimiter() {
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker;
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

// Basic macros for mem tracker, usually do not need to be modified and used.
#if defined(USE_MEM_TRACKER) && !defined(BE_TEST)
// used to fix the tracking accuracy of caches.
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker)                                            \
    do {                                                                                         \
        if (is_thread_context_init()) {                                                          \
            doris::thread_context()                                                              \
                    ->thread_mem_tracker_mgr->limiter_mem_tracker_raw()                          \
                    ->transfer_to(size, tracker);                                                \
        } else {                                                                                 \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->transfer_to(size, tracker); \
        }                                                                                        \
    } while (0)

#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker)                                          \
    do {                                                                                         \
        if (is_thread_context_init()) {                                                          \
            tracker->transfer_to(                                                                \
                    size,                                                                        \
                    doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_raw()); \
        } else {                                                                                 \
            tracker->transfer_to(size, doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()); \
        }                                                                                        \
    } while (0)

// Mem Hook to consume thread mem tracker, not use in brpc thread.
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size)             \
    do {                                                     \
        if (doris::use_mem_hook) {                           \
            DCHECK(doris::pthread_context_ptr_init);         \
            DCHECK(bthread_self() == 0);                     \
            doris::thread_context_ptr->consume_memory(size); \
        }                                                    \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK(size) CONSUME_THREAD_MEM_TRACKER_BY_HOOK(-size)
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)             \
    do {                                                                     \
        if (doris::use_mem_hook) {                                           \
            DCHECK(doris::pthread_context_ptr_init);                         \
            DCHECK(bthread_self() == 0);                                     \
            doris::thread_context_ptr->consume_memory(size_fn(__VA_ARGS__)); \
        }                                                                    \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)              \
    do {                                                                      \
        if (doris::use_mem_hook) {                                            \
            DCHECK(doris::pthread_context_ptr_init);                          \
            DCHECK(bthread_self() == 0);                                      \
            doris::thread_context_ptr->consume_memory(-size_fn(__VA_ARGS__)); \
        }                                                                     \
    } while (0)

// if use mem hook, avoid repeated consume.
// must call create_thread_local_if_not_exits() before use thread_context().
#define CONSUME_THREAD_MEM_TRACKER(size)                                                           \
    do {                                                                                           \
        if (doris::use_mem_hook) {                                                                 \
            break;                                                                                 \
        }                                                                                          \
        if (doris::pthread_context_ptr_init) {                                                     \
            DCHECK(bthread_self() == 0);                                                           \
            doris::thread_context_ptr->consume_memory(size);                                       \
        } else if (bthread_self() != 0) {                                                          \
            static_cast<doris::ThreadContext*>(bthread_getspecific(doris::btls_key))               \
                    ->consume_memory(size);                                                        \
        } else if (doris::ExecEnv::ready()) {                                                      \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume_no_update_peak(size); \
        }                                                                                          \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER(size) CONSUME_THREAD_MEM_TRACKER(-size)

#else
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) (void)0
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK(size) (void)0
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size) (void)0
#define CONSUME_THREAD_MEM_TRACKER(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER(size) (void)0
#endif
} // namespace doris
