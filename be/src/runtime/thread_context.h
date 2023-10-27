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

// Used to observe the memory usage of the specified code segment
#if defined(USE_MEM_TRACKER) && !defined(UNDEFINED_BEHAVIOR_SANITIZER)
// Count a code segment memory (memory malloc - memory free) to int64_t
// Usage example: int64_t scope_mem = 0; { SCOPED_MEM_COUNT(&scope_mem); xxx; xxx; }
#define SCOPED_MEM_COUNT(scope_mem) \
    auto VARNAME_LINENUM(scope_mem_count) = doris::ScopeMemCount(scope_mem)

// Count a code segment memory (memory malloc - memory free) to MemTracker.
// Compared to count `scope_mem`, MemTracker is easier to observe from the outside and is thread-safe.
// Usage example: std::unique_ptr<MemTracker> tracker = std::make_unique<MemTracker>("first_tracker");
//                { SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get()); xxx; xxx; }
// Usually used to record query more detailed memory, including ExecNode operators.
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumer(mem_tracker)
#else
#define SCOPED_MEM_COUNT(scope_mem) (void)0
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) (void)0
#endif

// Used to observe query/load/compaction/e.g. execution thread memory usage and respond when memory exceeds the limit.
#if defined(USE_MEM_TRACKER) && !defined(UNDEFINED_BEHAVIOR_SANITIZER)
// Attach to query/load/compaction/e.g. when thread starts.
// This will save some info about a working thread in the thread context.
// And count the memory during thread execution (is actually also the code segment that executes the function)
// to specify MemTrackerLimiter, and expect to handle when the memory exceeds the limit, for example cancel query.
// Usage is similar to SCOPED_CONSUME_MEM_TRACKER.
#define SCOPED_ATTACH_TASK(arg1) auto VARNAME_LINENUM(attach_task) = AttachTask(arg1)

#define SCOPED_ATTACH_TASK_WITH_ID(arg1, arg2, arg3) \
    auto VARNAME_LINENUM(attach_task) = AttachTask(arg1, arg2, arg3)

// Switch MemTrackerLimiter for count memory during thread execution.
// Usually used after SCOPED_ATTACH_TASK, in order to count the memory of the specified code segment into another
// MemTrackerLimiter instead of the MemTrackerLimiter added by the attach task.
// Note that, not use it in rpc done.run(), because bthread_setspecific may have errors when UBSAN compiles.
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) \
    auto VARNAME_LINENUM(switch_mem_tracker) = SwitchThreadMemTrackerLimiter(mem_tracker_limiter)

// Usually used to exclude a part of memory in query or load mem tracker and track it to Orphan Mem Tracker.
// Note that, not check whether it is currently a Bthread and switch Bthread Local, because this is usually meaningless,
// if used in Bthread, and pthread switching is expected, use SwitchThreadMemTrackerLimiter.
#define SCOPED_TRACK_MEMORY_TO_UNKNOWN() \
    auto VARNAME_LINENUM(track_memory_to_unknown) = TrackMemoryToUnknown()
#else
#define SCOPED_ATTACH_TASK(arg1, ...) (void)0
#define SCOPED_ATTACH_TASK_WITH_ID(arg1, arg2, arg3) (void)0
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) (void)0
#define SCOPED_TRACK_MEMORY_TO_UNKNOWN() (void)0
#endif

#define SKIP_MEMORY_CHECK(...)                                \
    do {                                                      \
        doris::ThreadLocalHandle::handle_thread_local();      \
        doris::thread_context()->skip_memory_check++;         \
        DEFER({                                               \
            doris::thread_context()->skip_memory_check--;     \
            doris::ThreadLocalHandle::release_thread_local(); \
        });                                                   \
        __VA_ARGS__;                                          \
    } while (0)

#define SKIP_LARGE_MEMORY_CHECK(...)                            \
    do {                                                        \
        doris::ThreadLocalHandle::handle_thread_local();        \
        doris::thread_context()->skip_large_memory_check++;     \
        DEFER({                                                 \
            doris::thread_context()->skip_large_memory_check--; \
            doris::ThreadLocalHandle::release_thread_local();   \
        });                                                     \
        __VA_ARGS__;                                            \
    } while (0)

namespace doris {

class ThreadContext;
class MemTracker;
class RuntimeState;

extern bthread_key_t btls_key;

// Is true after ThreadContext construction.
inline thread_local bool pthread_context_ptr_init = false;
inline thread_local constinit ThreadContext* thread_context_ptr;
// To avoid performance problems caused by frequently calling `bthread_getspecific` to obtain bthread TLS
// cache the key and value of bthread TLS in pthread TLS.
inline thread_local constinit ThreadContext* bthread_context = nullptr;
inline thread_local bthread_t bthread_id;

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
    ThreadContext() {
        thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
        if (ExecEnv::ready()) {
            thread_mem_tracker_mgr->init();
        }
    }

    ~ThreadContext() {
        pthread_context_ptr_init = false;
        bthread_context = nullptr;
        bthread_id = bthread_self(); // Avoid CONSUME_MEM_TRACKER call pthread_getspecific.
    }

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

    int handle_thread_local_count = 0;
    int skip_memory_check = 0;
    int skip_large_memory_check = 0;

private:
    TUniqueId _task_id;
    TUniqueId _fragment_instance_id;
};

class ThreadLocalHandle {
public:
    static void handle_thread_local() {
        if (bthread_self() == 0) {
            if (!pthread_context_ptr_init) {
                DCHECK(bthread_equal(0, bthread_id)); // Not used in bthread before.
                thread_context_ptr = new ThreadContext();
                pthread_context_ptr_init = true;
            }
            DCHECK(thread_context_ptr != nullptr);
            thread_context_ptr->handle_thread_local_count++;
        } else {
            // Avoid calling bthread_getspecific frequently to get bthread local.
            // Very frequent bthread_getspecific will slow, but handle_thread_local is not expected to be much.
            // Cache the pointer of bthread local in pthead local.
            bthread_id = bthread_self();
            bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
            if (bthread_context == nullptr) {
                // A new bthread starts, two scenarios:
                // 1. First call to bthread_getspecific (and before any bthread_setspecific) returns NULL
                // 2. There are not enough reusable btls in btls pool.
                // else, two scenarios:
                // 1. A new bthread starts, but get a reuses btls.
                // 2. A pthread switch occurs. Because the pthread switch cannot be accurately identified at the moment.
                // So tracker call reset 0 like reuses btls.
                // during this period, stop the use of thread_context.
                bthread_context = new ThreadContext;
                // The brpc server should respond as quickly as possible.
                bthread_context->thread_mem_tracker_mgr->disable_wait_gc();
                // set the data so that next time bthread_getspecific in the thread returns the data.
                CHECK(0 == bthread_setspecific(btls_key, bthread_context) || k_doris_exit);
            }
            DCHECK(bthread_context != nullptr);
            bthread_context->handle_thread_local_count++;
        }
    }

    // `handle_thread_local` and `handle_thread_local` should be used in pairs,
    // `release_thread_local` should only be called if `handle_thread_local` returns true
    static void release_thread_local() {
        if (bthread_self() == 0) {
            DCHECK(pthread_context_ptr_init);
            thread_context_ptr->handle_thread_local_count--;
            if (thread_context_ptr->handle_thread_local_count == 0) {
                pthread_context_ptr_init = false;
                delete doris::thread_context_ptr;
                thread_context_ptr = nullptr;
            }
        } else {
            if (!bthread_equal(bthread_self(), bthread_id)) {
                bthread_id = bthread_self();
                bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
                DCHECK(bthread_context != nullptr);
            }
            bthread_context->handle_thread_local_count--;
            if (bthread_context->handle_thread_local_count == 0) {
                bthread_id = 0;
                bthread_context = nullptr;
            }
        }
    }
};

static bool is_thread_context_init() {
    if (pthread_context_ptr_init) {
        // in pthread
        DCHECK(thread_context_ptr != nullptr);
        DCHECK(bthread_equal(0, bthread_id)); // Not used in bthread before.
        return true;
    } else if (bthread_self() != 0) {
        // in bthread
        DCHECK(!pthread_context_ptr_init);
        if (!bthread_equal(bthread_self(), bthread_id)) {
            // bthread switching pthread may be very frequent, remember not to use lock or other time-consuming operations.
            bthread_id = bthread_self();
            bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
        }
        if (doris::bthread_context == nullptr) {
            return false;
        }
        return true;
    } else {
        return false;
    }
}

// must call handle_thread_local() and is_thread_context_init() before use thread_context().
static ThreadContext* thread_context() {
    if (bthread_self() == 0) {
        // in pthread
        DCHECK(pthread_context_ptr_init);
        DCHECK(thread_context_ptr != nullptr);
        return thread_context_ptr;
    }
    // in bthread
    if (!bthread_equal(bthread_self(), bthread_id)) {
        // bthread switching pthread may be very frequent, remember not to use lock or other time-consuming operations.
        bthread_id = bthread_self();
        bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
    }
    DCHECK(bthread_context != nullptr);
    return bthread_context;
}

class ScopeMemCount {
public:
    explicit ScopeMemCount(int64_t* scope_mem) {
        ThreadLocalHandle::handle_thread_local();
        _scope_mem = scope_mem;
        thread_context()->thread_mem_tracker_mgr->start_count_scope_mem();
    }

    ~ScopeMemCount() {
        *_scope_mem += thread_context()->thread_mem_tracker_mgr->stop_count_scope_mem();
        ThreadLocalHandle::release_thread_local();
    }

private:
    int64_t* _scope_mem;
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
        ThreadLocalHandle::handle_thread_local();
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, TUniqueId());
    }

    ~SwitchThreadMemTrackerLimiter() {
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
        ThreadLocalHandle::release_thread_local();
    }

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker;
};

class TrackMemoryToUnknown {
public:
    explicit TrackMemoryToUnknown() {
        if (is_thread_context_init()) {
            if (bthread_self() != 0) {
                _tid = std::this_thread::get_id(); // save pthread id
            }
            _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
            thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(
                    ExecEnv::GetInstance()->orphan_mem_tracker(), TUniqueId());
        }
    }

    ~TrackMemoryToUnknown() {
        if (is_thread_context_init()) {
            DCHECK(_old_mem_tracker != nullptr);
            if (bthread_self() != 0) {
                // make sure pthread is not switch, if switch, mem tracker will be wrong, but not crash in release
                DCHECK(_tid == std::this_thread::get_id());
            }
            thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
        }
    }

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker = nullptr;
    std::thread::id _tid;
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
    std::shared_ptr<MemTracker> _mem_tracker = nullptr; // Avoid mem_tracker being released midway.
    bool _need_pop = false;
};

// Basic macros for mem tracker, usually do not need to be modified and used.
#ifdef USE_MEM_TRACKER
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

// Mem Hook to consume thread mem tracker
// TODO: In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior.
#define CONSUME_MEM_TRACKER(size)                                                                  \
    do {                                                                                           \
        if (doris::pthread_context_ptr_init) {                                                     \
            DCHECK(bthread_self() == 0);                                                           \
            DCHECK(doris::thread_context_ptr != nullptr);                                          \
            DCHECK(bthread_equal(0, doris::bthread_id));                                           \
            doris::thread_context_ptr->consume_memory(size);                                       \
        } else if (bthread_self() != 0) {                                                          \
            DCHECK(!doris::pthread_context_ptr_init);                                              \
            if (!bthread_equal(bthread_self(), doris::bthread_id)) {                               \
                doris::bthread_id = bthread_self();                                                \
                doris::bthread_context =                                                           \
                        static_cast<doris::ThreadContext*>(bthread_getspecific(doris::btls_key));  \
            }                                                                                      \
            if (doris::bthread_context == nullptr) {                                               \
                doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume_no_update_peak(   \
                        size);                                                                     \
            } else {                                                                               \
                doris::bthread_context->consume_memory(size);                                      \
            }                                                                                      \
        } else if (doris::ExecEnv::ready()) {                                                      \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume_no_update_peak(size); \
        }                                                                                          \
    } while (0)
#define RELEASE_MEM_TRACKER(size) CONSUME_MEM_TRACKER(-size)
#else
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) (void)0
#define CONSUME_MEM_TRACKER(size) (void)0
#define RELEASE_MEM_TRACKER(size) (void)0
#endif
} // namespace doris
