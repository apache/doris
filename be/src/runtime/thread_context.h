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
#include "runtime/threadlocal.h"
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

#define SKIP_MEMORY_CHECK(...)                                    \
    do {                                                          \
        doris::thread_context()->skip_memory_check++;             \
        DEFER({ doris::thread_context()->skip_memory_check--; }); \
        __VA_ARGS__;                                              \
    } while (0)

namespace doris {

class ThreadContext;
class MemTracker;
class RuntimeState;

extern bthread_key_t btls_key;

// Using gcc11 compiles thread_local variable on lower versions of GLIBC will report an error,
// see https://github.com/apache/doris/pull/7911
//
// If we want to avoid this error,
// 1. For non-trivial variables in thread_local, such as std::string, you need to store them as pointers to
//    ensure that thread_local is trivial, these non-trivial pointers will uniformly call destructors elsewhere.
// 2. The default destructor of the thread_local variable cannot be overridden.
//
// This is difficult to implement. Because the destructor is not overwritten, it means that the outside cannot
// be notified when the thread terminates, and the non-trivial pointers in thread_local cannot be released in time.
// The func provided by pthread and std::thread doesn't help either.
//
// So, kudu Class-scoped static thread local implementation was introduced. Solve the above problem by
// Thread-scoped thread local + Class-scoped thread local.
//
// This may look very trick, but it's the best way I can find.
//
// refer to:
//  https://gcc.gnu.org/onlinedocs/gcc-3.3.1/gcc/Thread-Local.html
//  https://stackoverflow.com/questions/12049684/
//  https://sourceware.org/glibc/wiki/Destructor%20support%20for%20thread_local%20variables
//  https://www.jianshu.com/p/756240e837dd
//  https://man7.org/linux/man-pages/man3/pthread_tryjoin_np.3.html
class ThreadContextPtr {
public:
    ThreadContextPtr();
    // Cannot add destructor `~ThreadContextPtr`, otherwise it will no longer be of type POD, the reason is as above.

    // TCMalloc hook is triggered during ThreadContext construction, which may lead to deadlock.
    bool init = false;

    DECLARE_STATIC_THREAD_LOCAL(ThreadContext, _ptr);
};

inline thread_local ThreadContextPtr thread_context_ptr;

// To avoid performance problems caused by frequently calling `bthread_getspecific` to obtain bthread TLS
// in tcmalloc hook, cache the key and value of bthread TLS in pthread TLS.
inline thread_local ThreadContext* bthread_context;
inline thread_local bthread_t bthread_id;

// The thread context saves some info about a working thread.
// 2 required info:
//   1. thread_id:   Current thread id, Auto generated.
//   2. type:        The type is a enum value indicating which type of task current thread is running.
//                   For example: QUERY, LOAD, COMPACTION, ...
//   3. task id:     A unique id to identify this task. maybe query id, load job id, etc.
//
// There may be other optional info to be added later.
class ThreadContext {
public:
    ThreadContext() {
        thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
        if (ExecEnv::ready()) thread_mem_tracker_mgr->init();
    }

    ~ThreadContext() { thread_context_ptr.init = false; }

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

    const TUniqueId& task_id() const { return _task_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    std::string get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }

    // After thread_mem_tracker_mgr is initialized, the current thread TCMalloc Hook starts to
    // consume/release mem_tracker.
    // Note that the use of shared_ptr will cause a crash. The guess is that there is an
    // intermediate state during the copy construction of shared_ptr. Shared_ptr is not equal
    // to nullptr, but the object it points to is not initialized. At this time, when the memory
    // is released somewhere, the TCMalloc hook is triggered to cause the crash.
    std::unique_ptr<ThreadMemTrackerMgr> thread_mem_tracker_mgr;
    MemTrackerLimiter* thread_mem_tracker() {
        return thread_mem_tracker_mgr->limiter_mem_tracker_raw();
    }

    void consume_memory(const int64_t size) const {
        thread_mem_tracker_mgr->consume(size, large_memory_check);
    }

    int switch_bthread_local_count = 0;
    int skip_memory_check = 0;
    bool large_memory_check = true;

private:
    TUniqueId _task_id;
    TUniqueId _fragment_instance_id;
};

// Switch thread context from pthread local to bthread local context.
// Cache the pointer of bthread local in pthead local,
// Avoid calling bthread_getspecific frequently to get bthread local, which has performance problems.
class SwitchBthreadLocal {
public:
    static void switch_to_bthread_local() {
        if (bthread_self() != 0) {
            // Very frequent bthread_getspecific will slow, but switch_to_bthread_local is not expected to be much.
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
                thread_context_ptr.init = false;
                bthread_context = new ThreadContext;
                // The brpc server should respond as quickly as possible.
                bthread_context->thread_mem_tracker_mgr->disable_wait_gc();
                // set the data so that next time bthread_getspecific in the thread returns the data.
                CHECK(0 == bthread_setspecific(btls_key, bthread_context) || k_doris_exit);
                thread_context_ptr.init = true;
            }
            bthread_id = bthread_self();
            bthread_context->switch_bthread_local_count++;
        }
    }

    // `switch_to_bthread_local` and `switch_back_pthread_local` should be used in pairs,
    // `switch_to_bthread_local` should only be called if `switch_to_bthread_local` returns true
    static void switch_back_pthread_local() {
        if (bthread_self() != 0) {
            if (!bthread_equal(bthread_self(), bthread_id)) {
                bthread_id = bthread_self();
                bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
                DCHECK(bthread_context != nullptr);
            }
            bthread_context->switch_bthread_local_count--;
            if (bthread_context->switch_bthread_local_count == 0) {
                bthread_context = thread_context_ptr._ptr;
            }
        }
    }
};

// Note: All use of thread_context() in bthread requires the use of SwitchBthreadLocal.
static ThreadContext* thread_context() {
    if (bthread_self() != 0) {
        // in bthread
        if (!bthread_equal(bthread_self(), bthread_id)) {
            // bthread switching pthread may be very frequent, remember not to use lock or other time-consuming operations.
            bthread_id = bthread_self();
            bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
            // if nullptr, a new bthread task start and no reusable bthread local,
            // or bthread switch pthread but not call switch_to_bthread_local, use pthread local context
            // else, bthread switch pthread and called switch_to_bthread_local, use bthread local context.
            if (bthread_context == nullptr) {
                bthread_context = thread_context_ptr._ptr;
            }
        }
        return bthread_context;
    } else {
        // in pthread
        return thread_context_ptr._ptr;
    }
}

class ScopeMemCount {
public:
    explicit ScopeMemCount(int64_t* scope_mem) {
        _scope_mem = scope_mem;
        thread_context()->thread_mem_tracker_mgr->start_count_scope_mem();
    }

    ~ScopeMemCount() {
        *_scope_mem += thread_context()->thread_mem_tracker_mgr->stop_count_scope_mem();
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
        SwitchBthreadLocal::switch_to_bthread_local();
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, TUniqueId());
    }

    ~SwitchThreadMemTrackerLimiter() {
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
        SwitchBthreadLocal::switch_back_pthread_local();
    }

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker;
};

class TrackMemoryToUnknown {
public:
    explicit TrackMemoryToUnknown() {
        if (bthread_self() != 0) {
            _tid = std::this_thread::get_id(); // save pthread id
        }
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(
                ExecEnv::GetInstance()->orphan_mem_tracker(), TUniqueId());
    }

    ~TrackMemoryToUnknown() {
        if (bthread_self() != 0) {
            // make sure pthread is not switch, if switch, mem tracker will be wrong, but not crash in release
            DCHECK(_tid == std::this_thread::get_id());
        }
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
    }

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker;
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
// For the memory that cannot be counted by mem hook, manually count it into the mem tracker, such as mmap.
#define CONSUME_THREAD_MEM_TRACKER(size) doris::thread_context()->consume_memory(size)
#define RELEASE_THREAD_MEM_TRACKER(size) doris::thread_context()->consume_memory(-size)

// used to fix the tracking accuracy of caches.
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker)                                        \
    doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_raw()->transfer_to( \
            size, tracker)
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) \
    tracker->transfer_to(                               \
            size, doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_raw())

// Mem Hook to consume thread mem tracker
// TODO: In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior.
#define CONSUME_MEM_TRACKER(size)                                                                  \
    do {                                                                                           \
        if (doris::thread_context_ptr.init) {                                                      \
            doris::thread_context()->consume_memory(size);                                         \
        } else if (doris::ExecEnv::ready()) {                                                      \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume_no_update_peak(size); \
        }                                                                                          \
    } while (0)
#define RELEASE_MEM_TRACKER(size)                                                            \
    do {                                                                                     \
        if (doris::thread_context_ptr.init) {                                                \
            doris::thread_context()->consume_memory(-size);                                  \
        } else if (doris::ExecEnv::ready()) {                                                \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker_raw()->consume_no_update_peak( \
                    -size);                                                                  \
        }                                                                                    \
    } while (0)
#else
#define CONSUME_THREAD_MEM_TRACKER(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER(size) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) (void)0
#define CONSUME_MEM_TRACKER(size) (void)0
#define RELEASE_MEM_TRACKER(size) (void)0
#endif
} // namespace doris
