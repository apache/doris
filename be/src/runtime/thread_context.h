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

#include <string>
#include <thread>

#include "common/logging.h"
#include "gen_cpp/PaloInternalService_types.h" // for TQueryType
#include "gutil/macros.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/threadlocal.h"

// Used to observe the memory usage of the specified code segment
#ifdef USE_MEM_TRACKER
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
#ifdef USE_MEM_TRACKER
// Attach to query/load/compaction/e.g. when thread starts.
// This will save some info about a working thread in the thread context.
// And count the memory during thread execution (is actually also the code segment that executes the function)
// to specify MemTrackerLimiter, and expect to handle when the memory exceeds the limit, for example cancel query.
// Usage is similar to SCOPED_CONSUME_MEM_TRACKER.
#define SCOPED_ATTACH_TASK(arg1, ...) \
    auto VARNAME_LINENUM(attach_task) = AttachTask(arg1, ##__VA_ARGS__)
// Switch MemTrackerLimiter for count memory during thread execution.
// Usually used after SCOPED_ATTACH_TASK, in order to count the memory of the specified code segment into another
// MemTrackerLimiter instead of the MemTrackerLimiter added by the attach task.
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) \
    auto VARNAME_LINENUM(switch_mem_tracker) = SwitchThreadMemTrackerLimiter(mem_tracker_limiter)
// If you don't want to cancel query after thread MemTrackerLimiter exceed limit in a code segment, then use it.
// Usually used after SCOPED_ATTACH_TASK.
#define STOP_CHECK_THREAD_MEM_TRACKER_LIMIT() \
    auto VARNAME_LINENUM(stop_check_limit) = StopCheckThreadMemTrackerLimit()
// If the thread MemTrackerLimiter exceeds the limit, an error status is returned.
// Usually used after SCOPED_ATTACH_TASK, during query execution.
#define RETURN_LIMIT_EXCEEDED(state, msg, ...)                                              \
    return doris::thread_context()                                                          \
            ->_thread_mem_tracker_mgr->limiter_mem_tracker()                                \
            ->fragment_mem_limit_exceeded(                                                  \
                    state,                                                                  \
                    fmt::format("exec node:<{}>, {}",                                       \
                                doris::thread_context()                                     \
                                        ->_thread_mem_tracker_mgr->last_consumer_tracker(), \
                                msg),                                                       \
                    ##__VA_ARGS__);
#else
#define SCOPED_ATTACH_TASK(arg1, ...) (void)0
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_limiter) (void)0
#define STOP_CHECK_THREAD_MEM_TRACKER_LIMIT() (void)0
#define RETURN_LIMIT_EXCEEDED(state, msg, ...) (void)0
#endif

namespace doris {

class TUniqueId;
class ThreadContext;

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
        _thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
        if (ExecEnv::GetInstance()->initialized()) _thread_mem_tracker_mgr->init();
    }

    ~ThreadContext() { thread_context_ptr.init = false; }

    void attach_task(const std::string& task_id, const TUniqueId& fragment_instance_id,
                     const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
#ifndef BE_TEST
        // will only attach_task at the beginning of the thread function, there should be no duplicate attach_task.
        DCHECK(mem_tracker);
        // Orphan is thread default tracker.
        DCHECK(_thread_mem_tracker_mgr->limiter_mem_tracker()->label() == "Orphan")
                << ", attach mem tracker label: " << mem_tracker->label();
#endif
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        _thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, fragment_instance_id);
    }

    void detach_task() {
        _task_id = "";
        _fragment_instance_id = TUniqueId();
        _thread_mem_tracker_mgr->detach_limiter_tracker();
    }

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    std::string get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }

    // After _thread_mem_tracker_mgr is initialized, the current thread TCMalloc Hook starts to
    // consume/release mem_tracker.
    // Note that the use of shared_ptr will cause a crash. The guess is that there is an
    // intermediate state during the copy construction of shared_ptr. Shared_ptr is not equal
    // to nullptr, but the object it points to is not initialized. At this time, when the memory
    // is released somewhere, the TCMalloc hook is triggered to cause the crash.
    std::unique_ptr<ThreadMemTrackerMgr> _thread_mem_tracker_mgr;

private:
    std::string _task_id = "";
    TUniqueId _fragment_instance_id;
};

// Cache the pointer of bthread local in pthead local,
// Avoid calling bthread_getspecific frequently to get bthread local, which has performance problems.
static void pthread_attach_bthread() {
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
        bthread_context = new ThreadContext;
        // set the data so that next time bthread_getspecific in the thread returns the data.
        CHECK_EQ(0, bthread_setspecific(btls_key, bthread_context));
    }
}

static ThreadContext* thread_context() {
    if (bthread_self() != 0) {
        if (bthread_self() != bthread_id) {
            // A new bthread starts or pthread switch occurs, during this period, stop the use of thread_context.
            thread_context_ptr.init = false;
            pthread_attach_bthread();
            thread_context_ptr.init = true;
        }
        return bthread_context;
    } else {
        return thread_context_ptr._ptr;
    }
}

class ScopeMemCount {
public:
    explicit ScopeMemCount(int64_t* scope_mem);

    ~ScopeMemCount();

private:
    int64_t* _scope_mem;
};

class AttachTask {
public:
    explicit AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                        const std::string& task_id = "",
                        const TUniqueId& fragment_instance_id = TUniqueId());

    explicit AttachTask(RuntimeState* runtime_state);

    ~AttachTask();
};

class SwitchThreadMemTrackerLimiter {
public:
    explicit SwitchThreadMemTrackerLimiter(const std::shared_ptr<MemTrackerLimiter>& mem_tracker);

    ~SwitchThreadMemTrackerLimiter();

private:
    std::shared_ptr<MemTrackerLimiter> _old_mem_tracker;
};

class AddThreadMemTrackerConsumer {
public:
    // The owner and user of MemTracker are in the same thread, and the raw pointer is faster.
    explicit AddThreadMemTrackerConsumer(MemTracker* mem_tracker);

    // The owner and user of MemTracker are in different threads.
    explicit AddThreadMemTrackerConsumer(const std::shared_ptr<MemTracker>& mem_tracker);

    ~AddThreadMemTrackerConsumer();

private:
    std::shared_ptr<MemTracker> _mem_tracker = nullptr; // Avoid mem_tracker being released midway.
    bool _need_pop = false;
};

class StopCheckThreadMemTrackerLimit {
public:
    explicit StopCheckThreadMemTrackerLimit() {
        _pre = thread_context()->_thread_mem_tracker_mgr->check_limit();
        thread_context()->_thread_mem_tracker_mgr->set_check_limit(false);
    }

    ~StopCheckThreadMemTrackerLimit() {
        thread_context()->_thread_mem_tracker_mgr->set_check_limit(_pre);
    }

private:
    bool _pre;
};

// Basic macros for mem tracker, usually do not need to be modified and used.
#ifdef USE_MEM_TRACKER
// For the memory that cannot be counted by mem hook, manually count it into the mem tracker, such as mmap.
#define CONSUME_THREAD_MEM_TRACKER(size) \
    doris::thread_context()->_thread_mem_tracker_mgr->consume(size)
#define RELEASE_THREAD_MEM_TRACKER(size) \
    doris::thread_context()->_thread_mem_tracker_mgr->consume(-size)

// used to fix the tracking accuracy of caches.
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker)                                         \
    doris::thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker_raw()->transfer_to( \
            size, tracker)
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) \
    tracker->transfer_to(                               \
            size, doris::thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker_raw())

// Mem Hook to consume thread mem tracker
// TODO: In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior.
#define MEM_MALLOC_HOOK(size)                                                \
    do {                                                                     \
        if (doris::thread_context_ptr.init) {                                \
            doris::thread_context()->_thread_mem_tracker_mgr->consume(size); \
        } else {                                                             \
            doris::ThreadMemTrackerMgr::consume_no_attach(size);             \
        }                                                                    \
    } while (0)
#define MEM_FREE_HOOK(size)                                                   \
    do {                                                                      \
        if (doris::thread_context_ptr.init) {                                 \
            doris::thread_context()->_thread_mem_tracker_mgr->consume(-size); \
        } else {                                                              \
            doris::ThreadMemTrackerMgr::consume_no_attach(-size);             \
        }                                                                     \
    } while (0)
#else
#define CONSUME_THREAD_MEM_TRACKER(size) (void)0
#define RELEASE_THREAD_MEM_TRACKER(size) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker) (void)0
#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker) (void)0
#define MEM_MALLOC_HOOK(size) (void)0
#define MEM_FREE_HOOK(size) (void)0
#endif
} // namespace doris
