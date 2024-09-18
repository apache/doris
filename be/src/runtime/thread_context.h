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
    Defer VARNAME_LINENUM(defer) {[&]() { doris::thread_context()->release_reserved_memory(); }};

#define ORPHAN_TRACKER_CHECK()                                                  \
    DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check || \
           doris::thread_context()->thread_mem_tracker()->label() != "Orphan")  \
            << doris::memory_orphan_check_msg

#define MEMORY_ORPHAN_CHECK()                                                 \
    DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check) \
            << doris::memory_orphan_check_msg;
#else
// thread context need to be initialized, required by Allocator and elsewhere.
#define SCOPED_ATTACH_TASK(arg1, ...) \
    auto VARNAME_LINENUM(scoped_tls_at) = doris::ScopedInitThreadContext()
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(arg1) \
    auto VARNAME_LINENUM(scoped_tls_stmtl) = doris::ScopedInitThreadContext()
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(scoped_tls_cmt) = doris::ScopedInitThreadContext()
#define DEFER_RELEASE_RESERVED() (void)0
#define ORPHAN_TRACKER_CHECK() (void)0
#define MEMORY_ORPHAN_CHECK() (void)0
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
        doris::thread_context()->skip_large_memory_check++;                \
        DEFER({                                                            \
            doris::thread_context()->skip_large_memory_check--;            \
            doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); \
        });                                                                \
        __VA_ARGS__;                                                       \
    } while (0)

#define LIMIT_LOCAL_SCAN_IO(data_dir, bytes_read)                     \
    std::shared_ptr<IOThrottle> iot = nullptr;                        \
    auto* t_ctx = doris::thread_context(true);                        \
    if (t_ctx) {                                                      \
        iot = t_ctx->get_local_scan_io_throttle(data_dir);            \
    }                                                                 \
    if (iot) {                                                        \
        iot->acquire(-1);                                             \
    }                                                                 \
    Defer defer {                                                     \
        [&]() {                                                       \
            if (iot) {                                                \
                iot->update_next_io_time(*bytes_read);                \
                t_ctx->update_total_local_scan_io_adder(*bytes_read); \
            }                                                         \
        }                                                             \
    }

#define LIMIT_REMOTE_SCAN_IO(bytes_read)               \
    std::shared_ptr<IOThrottle> iot = nullptr;         \
    if (auto* t_ctx = doris::thread_context(true)) {   \
        iot = t_ctx->get_remote_scan_io_throttle();    \
    }                                                  \
    if (iot) {                                         \
        iot->acquire(-1);                              \
    }                                                  \
    Defer defer {                                      \
        [&]() {                                        \
            if (iot) {                                 \
                iot->update_next_io_time(*bytes_read); \
            }                                          \
        }                                              \
    }

namespace doris {

class ThreadContext;
class MemTracker;
class RuntimeState;
class QueryThreadContext;
class WorkloadGroup;

extern bthread_key_t btls_key;

// Is true after ThreadContext construction.
inline thread_local bool pthread_context_ptr_init = false;
inline thread_local constinit ThreadContext* thread_context_ptr = nullptr;
// use mem hook to consume thread mem tracker.
inline thread_local bool use_mem_hook = false;

static std::string memory_orphan_check_msg =
        "If you crash here, it means that SCOPED_ATTACH_TASK and "
        "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER are not used correctly. starting position of "
        "each thread is expected to use SCOPED_ATTACH_TASK to bind a MemTrackerLimiter belonging "
        "to Query/Load/Compaction/Other Tasks, otherwise memory alloc using Doris Allocator in the "
        "thread will crash. If you want to switch MemTrackerLimiter during thread execution, "
        "please use SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER, do not repeat Attach.";

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

    void attach_task(const TUniqueId& task_id,
                     const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                     const std::weak_ptr<WorkloadGroup>& wg_wptr) {
        // will only attach_task at the beginning of the thread function, there should be no duplicate attach_task.
        DCHECK(mem_tracker);
        // Orphan is thread default tracker.
        DCHECK(thread_mem_tracker()->label() == "Orphan")
                << ", thread mem tracker label: " << thread_mem_tracker()->label()
                << ", attach mem tracker label: " << mem_tracker->label();
        _task_id = task_id;
        _wg_wptr = wg_wptr;
        thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker);
        thread_mem_tracker_mgr->set_query_id(_task_id);
        thread_mem_tracker_mgr->set_wg_wptr(_wg_wptr);
        thread_mem_tracker_mgr->enable_wait_gc();
        thread_mem_tracker_mgr->reset_query_cancelled_flag(false);
    }

    void detach_task() {
        _task_id = TUniqueId();
        _wg_wptr.reset();
        thread_mem_tracker_mgr->detach_limiter_tracker();
        thread_mem_tracker_mgr->set_query_id(TUniqueId());
        thread_mem_tracker_mgr->reset_wg_wptr();
        thread_mem_tracker_mgr->disable_wait_gc();
    }

    [[nodiscard]] const TUniqueId& task_id() const { return _task_id; }

    static std::string get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }
    // Note that if set global Memory Hook, After thread_mem_tracker_mgr is initialized,
    // the current thread Hook starts to consume/release mem_tracker.
    // the use of shared_ptr will cause a crash. The guess is that there is an
    // intermediate state during the copy construction of shared_ptr. Shared_ptr is not equal
    // to nullptr, but the object it points to is not initialized. At this time, when the memory
    // is released somewhere, the hook is triggered to cause the crash.
    std::unique_ptr<ThreadMemTrackerMgr> thread_mem_tracker_mgr;
    [[nodiscard]] MemTrackerLimiter* thread_mem_tracker() const {
        return thread_mem_tracker_mgr->limiter_mem_tracker().get();
    }

    QueryThreadContext query_thread_context();

    void consume_memory(const int64_t size) const {
#ifdef USE_MEM_TRACKER
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check ||
               thread_mem_tracker()->label() != "Orphan")
                << doris::memory_orphan_check_msg;
#endif
        thread_mem_tracker_mgr->consume(size, skip_large_memory_check);
    }

    doris::Status try_reserve_memory(const int64_t size) const {
#ifdef USE_MEM_TRACKER
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check ||
               thread_mem_tracker()->label() != "Orphan")
                << doris::memory_orphan_check_msg;
#endif
        return thread_mem_tracker_mgr->try_reserve(size);
    }

    void release_reserved_memory() const {
#ifdef USE_MEM_TRACKER
        DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check ||
               thread_mem_tracker()->label() != "Orphan")
                << doris::memory_orphan_check_msg;
#endif
        thread_mem_tracker_mgr->release_reserved();
    }

    std::weak_ptr<WorkloadGroup> workload_group() { return _wg_wptr; }

    std::shared_ptr<IOThrottle> get_local_scan_io_throttle(const std::string& data_dir) {
        if (std::shared_ptr<WorkloadGroup> wg_ptr = _wg_wptr.lock()) {
            return wg_ptr->get_local_scan_io_throttle(data_dir);
        }
        return nullptr;
    }

    std::shared_ptr<IOThrottle> get_remote_scan_io_throttle() {
        if (std::shared_ptr<WorkloadGroup> wg_ptr = _wg_wptr.lock()) {
            return wg_ptr->get_remote_scan_io_throttle();
        }
        return nullptr;
    }

    void update_total_local_scan_io_adder(size_t bytes_read) {
        if (std::shared_ptr<WorkloadGroup> wg_ptr = _wg_wptr.lock()) {
            wg_ptr->update_total_local_scan_io_adder(bytes_read);
        }
    }

    int thread_local_handle_count = 0;
    int skip_memory_check = 0;
    int skip_large_memory_check = 0;

private:
    TUniqueId _task_id;
    std::weak_ptr<WorkloadGroup> _wg_wptr;
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
                CHECK(0 == bthread_setspecific(btls_key, bthread_context) || doris::k_doris_exit);
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

// must call create_thread_local_if_not_exits() before use thread_context().
static ThreadContext* thread_context(bool allow_return_null = false) {
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
        DCHECK(bthread_context != nullptr && bthread_context->thread_local_handle_count > 0);
        return bthread_context;
    }
    if (allow_return_null) {
        return nullptr;
    }
    // It means that use thread_context() but this thread not attached a query/load using SCOPED_ATTACH_TASK macro.
    LOG(FATAL) << "__builtin_unreachable, " << doris::memory_orphan_check_msg;
    __builtin_unreachable();
}

// belong to one query object member, not be shared by multiple queries.
class QueryThreadContext {
public:
    QueryThreadContext() = default;
    QueryThreadContext(const TUniqueId& query_id,
                       const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                       const std::weak_ptr<WorkloadGroup>& wg_wptr)
            : query_id(query_id), query_mem_tracker(mem_tracker), wg_wptr(wg_wptr) {}
    // If use WorkloadGroup and can get WorkloadGroup ptr, must as a parameter.
    QueryThreadContext(const TUniqueId& query_id,
                       const std::shared_ptr<MemTrackerLimiter>& mem_tracker)
            : query_id(query_id), query_mem_tracker(mem_tracker) {}

    // Not thread safe, generally be called in class constructor, shared_ptr use_count may be
    // wrong when called by multiple threads, cause crash after object be destroyed prematurely.
    void init_unlocked() {
#ifndef BE_TEST
        ORPHAN_TRACKER_CHECK();
        query_id = doris::thread_context()->task_id();
        query_mem_tracker = doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        wg_wptr = doris::thread_context()->workload_group();
#else
        query_id = TUniqueId();
        query_mem_tracker = doris::ExecEnv::GetInstance()->orphan_mem_tracker();
#endif
    }

    std::shared_ptr<MemTrackerLimiter> get_memory_tracker() { return query_mem_tracker; }

    WorkloadGroupPtr get_workload_group_ptr() { return wg_wptr.lock(); }

    TUniqueId query_id;
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;
    std::weak_ptr<WorkloadGroup> wg_wptr;
};

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
    // not query or load, initialize with memory tracker, empty query id and default normal workload group.
    explicit AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker);

    // is query or load, initialize with memory tracker, query id and workload group wptr.
    explicit AttachTask(RuntimeState* runtime_state);

    explicit AttachTask(QueryContext* query_ctx);

    explicit AttachTask(const QueryThreadContext& query_thread_context);

    void init(const QueryThreadContext& query_thread_context);

    ~AttachTask();
};

class SwitchThreadMemTrackerLimiter {
public:
    explicit SwitchThreadMemTrackerLimiter(
            const std::shared_ptr<doris::MemTrackerLimiter>& mem_tracker) {
        DCHECK(mem_tracker);
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();
        if (mem_tracker != thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()) {
            _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
            thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker);
        }
    }

    explicit SwitchThreadMemTrackerLimiter(const doris::QueryThreadContext& query_thread_context) {
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();
        DCHECK(thread_context()->task_id() ==
               query_thread_context.query_id); // workload group alse not change
        DCHECK(query_thread_context.query_mem_tracker);
        if (query_thread_context.query_mem_tracker !=
            thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()) {
            _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
            thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(
                    query_thread_context.query_mem_tracker);
        }
    }

    ~SwitchThreadMemTrackerLimiter() {
        if (_old_mem_tracker != nullptr) {
            thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
        }
        doris::ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }

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
        doris::thread_context()->skip_memory_check++;
    }

    ~ScopeSkipMemoryCheck() {
        doris::thread_context()->skip_memory_check--;
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }
};

// Basic macros for mem tracker, usually do not need to be modified and used.
#if defined(USE_MEM_TRACKER) && !defined(BE_TEST)
// used to fix the tracking accuracy of caches.
#define THREAD_MEM_TRACKER_TRANSFER_TO(size, tracker)                                        \
    do {                                                                                     \
        ORPHAN_TRACKER_CHECK();                                                              \
        doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->transfer_to( \
                size, tracker);                                                              \
    } while (0)

#define THREAD_MEM_TRACKER_TRANSFER_FROM(size, tracker)                                        \
    do {                                                                                       \
        ORPHAN_TRACKER_CHECK();                                                                \
        tracker->transfer_to(                                                                  \
                size, doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()); \
    } while (0)

// Mem Hook to consume thread mem tracker
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size)           \
    do {                                                   \
        if (doris::use_mem_hook) {                         \
            doris::thread_context()->consume_memory(size); \
        }                                                  \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK(size) CONSUME_THREAD_MEM_TRACKER_BY_HOOK(-size)
#define CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)           \
    do {                                                                   \
        if (doris::use_mem_hook) {                                         \
            doris::thread_context()->consume_memory(size_fn(__VA_ARGS__)); \
        }                                                                  \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(size_fn, ...)            \
    do {                                                                    \
        if (doris::use_mem_hook) {                                          \
            doris::thread_context()->consume_memory(-size_fn(__VA_ARGS__)); \
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
            doris::thread_context_ptr->consume_memory(size);                                   \
        } else if (bthread_self() != 0) {                                                      \
            static_cast<doris::ThreadContext*>(bthread_getspecific(doris::btls_key))           \
                    ->consume_memory(size);                                                    \
        } else if (doris::ExecEnv::ready()) {                                                  \
            MEMORY_ORPHAN_CHECK();                                                             \
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
