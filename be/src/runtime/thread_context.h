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

#include <memory>
#include <string>
#include <thread>

#include "common/exception.h"
#include "common/logging.h"
#include "common/macros.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/workload_management/resource_context.h"
#include "util/defer_op.h" // IWYU pragma: keep

// Used to tracking query/load/compaction/e.g. execution thread memory usage.
// This series of methods saves some information to the thread local context of the current worker thread,
// including MemTracker, QueryID, etc. Use CONSUME_THREAD_MEM_TRACKER/RELEASE_THREAD_MEM_TRACKER in the code segment where
// the macro is located to record the memory into MemTracker.
// Not use it in rpc done.run(), because bthread_setspecific may have errors when UBSAN compiles.

// Attach to query/load/compaction/e.g. when thread starts.
// This will save some info about a working thread in the thread context.
// Looking forward to tracking memory during thread execution into MemTrackerLimiter.
#define SCOPED_ATTACH_TASK(arg1) auto VARNAME_LINENUM(attach_task) = AttachTask(arg1)

// If the current thread is not executing a Task, such as a StorageEngine thread,
// use SCOPED_INIT_THREAD_CONTEXT to initialize ThreadContext.
#define SCOPED_INIT_THREAD_CONTEXT() \
    auto VARNAME_LINENUM(scoped_tls_itc) = doris::ScopedInitThreadContext()

// Switch resource context in thread context, used after SCOPED_ATTACH_TASK.
// If just want to switch mem tracker, use SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER first.
#define SCOPED_SWITCH_RESOURCE_CONTEXT(arg1) \
    auto VARNAME_LINENUM(switch_resource_context) = doris::SwitchResourceContext(arg1)

// Switch MemTrackerLimiter for count memory during thread execution.
// Used after SCOPED_ATTACH_TASK, in order to count the memory into another
// MemTrackerLimiter instead of the MemTrackerLimiter added by the attach task.
#define SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(arg1) \
    auto VARNAME_LINENUM(switch_mem_tracker) = doris::SwitchThreadMemTrackerLimiter(arg1)

// Looking forward to tracking memory during thread execution into MemTracker.
// Usually used to record query more detailed memory, including ExecNode operators.
#define SCOPED_CONSUME_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(add_mem_consumer) = doris::AddThreadMemTrackerConsumer(mem_tracker)

#define DEFER_RELEASE_RESERVED()   \
    Defer VARNAME_LINENUM(defer) { \
            [&]() { doris::thread_context()->thread_mem_tracker_mgr->shrink_reserved(); }};

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

#define SCOPED_SKIP_MEMORY_CHECK() \
    auto VARNAME_LINENUM(scope_skip_memory_check) = doris::ScopeSkipMemoryCheck()

#define SKIP_LARGE_MEMORY_CHECK(...)                                                    \
    do {                                                                                \
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();                   \
        doris::thread_context()->thread_mem_tracker_mgr->skip_large_memory_check++;     \
        DEFER({                                                                         \
            doris::thread_context()->thread_mem_tracker_mgr->skip_large_memory_check--; \
            doris::ThreadLocalHandle::del_thread_local_if_count_is_zero();              \
        });                                                                             \
        __VA_ARGS__;                                                                    \
    } while (0)

#define LIMIT_LOCAL_SCAN_IO(data_dir, bytes_read)                                            \
    std::shared_ptr<IOThrottle> iot = nullptr;                                               \
    auto* t_ctx = doris::thread_context();                                                   \
    if (t_ctx->is_attach_task() && t_ctx->resource_ctx()->workload_group() != nullptr) {     \
        iot = t_ctx->resource_ctx()->workload_group()->get_local_scan_io_throttle(data_dir); \
    }                                                                                        \
    if (iot) {                                                                               \
        iot->acquire(-1);                                                                    \
    }                                                                                        \
    Defer defer {                                                                            \
        [&]() {                                                                              \
            if (iot) {                                                                       \
                iot->update_next_io_time(*bytes_read);                                       \
                t_ctx->resource_ctx()->workload_group()->update_local_scan_io(data_dir,      \
                                                                              *bytes_read);  \
            }                                                                                \
        }                                                                                    \
    }

#define LIMIT_REMOTE_SCAN_IO(bytes_read)                                                     \
    std::shared_ptr<IOThrottle> iot = nullptr;                                               \
    auto* t_ctx = doris::thread_context();                                                   \
    if (t_ctx->is_attach_task() && t_ctx->resource_ctx()->workload_group() != nullptr) {     \
        iot = t_ctx->resource_ctx()->workload_group()->get_remote_scan_io_throttle();        \
    }                                                                                        \
    if (iot) {                                                                               \
        iot->acquire(-1);                                                                    \
    }                                                                                        \
    Defer defer {                                                                            \
        [&]() {                                                                              \
            if (iot) {                                                                       \
                iot->update_next_io_time(*bytes_read);                                       \
                t_ctx->resource_ctx()->workload_group()->update_remote_scan_io(*bytes_read); \
            }                                                                                \
        }                                                                                    \
    }

namespace doris {

class ThreadContext;
class MemTracker;
class RuntimeState;
class SwitchResourceContext;

extern bthread_key_t btls_key;

static std::string NO_THREAD_CONTEXT_MSG =
        "Current thread not exist ThreadContext, usually after the thread is started, using "
        "SCOPED_ATTACH_TASK macro to create a ThreadContext and bind a Task.";

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
//
// Note: Keep the class simple and only add properties.
class ThreadContext {
public:
    ThreadContext() { thread_mem_tracker_mgr = std::make_unique<ThreadMemTrackerMgr>(); }

    ~ThreadContext() = default;

    void attach_task(const std::shared_ptr<ResourceContext>& rc) {
        // will only attach_task at the beginning of the thread function, there should be no duplicate attach_task.
        DCHECK(resource_ctx_ == nullptr);
        resource_ctx_ = rc;
        thread_mem_tracker_mgr->attach_limiter_tracker(rc->memory_context()->mem_tracker(),
                                                       rc->workload_group());
        thread_mem_tracker_mgr->enable_wait_gc();
    }

    void detach_task() {
        resource_ctx_.reset();
        thread_mem_tracker_mgr->detach_limiter_tracker();
        thread_mem_tracker_mgr->disable_wait_gc();
    }

    bool is_attach_task() { return resource_ctx_ != nullptr; }

    std::shared_ptr<ResourceContext> resource_ctx() {
#ifndef BE_TEST
        DCHECK(is_attach_task());
#endif
        if (is_attach_task()) {
            return resource_ctx_;
        } else {
            auto ctx = ResourceContext::create_shared();
            ctx->memory_context()->set_mem_tracker(
                    doris::ExecEnv::GetInstance()->orphan_mem_tracker());
            return ctx;
        }
    }

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

    int thread_local_handle_count = 0;

private:
    friend class SwitchResourceContext;
    std::shared_ptr<ResourceContext> resource_ctx_;
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
            throw Exception(Status::FatalError("__builtin_unreachable"));
        }
    }
};

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
        DCHECK(bthread_context != nullptr && bthread_context->thread_local_handle_count > 0);
        return bthread_context;
    }
    // It means that use thread_context() but this thread not attached a query/load using SCOPED_ATTACH_TASK macro.
    throw Exception(Status::FatalError("{}", doris::NO_THREAD_CONTEXT_MSG));
}

class ScopedPeakMem {
public:
    explicit ScopedPeakMem(int64_t* peak_mem)
            : _peak_mem(peak_mem),
              _mem_tracker("ScopedPeakMem:" + UniqueId::gen_uid().to_string()) {
        ThreadLocalHandle::create_thread_local_if_not_exits();
        thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(&_mem_tracker);
    }

    ~ScopedPeakMem() {
        thread_context()->thread_mem_tracker_mgr->pop_consumer_tracker();
        *_peak_mem += _mem_tracker.peak_consumption();
        ThreadLocalHandle::del_thread_local_if_count_is_zero();
    }

private:
    int64_t* _peak_mem;
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
    // you must use ResourceCtx or MemTracker initialization.
    explicit AttachTask() = delete;

    explicit AttachTask(const std::shared_ptr<ResourceContext>& rc);

    // Shortcut attach task, initialize an empty resource context, and set the memory tracker.
    explicit AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker);

    // is query or load, initialize with memory tracker, query id and workload group wptr.
    explicit AttachTask(RuntimeState* runtime_state);

    explicit AttachTask(QueryContext* query_ctx);

    void init(const std::shared_ptr<ResourceContext>& rc);

    ~AttachTask();
};

class SwitchResourceContext {
public:
    explicit SwitchResourceContext(const std::shared_ptr<ResourceContext>& rc);

    ~SwitchResourceContext();

private:
    std::shared_ptr<ResourceContext> old_resource_ctx_ {nullptr};
};

class SwitchThreadMemTrackerLimiter {
public:
    explicit SwitchThreadMemTrackerLimiter(
            const std::shared_ptr<doris::MemTrackerLimiter>& mem_tracker);

    ~SwitchThreadMemTrackerLimiter();

private:
    bool is_switched_ {false};
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
// must call create_thread_local_if_not_exits() before use thread_context().
#define CONSUME_THREAD_MEM_TRACKER(size)                                                           \
    do {                                                                                           \
        if (size == 0 || doris::use_mem_hook) {                                                    \
            break;                                                                                 \
        }                                                                                          \
        if (doris::pthread_context_ptr_init) {                                                     \
            DCHECK(bthread_self() == 0);                                                           \
            doris::thread_context_ptr->thread_mem_tracker_mgr->consume(size);                      \
        } else if (bthread_self() != 0) {                                                          \
            auto* bthread_context =                                                                \
                    static_cast<doris::ThreadContext*>(bthread_getspecific(doris::btls_key));      \
            DCHECK(bthread_context != nullptr);                                                    \
            if (bthread_context != nullptr) {                                                      \
                bthread_context->thread_mem_tracker_mgr->consume(size);                            \
            } else {                                                                               \
                doris::ExecEnv::GetInstance()->orphan_mem_tracker()->consume_no_update_peak(size); \
            }                                                                                      \
        } else if (doris::ExecEnv::ready()) {                                                      \
            DCHECK(doris::k_doris_exit || !doris::config::enable_memory_orphan_check)              \
                    << doris::NO_THREAD_CONTEXT_MSG;                                               \
            doris::ExecEnv::GetInstance()->orphan_mem_tracker()->consume_no_update_peak(size);     \
        }                                                                                          \
    } while (0)
#define RELEASE_THREAD_MEM_TRACKER(size) CONSUME_THREAD_MEM_TRACKER(-size)

} // namespace doris
