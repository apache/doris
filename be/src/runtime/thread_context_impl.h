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

#include <string>
#include <thread>

#include "common/exception.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"

namespace doris {

class ThreadContext;

extern bthread_key_t btls_key;

// Is true after ThreadContext construction.
inline thread_local bool pthread_context_ptr_init = false;
inline thread_local constinit ThreadContext* thread_context_ptr = nullptr;

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

    void attach_task(const std::shared_ptr<ResourceContext>& rc) {
        resource_ctx = rc;
        thread_mem_tracker_mgr->attach_task(rc->memory_context()->memtracker_limiter(), rc->workload_group_context()->workload_group());
    }

    void detach_task() {
        resource_ctx.reset();
        thread_mem_tracker_mgr->detach_task();
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
    std::shared_ptr<ResourceContext> resource_ctx;
    int thread_local_handle_count = 0;
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
    throw Exception(
            Status::FatalError("__builtin_unreachable, {}", doris::memory_orphan_check_msg));
}

} // namespace doris
