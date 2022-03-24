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

#include <string>
#include <thread>

#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_mem_tracker_mgr.h"
#include "runtime/threadlocal.h"
#include "util/doris_metrics.h"

// Attach to task when thread starts
#define SCOPED_ATTACH_TASK_THREAD(type, ...) \
    auto VARNAME_LINENUM(attach_task_thread) = AttachTaskThread(type, ##__VA_ARGS__)
// Be careful to stop the thread mem tracker, because the actual order of malloc and free memory
// may be different from the order of execution of instructions, which will cause the position of
// the memory track to be unexpected.
#define SCOPED_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = StopThreadMemTracker(true)
#define GLOBAL_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = StopThreadMemTracker(false)
// Switch thread mem tracker during task execution.
// After the non-query thread switches the mem tracker, if the thread will not switch the mem
// tracker again in the short term, can consider manually clear_untracked_mems.
// The query thread will automatically clear_untracked_mems when detach_task.
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker, false)
#define SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker, true);
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_ERR_CB(action_type, ...) \
    auto VARNAME_LINENUM(witch_tracker_cb) =                            \
            SwitchThreadMemTrackerErrCallBack(action_type, ##__VA_ARGS__)

namespace doris {

class TUniqueId;

// The thread context saves some info about a working thread.
// 2 requried info:
//   1. thread_id:   Current thread id, Auto generated.
//   2. type:        The type is a enum value indicating which type of task current thread is running.
//                   For example: QUERY, LOAD, COMPACTION, ...
//   3. task id:     A unique id to identify this task. maybe query id, load job id, etc.
//
// There may be other optional info to be added later.
class ThreadContext {
public:
    enum TaskType {
        UNKNOWN = 0,
        QUERY = 1,
        LOAD = 2,
        COMPACTION = 3
        // to be added ...
    };
    inline static const std::string TaskTypeStr[] = {"UNKNOWN", "QUERY", "LOAD", "COMPACTION"};

public:
    ThreadContext() : _thread_id(std::this_thread::get_id()), _type(TaskType::UNKNOWN) {
        _thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
        std::stringstream ss;
        ss << _thread_id;
        _thread_id_str = ss.str();
    }

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id,
                const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(_type == TaskType::UNKNOWN && _task_id == "");
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        _thread_mem_tracker_mgr->attach_task(TaskTypeStr[_type], task_id, fragment_instance_id,
                                             mem_tracker);
    }

    void detach() {
        _type = TaskType::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
        _thread_mem_tracker_mgr->detach_task();
    }

    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() const { return _thread_id; }
    const std::string& thread_id_str() const { return _thread_id_str; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    void consume_mem(int64_t size) {
        if (start_thread_mem_tracker) {
            _thread_mem_tracker_mgr->cache_consume(size);
        }
    }

    void release_mem(int64_t size) {
        if (start_thread_mem_tracker) {
            _thread_mem_tracker_mgr->cache_consume(-size);
        }
    }

    // After _thread_mem_tracker_mgr is initialized, the current thread TCMalloc Hook starts to
    // consume/release mem_tracker.
    // Note that the use of shared_ptr will cause a crash. The guess is that there is an
    // intermediate state during the copy construction of shared_ptr. Shared_ptr is not equal
    // to nullptr, but the object it points to is not initialized. At this time, when the memory
    // is released somewhere, the TCMalloc hook is triggered to cause the crash.
    std::unique_ptr<ThreadMemTrackerMgr> _thread_mem_tracker_mgr;

private:
    std::thread::id _thread_id;
    std::string _thread_id_str;
    TaskType _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
};

// Using gcc11 compiles thread_local variable on lower versions of GLIBC will report an error,
// see https://github.com/apache/incubator-doris/pull/7911
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
// Thread-scopedthread local + Class-scoped thread local.
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

    ThreadContext* get();

private:
    DECLARE_STATIC_THREAD_LOCAL(ThreadContext, thread_local_ctx);
};

inline thread_local ThreadContextPtr thread_local_ctx;

class AttachTaskThread {
public:
    explicit AttachTaskThread(const ThreadContext::TaskType& type, const std::string& task_id,
                              const TUniqueId& fragment_instance_id = TUniqueId(),
                              const std::shared_ptr<MemTracker>& mem_tracker = nullptr) {
        DCHECK(task_id != "");
        thread_local_ctx.get()->attach(type, task_id, fragment_instance_id, mem_tracker);
    }

    explicit AttachTaskThread(const ThreadContext::TaskType& type,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(mem_tracker);
        thread_local_ctx.get()->attach(type, "", TUniqueId(), mem_tracker);
    }

    explicit AttachTaskThread(const TQueryType::type& query_type,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(mem_tracker);
        thread_local_ctx.get()->attach(query_to_task_type(query_type), "", TUniqueId(),
                                       mem_tracker);
    }

    explicit AttachTaskThread(const TQueryType::type& query_type, const std::string& task_id,
                              const TUniqueId& fragment_instance_id,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(task_id != "");
        DCHECK(fragment_instance_id != TUniqueId());
        DCHECK(mem_tracker);
        thread_local_ctx.get()->attach(query_to_task_type(query_type), task_id,
                                       fragment_instance_id, mem_tracker);
    }

    explicit AttachTaskThread(const RuntimeState* runtime_state,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
#ifndef BE_TEST
        DCHECK(print_id(runtime_state->query_id()) != "");
        DCHECK(runtime_state->fragment_instance_id() != TUniqueId());
        DCHECK(mem_tracker);
        thread_local_ctx.get()->attach(query_to_task_type(runtime_state->query_type()),
                                       print_id(runtime_state->query_id()),
                                       runtime_state->fragment_instance_id(), mem_tracker);
#endif
    }

    const ThreadContext::TaskType query_to_task_type(const TQueryType::type& query_type) {
        switch (query_type) {
        case TQueryType::SELECT:
            return ThreadContext::TaskType::QUERY;
        case TQueryType::LOAD:
            return ThreadContext::TaskType::LOAD;
        default:
            DCHECK(false);
            return ThreadContext::TaskType::UNKNOWN;
        }
    }

    ~AttachTaskThread() {
#ifndef BE_TEST
        thread_local_ctx.get()->detach();
        DorisMetrics::instance()->attach_task_thread_count->increment(1);
#endif
    }
};

class StopThreadMemTracker {
public:
    explicit StopThreadMemTracker(const bool scope = true) : _scope(scope) {
        start_thread_mem_tracker = false;
    }

    ~StopThreadMemTracker() {
        if (_scope == true) start_thread_mem_tracker = true;
    }

private:
    bool _scope;
};

class SwitchThreadMemTracker {
public:
    explicit SwitchThreadMemTracker(const std::shared_ptr<MemTracker>& mem_tracker,
                                    bool in_task = true) {
#ifndef BE_TEST
        DCHECK(mem_tracker);
        // The thread tracker must be switched after the attach task, otherwise switching
        // in the main thread will cause the cached tracker not be cleaned up in time.
        DCHECK(in_task == false ||
               thread_local_ctx.get()->_thread_mem_tracker_mgr->is_attach_task());
        _old_tracker_id =
                thread_local_ctx.get()->_thread_mem_tracker_mgr->update_tracker(mem_tracker);
#endif
    }

    ~SwitchThreadMemTracker() {
#ifndef BE_TEST
        thread_local_ctx.get()->_thread_mem_tracker_mgr->update_tracker_id(_old_tracker_id);
        DorisMetrics::instance()->switch_thread_mem_tracker_count->increment(1);
#endif
    }

private:
    std::string _old_tracker_id;
};

class SwitchThreadMemTrackerErrCallBack {
public:
    explicit SwitchThreadMemTrackerErrCallBack(const std::string& action_type,
                                               bool cancel_work = true,
                                               ERRCALLBACK err_call_back_func = nullptr) {
        DCHECK(action_type != std::string());
        _old_tracker_cb = thread_local_ctx.get()->_thread_mem_tracker_mgr->update_consume_err_cb(
                action_type, cancel_work, err_call_back_func);
    }

    ~SwitchThreadMemTrackerErrCallBack() {
        thread_local_ctx.get()->_thread_mem_tracker_mgr->update_consume_err_cb(_old_tracker_cb);
        DorisMetrics::instance()->switch_thread_mem_tracker_err_cb_count->increment(1);
    }

private:
    ConsumeErrCallBackInfo _old_tracker_cb;
};

} // namespace doris
