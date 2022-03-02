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

#include "runtime/thread_mem_tracker_mgr.h"
#include "runtime/threadlocal.h"

// Attach to task when thread starts
#define SCOPED_ATTACH_TASK_THREAD_2ARG(type, mem_tracker) \
    auto VARNAME_LINENUM(attach_task_thread) = AttachTaskThread(type, mem_tracker)
#define SCOPED_ATTACH_TASK_THREAD_4ARG(query_type, task_id, fragment_instance_id, mem_tracker) \
    auto VARNAME_LINENUM(attach_task_thread) =                                                 \
            AttachTaskThread(query_type, task_id, fragment_instance_id, mem_tracker)
#define SCOPED_ATTACH_TASK_THREAD_4ARGP(query_type, task_id, fragment_instance_id, mem_tracker) \
    auto VARNAME_LINENUM(attach_task_thread) =                                                 \
            AttachTaskThreadP(query_type, task_id, fragment_instance_id, mem_tracker)
// Toggle MemTracker during thread execution
// 必须在 SCOPED_ATTACH_TASK_THREAD 中切换，否则可能导致缓存的mem tracker不被释放
// Must-see Notes: 不能频繁 SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER 和 SCOPED_STOP_THREAD_LOCAL_MEM_TRACKER，
// 因为变量的创建和析构顺序，以及内存的申请和释放顺序可能和指令的执行顺序不同（待进一步调研），这可能导致内存统计的tracker或位置不符合预期;
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_1ARG(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_1ARGP(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTrackerP(mem_tracker)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_2ARG(mem_tracker, action_type)                  \
    do {                                                                                       \
        auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker);            \
        auto VARNAME_LINENUM(switch_tracker_cb) = SwitchThreadMemTrackerCallBack(action_type); \
    } while (false)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_3ARG(mem_tracker, action_type, cancel_work) \
    do {                                                                                   \
        auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker);        \
        auto VARNAME_LINENUM(switch_tracker_cb) =                                          \
                SwitchThreadMemTrackerCallBack(action_type, cancel_work);                  \
    } while (false)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_4ARG(mem_tracker, action_type, cancel_work,    \
                                                    err_call_back_func)                       \
    do {                                                                                      \
        auto VARNAME_LINENUM(switch_tracker) = SwitchThreadMemTracker(mem_tracker);           \
        auto VARNAME_LINENUM(switch_tracker_cb) =                                             \
                SwitchThreadMemTrackerCallBack(action_type, cancel_work, err_call_back_func); \
    } while (false)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_CB(action_type) \
    auto VARNAME_LINENUM(switch_tracker_cb) = SwitchThreadMemTrackerCallBack(action_type)
#define SCOPED_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = StopThreadMemTracker(true)
#define GLOBAL_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = StopThreadMemTracker(false)
#define CHECK_MEM_LIMIT(size) thread_local_ctx.get()->_thread_mem_tracker_mgr->mem_tracker()->check_limit(size)

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

public:
    ThreadContext() : _thread_id(std::this_thread::get_id()), _type(TaskType::UNKNOWN) {
        _thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
    }

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id, std::shared_ptr<MemTracker> mem_tracker) {
        DCHECK(_type == TaskType::UNKNOWN && _task_id == "");
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        _thread_mem_tracker_mgr->attach_task(get_type(), task_id, fragment_instance_id,
                                             mem_tracker);
    }

    void detach() {
        _type = TaskType::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
        _thread_mem_tracker_mgr->detach_task();
    }

    const std::string get_type() const;
    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() const { return _thread_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    void consume_mem(int64_t size) {
        if (thread_mem_tracker_mgr_init == true) {
            _thread_mem_tracker_mgr->cache_consume(size);
        }
    }

    void release_mem(int64_t size) {
        if (thread_mem_tracker_mgr_init == true) {
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
    // ThreadMemTrackerMgr* _thread_mem_tracker_mgr;

private:
    std::thread::id _thread_id;
    TaskType _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
};

// inline thread_local ThreadContext thread_local_ctx;
// inline BLOCK_STATIC_THREAD_LOCAL2(ThreadContext, thread_local_ctx);

// static ThreadContext* load_tls() {
//     thread_mem_tracker_mgr_init = false;
//     BLOCK_STATIC_THREAD_LOCAL(ThreadContext, ctx2);
//     thread_mem_tracker_mgr_init = true;
//     return ctx2;
// }

class ThreadContextPtr {
public:
    ThreadContextPtr();

    ThreadContext* get();
private:
    DECLARE_STATIC_THREAD_LOCAL(ThreadContext, thread_local_ctx);
};

inline thread_local ThreadContextPtr thread_local_ctx;

inline const std::string task_type_string(ThreadContext::TaskType type) {
    switch (type) {
    case ThreadContext::TaskType::QUERY:
        return "QUERY";
    case ThreadContext::TaskType::LOAD:
        return "LOAD";
    case ThreadContext::TaskType::COMPACTION:
        return "COMPACTION";
    default:
        return "UNKNOWN";
    }
}

inline const std::string ThreadContext::get_type() const {
    return task_type_string(_type);
}

class AttachTaskThread {
public:
    explicit AttachTaskThread(const ThreadContext::TaskType& type,
                              const std::shared_ptr<MemTracker> mem_tracker) {
        DCHECK(mem_tracker != nullptr);
        init(type, "", TUniqueId(), mem_tracker);
    }

    explicit AttachTaskThread(const TQueryType::type& query_type, const std::string& task_id,
                              const TUniqueId& fragment_instance_id,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(task_id != "" && fragment_instance_id != TUniqueId() && mem_tracker != nullptr);
        if (query_type == TQueryType::SELECT) {
            init(ThreadContext::TaskType::QUERY, task_id, fragment_instance_id, mem_tracker);
        } else if (query_type == TQueryType::LOAD) {
            init(ThreadContext::TaskType::LOAD, task_id, fragment_instance_id, mem_tracker);
        }
    }

    void init(const ThreadContext::TaskType& type, const std::string& task_id,
              const TUniqueId& fragment_instance_id,
              const std::shared_ptr<MemTracker>& mem_tracker) {
        // thread_local_ctx.get()->attach(type, task_id, fragment_instance_id, mem_tracker);
    }

    // ~AttachTaskThread() { thread_local_ctx.get()->detach(); }
};

class AttachTaskThreadP {
public:
    explicit AttachTaskThreadP(const ThreadContext::TaskType& type,
                              const std::shared_ptr<MemTracker> mem_tracker) {
        DCHECK(mem_tracker != nullptr);
        init(type, "", TUniqueId(), mem_tracker);
    }

    explicit AttachTaskThreadP(const TQueryType::type& query_type, const std::string& task_id,
                              const TUniqueId& fragment_instance_id,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(task_id != "" && fragment_instance_id != TUniqueId() && mem_tracker != nullptr);
        if (query_type == TQueryType::SELECT) {
            init(ThreadContext::TaskType::QUERY, task_id, fragment_instance_id, mem_tracker);
        } else if (query_type == TQueryType::LOAD) {
            init(ThreadContext::TaskType::LOAD, task_id, fragment_instance_id, mem_tracker);
        }
    }

    void init(const ThreadContext::TaskType& type, const std::string& task_id,
              const TUniqueId& fragment_instance_id,
              const std::shared_ptr<MemTracker>& mem_tracker) {
        // thread_local_ctx.get()->attach(type, task_id, fragment_instance_id, mem_tracker);
    }

    // ~AttachTaskThreadP() { thread_local_ctx.get()->detach(); }
};

class SwitchThreadMemTracker {
public:
    explicit SwitchThreadMemTracker(const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(mem_tracker != nullptr);
        // _old_tracker_id = thread_local_ctx.get()->_thread_mem_tracker_mgr->update_tracker(mem_tracker);
    }

    ~SwitchThreadMemTracker() {
        // thread_local_ctx.get()->_thread_mem_tracker_mgr->set_tracker_id(_old_tracker_id);
    }

private:
    std::string _old_tracker_id;
};

class SwitchThreadMemTrackerP {
public:
    explicit SwitchThreadMemTrackerP(const std::shared_ptr<MemTracker>& mem_tracker) {
        DCHECK(mem_tracker != nullptr);
        // _old_tracker_id = thread_local_ctx.get()->_thread_mem_tracker_mgr->update_trackerP(mem_tracker);
    }

    ~SwitchThreadMemTrackerP() {
        // thread_local_ctx.get()->_thread_mem_tracker_mgr->set_tracker_idP(_old_tracker_id);
    }

private:
    std::string _old_tracker_id;
};

class SwitchThreadMemTrackerCallBack {
public:
    explicit SwitchThreadMemTrackerCallBack(const std::string& action_type) {
        DCHECK(action_type != std::string());
        init(action_type);
    }

    explicit SwitchThreadMemTrackerCallBack(const std::string& action_type, bool cancel_work) {
        DCHECK(action_type != std::string());
        init(action_type, cancel_work);
    }

    explicit SwitchThreadMemTrackerCallBack(const std::string& action_type, bool cancel_work,
                                            ERRCALLBACK err_call_back_func) {
        DCHECK(action_type != std::string() && err_call_back_func != nullptr);
        init(action_type, cancel_work, err_call_back_func);
    }

    void init(const std::string& action_type = std::string(), bool cancel_work = true,
              ERRCALLBACK err_call_back_func = nullptr) {
        _old_tracker_call_back = thread_local_ctx.get()->_thread_mem_tracker_mgr->update_consume_err_call_back(
                action_type, cancel_work, err_call_back_func);
    }

    ~SwitchThreadMemTrackerCallBack() {
        thread_local_ctx.get()->_thread_mem_tracker_mgr->update_consume_err_call_back(_old_tracker_call_back);
    }

private:
    ConsumeErrCallBackInfo _old_tracker_call_back;
};

class StopThreadMemTracker {
public:
    explicit StopThreadMemTracker(const bool scope = true) : _scope(scope) {
        thread_mem_tracker_mgr_init = false;
    }

    ~StopThreadMemTracker() {
        if (_scope == true) thread_mem_tracker_mgr_init = true;
    }

private:
    bool _scope;
};

} // namespace doris
