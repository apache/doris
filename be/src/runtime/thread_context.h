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

#include <service/brpc_conflict.h>
// After brpc_conflict.h
#include <bthread/bthread.h>

#include <string>
#include <thread>

#include "common/logging.h"
#include "gen_cpp/PaloInternalService_types.h" // for TQueryType
#include "runtime/thread_mem_tracker_mgr.h"
#include "runtime/threadlocal.h"

// Attach to task when thread starts
#define SCOPED_ATTACH_TASK_THREAD(type, ...) \
    auto VARNAME_LINENUM(attach_task_thread) = AttachTaskThread(type, ##__VA_ARGS__)
// Be careful to stop the thread mem tracker, because the actual order of malloc and free memory
// may be different from the order of execution of instructions, which will cause the position of
// the memory track to be unexpected.
#define SCOPED_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = doris::StopThreadMemTracker(true)
#define GLOBAL_STOP_THREAD_LOCAL_MEM_TRACKER() \
    auto VARNAME_LINENUM(stop_tracker) = doris::StopThreadMemTracker(false)
// Switch thread mem tracker during task execution.
// After the non-query thread switches the mem tracker, if the thread will not switch the mem
// tracker again in the short term, can consider manually clear_untracked_mems.
// The query thread will automatically clear_untracked_mems when detach_task.
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = doris::SwitchThreadMemTracker<false>(mem_tracker, false)
#define SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = doris::SwitchThreadMemTracker<false>(mem_tracker, true);
#define SCOPED_SWITCH_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = doris::SwitchThreadMemTracker<true>(mem_tracker, false)
#define SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = doris::SwitchThreadMemTracker<true>(mem_tracker, true)
// After the non-query thread switches the mem tracker, if the thread will not switch the mem
// tracker again in the short term, can consider manually clear_untracked_mems.
// The query thread will automatically clear_untracked_mems when detach_task.
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_END_CLEAR(mem_tracker) \
    auto VARNAME_LINENUM(switch_tracker) = doris::SwitchThreadMemTrackerEndClear(mem_tracker)
#define SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_ERR_CB(action_type, ...) \
    auto VARNAME_LINENUM(witch_tracker_cb) =                            \
            doris::SwitchThreadMemTrackerErrCallBack(action_type, ##__VA_ARGS__)
#define SCOPED_SWITCH_BTHREAD() auto VARNAME_LINENUM(switch_bthread) = SwitchBthread()
// Before switching the same tracker multiple times, add tracker as early as possible,
// and then call `SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER` to reduce one map find.
// For example, in the exec_node open phase `add tracker`, it is no longer necessary to determine
// whether the tracker exists in TLS when switching the tracker in the exec_node get_next phase.
// TODO(zxy): Duplicate add tracker is currently prohibited, because it will,
// 1. waste time 2. `_untracked_mems[mem_tracker->id()] = 0` will cause the memory track to be lost.
// Some places may have to repeat the add tracker. optimize after.
#define ADD_THREAD_LOCAL_MEM_TRACKER(mem_tracker) \
    doris::tls_ctx()->_thread_mem_tracker_mgr->add_tracker(mem_tracker)
#define CONSUME_THREAD_LOCAL_MEM_TRACKER(size) \
    doris::tls_ctx()->_thread_mem_tracker_mgr->noncache_consume(size)
#define RELEASE_THREAD_LOCAL_MEM_TRACKER(size) \
    doris::tls_ctx()->_thread_mem_tracker_mgr->noncache_consume(-size)

namespace doris {

class TUniqueId;

extern bthread_key_t btls_key;

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
        COMPACTION = 3,
        STORAGE = 4
        // to be added ...
    };
    inline static const std::string TaskTypeStr[] = {"UNKNOWN", "QUERY", "LOAD", "COMPACTION",
                                                     "STORAGE"};

public:
    ThreadContext() : _type(TaskType::UNKNOWN) {
        _thread_mem_tracker_mgr.reset(new ThreadMemTrackerMgr());
        _thread_mem_tracker_mgr->init();
        start_thread_mem_tracker = true;
        _thread_id = get_thread_id();
    }

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id,
                const std::shared_ptr<doris::MemTracker>& mem_tracker) {
        DCHECK(_type == TaskType::UNKNOWN && _task_id == "")
                << ",old tracker label: " << mem_tracker->label()
                << ",new tracker label: " << _thread_mem_tracker_mgr->mem_tracker()->label();
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
    const std::string& thread_id_str() const { return _thread_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    std::string get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }

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
    std::string _thread_id;
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

    ThreadContext* get();

private:
    DECLARE_STATIC_THREAD_LOCAL(ThreadContext, thread_local_ctx);
};

inline thread_local ThreadContextPtr thread_local_ctx;

static ThreadContext* tls_ctx() {
    ThreadContext* tls = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
    if (tls != nullptr) {
        return tls;
    } else {
        return thread_local_ctx.get();
    }
}

class AttachTaskThread {
public:
    explicit AttachTaskThread(const ThreadContext::TaskType& type, const std::string& task_id,
                              const TUniqueId& fragment_instance_id = TUniqueId(),
                              const std::shared_ptr<doris::MemTracker>& mem_tracker = nullptr);

    explicit AttachTaskThread(const ThreadContext::TaskType& type,
                              const std::shared_ptr<doris::MemTracker>& mem_tracker);

    explicit AttachTaskThread(const TQueryType::type& query_type,
                              const std::shared_ptr<doris::MemTracker>& mem_tracker);

    explicit AttachTaskThread(const TQueryType::type& query_type,
                              const std::shared_ptr<doris::MemTracker>& mem_tracker,
                              const std::string& task_id, const TUniqueId& fragment_instance_id);

    explicit AttachTaskThread(const RuntimeState* runtime_state,
                              const std::shared_ptr<doris::MemTracker>& mem_tracker);

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

    ~AttachTaskThread();
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
    bool _scope = true;
};

template <bool Existed>
class SwitchThreadMemTracker {
public:
    explicit SwitchThreadMemTracker(const std::shared_ptr<doris::MemTracker>& mem_tracker,
                                    bool in_task = true);

    ~SwitchThreadMemTracker();

protected:
    int64_t _old_tracker_id = 0;
};

class SwitchThreadMemTrackerEndClear : public SwitchThreadMemTracker<false> {
public:
    explicit SwitchThreadMemTrackerEndClear(const std::shared_ptr<doris::MemTracker>& mem_tracker)
            : SwitchThreadMemTracker<false>(mem_tracker, false) {}

    ~SwitchThreadMemTrackerEndClear() {
        tls_ctx()->_thread_mem_tracker_mgr->clear_untracked_mems();
    }
};

class SwitchThreadMemTrackerErrCallBack {
public:
    explicit SwitchThreadMemTrackerErrCallBack(const std::string& action_type,
                                               bool cancel_work = true,
                                               ERRCALLBACK err_call_back_func = nullptr);

    ~SwitchThreadMemTrackerErrCallBack();

private:
    ConsumeErrCallBackInfo _old_tracker_cb;
};

class SwitchBthread {
public:
    explicit SwitchBthread();

    ~SwitchBthread();

private:
    ThreadContext* tls;
};

} // namespace doris
