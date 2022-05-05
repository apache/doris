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

#include "runtime/thread_context.h"

#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_STATIC_THREAD_LOCAL(ThreadContext, ThreadContextPtr, thread_local_ctx);

ThreadContextPtr::ThreadContextPtr() {
    INIT_STATIC_THREAD_LOCAL(ThreadContext, thread_local_ctx);
}

ThreadContext* ThreadContextPtr::get() {
    return thread_local_ctx;
}

AttachTaskThread::AttachTaskThread(const ThreadContext::TaskType& type, const std::string& task_id,
                                   const TUniqueId& fragment_instance_id,
                                   const std::shared_ptr<doris::MemTracker>& mem_tracker) {
    DCHECK(task_id != "");
    tls_ctx()->attach(type, task_id, fragment_instance_id, mem_tracker);
}

AttachTaskThread::AttachTaskThread(const ThreadContext::TaskType& type,
                                   const std::shared_ptr<doris::MemTracker>& mem_tracker) {
#ifndef BE_TEST
    DCHECK(mem_tracker);
#endif
    tls_ctx()->attach(type, "", TUniqueId(), mem_tracker);
}

AttachTaskThread::AttachTaskThread(const TQueryType::type& query_type,
                                   const std::shared_ptr<doris::MemTracker>& mem_tracker) {
#ifndef BE_TEST
    DCHECK(mem_tracker);
#endif
    tls_ctx()->attach(query_to_task_type(query_type), "", TUniqueId(), mem_tracker);
}

AttachTaskThread::AttachTaskThread(const TQueryType::type& query_type,
                                   const std::shared_ptr<doris::MemTracker>& mem_tracker,
                                   const std::string& task_id,
                                   const TUniqueId& fragment_instance_id) {
#ifndef BE_TEST
    DCHECK(task_id != "");
    DCHECK(fragment_instance_id != TUniqueId());
    DCHECK(mem_tracker);
#endif
    tls_ctx()->attach(query_to_task_type(query_type), task_id, fragment_instance_id, mem_tracker);
}

AttachTaskThread::AttachTaskThread(const RuntimeState* runtime_state,
                                   const std::shared_ptr<doris::MemTracker>& mem_tracker) {
#ifndef BE_TEST
    DCHECK(print_id(runtime_state->query_id()) != "");
    DCHECK(runtime_state->fragment_instance_id() != TUniqueId());
    DCHECK(mem_tracker);
#endif
    tls_ctx()->attach(query_to_task_type(runtime_state->query_type()),
                      print_id(runtime_state->query_id()), runtime_state->fragment_instance_id(),
                      mem_tracker);
}

AttachTaskThread::~AttachTaskThread() {
    tls_ctx()->detach();
    DorisMetrics::instance()->attach_task_thread_count->increment(1);
}

template <bool Existed>
SwitchThreadMemTracker<Existed>::SwitchThreadMemTracker(
        const std::shared_ptr<doris::MemTracker>& mem_tracker, bool in_task) {
    if (config::memory_verbose_track) {
#ifndef BE_TEST
        DCHECK(mem_tracker);
        // The thread tracker must be switched after the attach task, otherwise switching
        // in the main thread will cause the cached tracker not be cleaned up in time.
        DCHECK(in_task == false || tls_ctx()->_thread_mem_tracker_mgr->is_attach_task());
        if (Existed) {
            _old_tracker_id = tls_ctx()->_thread_mem_tracker_mgr->update_tracker<true>(mem_tracker);
        } else {
            _old_tracker_id =
                    tls_ctx()->_thread_mem_tracker_mgr->update_tracker<false>(mem_tracker);
        }
#endif
#ifndef NDEBUG
        tls_ctx()->_thread_mem_tracker_mgr->switch_count += 1;
#endif
    }
}

template <bool Existed>
SwitchThreadMemTracker<Existed>::~SwitchThreadMemTracker() {
    if (config::memory_verbose_track) {
#ifndef NDEBUG
        tls_ctx()->_thread_mem_tracker_mgr->switch_count -= 1;
        DorisMetrics::instance()->switch_thread_mem_tracker_count->increment(1);
#endif
#ifndef BE_TEST
        tls_ctx()->_thread_mem_tracker_mgr->update_tracker_id(_old_tracker_id);
#endif
    }
}

SwitchThreadMemTrackerErrCallBack::SwitchThreadMemTrackerErrCallBack(
        const std::string& action_type, bool cancel_work, ERRCALLBACK err_call_back_func) {
    DCHECK(action_type != std::string());
    _old_tracker_cb = tls_ctx()->_thread_mem_tracker_mgr->update_consume_err_cb(
            action_type, cancel_work, err_call_back_func);
}

SwitchThreadMemTrackerErrCallBack::~SwitchThreadMemTrackerErrCallBack() {
    tls_ctx()->_thread_mem_tracker_mgr->update_consume_err_cb(_old_tracker_cb);
    DorisMetrics::instance()->switch_thread_mem_tracker_err_cb_count->increment(1);
}

SwitchBthread::SwitchBthread() {
    tls = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
    // First call to bthread_getspecific (and before any bthread_setspecific) returns NULL
    if (tls == nullptr) {
        // Create thread-local data on demand.
        tls = new ThreadContext;
        tls->_thread_mem_tracker_mgr->init_bthread();
        // set the data so that next time bthread_getspecific in the thread returns the data.
        CHECK_EQ(0, bthread_setspecific(btls_key, tls));
    } else {
        tls->_thread_mem_tracker_mgr->init_bthread();
    }
}

SwitchBthread::~SwitchBthread() {
    DCHECK(tls != nullptr);
    tls->_thread_mem_tracker_mgr->clear_untracked_mems();
#ifndef NDEBUG
    DorisMetrics::instance()->switch_bthread_count->increment(1);
#endif
}

template class SwitchThreadMemTracker<true>;
template class SwitchThreadMemTracker<false>;

} // namespace doris
