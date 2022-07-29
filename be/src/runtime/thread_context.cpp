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

DEFINE_STATIC_THREAD_LOCAL(ThreadContext, ThreadContextPtr, _ptr);

ThreadContextPtr::ThreadContextPtr() {
    INIT_STATIC_THREAD_LOCAL(ThreadContext, _ptr);
    _init = true;
}

AttachTask::AttachTask(MemTrackerLimiter* mem_tracker, const ThreadContext::TaskType& type,
                       const std::string& task_id, const TUniqueId& fragment_instance_id) {
    DCHECK(mem_tracker);
#ifdef USE_MEM_TRACKER
    thread_context()->attach_task(type, task_id, fragment_instance_id, mem_tracker);
#endif
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
#ifndef BE_TEST
    DCHECK(print_id(runtime_state->query_id()) != "");
    DCHECK(runtime_state->fragment_instance_id() != TUniqueId());
#endif // BE_TEST
    DCHECK(runtime_state->instance_mem_tracker());
#ifdef USE_MEM_TRACKER
    thread_context()->attach_task(
            query_to_task_type(runtime_state->query_type()), print_id(runtime_state->query_id()),
            runtime_state->fragment_instance_id(), runtime_state->instance_mem_tracker());
#endif // USE_MEM_TRACKER
}

AttachTask::~AttachTask() {
#ifdef USE_MEM_TRACKER
    thread_context()->detach_task();
#ifndef NDEBUG
    DorisMetrics::instance()->attach_task_thread_count->increment(1);
#endif // NDEBUG
#endif
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(MemTracker* mem_tracker) {
#ifdef USE_MEM_TRACKER
    if (config::memory_verbose_track) {
        thread_context()->_thread_mem_tracker_mgr->push_consumer_tracker(mem_tracker);
    }
#endif // USE_MEM_TRACKER
}

AddThreadMemTrackerConsumer::~AddThreadMemTrackerConsumer() {
#ifdef USE_MEM_TRACKER
    if (config::memory_verbose_track) {
#ifndef NDEBUG
        DorisMetrics::instance()->add_thread_mem_tracker_consumer_count->increment(1);
#endif // NDEBUG
        thread_context()->_thread_mem_tracker_mgr->pop_consumer_tracker();
    }
#endif // USE_MEM_TRACKER
}

UpdateMemExceedCallBack::UpdateMemExceedCallBack(const std::string& cancel_msg, bool cancel_task,
                                                 ExceedCallBack cb_func) {
#ifdef USE_MEM_TRACKER
    DCHECK(cancel_msg != std::string());
    _old_cb = thread_context()->_thread_mem_tracker_mgr->update_exceed_call_back(
            cancel_msg, cancel_task, cb_func);
#endif
}

UpdateMemExceedCallBack::~UpdateMemExceedCallBack() {
#ifdef USE_MEM_TRACKER
    thread_context()->_thread_mem_tracker_mgr->update_exceed_call_back(_old_cb);
#ifndef NDEBUG
    DorisMetrics::instance()->thread_mem_tracker_exceed_call_back_count->increment(1);
#endif
#endif // USE_MEM_TRACKER
}

SwitchBthread::SwitchBthread() {
#ifdef USE_MEM_TRACKER
    _bthread_context = static_cast<ThreadContext*>(bthread_getspecific(btls_key));
    // First call to bthread_getspecific (and before any bthread_setspecific) returns NULL
    if (_bthread_context == nullptr) {
        // Create thread-local data on demand.
        _bthread_context = new ThreadContext;
        // set the data so that next time bthread_getspecific in the thread returns the data.
        CHECK_EQ(0, bthread_setspecific(btls_key, _bthread_context));
    } else {
        DCHECK(_bthread_context->type() == ThreadContext::TaskType::UNKNOWN);
        _bthread_context->_thread_mem_tracker_mgr->flush_untracked_mem<false>();
    }
    _bthread_context->_thread_mem_tracker_mgr->init();
    _bthread_context->set_type(ThreadContext::TaskType::BRPC);
    bthread_context_key = btls_key;
    bthread_context = _bthread_context;
#endif
}

SwitchBthread::~SwitchBthread() {
#ifdef USE_MEM_TRACKER
    DCHECK(_bthread_context != nullptr);
    _bthread_context->_thread_mem_tracker_mgr->flush_untracked_mem<false>();
    _bthread_context->_thread_mem_tracker_mgr->init();
    _bthread_context->set_type(ThreadContext::TaskType::UNKNOWN);
    bthread_context = nullptr;
    bthread_context_key = EMPTY_BTLS_KEY;
#ifndef NDEBUG
    DorisMetrics::instance()->switch_bthread_count->increment(1);
#endif // NDEBUG
#endif // USE_MEM_TRACKER
}

} // namespace doris
