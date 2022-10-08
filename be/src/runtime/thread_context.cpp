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

namespace doris {

DEFINE_STATIC_THREAD_LOCAL(ThreadContext, ThreadContextPtr, _ptr);

ThreadContextPtr::ThreadContextPtr() {
    INIT_STATIC_THREAD_LOCAL(ThreadContext, _ptr);
    _init = true;
}

AttachTask::AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                       const ThreadContext::TaskType& type, const std::string& task_id,
                       const TUniqueId& fragment_instance_id) {
#ifndef BE_TEST
    DCHECK(mem_tracker);
#ifdef USE_MEM_TRACKER
    thread_context()->attach_task(type, task_id, fragment_instance_id, mem_tracker);
#endif // USE_MEM_TRACKER
#else
    if (ExecEnv::GetInstance()->new_process_mem_tracker() == nullptr) {
        std::shared_ptr<MemTrackerLimiter> process_mem_tracker =
            std::make_shared<MemTrackerLimiter>(-1, "Process");
        std::shared_ptr<MemTrackerLimiter> _orphan_mem_tracker =
                std::make_shared<MemTrackerLimiter>(-1, "Orphan", process_mem_tracker);
        ExecEnv::GetInstance()->set_global_mem_tracker(process_mem_tracker, _orphan_mem_tracker);
    }
    thread_context()->attach_task(type, task_id, fragment_instance_id, ExecEnv::GetInstance()->orphan_mem_tracker());
#endif // BE_TEST
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
#ifndef BE_TEST
    DCHECK(print_id(runtime_state->query_id()) != "");
    DCHECK(runtime_state->fragment_instance_id() != TUniqueId());
#ifdef USE_MEM_TRACKER
    DCHECK(runtime_state->new_instance_mem_tracker());
    thread_context()->attach_task(
            query_to_task_type(runtime_state->query_type()), print_id(runtime_state->query_id()),
            runtime_state->fragment_instance_id(), runtime_state->new_instance_mem_tracker());
#endif // USE_MEM_TRACKER
#endif // BE_TEST
}

AttachTask::~AttachTask() {
#ifdef USE_MEM_TRACKER
    thread_context()->detach_task();
#endif
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(NewMemTracker* mem_tracker) {
#ifdef USE_MEM_TRACKER
    if (config::memory_verbose_track) {
        thread_context()->_thread_mem_tracker_mgr->push_consumer_tracker(mem_tracker);
    }
#endif // USE_MEM_TRACKER
}

AddThreadMemTrackerConsumer::~AddThreadMemTrackerConsumer() {
#ifdef USE_MEM_TRACKER
    if (config::memory_verbose_track) {
        thread_context()->_thread_mem_tracker_mgr->pop_consumer_tracker();
    }
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
#endif // USE_MEM_TRACKER
}

} // namespace doris
