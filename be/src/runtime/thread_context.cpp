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

#include "common/signal_handler.h"
#include "runtime/runtime_state.h"

namespace doris {
class MemTracker;

QueryThreadContext ThreadContext::query_thread_context() {
    DCHECK(doris::pthread_context_ptr_init);
    ORPHAN_TRACKER_CHECK();
    return {_task_id, thread_mem_tracker_mgr->limiter_mem_tracker()};
}

AttachTask::AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                       const TUniqueId& task_id) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(task_id);
    thread_context()->attach_task(task_id, mem_tracker);
}

AttachTask::AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(TUniqueId());
    thread_context()->attach_task(TUniqueId(), mem_tracker);
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(runtime_state->query_id());
    signal::set_signal_is_nereids(runtime_state->is_nereids());
    thread_context()->attach_task(runtime_state->query_id(), runtime_state->query_mem_tracker());
}

AttachTask::AttachTask(const QueryThreadContext& query_thread_context) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(query_thread_context.query_id);
    thread_context()->attach_task(query_thread_context.query_id,
                                  query_thread_context.query_mem_tracker);
}

AttachTask::~AttachTask() {
    thread_context()->detach_task();
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(MemTracker* mem_tracker) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    if (mem_tracker) {
        _need_pop = thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(mem_tracker);
    }
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(
        const std::shared_ptr<MemTracker>& mem_tracker)
        : _mem_tracker(mem_tracker) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    if (_mem_tracker) {
        _need_pop =
                thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(_mem_tracker.get());
    }
}

AddThreadMemTrackerConsumer::~AddThreadMemTrackerConsumer() {
    if (_need_pop) {
        thread_context()->thread_mem_tracker_mgr->pop_consumer_tracker();
    }
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

AddThreadMemTrackerConsumerByHook::AddThreadMemTrackerConsumerByHook(
        const std::shared_ptr<MemTracker>& mem_tracker)
        : _mem_tracker(mem_tracker) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    DCHECK(mem_tracker != nullptr);
    use_mem_hook = true;
    thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(_mem_tracker.get());
}

AddThreadMemTrackerConsumerByHook::~AddThreadMemTrackerConsumerByHook() {
    thread_context()->thread_mem_tracker_mgr->pop_consumer_tracker();
    use_mem_hook = false;
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

} // namespace doris
