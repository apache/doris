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

namespace doris {
class MemTracker;

AttachTask::AttachTask(const std::shared_ptr<ResourceContext>& rc) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(rc->task_id());
    thread_context()->attach_task(rc);
}

AttachTask::~AttachTask() {
    thread_context()->detach_task();
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

SwitchThreadMemTrackerLimiter::SwitchThreadMemTrackerLimiter(
            const std::shared_ptr<doris::MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    if (mem_tracker != thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()) {
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker);
    }
}

SwitchThreadMemTrackerLimiter::SwitchThreadMemTrackerLimiter(ResourceContext* rc) {
        doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    DCHECK(rc->memory_context()->memtracker_limiter());
    if (rc->memory_context()->memtracker_limiter() !=
        thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()) {
        _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(
                rc->memory_context()->memtracker_limiter());
    }
}

SwitchThreadMemTrackerLimiter::~SwitchThreadMemTrackerLimiter() {
    if (_old_mem_tracker != nullptr) {
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
    }
    doris::ThreadLocalHandle::del_thread_local_if_count_is_zero();
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
