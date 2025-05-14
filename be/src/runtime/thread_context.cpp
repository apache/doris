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
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris {
class MemTracker;

void AttachTask::init(const std::shared_ptr<ResourceContext>& rc) {
    ThreadLocalHandle::create_thread_local_if_not_exits();
    signal::set_signal_task_id(rc->task_controller()->task_id());
    thread_context()->attach_task(rc);
}

AttachTask::AttachTask(const std::shared_ptr<ResourceContext>& rc) {
    init(rc);
}

AttachTask::AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    // if parameter is `orphan_mem_tracker`, if you do not switch thraed mem tracker afterwards,
    // alloc or free memory from Allocator will fail DCHECK. unless you know for sure that
    // the thread will not alloc or free memory from Allocator later.
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(mem_tracker);
    init(rc);
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
    signal::set_signal_is_nereids(runtime_state->is_nereids());
    init(runtime_state->get_query_ctx()->resource_ctx());
}

AttachTask::AttachTask(QueryContext* query_ctx) {
    init(query_ctx->resource_ctx());
}

AttachTask::~AttachTask() {
    signal::set_signal_task_id(TUniqueId());
    thread_context()->detach_task();
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

SwitchResourceContext::SwitchResourceContext(const std::shared_ptr<ResourceContext>& rc) {
    DCHECK(rc != nullptr);
    doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    DCHECK(thread_context()->is_attach_task());
    old_resource_ctx_ = thread_context()->resource_ctx();
    if (rc != old_resource_ctx_) {
        signal::set_signal_task_id(rc->task_controller()->task_id());
        thread_context()->resource_ctx_ = rc;
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(
                rc->memory_context()->mem_tracker(), rc->workload_group());
    }
}

SwitchResourceContext::~SwitchResourceContext() {
    if (old_resource_ctx_ != thread_context()->resource_ctx()) {
        DCHECK(old_resource_ctx_ != nullptr);
        signal::set_signal_task_id(old_resource_ctx_->task_controller()->task_id());
        thread_context()->resource_ctx_ = old_resource_ctx_;
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker();
    }
    doris::ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

SwitchThreadMemTrackerLimiter::SwitchThreadMemTrackerLimiter(
        const std::shared_ptr<doris::MemTrackerLimiter>& mem_tracker) {
    DCHECK(mem_tracker);
    doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    if (mem_tracker != thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr()) {
        thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker);
        is_switched_ = true;
    }
}

SwitchThreadMemTrackerLimiter::~SwitchThreadMemTrackerLimiter() {
    if (is_switched_) {
        thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker();
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
