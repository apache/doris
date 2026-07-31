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
    // Validate the ResourceContext chain before mutating any thread-local
    // or signal state. If any link is null we throw immediately, so the
    // caller's stack-unwind sees a clean state (no thread-local handle is
    // acquired and no signal task id is set). Without these the previous
    // code would silently propagate a null mem_tracker into
    // ThreadMemTrackerMgr and crash much later inside the allocator.
    if (UNLIKELY(rc == nullptr)) {
        throw Exception(
                Status::FatalError("AttachTask::init: rc is null. signal_query_id={:x}-{:x}",
                                   signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->memory_context() == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask::init: rc->memory_context() is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->memory_context()->mem_tracker() == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask::init: rc->memory_context()->mem_tracker() is null. "
                "ResourceContext was created but set_mem_tracker has not been called yet "
                "(likely a half-initialized QueryContext used before _init_query_mem_tracker). "
                "signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->task_controller() == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask::init: rc->task_controller() is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
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
    //
    // Validate before constructing the ResourceContext: MemoryContext::set_mem_tracker()
    // immediately dereferences mem_tracker->limit(), so a null shared_ptr would
    // crash there before reaching init()'s diagnostics.
    if (UNLIKELY(mem_tracker == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask(MemTrackerLimiter): mem_tracker is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(mem_tracker);
    init(rc);
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
    // Walk the chain `runtime_state -> get_query_ctx() -> resource_ctx()`
    // step by step so that an unexpected null pinpoints exactly which link
    // failed instead of crashing with a generic NPE inside attach_task() or
    // even later inside the allocator.
    if (UNLIKELY(runtime_state == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask(RuntimeState*): runtime_state is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(runtime_state->get_query_ctx() == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask(RuntimeState*): runtime_state->get_query_ctx() is null. "
                "signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(runtime_state->get_query_ctx()->resource_ctx() == nullptr)) {
        throw Exception(
                Status::FatalError("AttachTask(RuntimeState*): query_ctx->resource_ctx() is null. "
                                   "signal_query_id={:x}-{:x}",
                                   signal::query_id_hi, signal::query_id_lo));
    }
    signal::set_signal_is_nereids(runtime_state->is_nereids());
    init(runtime_state->get_query_ctx()->resource_ctx());
}

AttachTask::AttachTask(QueryContext* query_ctx) {
    if (UNLIKELY(query_ctx == nullptr)) {
        throw Exception(Status::FatalError(
                "AttachTask(QueryContext*): query_ctx is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    init(query_ctx->resource_ctx());
}

AttachTask::~AttachTask() {
    signal::set_signal_task_id(TUniqueId());
    thread_context()->detach_task();
    ThreadLocalHandle::del_thread_local_if_count_is_zero();
}

SwitchResourceContext::SwitchResourceContext(const std::shared_ptr<ResourceContext>& rc) {
    DCHECK(rc != nullptr);
    // Validate the chain before mutating any thread-local or signal state,
    // symmetric to AttachTask::init(). Throwing after the thread-local
    // handle was acquired or the signal task id was set would skip this
    // object's destructor (because construction failed) and leak the
    // handle / leave a stale signal task id behind.
    if (UNLIKELY(rc == nullptr)) {
        throw Exception(
                Status::FatalError("SwitchResourceContext: rc is null. signal_query_id={:x}-{:x}",
                                   signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->memory_context() == nullptr)) {
        throw Exception(Status::FatalError(
                "SwitchResourceContext: rc->memory_context() is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->memory_context()->mem_tracker() == nullptr)) {
        throw Exception(Status::FatalError(
                "SwitchResourceContext: rc->memory_context()->mem_tracker() is null. "
                "ResourceContext was switched in before _init_query_mem_tracker ran. "
                "signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
    if (UNLIKELY(rc->task_controller() == nullptr)) {
        throw Exception(Status::FatalError(
                "SwitchResourceContext: rc->task_controller() is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
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
    // Third entry point that calls attach_limiter_tracker(). Without this
    // null guard a null mem_tracker silently propagates and the next
    // allocation on this thread would NPE deep inside the allocator. Throw
    // before acquiring the thread-local handle / doing any side effect so
    // the destructor (which is noexcept) never runs in a dirty state.
    if (UNLIKELY(mem_tracker == nullptr)) {
        throw Exception(Status::FatalError(
                "SwitchThreadMemTrackerLimiter: mem_tracker is null. signal_query_id={:x}-{:x}",
                signal::query_id_hi, signal::query_id_lo));
    }
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

} // namespace doris
