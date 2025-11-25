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

#include "runtime/coordinator_context.h"

#include "runtime/query_context.h"

namespace doris {

CoordinatorContext::CoordinatorContext(TUniqueId query_id, TQueryOptions query_options,
                                       std::weak_ptr<QueryContext> query_ctx)
        : _query_id(std::move(query_id)),
          _query_options(std::move(query_options)),
          _query_ctx(std::move(query_ctx)),
          _timeout_second(query_options.execution_timeout) {
    _query_watcher.start();
    CHECK(_query_ctx.lock() != nullptr);
    _resource_ctx = _query_ctx.lock()->_resource_ctx;
}

CoordinatorContext::~CoordinatorContext() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker());
    // query mem tracker consumption is equal to 0, it means that after QueryContext is created,
    // it is found that query already exists in _query_ctx_map, and query mem tracker is not used.
    // query mem tracker consumption is not equal to 0 after use, because there is memory consumed
    // on query mem tracker, released on other trackers.
    std::string mem_tracker_msg;
    if (query_mem_tracker()->peak_consumption() != 0) {
        mem_tracker_msg = fmt::format(
                "deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                "PeakUsed={}",
                print_id(_query_id), PrettyPrinter::print_bytes(query_mem_tracker()->limit()),
                PrettyPrinter::print_bytes(query_mem_tracker()->consumption()),
                PrettyPrinter::print_bytes(query_mem_tracker()->peak_consumption()));
    }
    [[maybe_unused]] uint64_t group_id = 0;
    if (_resource_ctx->workload_group()) {
        group_id = _resource_ctx->workload_group()->id(); // before remove
    }

    _resource_ctx->task_controller()->finish();

    // the only one msg shows query's end. any other msg should append to it if need.
    LOG_INFO("Query {} deconstructed, mem_tracker: {}", print_id(this->_query_id), mem_tracker_msg);

#ifndef BE_TEST
    if (ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) [[unlikely]] {
        try {
            ExecEnv::GetInstance()->pipeline_tracer_context()->end_query(_query_id, group_id);
        } catch (std::exception& e) {
            LOG(WARNING) << "Dump trace log failed bacause " << e.what();
        }
    }
#endif
    _resource_ctx->memory_context()->mem_tracker();
    _merge_controller_handler.reset();
}

std::string CoordinatorContext::debug_string() const {
    return fmt::format(
            "CoordinatorContext(query_id={}): {}", print_id(_query_id),
            _merge_controller_handler ? _merge_controller_handler->debug_string() : "null");
}

void CoordinatorContext::set_merge_controller_handler(
        std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
    _merge_controller_handler = handler;
}
std::shared_ptr<RuntimeFilterMergeControllerEntity>
CoordinatorContext::get_merge_controller_handler() const {
    return _merge_controller_handler;
}

const TQueryOptions& CoordinatorContext::query_options() const {
    return _query_options;
}

std::weak_ptr<QueryContext> CoordinatorContext::weak_query_ctx() const {
    return _query_ctx;
}

int CoordinatorContext::execution_timeout() const {
    return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                    : _query_options.query_timeout;
}

TUniqueId CoordinatorContext::query_id() const {
    return _query_id;
}

std::shared_ptr<ResourceContext> CoordinatorContext::resource_ctx() const {
    return _resource_ctx;
}

std::shared_ptr<MemTrackerLimiter> CoordinatorContext::query_mem_tracker() const {
    DCHECK(_resource_ctx->memory_context()->mem_tracker() != nullptr);
    return _resource_ctx->memory_context()->mem_tracker();
}

} // namespace doris
