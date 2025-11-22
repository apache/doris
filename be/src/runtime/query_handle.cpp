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

#include "runtime/query_handle.h"

#include "runtime/query_context.h"

namespace doris {

QueryHandle::QueryHandle(TUniqueId query_id, TQueryOptions query_options,
                         std::weak_ptr<QueryContext> query_ctx)
        : _query_id(std::move(query_id)),
          _query_options(std::move(query_options)),
          _query_ctx(std::move(query_ctx)) {
    CHECK(_query_ctx.lock() != nullptr);
    _resource_ctx = _query_ctx.lock()->_resource_ctx;
}

QueryHandle::~QueryHandle() = default;

std::string QueryHandle::debug_string() const {
    return fmt::format(
            "QueryHandle(query_id={}): {}", print_id(_query_id),
            _merge_controller_handler ? _merge_controller_handler->debug_string() : "null");
}

void QueryHandle::set_merge_controller_handler(
        std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
    _merge_controller_handler = handler;
}
std::shared_ptr<RuntimeFilterMergeControllerEntity> QueryHandle::get_merge_controller_handler()
        const {
    return _merge_controller_handler;
}

const TQueryOptions& QueryHandle::query_options() const {
    return _query_options;
}

std::weak_ptr<QueryContext> QueryHandle::weak_query_ctx() const {
    return _query_ctx;
}

int QueryHandle::execution_timeout() const {
    return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                    : _query_options.query_timeout;
}

TUniqueId QueryHandle::query_id() const {
    return _query_id;
}

std::shared_ptr<ResourceContext> QueryHandle::resource_ctx() const {
    return _resource_ctx;
}

} // namespace doris
