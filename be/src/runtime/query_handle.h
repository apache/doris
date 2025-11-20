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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include <memory>
#include <string>
#include <utility>

#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter_mgr.h"

namespace doris {

class QueryHandle;

// The role of QueryHandle is similar to QueryContext, but its lifecycle is longer than QueryContext.
// QueryHandle will exist until the entire query ends, rather than being released as the current BE task ends like QueryContext.
// It is mainly used to store runtime states that need coordination between BEs, such as the MergeControllerHandler of RuntimeFilter.
// This way, even if the QueryContext of one BE has been released, other BEs can still access these coordination states through QueryHandle to ensure the correctness and consistency of the query.
// QueryContext hold shared_ptr of QueryHandle, and QueryHandle hold weak_ptr of QueryContext to avoid circular references.
class QueryHandle {
public:
    QueryHandle(TUniqueId query_id, TQueryOptions query_options,
                std::weak_ptr<QueryContext> query_ctx)
            : _query_id(std::move(query_id)),
              _query_options(std::move(query_options)),
              _query_ctx(std::move(query_ctx)) {}
    ~QueryHandle() = default;

    std::string debug_string() const {
        return fmt::format(
                "QueryHandle(query_id={}): {}", print_id(_query_id),
                _merge_controller_handler ? _merge_controller_handler->debug_string() : "null");
    }

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }
    std::shared_ptr<RuntimeFilterMergeControllerEntity> get_merge_controller_handler() const {
        return _merge_controller_handler;
    }

    const TQueryOptions& query_options() const { return _query_options; }

    std::weak_ptr<QueryContext> weak_query_ctx() const { return _query_ctx; }

    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }

    TUniqueId query_id() const { return _query_id; }

private:
    TUniqueId _query_id;
    TQueryOptions _query_options;
    std::weak_ptr<QueryContext> _query_ctx;
    // This shared ptr is never used. It is just a reference to hold the object.
    // There is a weak ptr in runtime filter manager to reference this object.
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;
};

} // namespace doris
