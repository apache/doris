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

#include "exec/pipeline/dependency.h"
#include "exec/runtime_filter/runtime_filter.h"
#include "runtime/query_context.h"
#include "util/brpc_closure.h"

namespace doris {

// Callback for sync-size RPCs. Handles errors (disable wrapper + sub dependency) in call().
class SyncSizeCallback : public DummyBrpcCallback<PSendFilterSizeResponse> {
    ENABLE_FACTORY_CREATOR(SyncSizeCallback);

public:
    SyncSizeCallback(std::shared_ptr<Dependency> dependency,
                     std::shared_ptr<RuntimeFilterWrapper> wrapper,
                     std::weak_ptr<QueryContext> context)
            : _dependency(std::move(dependency)), _wrapper(wrapper), _context(std::move(context)) {}

    void call() override {
        // On error: disable the wrapper and sub the dependency here, because set_synced_size()
        // will never be called (the merge node won't respond with a sync).
        // On success: do NOT sub here. The merge node will respond with sync_filter_size,
        // which calls set_synced_size() -> _dependency->sub().
        if (this->cntl_->Failed()) {
            LOG(WARNING) << fmt::format("RPC meet failed: {}", this->cntl_->ErrorText());
            if (auto w = _wrapper.lock()) {
                w->set_state(RuntimeFilterWrapper::State::DISABLED, this->cntl_->ErrorText());
            }
            if (auto ctx = _context.lock()) {
                if (!ctx->ignore_runtime_filter_error()) {
                    ctx->cancel(
                            Status::NetworkError("RPC meet failed: {}", this->cntl_->ErrorText()));
                }
            }
            auto p = std::dynamic_pointer_cast<CountedFinishDependency>(_dependency);
            DORIS_CHECK(p);
            p->sub();
            return;
        }

        Status status = Status::create(this->response_->status());
        if (!status.ok()) {
            LOG(WARNING) << "RPC meet error status: " << status;
            if (auto w = _wrapper.lock()) {
                w->set_state(RuntimeFilterWrapper::State::DISABLED, status.to_string());
            }
            if (auto ctx = _context.lock()) {
                if (!ctx->ignore_runtime_filter_error()) {
                    ctx->cancel(status);
                }
            }
            auto p = std::dynamic_pointer_cast<CountedFinishDependency>(_dependency);
            DORIS_CHECK(p);
            p->sub();
        }
    }

private:
    std::shared_ptr<Dependency> _dependency;
    // Should use weak ptr here, because when query context deconstructs, should also delete runtime filter
    // context, it not the memory is not released. And rpc is in another thread, it will hold rf context
    // after query context because the rpc is not returned.
    std::weak_ptr<RuntimeFilterWrapper> _wrapper;
    // input context is always meaningful. will check `ignore_runtime_filter_error` to decide whether to cancel query
    std::weak_ptr<QueryContext> _context;
};
} // namespace doris
