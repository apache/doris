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

#include <string>
#include <utility>
#include <vector>

#include "exec/pipeline/dependency.h"
#include "exec/runtime_filter/runtime_filter.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "runtime/query_context.h"
#include "util/brpc_closure.h"

namespace doris {

// Callback for sync-size RPCs. Handles errors (disable wrappers + sub dependencies) in call().
class SyncSizeCallback : public DummyBrpcCallback<PSendFilterSizeResponse> {
    ENABLE_FACTORY_CREATOR(SyncSizeCallback);

public:
    // `producers` are the local producers whose sizes are merged into the request, the sender
    // included.
    SyncSizeCallback(std::weak_ptr<QueryContext> context,
                     std::vector<std::weak_ptr<RuntimeFilterProducer>> producers)
            : _context(std::move(context)), _producers(std::move(producers)) {
        DORIS_CHECK(!_producers.empty());
    }

    void call() override {
        // On error: disable the filters and sub the dependencies here, because the merge node may
        // never respond with a sync. It still may, e.g. when it fails to sync another BE, so a
        // producer released here ignores a later set_synced_size().
        // On success: do NOT sub here. The merge node will respond with sync_filter_size,
        // which calls set_synced_size() -> dependency->sub() of each producer.
        if (this->cntl_->Failed()) {
            LOG(WARNING) << fmt::format("RPC meet failed: {}", this->cntl_->ErrorText());
            if (auto ctx = _context.lock()) {
                if (!ctx->ignore_runtime_filter_error()) {
                    ctx->cancel(
                            Status::NetworkError("RPC meet failed: {}", this->cntl_->ErrorText()));
                }
            }
            _release(this->cntl_->ErrorText());
            return;
        }

        Status status = Status::create(this->response_->status());
        if (!status.ok()) {
            LOG(WARNING) << "RPC meet error status: " << status;
            if (auto ctx = _context.lock()) {
                if (!ctx->ignore_runtime_filter_error()) {
                    ctx->cancel(status);
                }
            }
            _release(status.to_string());
        }
    }

private:
    void _release(const std::string& reason) {
        for (const auto& weak_producer : _producers) {
            if (auto producer = weak_producer.lock()) {
                producer->release_on_sync_size_error(reason);
            }
        }
    }

    // input context is always meaningful. will check `ignore_runtime_filter_error` to decide whether to cancel query
    std::weak_ptr<QueryContext> _context;
    // Should use weak ptrs here, because the rpc may return after the query context, which owns
    // the producers, is destroyed.
    std::vector<std::weak_ptr<RuntimeFilterProducer>> _producers;
};
} // namespace doris
