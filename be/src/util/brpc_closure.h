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

#include <google/protobuf/stubs/common.h>

#include <type_traits>
#include <utility>

#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "service/brpc.h" // IWYU pragma: keep

namespace doris {

template <typename T>
concept HasStatus = requires(T* response) { response->status(); };

template <typename Response>
class DummyBrpcCallback {
    ENABLE_FACTORY_CREATOR(DummyBrpcCallback);

public:
    using ResponseType = Response;
    DummyBrpcCallback() {
        cntl_ = std::make_shared<brpc::Controller>();
        call_id_ = cntl_->call_id();
        response_ = std::make_shared<Response>();
    }

    virtual ~DummyBrpcCallback() = default;

    virtual void call() {}

    virtual void join() { brpc::Join(call_id_); }

    // according to brpc doc, we MUST save the call_id before rpc done. use this id to join.
    // if a rpc is already done then we get the id and join, it's wrong.
    brpc::CallId call_id_;
    // controller has to be the same lifecycle with the closure, because brpc may use
    // it in any stage of the rpc.
    std::shared_ptr<brpc::Controller> cntl_;
    // We do not know if brpc will use request or response after brpc method returns.
    // So that we need keep a shared ptr here to ensure that brpc could use req/rep
    // at any stage.
    std::shared_ptr<Response> response_;
};

template <typename Response>
class HandleErrorBrpcCallback : public DummyBrpcCallback<Response> {
    ENABLE_FACTORY_CREATOR(HandleErrorBrpcCallback);

public:
    using ResponseType = Response;
    // input context must be held by caller
    HandleErrorBrpcCallback(std::weak_ptr<QueryContext> context = {})
            : _context(std::move(context)) {}

    ~HandleErrorBrpcCallback() override = default;

    void call() override {
        if (this->cntl_->Failed()) {
            LOG(WARNING) << fmt::format("RPC meet failed: {}", this->cntl_->ErrorText());
            if (auto ctx = _context.lock()) {
                ctx->cancel(Status::NetworkError("RPC meet failed: {}", this->cntl_->ErrorText()));
            }
            return;
        }
        if constexpr (HasStatus<Response>) {
            if (Status status = Status::create(this->response_->status()); !status.ok()) {
                if (!status.is<ErrorCode::END_OF_FILE>()) {
                    LOG(WARNING) << "RPC meet error status: " << status;
                    if (auto ctx = _context.lock()) {
                        ctx->cancel(std::move(status));
                    }
                }
            }
        }
    }

private:
    std::weak_ptr<QueryContext> _context;
};

// The closure will be deleted after callback.
// It could only be created by using shared ptr or unique ptr.
// Example:
//  std::unique_ptr<AutoReleaseClosure> a(b);
//  brpc_call(a.release());
// the closure doesn't own the callback, so the callback MUST be kept alive outside.
// closure only keep a weak ref. if outside owner destroyed (like query finish), the callback will be ignored.
template <typename Request, typename Callback>
class AutoReleaseClosure : public google::protobuf::Closure {
    using ResponseType = typename Callback::ResponseType;
    ENABLE_FACTORY_CREATOR(AutoReleaseClosure);

public:
    AutoReleaseClosure(std::shared_ptr<Request> req, std::shared_ptr<Callback> callback)
            : request_(std::move(req)), callback_(callback) {
        this->cntl_ = callback->cntl_;
        this->response_ = callback->response_;
    }

    ~AutoReleaseClosure() override = default;

    // Will delete itself. all operations should be done in callback's call(). Run() only do one thing.
    void Run() override {
        Defer defer {[&]() { delete this; }};
        if (auto tmp = callback_.lock()) {
            tmp->call();
        }
    }

    // controller has to be the same lifecycle with the closure, because brpc may use
    // it in any stage of the rpc.
    std::shared_ptr<brpc::Controller> cntl_;
    // We do not know if brpc will use request or response after brpc method returns.
    // So that we need keep a shared ptr here to ensure that brpc could use req/rep
    // at any stage.
    std::shared_ptr<Request> request_;
    std::shared_ptr<ResponseType> response_;

private:
    std::weak_ptr<Callback> callback_;
};

} // namespace doris
