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

#include <atomic>

#include "runtime/thread_context.h"
#include "service/brpc.h"

namespace doris {

template <typename Response>
class DummyBrpcCallback {
    ENABLE_FACTORY_CREATOR(DummyBrpcCallback);

public:
    using ResponseType = Response;
    DummyBrpcCallback() {
        cntl_ = std::make_shared<brpc::Controller>();
        response_ = std::make_shared<Response>();
    }

    virtual ~DummyBrpcCallback() = default;

    virtual void call() {}

    virtual void join() { brpc::Join(cntl_->call_id()); }

    // controller has to be the same lifecycle with the closure, because brpc may use
    // it in any stage of the rpc.
    std::shared_ptr<brpc::Controller> cntl_;
    // We do not know if brpc will use request or response after brpc method returns.
    // So that we need keep a shared ptr here to ensure that brpc could use req/rep
    // at any stage.
    std::shared_ptr<Response> response_;
};

// The closure will be deleted after callback.
// It could only be created by using shared ptr or unique ptr.
// It will hold a weak ptr of T and call run of T
// Callback() {
//  xxxx;
//  public
//  void run() {
//      logxxx
//  }
//  }
//
//  std::shared_ptr<Callback> b;
//
//  std::unique_ptr<AutoReleaseClosure> a(b);
//  brpc_call(a.release());

template <typename T>
concept HasStatus = requires(T* response) { response->status(); };

template <typename Request, typename Callback>
class AutoReleaseClosure : public google::protobuf::Closure {
    using Weak = typename std::shared_ptr<Callback>::weak_type;
    using ResponseType = typename Callback::ResponseType;
    ENABLE_FACTORY_CREATOR(AutoReleaseClosure);

public:
    AutoReleaseClosure(std::shared_ptr<Request> req, std::shared_ptr<Callback> callback)
            : request_(req), callback_(callback) {
        this->cntl_ = callback->cntl_;
        this->response_ = callback->response_;
    }

    ~AutoReleaseClosure() override = default;

    //  Will delete itself
    void Run() override {
        Defer defer {[&]() { delete this; }};
        // If lock failed, it means the callback object is deconstructed, then no need
        // to deal with the callback any more.
        if (auto tmp = callback_.lock()) {
            tmp->call();
        }
        if (cntl_->Failed()) {
            _process_if_rpc_failed();
        } else {
            _process_status<ResponseType>(response_.get());
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

protected:
    virtual void _process_if_rpc_failed() {
        LOG(WARNING) << "RPC meet failed: " << cntl_->ErrorText();
    }

    virtual void _process_if_meet_error_status(const Status& status) {
        LOG(WARNING) << "RPC meet error status: " << status;
    }

private:
    template <typename Response>
    void _process_status(Response* response) {}

    template <HasStatus Response>
    void _process_status(Response* response) {
        if (auto status = Status::create(response->status()); !status) {
            _process_if_meet_error_status(status);
        }
    }
    // Use a weak ptr to keep the callback, so that the callback can be deleted if the main
    // thread is freed.
    Weak callback_;
};

} // namespace doris
