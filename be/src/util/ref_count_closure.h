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

#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "service/brpc.h"

namespace doris {

template <typename T>
class RefCountClosure : public google::protobuf::Closure {
public:
    RefCountClosure() : _refs(0) {}
    ~RefCountClosure() override = default;

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    void Run() override {
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
        if (unref()) {
            delete this;
        }
    }

    void join() { brpc::Join(cntl.call_id()); }

    brpc::Controller cntl;
    T result;

private:
    std::atomic<int> _refs;
};

template <typename Response>
class DummyBrpcCallback {
    ENABLE_FACTORY_CREATOR(DummyCallback);

public:
    void call(std::shared_ptr<Response> rep) {}
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

template <typename Request, typename Response,
          typename Callback = std::shared_ptr<DummyBrpcCallback<Response>>>
class AutoReleaseClosure : public google::protobuf::Closure {
    using Weak = typename Callback::weak_type;
    ENABLE_FACTORY_CREATOR(AutoReleaseClosure);

public:
    AutoReleaseClosure(std::shared_ptr<Request> req, std::shared_ptr<Response> rep,
                       Callback callback, bool auto_release)
            : callback_(callback), auto_release_(auto_release) {}
    AutoReleaseClosure(std::shared_ptr<Request> req, std::shared_ptr<Response> rep,
                       bool auto_release)
            : auto_release_(auto_release) {}
    ~AutoReleaseClosure() override = default;

    //  Will delete itself
    void Run() override {
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
        Defer defer {[&]() {
            if (auto_release) {
                delete this;
            }
        }};
        // If lock failed, it means the callback object is deconstructed, then no need
        // to deal with the callback any more.
        if (callback_ != nullptr) {
            if (Callback tmp = callback_.lock()) {
                tmp->call(response_, cntl_);
            }
        }
    }

    void join() { brpc::Join(cntl.call_id()); }

public:
    // controller has to be the same lifecycle with the closure, because brpc may use
    // it in any stage of the rpc.
    std::shared_ptr<brpc::Controller> cntl_;
    // We do not know if brpc will use request or response after brpc method returns.
    // So that we need keep a shared ptr here to ensure that brpc could use req/rep
    // at any stage.
    std::shared_ptr<Request> request_;
    std::shared_ptr<Response> response_;

private:
    // Use a weak ptr to keep the callback, so that the callback can be deleted if the main
    // thread is freed.
    Weak callback_;
    bool auto_release_ = true;
};

} // namespace doris
