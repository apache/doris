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

} // namespace doris
