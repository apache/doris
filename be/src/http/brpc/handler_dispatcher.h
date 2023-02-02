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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "brpc_http_handler.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"

namespace doris {
using Closure = ::google::protobuf::Closure;
using RpcController = ::google::protobuf::RpcController;
using HttpHandlerPtr = BaseHttpHandler*;
using HandlerRegistry = std::unordered_map<std::string, HttpHandlerPtr>;
using RegistryPtr = std::unique_ptr<HandlerRegistry>;
class HandlerDispatcher {
public:
    HandlerDispatcher();
    ~HandlerDispatcher();

    void dispatch(const std::string& handler_name, RpcController* cntl, Closure* done);

    HandlerDispatcher* add_handler(BaseHttpHandler* handler);

private:
    RegistryPtr _registry;

    ObjectPool _pool;
};
} // namespace doris