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
#include <mutex>
#include <unordered_map>

#include "brpc_http_handler.h"

namespace doris {
class HandlerDispatcher {
    using Closure = ::google::protobuf::Closure;
    using RpcController = ::google::protobuf::RpcController;

public:
    void register_handlers();

    void dispatch(const std::string& handler_name, RpcController* cntl, Closure* done);

private:
    std::mutex _mutex;
    std::unordered_map<std::string, BaseHttpHandler> _registry;
    bool _registered;

    void _do_regsiter();
};
} // namespace doris