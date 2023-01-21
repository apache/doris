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
#include "handler_dispatcher.h"

#include <memory>

namespace doris {
void HandlerDispatcher::register_handlers(ExecEnv* exec_env) {
    if (_registered) {
        return;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_registered) {
        _do_regsiter(exec_env);
    }
}

void HandlerDispatcher::dispatch(const std::string& handler_name, RpcController* cntl,
                                 Closure* done) {
    _registry->at(handler_name)->handle(cntl, done);
}

void HandlerDispatcher::_do_regsiter(ExecEnv* exec_env) {}

HandlerDispatcher* HandlerDispatcher::_add_handler(BaseHttpHandler* handler) {
    HttpHandlerPtr handler_ptr = std::make_unique<BaseHttpHandler>(*handler);
    _registry->insert({handler_ptr->get_name(), handler_ptr});
    return this;
}

} // namespace doris