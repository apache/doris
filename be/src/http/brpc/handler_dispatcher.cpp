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

#include <glog/logging.h>

#include <memory>

#include "http/brpc/action/check_rpc_channel_action.h"

namespace doris {
HandlerDispatcher::HandlerDispatcher() : _registry(new HandlerRegistry()), _registered(false) {}

void HandlerDispatcher::dispatch(const std::string& handler_name, RpcController* cntl,
                                 Closure* done) {
    _registry->at(handler_name)->handle(cntl, done);
}

HandlerDispatcher* HandlerDispatcher::add_handler(BaseHttpHandler* handler) {
    LOG(INFO) << handler->get_name();
    _registry->insert({handler->get_name(), handler});
    return this;
}

} // namespace doris