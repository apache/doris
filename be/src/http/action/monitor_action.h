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

#ifndef DORIS_BE_SRC_COMMON_UTIL_MONITOR_ACTION_H
#define DORIS_BE_SRC_COMMON_UTIL_MONITOR_ACTION_H

#include <map>
#include <string>

#include "http/http_handler.h"

namespace doris {

class HttpRequest;
class HttpChannel;
class RestMonitorIface;

class MonitorAction : public HttpHandler {
public:
    MonitorAction();

    virtual ~MonitorAction() {}

    void register_module(const std::string& name, RestMonitorIface* module);

    void handle(HttpRequest* req) override;

private:
    std::map<std::string, RestMonitorIface*> _module_by_name;
};

} // namespace doris

#endif
