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

#include "monitor_action.h"

#include "http/rest_monitor_iface.h"

namespace doris {
MonitorAction::MonitorAction() : BaseHttpHandler("monitor") {}

const std::string MODULE_KEY = "module";

void MonitorAction::register_module(const std::string& name, RestMonitorIface* module) {
    _module_by_name.insert(std::make_pair(name, module));
}

void MonitorAction::handle_sync(brpc::Controller* cntl) {
    const std::string& module = *get_param(cntl, MODULE_KEY);
    if (module.empty()) {
        std::string err_msg = "No module params\n";
        on_succ(cntl, err_msg);
        return;
    }
    if (_module_by_name.find(module) == _module_by_name.end()) {
        std::string err_msg = "Unknown module(";
        err_msg += module + ")\n";
        on_succ(cntl, err_msg);
        return;
    }
    std::stringstream ss;
    _module_by_name[module]->debug(ss);
    std::string str = ss.str();
    on_succ(cntl, str);
}

} // namespace doris