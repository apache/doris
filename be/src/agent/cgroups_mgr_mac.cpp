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

#include "agent/cgroups_mgr.h"

namespace doris {

const std::string CgroupsMgr::_s_system_user = "system";
const std::string CgroupsMgr::_s_system_group = "normal";

std::map<TResourceType::type, std::string> CgroupsMgr::_s_resource_cgroups;

CgroupsMgr::CgroupsMgr(ExecEnv* exec_env, const std::string& root_cgroups_path) {}

CgroupsMgr::~CgroupsMgr() = default;

Status CgroupsMgr::init_cgroups() {
    return Status::OK();
}

void CgroupsMgr::apply_cgroup(const std::string& user_name, const std::string& level) {}

} // namespace doris
