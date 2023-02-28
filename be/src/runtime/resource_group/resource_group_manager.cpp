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

#include "resource_group_manager.h"

namespace doris::resourcegroup {

ResourceGroupManager::ResourceGroupManager() {
    _create_default_rs_group();
    _create_poc_rs_group();
}
ResourceGroupManager::~ResourceGroupManager() = default;

ResourceGroupPtr ResourceGroupManager::get_or_create_resource_group(uint64_t id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_resource_groups.count(id)) {
        return _resource_groups[id];
    } else {
        return _resource_groups[DEFAULT_RG_ID];
    }
}

void ResourceGroupManager::_create_default_rs_group() {
    _resource_groups[DEFAULT_RG_ID] =
            std::make_shared<ResourceGroup>(DEFAULT_RG_ID, "default_rs", DEFAULT_CPU_SHARE);
}

void ResourceGroupManager::_create_poc_rs_group() {
    _resource_groups[POC_RG_ID] =
            std::make_shared<ResourceGroup>(POC_RG_ID, "poc_rs", POC_RG_CPU_SHARE);
}

}