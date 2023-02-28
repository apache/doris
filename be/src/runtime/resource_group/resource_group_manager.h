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

#include <shared_mutex>
#include <unordered_map>

#include "resource_group.h"

namespace doris::resourcegroup {

class ResourceGroupManager {
    DECLARE_SINGLETON(ResourceGroupManager)
public:
    // TODO rs poc 根据id选rs group,目前还不能创建
    ResourceGroupPtr get_or_create_resource_group(uint64_t id);

    static constexpr uint64_t DEFAULT_RG_ID = 0;
    static constexpr int DEFAULT_CPU_SHARE = 64;

    static constexpr uint64_t POC_RG_ID = 1;
    static constexpr int POC_RG_CPU_SHARE = 128;

private:
    void _create_default_rs_group();
    // TODO rs poc
    void _create_poc_rs_group();

    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, ResourceGroupPtr> _resource_groups;
};

}

