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

#include "common/factory_creator.h"
#include "runtime/workload_group/workload_group.h"

namespace doris {

class WorkloadGroupContext {
    ENABLE_FACTORY_CREATOR(WorkloadGroupContext);

public:
    WorkloadGroupContext() = default;
    virtual ~WorkloadGroupContext() = default;

    int64_t workload_group_id() {
        if (workload_group() != nullptr) {
            return workload_group()->id();
        }
        return -1;
    }
    WorkloadGroupPtr workload_group() { return _workload_group; }
    void set_workload_group(WorkloadGroupPtr wg) { _workload_group = wg; }

protected:
    WorkloadGroupPtr _workload_group = nullptr;
};

} // namespace doris
