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

#include "runtime/workload_group/workload_group.h"

namespace doris {

const static std::string DUMMY_WORKLOAD_GROUP_NAME = "_dummpy_workload_group";
const static uint64_t DUMMY_WORKLOAD_GROUP_ID = 0;

class DummyWorkloadGroup : public WorkloadGroup {
    ENABLE_FACTORY_CREATOR(DummyWorkloadGroup)

public:
    DummyWorkloadGroup()
            : WorkloadGroup({.id = DUMMY_WORKLOAD_GROUP_ID, .name = DUMMY_WORKLOAD_GROUP_NAME},
                            false) {}

    void get_query_scheduler(doris::pipeline::TaskScheduler** exec_sched,
                             vectorized::SimplifiedScanScheduler** scan_sched,
                             vectorized::SimplifiedScanScheduler** remote_scan_sched) override;

    ThreadPool* get_memtable_flush_pool() override;
};

} // namespace doris