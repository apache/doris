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

#include "runtime/exec_env.h"
#include "runtime/workload_management/workload_sched_policy.h"
#include "util/countdown_latch.h"

namespace doris {

class WorkloadSchedPolicyMgr {
public:
    WorkloadSchedPolicyMgr() : _stop_latch(0) {}
    ~WorkloadSchedPolicyMgr() = default;

    void start(ExecEnv* exec_env);

    void stop();

    void update_workload_sched_policy(
            std::map<uint64_t, std::shared_ptr<WorkloadSchedPolicy>> policy_map);

private:
    void _schedule_workload();

    std::shared_mutex _policy_lock;
    std::map<uint64_t, std::shared_ptr<WorkloadSchedPolicy>> _id_policy_map;

    std::shared_mutex _stop_lock;
    CountDownLatch _stop_latch;
    scoped_refptr<Thread> _thread;
    ExecEnv* _exec_env;
};

}; // namespace doris