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

#include "agent/agent_server.h"

using apache::thrift::transport::TProcessor;
using std::deque;
using std::list;
using std::map;
using std::nothrow;
using std::set;
using std::string;
using std::to_string;
using std::vector;

namespace doris {

Status RoutineLoadTaskExecutor::submit_task(RoutineLoadTask task) {

    std::unique_lock<std::mutex> l(_lock); 
    if (_task_ids.find(task.id) != _task_ids.end()) {
        // already submitted
        return Status::OK;
    }

    _task_ids.insert(task.id);

    // 1. create a stream load context with ConsumerPipe
    StreamLoadContext* ctx = new StreamLoadContext(this);
    auto st = _exec_env->fragment_mgr()->exec_plan_fragment(
        plan_params,
        

    );


    
    // 2. execute plan

    // 3. activate the consumer to read data

}

} // end namespace
