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

#include <functional>
#include <map>
#include <mutex>

#include "util/thread_pool.hpp"
#include "util/uid_util.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTask;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
class RoutineLoadTaskExecutor {
public:
    // paramater: task id
    typedef std::function<void (StreamLoadContext*)> ExecFinishCallback; 

    RoutineLoadTaskExecutor(ExecEnv* exec_env):
        _exec_env(exec_env) {
        _thread_pool = new ThreadPool(10, 1000);    
    }

    ~RoutineLoadTaskExecutor() {
        if (_thread_pool) {
            delete _thread_pool;
        }
    }
    
    // submit a routine load task
    Status submit_task(const TRoutineLoadTask& task);

private:
    // execute the task
    void exec_task(StreamLoadContext* ctx, ExecFinishCallback cb);
    
    void err_handler(
            StreamLoadContext* ctx,
            const Status& st,
            const std::string& err_msg);

private:
    ExecEnv* _exec_env;
    ThreadPool* _thread_pool;    

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, StreamLoadContext*> _task_map;
};

} // end namespace
