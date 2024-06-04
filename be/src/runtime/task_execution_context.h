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

#include <memory>

namespace doris {

// This class act as a super class of all context like things such as
// plan fragment executor or pipelinefragmentcontext or pipelinexfragmentcontext
class TaskExecutionContext : public std::enable_shared_from_this<TaskExecutionContext> {
public:
    TaskExecutionContext() = default;
    virtual ~TaskExecutionContext() = default;
};

using TaskExecutionContextSPtr = std::shared_ptr<TaskExecutionContext>;

// Task Execution Context maybe plan fragment executor or pipelinefragmentcontext or pipelinexfragmentcontext
// In multi thread scenario, the object is created in main thread (such as FragmentExecThread), but the object
// maybe used in other thread(such as scanner thread, brpc->sender queue). If the main thread stopped and destroy
// the object, then the other thread may core. So the other thread must lock the context to ensure the object exists.
struct HasTaskExecutionCtx {
    using Weak = typename TaskExecutionContextSPtr::weak_type;

    HasTaskExecutionCtx(TaskExecutionContextSPtr task_exec_ctx) : task_exec_ctx_(task_exec_ctx) {}

    // Init task ctx from state, the state has to own a method named get_task_execution_context()
    // like runtime state
    template <typename T>
    HasTaskExecutionCtx(T* state) : task_exec_ctx_(state->get_task_execution_context()) {}

    virtual ~HasTaskExecutionCtx() = default;

public:
    inline TaskExecutionContextSPtr task_exec_ctx() const { return task_exec_ctx_.lock(); }
    inline Weak weak_task_exec_ctx() const { return task_exec_ctx_; }

private:
    Weak task_exec_ctx_;
};

} // namespace doris
