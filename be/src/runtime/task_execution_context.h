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

#include "runtime/runtime_state.h"

namespace doris {

class RuntimeState;

// Base class for execution contexts (e.g. PipelineFragmentContext).
//
// For recursive CTE, the PFC (which inherits from this class) is held by external threads
// (scanner threads, brpc callbacks, etc.) via weak_ptr<TaskExecutionContext>.
class TaskExecutionContext : public std::enable_shared_from_this<TaskExecutionContext> {
public:
    TaskExecutionContext();
    virtual ~TaskExecutionContext();
};

using TaskExecutionContextSPtr = std::shared_ptr<TaskExecutionContext>;

// Task Execution Context maybe plan fragment executor or pipelinefragmentcontext or pipelinexfragmentcontext
// In multi thread scenario, the object is created in main thread (such as FragmentExecThread), but the object
// maybe used in other thread(such as scanner thread, brpc->sender queue). If the main thread stopped and destroy
// the object, then the other thread may core. So the other thread must lock the context to ensure the object exists.
struct HasTaskExecutionCtx {
    using Weak = typename TaskExecutionContextSPtr::weak_type;

    HasTaskExecutionCtx(RuntimeState* state);

    virtual ~HasTaskExecutionCtx();

public:
    inline TaskExecutionContextSPtr task_exec_ctx() const { return task_exec_ctx_.lock(); }
    inline Weak weak_task_exec_ctx() const { return task_exec_ctx_; }

private:
    Weak task_exec_ctx_;
};

} // namespace doris
