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

#include "task_execution_context.h"

#include <glog/logging.h>

#include <condition_variable>

namespace doris {
void TaskExecutionContext::ref_task_execution_ctx() {
    ++_has_task_execution_ctx_ref_count;
}

void TaskExecutionContext::unref_task_execution_ctx() {
    --_has_task_execution_ctx_ref_count;
    if (_has_task_execution_ctx_ref_count == 0) {
        _notify_cv.notify_all();
    }
}

HasTaskExecutionCtx::HasTaskExecutionCtx(RuntimeState* state)
        : task_exec_ctx_(state->get_task_execution_context()) {}

HasTaskExecutionCtx::~HasTaskExecutionCtx() = default;
} // namespace doris
