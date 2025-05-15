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

#include "runtime/workload_management/task_controller.h"

#include "runtime/workload_management/resource_context.h"

namespace doris {
#include "common/compile_check_begin.h"

void TaskController::update_paused_reason(const Status& st) {
    if (paused_reason_.status().is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
        return;
    } else if (paused_reason_.status().is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
        if (st.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            paused_reason_.update(st);
            return;
        } else {
            return;
        }
    } else {
        paused_reason_.update(st);
    }
}

std::string TaskController::debug_string() {
    return fmt::format(
            "TaskId={}, Memory(Used={}, Limit={}, Peak={}), Spill(RunningSpillTaskCnt={}, "
            "TotalPausedPeriodSecs={}, LatestPausedReason={})",
            print_id(task_id_),
            PrettyPrinter::print_bytes(resource_ctx_->memory_context()->current_memory_bytes()),
            PrettyPrinter::print_bytes(resource_ctx_->memory_context()->mem_limit()),
            PrettyPrinter::print_bytes(resource_ctx_->memory_context()->peak_memory_bytes()),
            revoking_tasks_count_, memory_sufficient_time() / NANOS_PER_SEC,
            paused_reason_.status().to_string());
}

#include "common/compile_check_end.h"
} // namespace doris
