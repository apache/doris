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

#include "runtime/workload_management/memory_context.h"

#include "runtime/workload_management/resource_context.h"

namespace doris {
#include "common/compile_check_begin.h"

std::string MemoryContext::debug_string() {
    return fmt::format("TaskId={}, Memory(Used={}, Limit={}, Peak={})",
                       print_id(resource_ctx_->task_controller()->task_id()),
                       PrettyPrinter::print_bytes(current_memory_bytes()),
                       PrettyPrinter::print_bytes(mem_limit()),
                       PrettyPrinter::print_bytes(peak_memory_bytes()));
}

#include "common/compile_check_end.h"
} // namespace doris
