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

#include "runtime/workload_management/cpu_context.h"

#include <glog/logging.h>

#include "runtime/workload_management/resource_context.h"

namespace doris {

void CPUContext::update_cpu_cost_ms(int64_t delta) const {
    stats_.cpu_cost_ms_counter_->update(delta);
    if (resource_ctx_ != nullptr && resource_ctx_->workload_group() != nullptr) {
        resource_ctx_->workload_group()->update_cpu_time(delta);
    }
}

} // namespace doris
