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

#include "runtime/query_context.h"

#include "pipeline/pipeline_fragment_context.h"

namespace doris {

bool QueryContext::cancel(bool v, std::string msg, Status new_status, int fragment_id) {
    if (_is_cancelled) {
        return false;
    }
    set_exec_status(new_status);
    _is_cancelled.store(v);

    set_ready_to_execute(true);
    {
        std::lock_guard<std::mutex> plock(pipeline_lock);
        for (auto& ctx : fragment_id_to_pipeline_ctx) {
            if (fragment_id == ctx.first) {
                continue;
            }
            ctx.second->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, msg);
        }
    }
    return true;
}
} // namespace doris
