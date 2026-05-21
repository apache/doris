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

#include <gen_cpp/PlanNodes_types.h>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/runtime_state.h"

namespace doris {

inline Status validate_hash_join_mark_join_plan(TJoinOp::type join_op, bool is_mark_join,
                                                RuntimeState* state, int node_id) {
    if (!is_mark_join || join_op != TJoinOp::RIGHT_ANTI_JOIN) {
        return Status::OK();
    }
    return Status::InternalError(
            "Hash join does not support right anti mark join, query={}, node={}, join_op={}",
            print_id(state->query_id()), node_id, to_string(join_op));
}

} // namespace doris
