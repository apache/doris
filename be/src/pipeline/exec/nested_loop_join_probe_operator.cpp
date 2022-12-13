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

#include "nested_loop_join_probe_operator.h"

#include "vec/exec/join/vnested_loop_join_node.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(NestLoopJoinProbeOperator, StatefulOperator)

Status NestLoopJoinProbeOperator::prepare(doris::RuntimeState* state) {
    // just for speed up, the way is dangerous
    _child_block.reset(_node->get_left_block());
    return StatefulOperator::prepare(state);
}

Status NestLoopJoinProbeOperator::close(doris::RuntimeState* state) {
    _child_block.release();
    return StatefulOperator::close(state);
}

} // namespace doris::pipeline
