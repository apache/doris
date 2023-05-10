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

#include "repeat_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/core/block.h"
#include "vec/exec/vrepeat_node.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(RepeatOperator, StatefulOperator)

Status RepeatOperator::prepare(doris::RuntimeState* state) {
    // just for speed up, the way is dangerous
    _child_block.reset(_node->get_child_block());
    return StatefulOperator::prepare(state);
}

Status RepeatOperator::close(doris::RuntimeState* state) {
    _child_block.release();
    return StatefulOperator::close(state);
}

} // namespace doris::pipeline
