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

#include "operator.h"

namespace doris {
namespace vectorized {
class VRepeatNode;
class VExprContext;
class Block;
} // namespace vectorized
namespace pipeline {
class RepeatOperatorBuilder;
class RepeatOperator : public Operator {
public:
    RepeatOperator(RepeatOperatorBuilder* operator_builder, vectorized::VRepeatNode* repeat_node);

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

private:
    vectorized::VRepeatNode* _repeat_node;
    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
};

class RepeatOperatorBuilder : public OperatorBuilder {
public:
    RepeatOperatorBuilder(int32_t id, vectorized::VRepeatNode* repeat_node);

    OperatorPtr build_operator() override;

private:
    vectorized::VRepeatNode* _repeat_node;
};

} // namespace pipeline
} // namespace doris
