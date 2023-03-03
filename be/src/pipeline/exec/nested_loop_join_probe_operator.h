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
class VNestedLoopJoinNode;
class VExprContext;
class Block;
} // namespace vectorized
namespace pipeline {

class NestLoopJoinProbeOperatorBuilder final
        : public OperatorBuilder<vectorized::VNestedLoopJoinNode> {
public:
    NestLoopJoinProbeOperatorBuilder(int32_t id, ExecNode* node);

    OperatorPtr build_operator() override;
};

class NestLoopJoinProbeOperator final : public StatefulOperator<NestLoopJoinProbeOperatorBuilder> {
public:
    NestLoopJoinProbeOperator(OperatorBuilderBase* operator_builder, ExecNode* node);

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState*) override { return Status::OK(); }

    Status close(RuntimeState* state) override;
};

} // namespace pipeline
} // namespace doris
