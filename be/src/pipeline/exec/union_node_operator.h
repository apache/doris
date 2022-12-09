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
class VUnionNode;
} // namespace vectorized

namespace pipeline {

class UnionNodeOperatorBuilder final : public OperatorBuilder<vectorized::VUnionNode> {
public:
    UnionNodeOperatorBuilder(int32_t id, ExecNode* node)
            : OperatorBuilder(id, "UnionNodeOperatorBuilder", node) {};

    OperatorPtr build_operator() override;

    bool is_source() const override { return true; }
};

class UnionNodeOperator final : public Operator<UnionNodeOperatorBuilder> {
public:
    UnionNodeOperator(OperatorBuilderBase* operator_builder, ExecNode* node)
            : Operator(operator_builder, node) {};

    bool can_read() override { return true; };
};

OperatorPtr UnionNodeOperatorBuilder::build_operator() {
    return std::make_shared<UnionNodeOperator>(this, _node);
}

} // namespace pipeline
} // namespace doris