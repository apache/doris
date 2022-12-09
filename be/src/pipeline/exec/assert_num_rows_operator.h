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
#include "vec/exec/vassert_num_rows_node.h"

namespace doris {

namespace pipeline {

class AssertNumRowsOperatorBuilder final : public OperatorBuilder<vectorized::VAssertNumRowsNode> {
public:
    AssertNumRowsOperatorBuilder(int32_t id, ExecNode* node)
            : OperatorBuilder(id, "AssertNumRowsOperatorBuilder", node) {};

    OperatorPtr build_operator() override;
};

class AssertNumRowsOperator final : public Operator<AssertNumRowsOperatorBuilder> {
public:
    AssertNumRowsOperator(OperatorBuilderBase* operator_builder, ExecNode* node)
            : Operator(operator_builder, node) {};
};

OperatorPtr AssertNumRowsOperatorBuilder::build_operator() {
    return std::make_shared<AssertNumRowsOperator>(this, _node);
}

} // namespace pipeline
} // namespace doris