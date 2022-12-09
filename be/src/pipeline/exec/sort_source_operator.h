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
class VSortNode;
}

namespace pipeline {

class SortSourceOperatorBuilder final : public OperatorBuilder<vectorized::VSortNode> {
public:
    SortSourceOperatorBuilder(int32_t id, ExecNode* sort_node);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class SortSourceOperator final : public Operator<SortSourceOperatorBuilder> {
public:
    SortSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node);
    Status open(RuntimeState*) override { return Status::OK(); }
};

} // namespace pipeline
} // namespace doris