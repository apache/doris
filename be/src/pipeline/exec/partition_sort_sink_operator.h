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

#include <stdint.h>

#include "operator.h"
#include "vec/exec/vpartition_sort_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class PartitionSortSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::VPartitionSortNode> {
public:
    PartitionSortSinkOperatorBuilder(int32_t id, ExecNode* sort_node)
            : OperatorBuilder(id, "PartitionSortSinkOperator", sort_node) {}

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;
};

class PartitionSortSinkOperator final : public StreamingOperator<PartitionSortSinkOperatorBuilder> {
public:
    PartitionSortSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node)
            : StreamingOperator(operator_builder, sort_node) {};

    bool can_write() override { return true; }
};

OperatorPtr PartitionSortSinkOperatorBuilder::build_operator() {
    return std::make_shared<PartitionSortSinkOperator>(this, _node);
}

} // namespace pipeline
} // namespace doris
