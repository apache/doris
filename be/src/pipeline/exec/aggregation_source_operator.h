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
class AggregationNode;
}

namespace pipeline {

class AggSourceOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    AggSourceOperatorBuilder(int32_t, ExecNode*);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class AggSourceOperator final : public SourceOperator<AggSourceOperatorBuilder> {
public:
    AggSourceOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
};

} // namespace pipeline
} // namespace doris
