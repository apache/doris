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

// For read none streaming agg sink operator's data
class AggregationSourceOperator : public Operator {
public:
    AggregationSourceOperator(OperatorBuilder*, vectorized::AggregationNode*);
    Status prepare(RuntimeState* state) override;
    bool can_read() override;
    Status close(RuntimeState* state) override;
    Status get_block(RuntimeState*, vectorized::Block*, SourceState&) override;

private:
    vectorized::AggregationNode* _agg_node;
};

class AggregationSourceOperatorBuilder : public OperatorBuilder {
public:
    AggregationSourceOperatorBuilder(int32_t, const std::string&, vectorized::AggregationNode*);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

} // namespace pipeline
} // namespace doris