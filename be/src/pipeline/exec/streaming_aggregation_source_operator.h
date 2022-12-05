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

#include "agg_context.h"
#include "operator.h"

namespace doris {
namespace vectorized {
class AggregationNode;
}
namespace pipeline {

class StreamingAggSourceOperator : public Operator {
public:
    StreamingAggSourceOperator(OperatorBuilder*, vectorized::AggregationNode*,
                               std::shared_ptr<AggContext>);
    Status prepare(RuntimeState* state) override;
    bool can_read() override;
    Status close(RuntimeState* state) override;
    Status get_block(RuntimeState*, vectorized::Block*, SourceState& source_state) override;

private:
    vectorized::AggregationNode* _agg_node;
    std::shared_ptr<AggContext> _agg_context;
};

class StreamingAggSourceOperatorBuilder : public OperatorBuilder {
public:
    StreamingAggSourceOperatorBuilder(int32_t, const std::string&, vectorized::AggregationNode*,
                                      std::shared_ptr<AggContext>);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

private:
    std::shared_ptr<AggContext> _agg_context;
};

} // namespace pipeline
} // namespace doris