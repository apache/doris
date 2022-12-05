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
class VResultSink;
}

namespace pipeline {

class ResultSinkOperator : public Operator {
public:
    ResultSinkOperator(OperatorBuilder* operator_builder, vectorized::VResultSink* sink);

    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    bool can_write() override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    Status finalize(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

private:
    vectorized::VResultSink* _sink;
    bool _finalized = false;
};

class ResultSinkOperatorBuilder : public OperatorBuilder {
public:
    ResultSinkOperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node,
                              vectorized::VResultSink* sink)
            : OperatorBuilder(id, name, exec_node), _sink(sink) {}

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<ResultSinkOperator>(this, _sink);
    }

private:
    vectorized::VResultSink* _sink;
};

} // namespace pipeline
} // namespace doris