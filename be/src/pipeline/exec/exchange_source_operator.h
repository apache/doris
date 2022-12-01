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

namespace doris::vectorized {
class VExchangeNode;
}

namespace doris::pipeline {

class ExchangeSourceOperator : public Operator {
public:
    explicit ExchangeSourceOperator(OperatorBuilder*, vectorized::VExchangeNode*);
    Status open(RuntimeState* state) override;
    bool can_read() override;
    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;
    bool is_pending_finish() const override;
    Status close(RuntimeState* state) override;

private:
    vectorized::VExchangeNode* _exchange_node;
};

class ExchangeSourceOperatorBuilder : public OperatorBuilder {
public:
    ExchangeSourceOperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node)
            : OperatorBuilder(id, name, exec_node) {}

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<ExchangeSourceOperator>(
                this, reinterpret_cast<vectorized::VExchangeNode*>(_related_exec_node));
    }
};

} // namespace doris::pipeline