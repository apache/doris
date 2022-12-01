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

#include <utility>

#include "operator.h"

namespace doris::vectorized {
class VScanNode;
class VScanner;
class ScannerContext;
} // namespace doris::vectorized

namespace doris::pipeline {

class ScanOperator : public Operator {
public:
    ScanOperator(OperatorBuilder* operator_builder, vectorized::VScanNode* scan_node);

    bool can_read() override; // for source

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& result_state) override;

    bool is_pending_finish() const override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

private:
    vectorized::VScanNode* _scan_node;
};

class ScanOperatorBuilder : public OperatorBuilder {
public:
    ScanOperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node)
            : OperatorBuilder(id, name, exec_node) {}

    bool is_source() const override { return true; }
};

} // namespace doris::pipeline