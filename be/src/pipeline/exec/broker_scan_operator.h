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
#include "vec/exec/vbroker_scan_node.h"

namespace doris::pipeline {

class BrokerScanOperatorBuilder : public OperatorBuilder<vectorized::VBrokerScanNode> {
public:
    BrokerScanOperatorBuilder(int32_t id, ExecNode* node)
            : OperatorBuilder(id, "BrokerScanOperator", node) {}
    bool is_source() const override { return true; }
    OperatorPtr build_operator() override;
};

class BrokerScanOperator : public SourceOperator<BrokerScanOperatorBuilder> {
public:
    BrokerScanOperator(OperatorBuilderBase* operator_builder, ExecNode* scan_node)
            : SourceOperator(operator_builder, scan_node) {}

    bool can_read() override { return _node->can_read(); }

    bool is_pending_finish() const override { return !_node->can_finish(); }

    Status open(RuntimeState* state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        RETURN_IF_ERROR(SourceOperator::open(state));
        return _node->open(state);
    }

    Status close(RuntimeState* state) override {
        RETURN_IF_ERROR(SourceOperator::close(state));
        _node->close(state);
        return Status::OK();
    }
};

OperatorPtr BrokerScanOperatorBuilder::build_operator() {
    return std::make_shared<BrokerScanOperator>(this, _node);
}

} // namespace doris::pipeline