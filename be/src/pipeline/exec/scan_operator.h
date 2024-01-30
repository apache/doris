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

#include <string>

#include "common/status.h"
#include "operator.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

class ScanOperatorBuilder : public OperatorBuilder<vectorized::VScanNode> {
public:
    ScanOperatorBuilder(int32_t id, ExecNode* exec_node);
    bool is_source() const override { return true; }
    OperatorPtr build_operator() override;
};

class ScanOperator : public SourceOperator<ScanOperatorBuilder> {
public:
    ScanOperator(OperatorBuilderBase* operator_builder, ExecNode* scan_node);

    bool can_read() override; // for source

    bool is_pending_finish() const override;

    bool runtime_filters_are_ready_or_timeout() override;

    std::string debug_string() const override;

    Status try_close(RuntimeState* state) override;
};

} // namespace doris::pipeline
