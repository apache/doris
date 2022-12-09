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
class VDataGenFunctionScanNode;
} // namespace doris::vectorized

namespace doris::pipeline {

class DataGenOperatorBuilder : public OperatorBuilder<vectorized::VDataGenFunctionScanNode> {
public:
    DataGenOperatorBuilder(int32_t id, ExecNode* exec_node);
    bool is_source() const override { return true; }
    OperatorPtr build_operator() override;
};

class DataGenOperator : public Operator<DataGenOperatorBuilder> {
public:
    DataGenOperator(OperatorBuilderBase* operator_builder, ExecNode* datagen_node);

    bool can_read() override { return true; };

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;
};

} // namespace doris::pipeline