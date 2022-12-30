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
#include "pipeline/exec/data_queue.h"
#include "vec/core/block.h"
namespace doris {
namespace vectorized {
class VUnionNode;
class Block;
} // namespace vectorized

namespace pipeline {

class UnionSinkOperatorBuilder final : public OperatorBuilder<vectorized::VUnionNode> {
public:
    UnionSinkOperatorBuilder(int32_t id, int child_id, ExecNode* node,
                             std::shared_ptr<DataQueue> queue);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; };

private:
    int _cur_child_id;
    std::shared_ptr<DataQueue> _data_queue;
};

class UnionSinkOperator final : public StreamingOperator<UnionSinkOperatorBuilder> {
public:
    UnionSinkOperator(OperatorBuilderBase* operator_builder, int child_id, ExecNode* node,
                      std::shared_ptr<DataQueue> queue);

    bool can_write() override { return true; };

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;
    // this operator in sink open directly return, do this work in source
    Status open(RuntimeState* /*state*/) override { return Status::OK(); }

    Status close(RuntimeState* state) override;

private:
    int _cur_child_id;
    std::shared_ptr<DataQueue> _data_queue;
    std::unique_ptr<vectorized::Block> _output_block;
};
} // namespace pipeline
} // namespace doris