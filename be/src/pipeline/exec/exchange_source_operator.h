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

#include "operator.h"
#include "vec/exec/vexchange_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace vectorized {
class VDataStreamRecvr;
class Block;
} // namespace vectorized

namespace doris::pipeline {

class ExchangeSourceOperatorBuilder final : public OperatorBuilder<vectorized::VExchangeNode> {
public:
    ExchangeSourceOperatorBuilder(int32_t id, ExecNode* exec_node);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class ExchangeSourceOperator final : public SourceOperator<ExchangeSourceOperatorBuilder> {
public:
    ExchangeSourceOperator(OperatorBuilderBase*, ExecNode*);
    bool can_read() override;
    bool is_pending_finish() const override;
};

class ExchangeSourceOperatorX;
class ExchangeLocalState : public PipelineXLocalState {
    ENABLE_FACTORY_CREATOR(ExchangeLocalState);
    ExchangeLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

    std::shared_ptr<doris::vectorized::VDataStreamRecvr> stream_recvr;
    doris::vectorized::VSortExecExprs vsort_exec_exprs;
    int64_t num_rows_skipped;
    bool is_ready;
};

class ExchangeSourceOperatorX final : public OperatorXBase {
public:
    ExchangeSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                            std::string op_name, int num_senders);
    bool can_read(RuntimeState* state) override;
    bool is_pending_finish(RuntimeState* state) const override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    Status close(RuntimeState* state) override;
    bool is_source() const override { return true; }

private:
    friend class ExchangeLocalState;
    const int _num_senders;
    const bool _is_merging;
    RowDescriptor _input_row_desc;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // use in merge sort
    size_t _offset;

    doris::vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
};

} // namespace doris::pipeline