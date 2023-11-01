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
#include "pipeline/pipeline_x/operator.h"
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

struct ExchangeDataDependency final : public Dependency {
public:
    ENABLE_FACTORY_CREATOR(ExchangeDataDependency);
    ExchangeDataDependency(int id, vectorized::VDataStreamRecvr::SenderQueue* sender_queue)
            : Dependency(id, "DataDependency"), _always_done(false) {}
    void* shared_state() override { return nullptr; }

    void set_always_done() {
        _always_done = true;
        if (_ready_for_read) {
            return;
        }
        _read_dependency_watcher.stop();
        _ready_for_read = true;
    }

    void block_reading() override {
        if (_always_done) {
            return;
        }
        _ready_for_read = false;
    }

private:
    std::atomic<bool> _always_done;
};

class ExchangeSourceOperatorX;
class ExchangeLocalState final : public PipelineXLocalState<> {
    ENABLE_FACTORY_CREATOR(ExchangeLocalState);
    ExchangeLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Dependency* dependency() override { return source_dependency.get(); }
    std::shared_ptr<doris::vectorized::VDataStreamRecvr> stream_recvr;
    doris::vectorized::VSortExecExprs vsort_exec_exprs;
    int64_t num_rows_skipped;
    bool is_ready;

    std::shared_ptr<AndDependency> source_dependency;
    std::vector<std::shared_ptr<ExchangeDataDependency>> deps;

    std::vector<RuntimeProfile::Counter*> metrics;
};

class ExchangeSourceOperatorX final : public OperatorX<ExchangeLocalState> {
public:
    ExchangeSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs, int num_senders);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    Status close(RuntimeState* state) override;
    [[nodiscard]] bool is_source() const override { return true; }

    [[nodiscard]] RowDescriptor input_row_desc() const { return _input_row_desc; }

    [[nodiscard]] int num_senders() const { return _num_senders; }
    [[nodiscard]] bool is_merging() const { return _is_merging; }

    std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr() {
        return _sub_plan_query_statistics_recvr;
    }

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
