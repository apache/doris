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

class ExchangeSourceOperator final : public SourceOperator<vectorized::VExchangeNode> {
public:
    ExchangeSourceOperator(OperatorBuilderBase*, ExecNode*);
    bool can_read() override;
    bool is_pending_finish() const override;
};

struct ExchangeDataDependency final : public Dependency {
public:
    ENABLE_FACTORY_CREATOR(ExchangeDataDependency);
    ExchangeDataDependency(int id, int node_id, QueryContext* query_ctx,
                           vectorized::VDataStreamRecvr::SenderQueue* sender_queue)
            : Dependency(id, node_id, "DataDependency", query_ctx), _queue(sender_queue) {}

    std::string debug_string(int indentation_level) override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer,
                       "{}, _is_cancelled = {}, _block_queue size = {},_num_remaining_senders = {}",
                       Dependency::debug_string(indentation_level), _queue->_is_cancelled,
                       _queue->_block_queue.size(), _queue->_num_remaining_senders);
        return fmt::to_string(debug_string_buffer);
    }

private:
    vectorized::VDataStreamRecvr::SenderQueue* _queue;
};

class ExchangeSourceOperatorX;
class ExchangeLocalState final : public PipelineXLocalState<AndDependency> {
    ENABLE_FACTORY_CREATOR(ExchangeLocalState);

public:
    using Base = PipelineXLocalState<AndDependency>;
    ExchangeLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const override;
    std::shared_ptr<doris::vectorized::VDataStreamRecvr> stream_recvr;
    doris::vectorized::VSortExecExprs vsort_exec_exprs;
    int64_t num_rows_skipped;
    bool is_ready;

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

    std::string debug_string(int indentation_level = 0) const override;

    Status close(RuntimeState* state) override;
    [[nodiscard]] bool is_source() const override { return true; }

    [[nodiscard]] RowDescriptor input_row_desc() const { return _input_row_desc; }

    [[nodiscard]] int num_senders() const { return _num_senders; }
    [[nodiscard]] bool is_merging() const { return _is_merging; }

    std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr() {
        return _sub_plan_query_statistics_recvr;
    }

    DataDistribution required_data_distribution() const override {
        if (OperatorX<ExchangeLocalState>::ignore_data_distribution()) {
            return {ExchangeType::NOOP};
        }
        return _partition_type == TPartitionType::HASH_PARTITIONED
                       ? DataDistribution(ExchangeType::HASH_SHUFFLE)
               : _partition_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE)
                       : DataDistribution(ExchangeType::NOOP);
    }

private:
    friend class ExchangeLocalState;
    const int _num_senders;
    const bool _is_merging;
    const TPartitionType::type _partition_type;
    RowDescriptor _input_row_desc;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // use in merge sort
    size_t _offset;

    doris::vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
};

} // namespace doris::pipeline
