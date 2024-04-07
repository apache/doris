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
#include "pipeline/pipeline_x/operator.h"
#include "vec/sink/group_commit_block_sink.h"

namespace doris {

namespace pipeline {

class GroupCommitBlockSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::GroupCommitBlockSink> {
public:
    GroupCommitBlockSinkOperatorBuilder(int32_t id, DataSink* sink)
            : DataSinkOperatorBuilder(id, "GroupCommitBlockSinkOperator", sink) {}

    inline OperatorPtr build_operator() override;
};

class GroupCommitBlockSinkOperator final
        : public DataSinkOperator<vectorized::GroupCommitBlockSink> {
public:
    GroupCommitBlockSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
            : DataSinkOperator(operator_builder, sink) {}

    bool can_write() override { return true; }
};

OperatorPtr GroupCommitBlockSinkOperatorBuilder::build_operator() {
    return std::make_shared<GroupCommitBlockSinkOperator>(this, _sink);
}

class GroupCommitBlockSinkOperatorX;
class GroupCommitBlockSinkLocalState final
        : public AsyncWriterSink<vectorized::VGroupCommitBlockWriter,
                                 GroupCommitBlockSinkOperatorX> {
    ENABLE_FACTORY_CREATOR(GroupCommitBlockSinkLocalState);

public:
    using Base =
            AsyncWriterSink<vectorized::VGroupCommitBlockWriter, GroupCommitBlockSinkOperatorX>;
    GroupCommitBlockSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : AsyncWriterSink<vectorized::VGroupCommitBlockWriter, GroupCommitBlockSinkOperatorX>(
                      parent, state) {}

private:
    friend class GroupCommitBlockSinkOperatorX;
};

class GroupCommitBlockSinkOperatorX final
        : public DataSinkOperatorX<GroupCommitBlockSinkLocalState> {
public:
    using Base = DataSinkOperatorX<GroupCommitBlockSinkLocalState>;
    GroupCommitBlockSinkOperatorX(const RowDescriptor& row_desc, int operator_id,
                                  const std::vector<TExpr>& select_exprs)
            : Base(operator_id, 0), _row_desc(row_desc), _t_output_expr(select_exprs) {};
    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(Base::init(thrift_sink));
        // From the thrift expressions create the real exprs.
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        return vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc);
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::open(state));
        return vectorized::VExpr::open(_output_vexpr_ctxs, state);
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        return local_state.sink(state, in_block, eos);
    }

private:
    friend class GroupCommitBlockSinkLocalState;
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
};

} // namespace pipeline
} // namespace doris