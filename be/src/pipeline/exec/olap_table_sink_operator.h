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
#include "vec/sink/vtablet_sink.h"

namespace doris {

namespace pipeline {

class OlapTableSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::VOlapTableSink> {
public:
    OlapTableSinkOperatorBuilder(int32_t id, DataSink* sink)
            : DataSinkOperatorBuilder(id, "OlapTableSinkOperator", sink) {}

    OperatorPtr build_operator() override;
};

class OlapTableSinkOperator final : public DataSinkOperator<OlapTableSinkOperatorBuilder> {
public:
    OlapTableSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
            : DataSinkOperator(operator_builder, sink) {}

    bool can_write() override { return true; } // TODO: need use mem_limit
};

class OlapTableSinkOperatorX;

class OlapTableSinkLocalState final
        : public AsyncWriterSink<vectorized::VTabletWriter, OlapTableSinkOperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VTabletWriter, OlapTableSinkOperatorX>;
    using Parent = OlapTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(OlapTableSinkLocalState);
    OlapTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {};
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override {
        SCOPED_TIMER(profile()->total_time_counter());
        SCOPED_TIMER(_open_timer);
        return Base::open(state);
    }

    Status close(RuntimeState* state, Status exec_status) override;
    friend class OlapTableSinkOperatorX;

private:
    Status _close_status = Status::OK();
};
class OlapTableSinkOperatorX final : public DataSinkOperatorX<OlapTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<OlapTableSinkLocalState>;
    OlapTableSinkOperatorX(ObjectPool* pool, const RowDescriptor& row_desc,
                           const std::vector<TExpr>& t_output_expr, bool group_commit)
            : Base(0),
              _row_desc(row_desc),
              _t_output_expr(t_output_expr),
              _group_commit(group_commit),
              _pool(pool) {};

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
    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
        SCOPED_TIMER(local_state.profile()->total_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        return local_state.sink(state, in_block, source_state);
    }

    FinishDependency* finish_blocked_by(RuntimeState* state) const override {
        auto& local_state = state->get_sink_local_state(id())->cast<OlapTableSinkLocalState>();
        return local_state._finish_dependency->finish_blocked_by();
    };

    WriteDependency* wait_for_dependency(RuntimeState* state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
        return local_state.write_blocked_by();
    }

private:
    friend class OlapTableSinkLocalState;
    template <typename Writer, typename Parent>
    friend class AsyncWriterSink;
    const RowDescriptor& _row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    const bool _group_commit;
    ObjectPool* _pool;
};

} // namespace pipeline
} // namespace doris