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

#include <memory>

#include "exec/operator/operator.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

class IcebergTableSinkOperatorX;

class IcebergTableSinkLocalState final : public PipelineXSinkLocalState<FakeSharedState> {
public:
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    using Parent = IcebergTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(IcebergTableSinkLocalState);
    IcebergTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {};
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] bool is_blockable() const override { return true; }

private:
    friend class IcebergTableSinkOperatorX;

    VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<VIcebergTableWriter> _writer;
};

class IcebergTableSinkOperatorX final : public DataSinkOperatorX<IcebergTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<IcebergTableSinkLocalState>;
    IcebergTableSinkOperatorX(ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
                              const std::vector<TExpr>& t_output_expr)
            : Base(operator_id, 0, 0),
              _row_desc(row_desc),
              _t_output_expr(t_output_expr),
              _pool(pool) {};

    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(Base::init(thrift_sink));
        // From the thrift expressions create the real exprs.
        RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
        return VExpr::open(_output_vexpr_ctxs, state);
    }

    Status sink_impl(RuntimeState* state, Block* in_block, bool /*eos*/) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        if (in_block->rows() == 0) {
            return Status::OK();
        }
        DCHECK(local_state._writer);
        return local_state._writer->write(state, *in_block);
    }

private:
    friend class IcebergTableSinkLocalState;
    const RowDescriptor& _row_desc;
    VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris
