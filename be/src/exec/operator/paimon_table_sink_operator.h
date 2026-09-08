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

#include <gen_cpp/DataSinks_types.h>

#include <memory>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/operator.h"
#include "exec/sink/writer/paimon/paimon_table_writer.h"
#include "runtime/runtime_state.h"

namespace doris {

/// Paimon table sink operator.
///
/// Each pipeline instance (LocalState) owns one PaimonTableWriter, which in
/// turn owns one IPaimonWriteBackend + IPaimonWriter. Pipeline parallelism
/// determines the number of concurrent Paimon writer sessions per table.
/// Paimon writes are synchronous: sink_impl() returns only after the SDK has
/// consumed the input Block. The LocalState is therefore always blockable so
/// that open, write, and close run on the pipeline blocking scheduler.
/// Doris-owned Arrow buffers remain under the query MemTracker, while Paimon
/// pages are allocated lazily under DorisMemorySegmentPool's fixed cap. The
/// sink uses the generic pipeline minimum reservation only as an admission
/// guard; it does not try to predict Paimon's future page demand.
///
/// The upstream sink Exchange may reproduce Paimon's stateless HASH_FIXED
/// selector to establish unique writer ownership. The writer still passes
/// complete Blocks to the SDK, which independently computes partition and
/// bucket values for file writing; no routing column is appended to the row.
class PaimonTableSinkOperatorX;

class PaimonTableSinkLocalState final : public PipelineXSinkLocalState<FakeSharedState> {
public:
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    using Parent = PaimonTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(PaimonTableSinkLocalState);
    PaimonTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] bool is_blockable() const override { return true; }

private:
    friend class PaimonTableSinkOperatorX;

    VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<PaimonTableWriter> _writer;
};

class PaimonTableSinkOperatorX final : public DataSinkOperatorX<PaimonTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<PaimonTableSinkLocalState>;
    PaimonTableSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& t_output_expr)
            : Base(operator_id, 0, 0), _row_desc(row_desc), _t_output_expr(t_output_expr) {}

    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(Base::init(thrift_sink));
        DCHECK(thrift_sink.__isset.paimon_table_sink);
        RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
        return VExpr::open(_output_vexpr_ctxs, state);
    }

    Status sink_impl(RuntimeState* state, Block* in_block, bool eos) override;

private:
    friend class PaimonTableSinkLocalState;

    const RowDescriptor& _row_desc;
    VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
};

} // namespace doris
