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
#include <string>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/operator.h"
#include "exec/sink/writer/paimon/vpaimon_table_writer.h"
#include "runtime/runtime_state.h"

namespace doris {

/// Paimon table sink operator — simple pass-through to AsyncWriterSink.
///
/// Each pipeline instance (LocalState) owns one VPaimonTableWriter, which in
/// turn owns one IPaimonWriteBackend + IPaimonWriter. Pipeline parallelism
/// determines the number of concurrent Paimon writer sessions per table.
///
/// Partition and bucket routing is performed internally by the Paimon SDK
/// (Java via JNI, or Rust via FFI). Doris does not compute partition values
/// or bucket ids; it passes complete Blocks through the backend to the SDK,
/// where each row is routed via getPartition(row) + getBucket(row).
///
/// This mirrors Iceberg's approach: IcebergTableSinkOperatorX delegates to
/// AsyncWriterSink<VIcebergTableWriter>, with partition routing inside
/// VIcebergTableWriter::write().
class PaimonTableSinkOperatorX;

class PaimonTableSinkLocalState final
        : public AsyncWriterSink<VPaimonTableWriter, PaimonTableSinkOperatorX> {
public:
    using Base = AsyncWriterSink<VPaimonTableWriter, PaimonTableSinkOperatorX>;
    using Parent = PaimonTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(PaimonTableSinkLocalState);
    PaimonTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override {
        SCOPED_TIMER(exec_time_counter());
        SCOPED_TIMER(_open_timer);
        return Base::open(state);
    }

    friend class PaimonTableSinkOperatorX;
};

class PaimonTableSinkOperatorX final : public DataSinkOperatorX<PaimonTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<PaimonTableSinkLocalState>;
    PaimonTableSinkOperatorX(ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& t_output_expr)
            : Base(operator_id, 0, 0),
              _row_desc(row_desc),
              _t_output_expr(t_output_expr),
              _pool(pool) {}

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
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

} // namespace doris
