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
#include <mutex>

#include "exec/operator/operator.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

class SpillIcebergTableSinkLocalState;
class SpillIcebergTableSinkOperatorX;

class SpillIcebergTableSinkLocalState final : public PipelineXSinkLocalState<FakeSharedState> {
public:
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    using Parent = SpillIcebergTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(SpillIcebergTableSinkLocalState);

    SpillIcebergTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~SpillIcebergTableSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status sink(RuntimeState* state, Block* block, bool eos);
    Status close(RuntimeState* state, Status exec_status) override;

    bool is_blockable() const override;
    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state, bool eos);
    Status revoke_memory(RuntimeState* state);
    size_t get_revocable_mem_size(RuntimeState* state) const;

private:
    Status _close_writer(Status close_status);
    void _init_spill_counters();
    friend class SpillIcebergTableSinkOperatorX;
    friend class IcebergTableSinkOperatorTest;

    VExprContextSPtrs _output_vexpr_ctxs;
    // Protects table-writer pointer access/reset against concurrent workload-memory callbacks.
    // Blocking I/O stays outside this lock; a callback keeps a child-writer shared_ptr whose
    // sorter mutex serializes it with write/close.
    mutable std::mutex _writer_mutex;
    std::unique_ptr<VIcebergTableWriter> _writer;
    bool _writer_closed = false;
    Status _writer_close_status;
};

class SpillIcebergTableSinkOperatorX final
        : public DataSinkOperatorX<SpillIcebergTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<SpillIcebergTableSinkLocalState>;
    using LocalStateType = SpillIcebergTableSinkLocalState;

    SpillIcebergTableSinkOperatorX(ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& t_output_expr);

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;

    Status sink_impl(RuntimeState* state, Block* in_block, bool eos) override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

    using DataSinkOperatorX<LocalStateType>::node_id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    friend class SpillIcebergTableSinkLocalState;

    const RowDescriptor& _row_desc;
    VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris
