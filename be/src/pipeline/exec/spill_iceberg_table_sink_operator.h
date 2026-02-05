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

#include "operator.h"
#include "vec/sink/writer/iceberg/viceberg_table_writer.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class SpillIcebergTableSinkLocalState;
class SpillIcebergTableSinkOperatorX;

class SpillIcebergTableSinkLocalState final
        : public AsyncWriterSink<vectorized::VIcebergTableWriter, SpillIcebergTableSinkOperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VIcebergTableWriter, SpillIcebergTableSinkOperatorX>;
    using Parent = SpillIcebergTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(SpillIcebergTableSinkLocalState);

    SpillIcebergTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~SpillIcebergTableSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;

    bool is_blockable() const override;
    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state, bool eos);
    Status revoke_memory(RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context);
    size_t get_revocable_mem_size(RuntimeState* state) const;

private:
    void _init_spill_counters();
    friend class SpillIcebergTableSinkOperatorX;
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

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state,
                         const std::shared_ptr<SpillContext>& spill_context) override;

    using DataSinkOperatorX<LocalStateType>::node_id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    friend class SpillIcebergTableSinkLocalState;
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline