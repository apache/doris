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

#include "exec/operator/spill_iceberg_table_sink_operator.h"

#include <algorithm>

#include "common/status.h"
#include "exec/operator/iceberg_table_sink_operator.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"

namespace doris {

size_t bounded_iceberg_reserve_size(const std::vector<size_t>& per_partition_reservations) {
    return per_partition_reservations.empty()
                   ? 0
                   : *std::max_element(per_partition_reservations.begin(),
                                       per_partition_reservations.end());
}

SpillIcebergTableSinkLocalState::SpillIcebergTableSinkLocalState(DataSinkOperatorXBase* parent,
                                                                 RuntimeState* state)
        : Base(parent, state) {}

Status SpillIcebergTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _init_spill_counters();

    auto& p = _parent->cast<Parent>();
    RETURN_IF_ERROR(_writer->init_properties(p._pool, p._row_desc));
    return Status::OK();
}

Status SpillIcebergTableSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

bool SpillIcebergTableSinkLocalState::is_blockable() const {
    return true;
}

size_t SpillIcebergTableSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    if (!_writer) {
        return 0;
    }
    std::vector<size_t> per_partition_reservations;
    auto active_writers = _writer->active_writers();
    per_partition_reservations.reserve(active_writers->size());
    for (const auto& writer : *active_writers) {
        if (auto* sort_writer = dynamic_cast<VIcebergSortWriter*>(writer.get())) {
            per_partition_reservations.push_back(sort_writer->get_reserve_mem_size(state, eos));
        }
    }
    // One input block is partitioned among writers and consumed serially, so their full-batch estimates overlap.
    return bounded_iceberg_reserve_size(per_partition_reservations);
}

size_t SpillIcebergTableSinkLocalState::get_revocable_mem_size(RuntimeState* state) const {
    if (!_writer) {
        return 0;
    }
    size_t revocable_size = 0;
    // Retain the published container while the async writer may replace the current snapshot.
    auto active_writers = _writer->active_writers();
    for (const auto& writer : *active_writers) {
        if (auto* sort_writer = dynamic_cast<VIcebergSortWriter*>(writer.get())) {
            revocable_size += sort_writer->data_size();
        }
    }
    return revocable_size;
}

Status SpillIcebergTableSinkLocalState::revoke_memory(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    if (!_writer) {
        return Status::OK();
    }
    std::shared_ptr<IPartitionWriterBase> largest_writer;
    size_t largest_size = 0;
    // Retain the published container while the async writer may replace the current snapshot.
    auto active_writers = _writer->active_writers();
    for (const auto& writer : *active_writers) {
        if (auto* sort_writer = dynamic_cast<VIcebergSortWriter*>(writer.get())) {
            size_t size = sort_writer->data_size();
            if (size > largest_size) {
                largest_size = size;
                largest_writer = writer;
            }
        }
    }
    if (largest_writer != nullptr) {
        // Repeated revocation drains the largest partition first without launching O(P) spill jobs at once.
        auto* sort_writer = dynamic_cast<VIcebergSortWriter*>(largest_writer.get());
        RETURN_IF_CATCH_EXCEPTION({ RETURN_IF_ERROR(sort_writer->trigger_spill()); });
    }
    return Status::OK();
}

SpillIcebergTableSinkOperatorX::SpillIcebergTableSinkOperatorX(
        ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
        const std::vector<TExpr>& t_output_expr)
        : Base(operator_id, 0, 0), _row_desc(row_desc), _t_output_expr(t_output_expr), _pool(pool) {
    _spillable = true;
}

Status SpillIcebergTableSinkOperatorX::init(const TDataSink& thrift_sink) {
    RETURN_IF_ERROR(Base::init(thrift_sink));
    _name = "SPILL_ICEBERG_TABLE_SINK_OPERATOR";
    RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    return Status::OK();
}

Status SpillIcebergTableSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status SpillIcebergTableSinkOperatorX::sink_impl(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    return local_state.sink(state, in_block, eos);
}

size_t SpillIcebergTableSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

size_t SpillIcebergTableSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state.get_revocable_mem_size(state);
}

Status SpillIcebergTableSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

void SpillIcebergTableSinkLocalState::_init_spill_counters() {
    auto* profile = custom_profile();
    //seems init_spill_write_counters()
    ADD_TIMER_WITH_LEVEL(profile, "SpillWriteTime", 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteTaskCount", TUnit::UNIT, 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillWriteTaskWaitInQueueTime", 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillWriteFileTime", 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillWriteSerializeBlockTime", 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteBlockCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteBlockBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteFileBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteRows", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteFileTotalCount", TUnit::UNIT, 1);

    //seems init_spill_read_counters()
    ADD_TIMER_WITH_LEVEL(profile, "SpillTotalTime", 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillRecoverTime", 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadTaskWaitInQueueCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadTaskCount", TUnit::UNIT, 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillReadTaskWaitInQueueTime", 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillReadFileTime", 1);
    ADD_TIMER_WITH_LEVEL(profile, "SpillReadDeserializeBlockTime", 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadBlockCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadBlockBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadFileBytes", TUnit::BYTES, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadRows", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillReadFileCount", TUnit::UNIT, 1);
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteFileCurrentBytes", TUnit::BYTES, 1);
}

} // namespace doris
