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

#include "common/status.h"
#include "exec/operator/iceberg_table_sink_operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"
#include "exec/spill/spill_file.h"

namespace doris {
#include "common/compile_check_begin.h"

SpillIcebergTableSinkLocalState::SpillIcebergTableSinkLocalState(DataSinkOperatorXBase* parent,
                                                                 RuntimeState* state)
        : Base(parent, state) {}

Status SpillIcebergTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _init_spill_counters();
    _writer = std::make_unique<VIcebergTableWriter>(info.tsink, _output_vexpr_ctxs);
    _writer->defer_file_cleanup_until_outer_close();

    auto& parent = _parent->cast<Parent>();
    RETURN_IF_ERROR(_writer->init_properties(parent._pool, parent._row_desc));
    return Status::OK();
}

Status SpillIcebergTableSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& parent = _parent->cast<Parent>();
    _output_vexpr_ctxs.resize(parent._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(parent._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    return _writer->open(state, operator_profile());
}

Status SpillIcebergTableSinkLocalState::sink(RuntimeState* state, Block* block, bool eos) {
    if (block->rows() > 0) {
        DCHECK(_writer);
        RETURN_IF_ERROR(_writer->write(state, *block));
    }
    if (!eos) {
        return Status::OK();
    }

    Status close_status = Status::OK();
    if (state->is_cancelled()) {
        close_status = state->cancel_reason();
    }
    return _close_writer(close_status);
}

Status SpillIcebergTableSinkLocalState::_close_writer(Status close_status) {
    DCHECK(_writer);
    if (!_writer_closed) {
        _writer_close_status = _writer->close(close_status);
        _writer_closed = true;
    }
    if (close_status.ok() && !_writer_close_status.ok()) {
        close_status = _writer_close_status;
    }
    return close_status;
}

Status SpillIcebergTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    Status final_status = exec_status;
    if (final_status.ok() && state->is_cancelled()) {
        final_status = state->cancel_reason();
    }
    final_status = _close_writer(final_status);
    DCHECK(_writer);
    _writer->finish_deferred_file_cleanup(final_status);
    {
        std::lock_guard lock(_writer_mutex);
        _writer.reset();
    }

    Status base_status = Base::close(state, final_status);
    if (final_status.ok() && !base_status.ok()) {
        final_status = base_status;
    }
    return final_status;
}

bool SpillIcebergTableSinkLocalState::is_blockable() const {
    return true;
}

size_t SpillIcebergTableSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    DCHECK(_writer);
    return _writer->get_reserve_mem_size(state, eos);
}

size_t SpillIcebergTableSinkLocalState::get_revocable_mem_size(RuntimeState* state) const {
    std::shared_ptr<const VIcebergTableWriter::PartitionWriterSnapshot> partition_writers;
    std::shared_ptr<IPartitionWriterBase> current_writer;
    {
        std::lock_guard lock(_writer_mutex);
        if (!_writer) {
            return 0;
        }
        partition_writers = _writer->partition_writers_snapshot();
        if (!partition_writers) {
            current_writer = _writer->current_writer();
        }
    }

    if (partition_writers) {
        size_t revocable_size = 0;
        for (const auto& sort_writer : *partition_writers) {
            size_t writer_size = sort_writer->data_size();
            if (writer_size >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                revocable_size += writer_size;
            }
        }
        return revocable_size;
    }

    if (!current_writer) {
        return 0;
    }
    auto* sort_writer = dynamic_cast<VIcebergSortWriter*>(current_writer.get());
    DORIS_CHECK(sort_writer != nullptr);
    return sort_writer->data_size();
}

Status SpillIcebergTableSinkLocalState::revoke_memory(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    std::shared_ptr<const VIcebergTableWriter::PartitionWriterSnapshot> partition_writers;
    std::shared_ptr<IPartitionWriterBase> current_writer;
    {
        std::lock_guard lock(_writer_mutex);
        if (!_writer) {
            return Status::OK();
        }
        partition_writers = _writer->partition_writers_snapshot();
        if (!partition_writers) {
            current_writer = _writer->current_writer();
        }
    }

    std::vector<std::shared_ptr<VIcebergSortWriter>> writers_to_spill;
    if (partition_writers) {
        for (const auto& sort_writer : *partition_writers) {
            if (sort_writer->data_size() >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                writers_to_spill.emplace_back(sort_writer);
            }
        }
    } else if (current_writer) {
        auto sort_writer = std::dynamic_pointer_cast<VIcebergSortWriter>(current_writer);
        DORIS_CHECK(sort_writer != nullptr);
        writers_to_spill.emplace_back(std::move(sort_writer));
    }
    if (writers_to_spill.empty()) {
        return Status::OK();
    }

    auto exception_catch_func = [writers = std::move(writers_to_spill)]() {
        auto status = [&]() {
            for (const auto& sort_writer : writers) {
                RETURN_IF_CATCH_EXCEPTION({ RETURN_IF_ERROR(sort_writer->trigger_spill()); });
            }
            return Status::OK();
        }();
        return status;
    };

    return run_spill_task(state, exception_catch_func);
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
    ADD_COUNTER_WITH_LEVEL(profile, "SpillWriteFileCurrentCount", TUnit::UNIT, 1);
}

#include "common/compile_check_end.h"
} // namespace doris
