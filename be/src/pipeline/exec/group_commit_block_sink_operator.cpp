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

#include "group_commit_block_sink_operator.h"

#include <gen_cpp/DataSinks_types.h>

#include "runtime/group_commit_mgr.h"

namespace doris::pipeline {

GroupCommitBlockSinkLocalState::~GroupCommitBlockSinkLocalState() {
    if (_load_block_queue) {
        _remove_estimated_wal_bytes();
        _load_block_queue->remove_load_id(_parent->cast<GroupCommitBlockSinkOperatorX>()._load_id);
        _load_block_queue->group_commit_load_count.fetch_add(1);
    }
}

Status GroupCommitBlockSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<GroupCommitBlockSinkOperatorX>();
    _group_commit_mode = p._group_commit_mode;
    _vpartition = std::make_unique<doris::VOlapTablePartitionParam>(p._schema, p._partition);
    RETURN_IF_ERROR(_vpartition->init());
    _state = state;
    // profile must add to state's object pool
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    _block_convertor = std::make_unique<vectorized::OlapTableBlockConvertor>(p._output_tuple_desc);
    _block_convertor->init_autoinc_info(p._schema->db_id(), p._schema->table_id(),
                                        _state->batch_size());

    _output_vexpr_ctxs.resize(p._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    return Status::OK();
}

Status GroupCommitBlockSinkLocalState::close(RuntimeState* state, Status close_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    RETURN_IF_ERROR(Base::close(state, close_status));
    RETURN_IF_ERROR(close_status);
    int64_t total_rows = state->num_rows_load_total();
    int64_t loaded_rows = state->num_rows_load_total();
    state->set_num_rows_load_total(loaded_rows + state->num_rows_load_unselected() +
                                   state->num_rows_load_filtered());
    state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() + total_rows -
                                         loaded_rows);
    auto& p = _parent->cast<GroupCommitBlockSinkOperatorX>();
    if (!_is_block_appended) {
        // if not meet the max_filter_ratio, we should return error status directly
        int64_t num_selected_rows =
                state->num_rows_load_total() - state->num_rows_load_unselected();
        if (num_selected_rows > 0 &&
            (double)state->num_rows_load_filtered() / num_selected_rows > p._max_filter_ratio) {
            return Status::DataQualityError("too many filtered rows");
        }
        RETURN_IF_ERROR(_add_blocks(state, true));
    }
    if (_load_block_queue) {
        _remove_estimated_wal_bytes();
        _load_block_queue->remove_load_id(p._load_id);
    }
    // wait to wal
    auto st = Status::OK();
    if (_load_block_queue && (_load_block_queue->wait_internal_group_commit_finish ||
                              _group_commit_mode == TGroupCommitMode::SYNC_MODE)) {
        std::unique_lock l(_load_block_queue->mutex);
        if (!_load_block_queue->process_finish) {
            _load_block_queue->internal_group_commit_finish_cv.wait(l);
        }
        st = _load_block_queue->status;
    }
    return st;
}

Status GroupCommitBlockSinkLocalState::_add_block(RuntimeState* state,
                                                  std::shared_ptr<vectorized::Block> block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    // the insert group commit tvf always accept nullable columns, so we should convert
    // the non-nullable columns to nullable columns
    for (int i = 0; i < block->columns(); ++i) {
        if (block->get_by_position(i).type->is_nullable()) {
            continue;
        }
        block->get_by_position(i).column = make_nullable(block->get_by_position(i).column);
        block->get_by_position(i).type = make_nullable(block->get_by_position(i).type);
    }
    // add block to queue
    auto cur_mutable_block = vectorized::MutableBlock::create_unique(block->clone_empty());
    {
        vectorized::IColumn::Selector selector;
        for (auto i = 0; i < block->rows(); i++) {
            selector.emplace_back(i);
        }
        block->append_to_block_by_selector(cur_mutable_block.get(), selector);
    }
    std::shared_ptr<vectorized::Block> output_block = vectorized::Block::create_shared();
    output_block->swap(cur_mutable_block->to_block());
    if (!_is_block_appended && state->num_rows_load_total() + state->num_rows_load_unselected() +
                                               state->num_rows_load_filtered() <=
                                       config::group_commit_memory_rows_for_max_filter_ratio) {
        _blocks.emplace_back(output_block);
    } else {
        if (!_is_block_appended) {
            RETURN_IF_ERROR(_add_blocks(state, false));
        }
        RETURN_IF_ERROR(_load_block_queue->add_block(
                state, output_block, _group_commit_mode == TGroupCommitMode::ASYNC_MODE));
    }
    return Status::OK();
}

size_t GroupCommitBlockSinkLocalState::_calculate_estimated_wal_bytes(
        bool is_blocks_contain_all_load_data) {
    size_t blocks_size = 0;
    for (auto block : _blocks) {
        blocks_size += block->bytes();
    }
    return is_blocks_contain_all_load_data
                   ? blocks_size
                   : (blocks_size > _state->content_length() ? blocks_size
                                                             : _state->content_length());
}

void GroupCommitBlockSinkLocalState::_remove_estimated_wal_bytes() {
    if (_estimated_wal_bytes == 0) {
        return;
    } else {
        std::string wal_path;
        Status st = ExecEnv::GetInstance()->wal_mgr()->get_wal_path(_load_block_queue->txn_id,
                                                                    wal_path);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to get wal path in remove estimated wal bytes, reason: "
                         << st.to_string();
            return;
        }
        st = ExecEnv::GetInstance()->wal_mgr()->update_wal_dir_estimated_wal_bytes(
                WalManager::get_base_wal_path(wal_path), 0, _estimated_wal_bytes);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to remove estimated wal bytes, reason: " << st.to_string();
            return;
        }
        _estimated_wal_bytes = 0;
    }
};

Status GroupCommitBlockSinkLocalState::_add_blocks(RuntimeState* state,
                                                   bool is_blocks_contain_all_load_data) {
    DCHECK(_is_block_appended == false);
    auto& p = _parent->cast<GroupCommitBlockSinkOperatorX>();
    TUniqueId load_id;
    load_id.__set_hi(p._load_id.hi);
    load_id.__set_lo(p._load_id.lo);
    if (_load_block_queue == nullptr) {
        if (_state->exec_env()->wal_mgr()->is_running()) {
            RETURN_IF_ERROR(_state->exec_env()->group_commit_mgr()->get_first_block_load_queue(
                    p._db_id, p._table_id, p._base_schema_version, load_id, _load_block_queue,
                    _state->be_exec_version(), _state->query_mem_tracker()));
            if (_group_commit_mode == TGroupCommitMode::ASYNC_MODE) {
                size_t estimated_wal_bytes =
                        _calculate_estimated_wal_bytes(is_blocks_contain_all_load_data);
                _group_commit_mode =
                        _load_block_queue->has_enough_wal_disk_space(estimated_wal_bytes)
                                ? TGroupCommitMode::ASYNC_MODE
                                : TGroupCommitMode::SYNC_MODE;
                if (_group_commit_mode == TGroupCommitMode::SYNC_MODE) {
                    LOG(INFO) << "Load id=" << print_id(_state->query_id())
                              << ", use group commit label=" << _load_block_queue->label
                              << " will not write wal because wal disk space usage reach max "
                                 "limit. Detail info: "
                              << _state->exec_env()->wal_mgr()->get_wal_dirs_info_string();
                } else {
                    _estimated_wal_bytes = estimated_wal_bytes;
                }
            }
            _state->set_import_label(_load_block_queue->label);
            _state->set_wal_id(_load_block_queue->txn_id);
        } else {
            return Status::InternalError("be is stopping");
        }
    }
    for (auto it = _blocks.begin(); it != _blocks.end(); ++it) {
        RETURN_IF_ERROR(_load_block_queue->add_block(
                state, *it, _group_commit_mode == TGroupCommitMode::ASYNC_MODE));
    }
    _is_block_appended = true;
    _blocks.clear();
    DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg", {
        if (_load_block_queue) {
            _remove_estimated_wal_bytes();
            _load_block_queue->remove_load_id(p._load_id);
        }
        if (ExecEnv::GetInstance()->group_commit_mgr()->debug_future.wait_for(
                    std ::chrono ::seconds(60)) == std ::future_status ::ready) {
            auto st = ExecEnv::GetInstance()->group_commit_mgr()->debug_future.get();
            ExecEnv::GetInstance()->group_commit_mgr()->debug_promise = std::promise<Status>();
            ExecEnv::GetInstance()->group_commit_mgr()->debug_future =
                    ExecEnv::GetInstance()->group_commit_mgr()->debug_promise.get_future();
            LOG(INFO) << "debug future output: " << st.to_string();
            RETURN_IF_ERROR(st);
        }
    });
    return Status::OK();
}

Status GroupCommitBlockSinkOperatorX::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _tuple_desc_id = table_sink.tuple_id;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _db_id = table_sink.db_id;
    _table_id = table_sink.table_id;
    _base_schema_version = table_sink.base_schema_version;
    _partition = table_sink.partition;
    _group_commit_mode = table_sink.group_commit_mode;
    _load_id = table_sink.load_id;
    _max_filter_ratio = table_sink.max_filter_ratio;
    return Status::OK();
}

Status GroupCommitBlockSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }
    return vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc);
}

Status GroupCommitBlockSinkOperatorX::open(RuntimeState* state) {
    // Prepare the exprs to run.
    return vectorized::VExpr::open(_output_vexpr_ctxs, state);
}

Status GroupCommitBlockSinkOperatorX::sink(RuntimeState* state, vectorized::Block* input_block,
                                           bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
    SCOPED_CONSUME_MEM_TRACKER(local_state._mem_tracker.get());
    Status status = Status::OK();

    auto rows = input_block->rows();
    auto bytes = input_block->bytes();
    if (UNLIKELY(rows == 0)) {
        return status;
    }

    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(rows);
    state->update_num_bytes_load_total(bytes);

    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    RETURN_IF_ERROR(local_state._block_convertor->validate_and_convert_block(
            state, input_block, block, local_state._output_vexpr_ctxs, rows, has_filtered_rows));
    local_state._has_filtered_rows = false;
    if (!local_state._vpartition->is_auto_partition()) {
        //reuse vars for find_partition
        local_state._partitions.assign(rows, nullptr);
        local_state._filter_bitmap.Reset(rows);

        for (int index = 0; index < rows; index++) {
            local_state._vpartition->find_partition(block.get(), index,
                                                    local_state._partitions[index]);
        }
        for (int row_index = 0; row_index < rows; row_index++) {
            if (local_state._partitions[row_index] == nullptr) [[unlikely]] {
                local_state._filter_bitmap.Set(row_index, true);
                LOG(WARNING) << "no partition for this tuple. tuple="
                             << block->dump_data(row_index, 1);
            }
            local_state._has_filtered_rows = true;
        }
    }

    if (local_state._block_convertor->num_filtered_rows() > 0 || local_state._has_filtered_rows) {
        auto cloneBlock = block->clone_without_columns();
        auto res_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        for (int i = 0; i < rows; ++i) {
            if (local_state._block_convertor->filter_map()[i]) {
                continue;
            }
            if (local_state._filter_bitmap.Get(i)) {
                continue;
            }
            res_block.add_row(block.get(), i);
        }
        block->swap(res_block.to_block());
    }
    // add block into block queue
    return local_state._add_block(state, block);
}

} // namespace doris::pipeline
