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

#include "format/transformer/merge_partitioner.h"

#include <algorithm>
#include <cstdint>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/sink/sink_common.h"
#include "format/transformer/iceberg_partition_function.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {
int64_t scale_threshold_by_task(int64_t value, int task_num) {
    if (task_num <= 0) {
        return value;
    }
    int64_t scaled = value / task_num;
    return scaled == 0 ? value : scaled;
}
} // namespace

MergePartitioner::MergePartitioner(size_t partition_count, const TMergePartitionInfo& merge_info,
                                   bool use_new_shuffle_hash_method)
        : PartitionerBase(static_cast<HashValType>(partition_count)),
          _merge_info(merge_info),
          _use_new_shuffle_hash_method(use_new_shuffle_hash_method),
          _insert_random(merge_info.insert_random) {}

Status MergePartitioner::init(const std::vector<TExpr>& /*texprs*/) {
    VExprContextSPtr op_ctx;
    RETURN_IF_ERROR(VExpr::create_expr_tree(_merge_info.operation_expr, op_ctx));
    _operation_expr_ctxs.emplace_back(std::move(op_ctx));

    std::vector<TExpr> insert_exprs;
    std::vector<TIcebergPartitionField> insert_fields;
    if (_merge_info.__isset.insert_partition_exprs) {
        insert_exprs = _merge_info.insert_partition_exprs;
    }
    if (_merge_info.__isset.insert_partition_fields) {
        insert_fields = _merge_info.insert_partition_fields;
    }
    if (!insert_exprs.empty() || !insert_fields.empty()) {
        _insert_partition_function = std::make_unique<IcebergInsertPartitionFunction>(
                _partition_count, _hash_method(), std::move(insert_exprs),
                std::move(insert_fields));
        RETURN_IF_ERROR(_insert_partition_function->init({}));
    }

    if (_merge_info.__isset.delete_partition_exprs && !_merge_info.delete_partition_exprs.empty()) {
        _delete_partition_function = std::make_unique<IcebergDeletePartitionFunction>(
                _partition_count, _hash_method(), _merge_info.delete_partition_exprs);
        RETURN_IF_ERROR(_delete_partition_function->init({}));
    }
    return Status::OK();
}

Status MergePartitioner::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(VExpr::prepare(_operation_expr_ctxs, state, row_desc));
    if (_insert_partition_function != nullptr) {
        RETURN_IF_ERROR(_insert_partition_function->prepare(state, row_desc));
    }
    if (_delete_partition_function != nullptr) {
        RETURN_IF_ERROR(_delete_partition_function->prepare(state, row_desc));
    }
    return Status::OK();
}

Status MergePartitioner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VExpr::open(_operation_expr_ctxs, state));
    if (_insert_partition_function != nullptr) {
        RETURN_IF_ERROR(_insert_partition_function->open(state));
        if (auto* insert_function =
                    dynamic_cast<IcebergInsertPartitionFunction*>(_insert_partition_function.get());
            insert_function != nullptr && insert_function->fallback_to_random()) {
            _insert_random = true;
        }
    }
    if (_delete_partition_function != nullptr) {
        RETURN_IF_ERROR(_delete_partition_function->open(state));
    }
    _init_insert_scaling(state);
    return Status::OK();
}

Status MergePartitioner::close(RuntimeState* /*state*/) {
    return Status::OK();
}

Status MergePartitioner::do_partitioning(RuntimeState* state, Block* block) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        _channel_ids.clear();
        return Status::OK();
    }

    const size_t column_to_keep = block->columns();
    if (_operation_expr_ctxs.empty()) {
        return Status::InternalError("Merge partitioning missing operation expression");
    }

    int op_idx = -1;
    RETURN_IF_ERROR(_operation_expr_ctxs[0]->execute(block, &op_idx));
    if (op_idx < 0 || op_idx >= block->columns()) {
        return Status::InternalError("Merge partitioning missing operation column");
    }
    if (op_idx >= cast_set<int>(column_to_keep)) {
        return Status::InternalError("Merge partitioning requires operation column in input block");
    }

    const auto& op_column = block->get_by_position(op_idx).column;
    const auto* op_data = remove_nullable(op_column).get();
    std::vector<int8_t> ops(rows);
    bool has_insert = false;
    bool has_delete = false;
    bool has_update = false;
    for (size_t i = 0; i < rows; ++i) {
        int8_t op = static_cast<int8_t>(op_data->get_int(i));
        ops[i] = op;
        if (is_insert_op(op)) {
            has_insert = true;
        }
        if (is_delete_op(op)) {
            has_delete = true;
        }
        if (op == kUpdateOperation) {
            has_update = true;
        }
    }

    if (has_insert && !_insert_random && _insert_partition_function == nullptr) {
        return Status::InternalError("Merge partitioning insert exprs are empty");
    }
    if (has_delete && _delete_partition_function == nullptr) {
        return Status::InternalError("Merge partitioning delete exprs are empty");
    }

    std::vector<uint32_t> insert_hashes;
    std::vector<uint32_t> delete_hashes;
    const size_t insert_partition_count =
            _enable_insert_rebalance ? _insert_partition_count : _partition_count;
    if (has_insert && !_insert_random) {
        RETURN_IF_ERROR(_insert_partition_function->get_partitions(
                state, block, insert_partition_count, insert_hashes));
    }
    if (has_delete) {
        RETURN_IF_ERROR(_delete_partition_function->get_partitions(state, block, _partition_count,
                                                                   delete_hashes));
    }
    if (has_insert) {
        if (_insert_random) {
            if (_non_partition_scaling_threshold > 0) {
                _insert_data_processed += static_cast<int64_t>(block->bytes());
                if (_insert_writer_count < static_cast<int>(_partition_count) &&
                    _insert_data_processed >=
                            _insert_writer_count * _non_partition_scaling_threshold) {
                    _insert_writer_count++;
                }
            } else {
                _insert_writer_count = static_cast<int>(_partition_count);
            }
        } else if (_enable_insert_rebalance) {
            _apply_insert_rebalance(ops, insert_hashes, block->bytes());
        }
    }

    Block::erase_useless_column(block, column_to_keep);

    _channel_ids.resize(rows);
    for (size_t i = 0; i < rows; ++i) {
        const int8_t op = ops[i];
        if (op == kUpdateOperation) {
            _channel_ids[i] = delete_hashes[i];
            continue;
        }
        if (is_insert_op(op)) {
            _channel_ids[i] = _insert_random ? _next_rr_channel() : insert_hashes[i];
        } else if (is_delete_op(op)) {
            _channel_ids[i] = delete_hashes[i];
        } else {
            return Status::InternalError("Unknown Iceberg merge operation {}", op);
        }
    }

    if (has_update) {
        for (size_t col_idx = 0; col_idx < block->columns(); ++col_idx) {
            block->replace_by_position_if_const(col_idx);
        }

        MutableColumns mutable_columns = block->mutate_columns();
        MutableColumnPtr& op_mut = mutable_columns[op_idx];
        ColumnInt8* op_values_col = nullptr;
        if (auto* nullable_col = check_and_get_column<ColumnNullable>(op_mut.get())) {
            op_values_col =
                    check_and_get_column<ColumnInt8>(nullable_col->get_nested_column_ptr().get());
        } else {
            op_values_col = check_and_get_column<ColumnInt8>(op_mut.get());
        }
        if (op_values_col == nullptr) {
            block->set_columns(std::move(mutable_columns));
            return Status::InternalError("Merge operation column must be tinyint");
        }
        auto& op_values = op_values_col->get_data();
        // First pass: collect update row indices and mark original rows as DELETE.
        std::vector<size_t> update_rows;
        for (size_t row = 0; row < rows; ++row) {
            if (ops[row] != kUpdateOperation) {
                continue;
            }
            op_values[row] = kUpdateDeleteOperation;
            update_rows.push_back(row);
        }
        // Second pass: extract only the update rows into a temporary column,
        // then batch-append from it. This avoids cloning the entire column.
        for (size_t col_idx = 0; col_idx < mutable_columns.size(); ++col_idx) {
            auto tmp = mutable_columns[col_idx]->clone_empty();
            for (size_t row : update_rows) {
                tmp->insert_from(*mutable_columns[col_idx], row);
            }
            mutable_columns[col_idx]->insert_range_from(*tmp, 0, tmp->size());
        }
        // Mark the newly appended rows as INSERT and assign their channels.
        DCHECK(_insert_random || !insert_hashes.empty());
        const size_t appended_update_begin = rows;
        for (size_t idx = 0; idx < update_rows.size(); ++idx) {
            const size_t row = update_rows[idx];
            op_values[appended_update_begin + idx] = kUpdateInsertOperation;
            const uint32_t insert_channel =
                    _insert_random ? _next_rr_channel() : insert_hashes[row];
            _channel_ids.push_back(insert_channel);
        }
        block->set_columns(std::move(mutable_columns));
    }

    return Status::OK();
}

Status MergePartitioner::clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner =
            new MergePartitioner(_partition_count, _merge_info, _use_new_shuffle_hash_method);
    partitioner.reset(new_partitioner);
    RETURN_IF_ERROR(
            _clone_expr_ctxs(state, _operation_expr_ctxs, new_partitioner->_operation_expr_ctxs));
    if (_insert_partition_function != nullptr) {
        RETURN_IF_ERROR(_insert_partition_function->clone(
                state, new_partitioner->_insert_partition_function));
    }
    if (_delete_partition_function != nullptr) {
        RETURN_IF_ERROR(_delete_partition_function->clone(
                state, new_partitioner->_delete_partition_function));
    }
    new_partitioner->_insert_random = _insert_random;
    new_partitioner->_rr_offset = _rr_offset;
    return Status::OK();
}

void MergePartitioner::_apply_insert_rebalance(const std::vector<int8_t>& ops,
                                               std::vector<uint32_t>& insert_hashes,
                                               size_t block_bytes) const {
    if (!_enable_insert_rebalance || _insert_writer_assigner == nullptr) {
        return;
    }
    if (insert_hashes.empty() || _insert_partition_count == 0) {
        return;
    }
    std::vector<uint8_t> mask(ops.size(), 0);
    for (size_t i = 0; i < ops.size(); ++i) {
        if (is_insert_op(ops[i])) {
            mask[i] = 1;
        }
    }
    _insert_writer_assigner->assign(insert_hashes, &mask, ops.size(), block_bytes, insert_hashes);
}

void MergePartitioner::_init_insert_scaling(RuntimeState* state) {
    _enable_insert_rebalance = false;
    _insert_partition_count = 0;
    _insert_data_processed = 0;
    _insert_writer_count = 1;
    _insert_writer_assigner.reset();
    _non_partition_scaling_threshold =
            config::table_sink_non_partition_write_scaling_data_processed_threshold;

    if (_partition_count == 0) {
        return;
    }
    if (_insert_random) {
        return;
    }
    if (_insert_partition_function == nullptr) {
        return;
    }

    int max_partitions_per_writer =
            config::table_sink_partition_write_max_partition_nums_per_writer;
    if (max_partitions_per_writer <= 0) {
        return;
    }
    _insert_partition_count = _partition_count * max_partitions_per_writer;
    if (_insert_partition_count == 0) {
        return;
    }

    int task_num = state == nullptr ? 0 : state->task_num();
    int64_t min_partition_threshold = scale_threshold_by_task(
            config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold,
            task_num);
    int64_t min_data_threshold = scale_threshold_by_task(
            config::table_sink_partition_write_min_data_processed_rebalance_threshold, task_num);

    _insert_writer_assigner = std::make_unique<SkewedWriterAssigner>(
            static_cast<int>(_insert_partition_count), static_cast<int>(_partition_count), 1,
            min_partition_threshold, min_data_threshold);
    _enable_insert_rebalance = true;
}

uint32_t MergePartitioner::_next_rr_channel() const {
    uint32_t writer_count = static_cast<uint32_t>(_partition_count);
    if (_insert_random && _insert_writer_count > 0) {
        writer_count = std::min<uint32_t>(static_cast<uint32_t>(_partition_count),
                                          static_cast<uint32_t>(_insert_writer_count));
    }
    if (writer_count == 0) {
        return 0;
    }
    const uint32_t channel = _rr_offset % writer_count;
    _rr_offset = (_rr_offset + 1) % writer_count;
    return channel;
}

Status MergePartitioner::_clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                                          VExprContextSPtrs& dst) const {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        RETURN_IF_ERROR(src[i]->clone(state, dst[i]));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
