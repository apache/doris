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

#include "vec/sink/vrow_distribution.h"

#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/thrift_rpc_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/sink/writer/vtablet_writer.h"

namespace doris::vectorized {

std::pair<vectorized::VExprContextSPtrs, vectorized::VExprSPtrs>
VRowDistribution::_get_partition_function() {
    return {_vpartition->get_part_func_ctx(), _vpartition->get_partition_function()};
}

Status VRowDistribution::_save_missing_values(
        std::vector<std::vector<std::string>>& col_strs, // non-const ref for move
        int col_size, Block* block, std::vector<int64_t> filter,
        const std::vector<const NullMap*>& col_null_maps) {
    // de-duplication for new partitions but save all rows.
    RETURN_IF_ERROR(_batching_block->add_rows(block, filter));
    std::vector<TNullableStringLiteral> cur_row_values;
    for (int row = 0; row < col_strs[0].size(); ++row) {
        cur_row_values.clear();
        for (int col = 0; col < col_size; ++col) {
            TNullableStringLiteral node;
            const auto* null_map = col_null_maps[col]; // null map for this col
            node.__set_is_null((null_map && (*null_map)[filter[row]])
                                       ? true
                                       : node.is_null); // if not, dont change(default false)
            if (!node.is_null) {
                node.__set_value(col_strs[col][row]);
            }
            cur_row_values.push_back(node);
        }
        //For duplicate cur_values, they will be filtered in FE
        _partitions_need_create.emplace_back(cur_row_values);
    }

    // to avoid too large mem use
    if (_batching_block->rows() > _batch_size) {
        _deal_batched = true;
    }

    VLOG_NOTICE << "pushed some batching lines, now numbers = " << _batching_rows;

    return Status::OK();
}

void VRowDistribution::clear_batching_stats() {
    _partitions_need_create.clear();
    _batching_rows = 0;
    _batching_bytes = 0;
}

Status VRowDistribution::automatic_create_partition() {
    SCOPED_TIMER(_add_partition_request_timer);
    TCreatePartitionRequest request;
    TCreatePartitionResult result;
    string be_endpoint = BackendOptions::get_be_endpoint();
    request.__set_txn_id(_txn_id);
    request.__set_db_id(_vpartition->db_id());
    request.__set_table_id(_vpartition->table_id());
    request.__set_partitionValues(_partitions_need_create);
    request.__set_be_endpoint(be_endpoint);

    VLOG_NOTICE << "automatic partition rpc begin request " << request;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    int time_out = _state->execution_timeout() * 1000;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->createPartition(result, request);
            },
            time_out));

    Status status(Status::create(result.status));
    VLOG_NOTICE << "automatic partition rpc end response " << result;
    if (result.status.status_code == TStatusCode::OK) {
        // add new created partitions
        RETURN_IF_ERROR(_vpartition->add_partitions(result.partitions));
        RETURN_IF_ERROR(_create_partition_callback(_caller, &result));
    }

    return status;
}

// for reuse the same create callback of create-partition
static TCreatePartitionResult cast_as_create_result(TReplacePartitionResult& arg) {
    TCreatePartitionResult result;
    result.status = arg.status;
    result.nodes = std::move(arg.nodes);
    result.partitions = std::move(arg.partitions);
    result.tablets = std::move(arg.tablets);
    return result;
}

// use _partitions and replace them
Status VRowDistribution::_replace_overwriting_partition() {
    SCOPED_TIMER(_add_partition_request_timer);
    TReplacePartitionRequest request;
    TReplacePartitionResult result;
    request.__set_overwrite_group_id(_vpartition->get_overwrite_group_id());
    request.__set_db_id(_vpartition->db_id());
    request.__set_table_id(_vpartition->table_id());

    // only request for partitions not recorded for replacement
    std::set<int64_t> id_deduper;
    for (const auto* part : _partitions) {
        if (part == nullptr) [[unlikely]] {
            return Status::InternalError(
                    "Cannot found origin partitions in auto detect overwriting, stop processing");
        }
        if (_new_partition_ids.contains(part->id)) {
            // this is a new partition. dont replace again.
        } else {
            // request for replacement
            id_deduper.insert(part->id);
        }
    }
    if (id_deduper.empty()) {
        return Status::OK(); // no need to request
    }
    // de-duplicate. there's no check in FE
    std::vector<int64_t> request_part_ids(id_deduper.begin(), id_deduper.end());

    request.__set_partition_ids(request_part_ids);

    string be_endpoint = BackendOptions::get_be_endpoint();
    request.__set_be_endpoint(be_endpoint);

    VLOG_NOTICE << "auto detect replace partition request: " << request;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    int time_out = _state->execution_timeout() * 1000;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->replacePartition(result, request);
            },
            time_out));

    Status status(Status::create(result.status));
    VLOG_NOTICE << "auto detect replace partition result: " << result;
    if (result.status.status_code == TStatusCode::OK) {
        // record new partitions
        for (const auto& part : result.partitions) {
            _new_partition_ids.insert(part.id);
        }
        // replace data in _partitions
        RETURN_IF_ERROR(_vpartition->replace_partitions(request_part_ids, result.partitions));
        // reuse the function as the args' structure are same. it add nodes/locations and incremental_open
        auto result_as_create = cast_as_create_result(result);
        RETURN_IF_ERROR(_create_partition_callback(_caller, &result_as_create));
    }

    return status;
}

void VRowDistribution::_get_tablet_ids(vectorized::Block* block, int32_t index_idx,
                                       std::vector<int64_t>& tablet_ids) {
    tablet_ids.reserve(block->rows());
    for (int row_idx = 0; row_idx < block->rows(); row_idx++) {
        if (_skip[row_idx]) {
            continue;
        }
        auto& partition = _partitions[row_idx];
        auto& tablet_index = _tablet_indexes[row_idx];
        auto& index = partition->indexes[index_idx];

        auto tablet_id = index.tablets[tablet_index];
        tablet_ids[row_idx] = tablet_id;
    }
}

void VRowDistribution::_filter_block_by_skip(vectorized::Block* block,
                                             RowPartTabletIds& row_part_tablet_id) {
    auto& row_ids = row_part_tablet_id.row_ids;
    auto& partition_ids = row_part_tablet_id.partition_ids;
    auto& tablet_ids = row_part_tablet_id.tablet_ids;

    for (size_t i = 0; i < block->rows(); i++) {
        if (!_skip[i]) {
            row_ids.emplace_back(i);
            partition_ids.emplace_back(_partitions[i]->id);
            tablet_ids.emplace_back(_tablet_ids[i]);
        }
    }
}

Status VRowDistribution::_filter_block_by_skip_and_where_clause(
        vectorized::Block* block, const vectorized::VExprContextSPtr& where_clause,
        RowPartTabletIds& row_part_tablet_id) {
    // TODO
    //SCOPED_RAW_TIMER(&_stat.where_clause_ns);
    int result_index = -1;
    size_t column_number = block->columns();
    RETURN_IF_ERROR(where_clause->execute(block, &result_index));

    auto filter_column = block->get_by_position(result_index).column;

    auto& row_ids = row_part_tablet_id.row_ids;
    auto& partition_ids = row_part_tablet_id.partition_ids;
    auto& tablet_ids = row_part_tablet_id.tablet_ids;
    if (const auto* nullable_column =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(*filter_column)) {
        for (size_t i = 0; i < block->rows(); i++) {
            if (nullable_column->get_bool_inline(i) && !_skip[i]) {
                row_ids.emplace_back(i);
                partition_ids.emplace_back(_partitions[i]->id);
                tablet_ids.emplace_back(_tablet_ids[i]);
            }
        }
    } else if (const auto* const_column =
                       vectorized::check_and_get_column<vectorized::ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            return Status::OK();
        }
        // should we optimize?
        _filter_block_by_skip(block, row_part_tablet_id);
    } else {
        const auto& filter = assert_cast<const vectorized::ColumnUInt8&>(*filter_column).get_data();
        for (size_t i = 0; i < block->rows(); i++) {
            if (filter[i] != 0 && !_skip[i]) {
                row_ids.emplace_back(i);
                partition_ids.emplace_back(_partitions[i]->id);
                tablet_ids.emplace_back(_tablet_ids[i]);
            }
        }
    }

    for (size_t i = block->columns() - 1; i >= column_number; i--) {
        block->erase(i);
    }
    return Status::OK();
}

Status VRowDistribution::_filter_block(vectorized::Block* block,
                                       std::vector<RowPartTabletIds>& row_part_tablet_ids) {
    for (int i = 0; i < _schema->indexes().size(); i++) {
        _get_tablet_ids(block, i, _tablet_ids);
        auto& where_clause = _schema->indexes()[i]->where_clause;
        if (where_clause != nullptr) {
            RETURN_IF_ERROR(_filter_block_by_skip_and_where_clause(block, where_clause,
                                                                   row_part_tablet_ids[i]));
        } else {
            _filter_block_by_skip(block, row_part_tablet_ids[i]);
        }
    }
    return Status::OK();
}

Status VRowDistribution::_generate_rows_distribution_for_non_auto_partition(
        vectorized::Block* block, bool has_filtered_rows,
        std::vector<RowPartTabletIds>& row_part_tablet_ids) {
    auto num_rows = block->rows();

    bool stop_processing = false;
    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, num_rows, _partitions,
                                                 _tablet_indexes, stop_processing, _skip));
    if (has_filtered_rows) {
        for (int i = 0; i < num_rows; i++) {
            _skip[i] = _skip[i] || _block_convertor->filter_map()[i];
        }
    }
    RETURN_IF_ERROR(_filter_block(block, row_part_tablet_ids));
    return Status::OK();
}

Status VRowDistribution::_generate_rows_distribution_for_auto_partition(
        vectorized::Block* block, const std::vector<uint16_t>& partition_cols_idx,
        bool has_filtered_rows, std::vector<RowPartTabletIds>& row_part_tablet_ids,
        int64_t& rows_stat_val) {
    auto num_rows = block->rows();
    std::vector<uint16_t> partition_keys = _vpartition->get_partition_keys();

    auto partition_col = block->get_by_position(partition_keys[0]);
    _missing_map.clear();
    _missing_map.reserve(partition_col.column->size());
    bool stop_processing = false;

    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, num_rows, _partitions,
                                                 _tablet_indexes, stop_processing, _skip,
                                                 &_missing_map));

    // the missing vals for auto partition are also skipped.
    if (has_filtered_rows) {
        for (int i = 0; i < num_rows; i++) {
            _skip[i] = _skip[i] || _block_convertor->filter_map()[i];
        }
    }
    RETURN_IF_ERROR(_filter_block(block, row_part_tablet_ids));

    if (!_missing_map.empty()) {
        // for missing partition keys, calc the missing partition and save in _partitions_need_create
        auto [part_ctxs, part_exprs] = _get_partition_function();
        auto part_col_num = part_exprs.size();
        // the two vectors are in column-first-order
        std::vector<std::vector<std::string>> col_strs;
        std::vector<const NullMap*> col_null_maps;
        col_strs.resize(part_col_num);
        col_null_maps.reserve(part_col_num);

        for (int i = 0; i < part_col_num; ++i) {
            auto return_type = part_exprs[i]->data_type();
            // expose the data column. the return type would be nullable
            const auto& [range_left_col, col_const] =
                    unpack_if_const(block->get_by_position(partition_cols_idx[i]).column);
            if (range_left_col->is_nullable()) {
                col_null_maps.push_back(&(assert_cast<const ColumnNullable*>(range_left_col.get())
                                                  ->get_null_map_data()));
            } else {
                col_null_maps.push_back(nullptr);
            }
            for (auto row : _missing_map) {
                col_strs[i].push_back(
                        return_type->to_string(*range_left_col, index_check_const(row, col_const)));
            }
        }

        // calc the end value and save them. in the end of sending, we will create partitions for them and deal them.
        RETURN_IF_ERROR(
                _save_missing_values(col_strs, part_col_num, block, _missing_map, col_null_maps));

        size_t new_bt_rows = _batching_block->rows();
        size_t new_bt_bytes = _batching_block->bytes();
        rows_stat_val -= new_bt_rows - _batching_rows;
        _state->update_num_rows_load_total(_batching_rows - new_bt_rows);
        _state->update_num_bytes_load_total(_batching_bytes - new_bt_bytes);
        DorisMetrics::instance()->load_rows->increment(_batching_rows - new_bt_rows);
        DorisMetrics::instance()->load_bytes->increment(_batching_bytes - new_bt_bytes);
        _batching_rows = new_bt_rows;
        _batching_bytes = new_bt_bytes;
    }
    return Status::OK();
}

Status VRowDistribution::_generate_rows_distribution_for_auto_overwrite(
        vectorized::Block* block, bool has_filtered_rows,
        std::vector<RowPartTabletIds>& row_part_tablet_ids) {
    auto num_rows = block->rows();

    bool stop_processing = false;
    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, num_rows, _partitions,
                                                 _tablet_indexes, stop_processing, _skip));
    RETURN_IF_ERROR(_replace_overwriting_partition());

    // regenerate locations for new partitions & tablets
    _reset_find_tablets(num_rows);
    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, num_rows, _partitions,
                                                 _tablet_indexes, stop_processing, _skip));
    if (has_filtered_rows) {
        for (int i = 0; i < num_rows; i++) {
            _skip[i] = _skip[i] || _block_convertor->filter_map()[i];
        }
    }
    RETURN_IF_ERROR(_filter_block(block, row_part_tablet_ids));
    return Status::OK();
}

void VRowDistribution::_reset_row_part_tablet_ids(
        std::vector<RowPartTabletIds>& row_part_tablet_ids, int64_t rows) {
    row_part_tablet_ids.resize(_schema->indexes().size());
    for (auto& row_part_tablet_id : row_part_tablet_ids) {
        auto& row_ids = row_part_tablet_id.row_ids;
        auto& partition_ids = row_part_tablet_id.partition_ids;
        auto& tablet_ids = row_part_tablet_id.tablet_ids;

        row_ids.clear();
        partition_ids.clear();
        tablet_ids.clear();
        // This is important for performance.
        row_ids.reserve(rows);
        partition_ids.reserve(rows);
        tablet_ids.reserve(rows);
    }
}

Status VRowDistribution::generate_rows_distribution(
        vectorized::Block& input_block, std::shared_ptr<vectorized::Block>& block,
        int64_t& filtered_rows, bool& has_filtered_rows,
        std::vector<RowPartTabletIds>& row_part_tablet_ids, int64_t& rows_stat_val) {
    auto input_rows = input_block.rows();
    _reset_row_part_tablet_ids(row_part_tablet_ids, input_rows);

    int64_t prev_filtered_rows =
            _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows();
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            _state, &input_block, block, *_vec_output_expr_ctxs, input_rows, has_filtered_rows));

    // batching block rows which need new partitions. deal together at finish.
    if (!_batching_block) [[unlikely]] {
        std::unique_ptr<Block> tmp_block = block->create_same_struct_block(0);
        _batching_block = MutableBlock::create_unique(std::move(*tmp_block));
    }

    auto num_rows = block->rows();
    _reset_find_tablets(num_rows);

    // if there's projection of partition calc, we need to calc it first.
    auto [part_ctxs, part_funcs] = _get_partition_function();
    std::vector<uint16_t> partition_cols_idx;
    if (_vpartition->is_projection_partition()) {
        // calc the start value of missing partition ranges.
        auto func_size = part_funcs.size();
        for (int i = 0; i < func_size; ++i) {
            int result_idx = -1;
            // we just calc left range here. leave right to FE to avoid dup calc.
            RETURN_IF_ERROR(part_funcs[i]->execute(part_ctxs[i].get(), block.get(), &result_idx));

            VLOG_DEBUG << "Partition-calculated block:" << block->dump_data(0, 1);
            DCHECK(result_idx != -1);

            partition_cols_idx.push_back(result_idx);
        }

        // change the column to compare to transformed.
        _vpartition->set_transformed_slots(partition_cols_idx);
    }

    Status st = Status::OK();
    if (_vpartition->is_auto_detect_overwrite()) {
        // when overwrite, no auto create partition allowed.
        st = _generate_rows_distribution_for_auto_overwrite(block.get(), has_filtered_rows,
                                                            row_part_tablet_ids);
    } else if (_vpartition->is_auto_partition() && !_deal_batched) {
        st = _generate_rows_distribution_for_auto_partition(block.get(), partition_cols_idx,
                                                            has_filtered_rows, row_part_tablet_ids,
                                                            rows_stat_val);
    } else { // not auto partition
        st = _generate_rows_distribution_for_non_auto_partition(block.get(), has_filtered_rows,
                                                                row_part_tablet_ids);
    }

    filtered_rows = _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows() -
                    prev_filtered_rows;
    return st;
}

// reuse vars for find_tablets
void VRowDistribution::_reset_find_tablets(int64_t rows) {
    _tablet_finder->filter_bitmap().Reset(rows);
    _partitions.assign(rows, nullptr);
    _skip.assign(rows, false);
    _tablet_indexes.assign(rows, 0);
}

} // namespace doris::vectorized
