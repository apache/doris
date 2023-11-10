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

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/sink/writer/vtablet_writer.h"

namespace doris::vectorized {

std::pair<vectorized::VExprContextSPtr, vectorized::VExprSPtr>
VRowDistribution::_get_partition_function() {
    return {_vpartition->get_part_func_ctx(), _vpartition->get_partition_function()};
}

void VRowDistribution::_save_missing_values(vectorized::ColumnPtr col,
                                            vectorized::DataTypePtr value_type) {
    _partitions_need_create.clear();
    std::set<std::string> deduper;
    // de-duplication
    for (auto row : _missing_map) {
        deduper.emplace(value_type->to_string(*col, row));
    }
    for (auto& value : deduper) {
        TStringLiteral node;
        node.value = value;
        _partitions_need_create.emplace_back(std::vector {node}); // only 1 partition column now
    }
}

Status VRowDistribution::_automatic_create_partition() {
    SCOPED_TIMER(_add_partition_request_timer);
    TCreatePartitionRequest request;
    TCreatePartitionResult result;
    request.__set_txn_id(_txn_id);
    request.__set_db_id(_vpartition->db_id());
    request.__set_table_id(_vpartition->table_id());
    request.__set_partitionValues(_partitions_need_create);

    VLOG(1) << "automatic partition rpc begin request " << request;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    int time_out = _state->execution_timeout() * 1000;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->createPartition(result, request);
            },
            time_out));

    Status status(Status::create(result.status));
    VLOG(1) << "automatic partition rpc end response " << result;
    if (result.status.status_code == TStatusCode::OK) {
        // add new created partitions
        RETURN_IF_ERROR(_vpartition->add_partitions(result.partitions));
        RETURN_IF_ERROR(_on_partitions_created(_caller, &result));
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
    if (auto* nullable_column =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(*filter_column)) {
        for (size_t i = 0; i < block->rows(); i++) {
            if (nullable_column->get_bool_inline(i) && !_skip[i]) {
                row_ids.emplace_back(i);
                partition_ids.emplace_back(_partitions[i]->id);
                tablet_ids.emplace_back(_tablet_ids[i]);
            }
        }
    } else if (auto* const_column =
                       vectorized::check_and_get_column<vectorized::ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            return Status::OK();
        }
        // should we optimize?
        _filter_block_by_skip(block, row_part_tablet_id);
    } else {
        auto& filter = assert_cast<const vectorized::ColumnUInt8&>(*filter_column).get_data();
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

Status VRowDistribution::_generate_rows_distribution_for_non_auto_parititon(
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

Status VRowDistribution::_generate_rows_distribution_for_auto_parititon(
        vectorized::Block* block, int partition_col_idx, bool has_filtered_rows,
        std::vector<RowPartTabletIds>& row_part_tablet_ids) {
    auto num_rows = block->rows();
    std::vector<uint16_t> partition_keys = _vpartition->get_partition_keys();

    //TODO: use loop to create missing_vals for multi column.
    CHECK(partition_keys.size() == 1) << "now support only 1 partition column for auto partitions.";
    auto partition_col = block->get_by_position(partition_keys[0]);
    _missing_map.clear();
    _missing_map.reserve(partition_col.column->size());
    bool stop_processing = false;
    //TODO: we could use the buffer to save tablets we found so that no need to find them again when we created partitions and try to append block next time.
    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, num_rows, _partitions,
                                                 _tablet_indexes, stop_processing, _skip,
                                                 &_missing_map));
    if (_missing_map.empty()) {
        // we don't calculate it distribution when have missing values
        if (has_filtered_rows) {
            for (int i = 0; i < num_rows; i++) {
                _skip[i] = _skip[i] || _block_convertor->filter_map()[i];
            }
        }
        RETURN_IF_ERROR(_filter_block(block, row_part_tablet_ids));
    } else { // for missing partition keys, calc the missing partition and save in _partitions_need_create
        auto [part_ctx, part_func] = _get_partition_function();
        auto return_type = part_func->data_type();

        // expose the data column
        vectorized::ColumnPtr range_left_col = block->get_by_position(partition_col_idx).column;
        if (const auto* nullable =
                    check_and_get_column<vectorized::ColumnNullable>(*range_left_col)) {
            range_left_col = nullable->get_nested_column_ptr();
            return_type = assert_cast<const vectorized::DataTypeNullable*>(return_type.get())
                                  ->get_nested_type();
        }
        // calc the end value and save them.
        _save_missing_values(range_left_col, return_type);
        // then call FE to create it. then FragmentExecutor will redo the load.
        RETURN_IF_ERROR(_automatic_create_partition());
        // In the next round, we will _generate_rows_distribution_payload again to get right payload of new tablet
        LOG(INFO) << "Auto created partition. Send block again.";
        return Status::NeedSendAgain("");
    } // creating done

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
        row_ids.reserve(rows);
        partition_ids.reserve(rows);
        tablet_ids.reserve(rows);
    }
}

Status VRowDistribution::generate_rows_distribution(
        vectorized::Block& input_block, std::shared_ptr<vectorized::Block>& block,
        int64_t& filtered_rows, bool& has_filtered_rows,
        std::vector<RowPartTabletIds>& row_part_tablet_ids) {
    auto input_rows = input_block.rows();
    _reset_row_part_tablet_ids(row_part_tablet_ids, input_rows);

    int64_t prev_filtered_rows =
            _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows();
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            _state, &input_block, block, *_vec_output_expr_ctxs, input_rows, has_filtered_rows));

    _tablet_finder->clear_for_new_batch();
    _row_distribution_watch.start();
    auto num_rows = block->rows();
    _tablet_finder->filter_bitmap().Reset(num_rows);

    //reuse vars for find_tablets
    _partitions.assign(num_rows, nullptr);
    _skip.assign(num_rows, false);
    _tablet_indexes.assign(num_rows, 0);

    // if there's projection of partition calc, we need to calc it first.
    auto [part_ctx, part_func] = _get_partition_function();
    int partition_col_idx = -1;
    if (_vpartition->is_projection_partition()) {
        // calc the start value of missing partition ranges.
        RETURN_IF_ERROR(part_func->execute(part_ctx.get(), block.get(), &partition_col_idx));
        VLOG_DEBUG << "Partition-calculated block:" << block->dump_data();
        // change the column to compare to transformed.
        _vpartition->set_transformed_slots({(uint16_t)partition_col_idx});
    }

    if (_vpartition->is_auto_partition()) {
        RETURN_IF_ERROR(_generate_rows_distribution_for_auto_parititon(
                block.get(), partition_col_idx, has_filtered_rows, row_part_tablet_ids));
    } else { // not auto partition
        RETURN_IF_ERROR(_generate_rows_distribution_for_non_auto_parititon(
                block.get(), has_filtered_rows, row_part_tablet_ids));
    }
    _row_distribution_watch.stop();
    filtered_rows = _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows() -
                    prev_filtered_rows;
    return Status::OK();
}

} // namespace doris::vectorized
