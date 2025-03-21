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

#include "vec/sink/tablet_sink_hash_partitioner.h"

#include "pipeline/exec/operator.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
TabletSinkHashPartitioner::TabletSinkHashPartitioner(
        size_t partition_count, int64_t txn_id, const TOlapTableSchemaParam& tablet_sink_schema,
        const TOlapTablePartitionParam& tablet_sink_partition,
        const TOlapTableLocationParam& tablet_sink_location, const TTupleId& tablet_sink_tuple_id,
        pipeline::ExchangeSinkLocalState* local_state)
        : PartitionerBase(partition_count),
          _txn_id(txn_id),
          _tablet_sink_schema(tablet_sink_schema),
          _tablet_sink_partition(tablet_sink_partition),
          _tablet_sink_location(tablet_sink_location),
          _tablet_sink_tuple_id(tablet_sink_tuple_id),
          _local_state(local_state) {}

Status TabletSinkHashPartitioner::init(const std::vector<TExpr>& texprs) {
    return Status::OK();
}

Status TabletSinkHashPartitioner::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    return Status::OK();
}

Status TabletSinkHashPartitioner::open(RuntimeState* state) {
    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(_tablet_sink_schema));
    _vpartition = std::make_unique<VOlapTablePartitionParam>(_schema, _tablet_sink_partition);
    RETURN_IF_ERROR(_vpartition->init());
    auto find_tablet_mode = vectorized::OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_ROW;
    _tablet_finder =
            std::make_unique<vectorized::OlapTabletFinder>(_vpartition.get(), find_tablet_mode);
    _tablet_sink_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tablet_sink_tuple_id);
    _tablet_sink_row_desc =
            state->obj_pool()->add(new RowDescriptor(_tablet_sink_tuple_desc, false));
    auto& ctxs =
            _local_state->parent()->cast<pipeline::ExchangeSinkOperatorX>().tablet_sink_expr_ctxs();
    _tablet_sink_expr_ctxs.resize(ctxs.size());
    for (size_t i = 0; i < _tablet_sink_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, _tablet_sink_expr_ctxs[i]));
    }
    // if _part_type == TPartitionType::OLAP_TABLE_SINK_HASH_PARTITIONED, we handle the processing of auto_increment column
    // on exchange node rather than on TabletWriter
    _block_convertor =
            std::make_unique<vectorized::OlapTableBlockConvertor>(_tablet_sink_tuple_desc);
    _block_convertor->init_autoinc_info(_schema->db_id(), _schema->table_id(), state->batch_size());
    _location = state->obj_pool()->add(new OlapTableLocationParam(_tablet_sink_location));
    _row_distribution.init(
            {.state = state,
             .block_convertor = _block_convertor.get(),
             .tablet_finder = _tablet_finder.get(),
             .vpartition = _vpartition.get(),
             .add_partition_request_timer = _local_state->add_partition_request_timer(),
             .txn_id = _txn_id,
             .pool = state->obj_pool(),
             .location = _location,
             .vec_output_expr_ctxs = &_tablet_sink_expr_ctxs,
             .schema = _schema,
             .caller = (void*)this,
             .create_partition_callback = &TabletSinkHashPartitioner::empty_callback_function});
    RETURN_IF_ERROR(_row_distribution.open(_tablet_sink_row_desc));
    return Status::OK();
}

Status TabletSinkHashPartitioner::do_partitioning(RuntimeState* state, Block* block, bool eos,
                                                  bool* already_sent) const {
    _hash_vals.resize(block->rows());
    if (block->empty()) {
        return Status::OK();
    }
    std::fill(_hash_vals.begin(), _hash_vals.end(), -1);
    bool has_filtered_rows = false;
    int64_t filtered_rows = 0;
    int64_t number_input_rows = _local_state->rows_input_counter()->value();
    std::shared_ptr<vectorized::Block> convert_block = std::make_shared<vectorized::Block>();
    RETURN_IF_ERROR(_row_distribution.generate_rows_distribution(
            *block, convert_block, filtered_rows, has_filtered_rows, _row_part_tablet_ids,
            number_input_rows));
    if (_row_distribution.batching_rows() > 0) {
        SCOPED_TIMER(_local_state->send_new_partition_timer());
        RETURN_IF_ERROR(_send_new_partition_batch(state, block, eos));
        *already_sent = true;
    } else {
        const auto& row_ids = _row_part_tablet_ids[0].row_ids;
        const auto& tablet_ids = _row_part_tablet_ids[0].tablet_ids;
        for (int idx = 0; idx < row_ids.size(); ++idx) {
            const auto& row = row_ids[idx];
            const auto& tablet_id_hash =
                    HashUtil::zlib_crc_hash(&tablet_ids[idx], sizeof(HashValType), 0);
            _hash_vals[row] = tablet_id_hash % _partition_count;
        }
    }

    return Status::OK();
}

ChannelField TabletSinkHashPartitioner::get_channel_ids() const {
    return {_hash_vals.data(), sizeof(HashValType)};
}

Status TabletSinkHashPartitioner::clone(RuntimeState* state,
                                        std::unique_ptr<PartitionerBase>& partitioner) {
    partitioner.reset(new TabletSinkHashPartitioner(_partition_count, _txn_id, _tablet_sink_schema,
                                                    _tablet_sink_partition, _tablet_sink_location,
                                                    _tablet_sink_tuple_id, _local_state));
    return Status::OK();
}

Status TabletSinkHashPartitioner::close(RuntimeState* state) {
    if (_block_convertor != nullptr && _tablet_finder != nullptr) {
        state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() +
                                             _tablet_finder->num_filtered_rows());
        state->update_num_rows_load_unselected(
                _tablet_finder->num_immutable_partition_filtered_rows());
        // sink won't see those filtered rows, we should compensate here
        state->set_num_rows_load_total(state->num_rows_load_filtered() +
                                       state->num_rows_load_unselected());
    }
    return Status::OK();
}

Status TabletSinkHashPartitioner::_send_new_partition_batch(RuntimeState* state,
                                                            vectorized::Block* input_block,
                                                            bool eos) const {
    RETURN_IF_ERROR(_row_distribution.automatic_create_partition());
    auto& p = _local_state->parent()->cast<pipeline::ExchangeSinkOperatorX>();
    // Recovery back
    _row_distribution.clear_batching_stats();
    _row_distribution._batching_block->clear_column_data();
    _row_distribution._deal_batched = false;
    RETURN_IF_ERROR(p.sink(state, input_block, eos));
    return Status::OK();
}

} // namespace doris::vectorized