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


#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/writer/vtablet_writer.h"

namespace doris::vectorized {

std::pair<vectorized::VExprContextSPtr, vectorized::VExprSPtr>
VRowDistribution::_get_partition_function() {
    return {_vpartition->get_part_func_ctx(), _vpartition->get_partition_function()};
}

void VRowDistribution::_save_missing_values(vectorized::ColumnPtr col,
                                            vectorized::DataTypePtr value_type,
                                            std::vector<int64_t> filter) {
    _partitions_need_create.clear();
    std::set<std::string> deduper;
    // de-duplication
    for (auto row : filter) {
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

// Generate channel payload for sinking data to differenct node channel
// Payload = std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>;
//   first = row_id, second = vector<tablet_id>
void VRowDistribution::_generate_rows_distribution_payload(
        ChannelDistributionPayload& channel_to_payload,
        const std::vector<VOlapTablePartition*>& partitions,
        const std::vector<uint32_t>& tablet_indexes, const std::vector<bool>& skip,
        size_t rows_cnt) {
    for (int row_idx = 0; row_idx < rows_cnt; row_idx++) {
        if (skip[row_idx]) {
            continue;
        }
        const auto& partition = partitions[row_idx];
        const auto& tablet_index = tablet_indexes[row_idx];

        for (int index_num = 0; index_num < partition->indexes.size();
             ++index_num) { // partition->indexes = [index, tablets...]

            auto tablet_id = partition->indexes[index_num].tablets[tablet_index];
            LOG(WARNING) << "got tablet it " << tablet_id << " at row " << row_idx;
            auto it = (*_channels)[index_num]->_channels_by_tablet.find(
                    tablet_id); // (tablet_id, VNodeChannel) where this tablet locate

            DCHECK(it != (*_channels)[index_num]->_channels_by_tablet.end())
                    << "unknown tablet, tablet_id=" << tablet_index;

            std::vector<std::shared_ptr<VNodeChannel>>& tablet_locations = it->second;
            std::unordered_map<VNodeChannel*, Payload>& payloads_this_index =
                    channel_to_payload[index_num]; // payloads of this index in every node

            for (const auto& locate_node : tablet_locations) {
                auto payload_it =
                        payloads_this_index.find(locate_node.get()); // <VNodeChannel*, Payload>
                if (payload_it == payloads_this_index.end()) {
                    auto [tmp_it, _] = payloads_this_index.emplace(
                            locate_node.get(),
                            Payload {std::make_unique<vectorized::IColumn::Selector>(),
                                     std::vector<int64_t>()});
                    payload_it = tmp_it;
                    payload_it->second.first->reserve(rows_cnt);
                    payload_it->second.second.reserve(rows_cnt);
                }
                payload_it->second.first->push_back(row_idx);
                payload_it->second.second.push_back(tablet_id);
            }
            _number_output_rows++;
        }
    }
}

Status VRowDistribution::_single_partition_generate(vectorized::Block* block,
                                                    ChannelDistributionPayload& channel_to_payload,
                                                    size_t num_rows, bool has_filtered_rows) {
    // only need to calculate one value for single partition.
    std::vector<VOlapTablePartition*> partitions {1, nullptr};
    std::vector<bool> skip;
    skip.resize(1);
    std::vector<uint32_t> tablet_indexes;
    tablet_indexes.resize(1);
    bool stop_processing = false;

    RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block, 1, partitions, tablet_indexes,
                                                 stop_processing, skip));

    const auto* partition = partitions[0];
    const auto& tablet_index = tablet_indexes[0];

    if (partition == nullptr) {
        return Status::OK();
    }
    for (int j = 0; j < partition->indexes.size(); ++j) {
        auto tid = partition->indexes[j].tablets[tablet_index];
        auto it = (*_channels)[j]->_channels_by_tablet.find(tid);
        DCHECK(it != (*_channels)[j]->_channels_by_tablet.end())
                << "unknown tablet, tablet_id=" << tablet_index;
        int64_t row_cnt = 0;
        for (const auto& channel : it->second) {
            if (!channel_to_payload[j].contains(channel.get())) {
                channel_to_payload[j].insert(
                        {channel.get(), Payload {std::make_unique<vectorized::IColumn::Selector>(),
                                                 std::vector<int64_t>()}});
            }
            auto& selector = channel_to_payload[j][channel.get()].first;
            auto& tablet_ids = channel_to_payload[j][channel.get()].second;
            for (int32_t i = 0; i < num_rows; ++i) {
                if (UNLIKELY(has_filtered_rows) && _block_convertor->filter_map()[i]) {
                    continue;
                }
                selector->push_back(i);
            }
            tablet_ids.resize(selector->size(), tid);
            row_cnt = selector->size();
        }
        _number_output_rows += row_cnt;
    }
    return Status::OK();
}

Status VRowDistribution::generate_rows_distribution(vectorized::Block& input_block,
                                                  std::shared_ptr<vectorized::Block>& block,
                                                  int64_t& filtered_rows, bool& has_filtered_rows,
                                                  ChannelDistributionPayload& channel_to_payload) {
    auto rows = input_block.rows();

    int64_t prev_filtered_rows =
            _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows();
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            _state, &input_block, block, *_vec_output_expr_ctxs, rows, has_filtered_rows));

    // This is just for passing compilation.
    bool stop_processing = false;
    channel_to_payload.resize(_channels->size());
    _tablet_finder->clear_for_new_batch();
    _row_distribution_watch.start();
    auto num_rows = block->rows();
    _tablet_finder->filter_bitmap().Reset(num_rows);
    size_t partition_num = _vpartition->get_partitions().size();
    if (!_vpartition->is_auto_partition() && partition_num == 1 &&
        _tablet_finder->is_find_tablet_every_sink()) {
        RETURN_IF_ERROR(_single_partition_generate(block.get(), channel_to_payload,
                                                   num_rows, has_filtered_rows));
    } else {
        // if there's projection of partition calc, we need to calc it first.
        auto [part_ctx, part_func] = _get_partition_function();
        int result_idx = -1;
        if (_vpartition->is_projection_partition()) {
            // calc the start value of missing partition ranges.
            RETURN_IF_ERROR(part_func->execute(part_ctx.get(), block.get(), &result_idx));
            VLOG_DEBUG << "Partition-calculated block:" << block->dump_data();
            // change the column to compare to transformed.
            _vpartition->set_transformed_slots({(uint16_t)result_idx});
        }

        if (_vpartition->is_auto_partition()) {
            std::vector<uint16_t> partition_keys = _vpartition->get_partition_keys();
            //TODO: use loop to create missing_vals for multi column.
            CHECK(partition_keys.size() == 1)
                    << "now support only 1 partition column for auto partitions.";
            auto partition_col = block->get_by_position(partition_keys[0]);

            std::vector<int64_t> missing_map; // indice of missing values in partition_col
            missing_map.reserve(partition_col.column->size());

            // try to find tablet and save missing value
            std::vector<VOlapTablePartition*> partitions {num_rows, nullptr};
            std::vector<bool> skip;
            skip.resize(num_rows);
            std::vector<uint32_t> tablet_indexes;
            tablet_indexes.resize(num_rows);

            //TODO: we could use the buffer to save tablets we found so that no need to find them again when we created partitions and try to append block next time.
            RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block.get(), num_rows, partitions,
                                                         tablet_indexes, stop_processing, skip,
                                                         &missing_map));

            if (missing_map.empty()) {
                // we don't calculate it distribution when have missing values
                if (has_filtered_rows) {
                    for (int i = 0; i < num_rows; i++) {
                        skip[i] = skip[i] || _block_convertor->filter_map()[i];
                    }
                }
                _generate_rows_distribution_payload(channel_to_payload, partitions, tablet_indexes,
                                                   skip, num_rows);
            } else { // for missing partition keys, calc the missing partition and save in _partitions_need_create
                auto return_type = part_func->data_type();

                // expose the data column
                vectorized::ColumnPtr range_left_col = block->get_by_position(result_idx).column;
                if (const auto* nullable =
                            check_and_get_column<vectorized::ColumnNullable>(*range_left_col)) {
                    range_left_col = nullable->get_nested_column_ptr();
                    return_type =
                            assert_cast<const vectorized::DataTypeNullable*>(return_type.get())
                                    ->get_nested_type();
                }
                // calc the end value and save them.
                _save_missing_values(range_left_col, return_type, missing_map);
                // then call FE to create it. then FragmentExecutor will redo the load.
                RETURN_IF_ERROR(_automatic_create_partition());
                // In the next round, we will _generate_rows_distribution_payload again to get right payload of new tablet
                LOG(INFO) << "Auto created partition. Send block again.";
                return Status::NeedSendAgain("");
            }    // creating done
        } else { // not auto partition
            std::vector<VOlapTablePartition*> partitions {num_rows, nullptr};
            std::vector<bool> skip;
            skip.resize(num_rows);
            std::vector<uint32_t> tablet_indexes;
            tablet_indexes.resize(num_rows);

            RETURN_IF_ERROR(_tablet_finder->find_tablets(_state, block.get(), num_rows, partitions,
                                                         tablet_indexes, stop_processing, skip));

            if (has_filtered_rows) {
                for (int i = 0; i < num_rows; i++) {
                    skip[i] = skip[i] || _block_convertor->filter_map()[i];
                }
            }
            _generate_rows_distribution_payload(channel_to_payload, partitions, tablet_indexes, skip,
                                               num_rows);
        }
    }
    _row_distribution_watch.stop();
    filtered_rows = _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows() - prev_filtered_rows;
    return Status::OK();
}

} // namespace doris::vectorized

