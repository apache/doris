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

#include "vec/sink/vtablet_sink_v2.h"

#include <brpc/uri.h>
#include <bthread/bthread.h>
#include <fmt/format.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <algorithm>
#include <execution>
#include <mutex>
#include <ranges>
#include <string>
#include <unordered_map>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "olap/delta_writer_v2.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/telemetry/telemetry.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_stub.h"
#include "vec/sink/load_stream_stub_pool.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
class TExpr;

namespace vectorized {

VOlapTableSinkV2::VOlapTableSinkV2(ObjectPool* pool, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& texprs, Status* status)
        : DataSink(row_desc), _pool(pool) {
    // From the thrift expressions create the real exprs.
    *status = vectorized::VExpr::create_expr_trees(texprs, _output_vexpr_ctxs);
    _name = "VOlapTableSinkV2";
}

VOlapTableSinkV2::~VOlapTableSinkV2() = default;

Status VOlapTableSinkV2::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _num_replicas = table_sink.num_replicas;
    _tuple_desc_id = table_sink.tuple_id;
    _write_file_cache = table_sink.write_file_cache;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new DorisNodesInfo(table_sink.nodes_info));

    // if distributed column list is empty, we can ensure that tablet is with random distribution info
    // and if load_to_single_tablet is set and set to true, we should find only one tablet in one partition
    // for the whole olap table sink
    auto find_tablet_mode = OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_ROW;
    if (table_sink.partition.distributed_columns.empty()) {
        if (table_sink.__isset.load_to_single_tablet && table_sink.load_to_single_tablet) {
            find_tablet_mode = OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_SINK;
        } else {
            find_tablet_mode = OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_BATCH;
        }
    }
    _vpartition = _pool->add(new doris::VOlapTablePartitionParam(_schema, table_sink.partition));
    _tablet_finder = std::make_unique<OlapTabletFinder>(_vpartition, find_tablet_mode);
    return _vpartition->init();
}

Status VOlapTableSinkV2::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _state = state;

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();
    _is_high_priority =
            (state->execution_timeout() <= config::load_task_high_priority_threshold_second);

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("VOlapTableSinkV2"));
    _mem_tracker = std::make_shared<MemTracker>("VOlapTableSinkV2:" +
                                                std::to_string(state->load_job_id()));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        return Status::InternalError("unknown destination tuple descriptor, id = {}",
                                     _tuple_desc_id);
    }
    _block_convertor = std::make_unique<OlapTableBlockConvertor>(_output_tuple_desc);

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _wait_mem_limit_timer = ADD_CHILD_TIMER(_profile, "WaitMemLimitTime", "SendDataTime");
    _row_distribution_timer = ADD_CHILD_TIMER(_profile, "RowDistributionTime", "SendDataTime");
    _write_memtable_timer = ADD_CHILD_TIMER(_profile, "WriteMemTableTime", "SendDataTime");
    _validate_data_timer = ADD_TIMER(_profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _close_writer_timer = ADD_CHILD_TIMER(_profile, "CloseWriterTime", "CloseWaitTime");
    _close_load_timer = ADD_CHILD_TIMER(_profile, "CloseLoadTime", "CloseWaitTime");

    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status VOlapTableSinkV2::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (config::share_delta_writers) {
        _delta_writer_for_tablet =
                ExecEnv::GetInstance()->delta_writer_v2_pool()->get_or_create(_load_id);
    } else {
        _delta_writer_for_tablet = std::make_shared<DeltaWriterV2Map>(_load_id);
    }
    _build_tablet_node_mapping();
    RETURN_IF_ERROR(_open_streams(state->backend_id()));

    return Status::OK();
}

Status VOlapTableSinkV2::_open_streams(int64_t src_id) {
    for (auto& [dst_id, _] : _tablets_for_node) {
        auto node_info = _nodes_info->find_node(dst_id);
        if (node_info == nullptr) {
            return Status::InternalError("Unknown node {} in tablet location", dst_id);
        }
        std::shared_ptr<Streams> streams;
        streams = ExecEnv::GetInstance()->load_stream_stub_pool()->get_or_create(_load_id, src_id,
                                                                                 dst_id);
        // get tablet schema from each backend only in the 1st stream
        for (auto& stream : *streams | std::ranges::views::take(1)) {
            const std::vector<PTabletID>& tablets_for_schema = _indexes_from_node[node_info->id];
            RETURN_IF_ERROR(stream->open(_state->exec_env()->brpc_internal_client_cache(),
                                         *node_info, _txn_id, *_schema, tablets_for_schema,
                                         _state->enable_profile()));
        }
        // for the rest streams, open without getting tablet schema
        for (auto& stream : *streams | std::ranges::views::drop(1)) {
            RETURN_IF_ERROR(stream->open(_state->exec_env()->brpc_internal_client_cache(),
                                         *node_info, _txn_id, *_schema, {},
                                         _state->enable_profile()));
        }
        _streams_for_node[dst_id] = streams;
    }
    return Status::OK();
}

void VOlapTableSinkV2::_build_tablet_node_mapping() {
    std::unordered_set<int64_t> known_indexes;
    for (const auto& partition : _vpartition->get_partitions()) {
        for (const auto& index : partition->indexes) {
            for (const auto& tablet_id : index.tablets) {
                auto nodes = _location->find_tablet(tablet_id)->node_ids;
                for (auto& node : nodes) {
                    PTabletID tablet;
                    tablet.set_partition_id(partition->id);
                    tablet.set_index_id(index.index_id);
                    tablet.set_tablet_id(tablet_id);
                    _tablets_for_node[node].emplace_back(tablet);
                    if (known_indexes.contains(index.index_id)) [[likely]] {
                        continue;
                    }
                    _indexes_from_node[node].emplace_back(tablet);
                    known_indexes.insert(index.index_id);
                }
            }
        }
    }
}

void VOlapTableSinkV2::_generate_rows_for_tablet(RowsForTablet& rows_for_tablet,
                                                 const VOlapTablePartition* partition,
                                                 uint32_t tablet_index, int row_idx) {
    // Generate channel payload for sinking data to each tablet
    for (const auto& index : partition->indexes) {
        auto tablet_id = index.tablets[tablet_index];
        if (rows_for_tablet.count(tablet_id) == 0) {
            Rows rows;
            rows.partition_id = partition->id;
            rows.index_id = index.index_id;
            rows_for_tablet.insert({tablet_id, rows});
        }
        rows_for_tablet[tablet_id].row_idxes.push_back(row_idx);
        _number_output_rows++;
    }
}

Status VOlapTableSinkV2::_select_streams(int64_t tablet_id, Streams& streams) {
    auto location = _location->find_tablet(tablet_id);
    if (location == nullptr) {
        return Status::InternalError("unknown tablet location, tablet id = {}", tablet_id);
    }
    for (auto& node_id : location->node_ids) {
        streams.emplace_back(_streams_for_node[node_id]->at(_stream_index));
    }
    _stream_index = (_stream_index + 1) % config::num_streams_per_load;
    return Status::OK();
}

Status VOlapTableSinkV2::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();

    if (state->query_options().dry_run_query) {
        return status;
    }

    auto input_rows = input_block->rows();
    auto input_bytes = input_block->bytes();
    if (UNLIKELY(input_rows == 0)) {
        return status;
    }
    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += input_rows;
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(input_rows);
    state->update_num_bytes_load_total(input_bytes);
    DorisMetrics::instance()->load_rows->increment(input_rows);
    DorisMetrics::instance()->load_bytes->increment(input_bytes);

    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            state, input_block, block, _output_vexpr_ctxs, input_rows, has_filtered_rows));

    // clear and release the references of columns
    input_block->clear();

    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    RowsForTablet rows_for_tablet;
    _tablet_finder->clear_for_new_batch();
    _row_distribution_watch.start();
    const auto num_rows = input_rows;
    const auto* __restrict filter_map = _block_convertor->filter_map();
    for (int i = 0; i < num_rows; ++i) {
        if (UNLIKELY(has_filtered_rows) && filter_map[i]) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        bool is_continue = false;
        uint32_t tablet_index = 0;
        RETURN_IF_ERROR(_tablet_finder->find_tablet(state, block.get(), i, &partition, tablet_index,
                                                    stop_processing, is_continue));
        if (is_continue) {
            continue;
        }
        _generate_rows_for_tablet(rows_for_tablet, partition, tablet_index, i);
    }
    _row_distribution_watch.stop();

    // For each tablet, send its input_rows from block to delta writer
    for (const auto& [tablet_id, rows] : rows_for_tablet) {
        Streams streams;
        RETURN_IF_ERROR(_select_streams(tablet_id, streams));
        RETURN_IF_ERROR(_write_memtable(block, tablet_id, rows, streams));
    }

    return Status::OK();
}

Status VOlapTableSinkV2::_write_memtable(std::shared_ptr<vectorized::Block> block,
                                         int64_t tablet_id, const Rows& rows,
                                         const Streams& streams) {
    DeltaWriterV2* delta_writer = _delta_writer_for_tablet->get_or_create(tablet_id, [&]() {
        WriteRequest req {
                .tablet_id = tablet_id,
                .txn_id = _txn_id,
                .index_id = rows.index_id,
                .partition_id = rows.partition_id,
                .load_id = _load_id,
                .tuple_desc = _output_tuple_desc,
                .table_schema_param = _schema.get(),
                .is_high_priority = _is_high_priority,
                .write_file_cache = _write_file_cache,
        };
        for (auto& index : _schema->indexes()) {
            if (index->index_id == rows.index_id) {
                req.slots = &index->slots;
                req.schema_hash = index->schema_hash;
                break;
            }
        }
        DeltaWriterV2* delta_writer = nullptr;
        static_cast<void>(DeltaWriterV2::open(&req, streams, &delta_writer, _profile));
        return delta_writer;
    });
    {
        SCOPED_TIMER(_wait_mem_limit_timer);
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_memtable_flush();
    }
    SCOPED_TIMER(_write_memtable_timer);
    auto st = delta_writer->write(block.get(), rows.row_idxes, false);
    return st;
}

Status VOlapTableSinkV2::_cancel(Status status) {
    LOG(INFO) << "canceled olap table sink. load_id=" << print_id(_load_id)
              << ", txn_id=" << _txn_id << ", due to error: " << status;
    if (_delta_writer_for_tablet) {
        _delta_writer_for_tablet->cancel(status);
        _delta_writer_for_tablet.reset();
    }
    return Status::OK();
}

Status VOlapTableSinkV2::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return _close_status;
    }
    SCOPED_TIMER(_close_timer);
    Status status = exec_status;
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());

        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter,
                    _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows());
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_row_distribution_timer, (int64_t)_row_distribution_watch.elapsed_time());
        COUNTER_SET(_validate_data_timer, _block_convertor->validate_data_ns());

        {
            SCOPED_TIMER(_close_writer_timer);
            // close all delta writers if this is the last user
            static_cast<void>(_delta_writer_for_tablet->close());
            _delta_writer_for_tablet.reset();
        }

        {
            // send CLOSE_LOAD to all streams, return ERROR if any
            for (const auto& [_, streams] : _streams_for_node) {
                RETURN_IF_ERROR(_close_load(*streams));
            }
        }

        {
            SCOPED_TIMER(_close_load_timer);
            for (const auto& [_, streams] : _streams_for_node) {
                for (const auto& stream : *streams) {
                    static_cast<void>(stream->close_wait());
                }
            }
        }

        std::vector<TTabletCommitInfo> tablet_commit_infos;
        for (const auto& [node_id, streams] : _streams_for_node) {
            for (const auto& stream : *streams) {
                for (auto tablet_id : stream->success_tablets()) {
                    TTabletCommitInfo commit_info;
                    commit_info.tabletId = tablet_id;
                    commit_info.backendId = node_id;
                    tablet_commit_infos.emplace_back(std::move(commit_info));
                }
            }
        }
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(tablet_commit_infos.begin()),
                                            std::make_move_iterator(tablet_commit_infos.end()));
        _streams_for_node.clear();

        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + state->num_rows_load_filtered() +
                                      state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() +
                                             _tablet_finder->num_filtered_rows());
        state->update_num_rows_load_unselected(
                _tablet_finder->num_immutable_partition_filtered_rows());

        LOG(INFO) << "finished to close olap table sink. load_id=" << print_id(_load_id)
                  << ", txn_id=" << _txn_id;
    } else {
        static_cast<void>(_cancel(status));
    }

    _close_status = status;
    static_cast<void>(DataSink::close(state, exec_status));
    return status;
}

Status VOlapTableSinkV2::_close_load(const Streams& streams) {
    auto node_id = streams[0]->dst_id();
    std::vector<PTabletID> tablets_to_commit;
    for (auto tablet : _tablets_for_node[node_id]) {
        if (_tablet_finder->partition_ids().contains(tablet.partition_id())) {
            tablets_to_commit.push_back(tablet);
        }
    }
    for (const auto& stream : streams) {
        RETURN_IF_ERROR(stream->close_load(tablets_to_commit));
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
