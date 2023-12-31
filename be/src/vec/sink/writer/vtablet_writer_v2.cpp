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

#include "vec/sink/writer/vtablet_writer_v2.h"

#include <brpc/uri.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <execution>
#include <mutex>
#include <ranges>
#include <string>
#include <unordered_map>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/object_pool.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "olap/delta_writer_v2.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_stub.h"
#include "vec/sink/load_stream_stub_pool.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

VTabletWriterV2::VTabletWriterV2(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs)
        : AsyncResultWriter(output_exprs), _t_sink(t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
}

VTabletWriterV2::~VTabletWriterV2() = default;

Status VTabletWriterV2::on_partitions_created(TCreatePartitionResult* result) {
    // add new tablet locations. it will use by address. so add to pool
    auto* new_locations = _pool->add(new std::vector<TTabletLocation>(result->tablets));
    _location->add_locations(*new_locations);

    // update new node info
    _nodes_info->add_nodes(result->nodes);

    // incremental open stream
    RETURN_IF_ERROR(_incremental_open_streams(result->partitions));

    return Status::OK();
}

static Status on_partitions_created(void* writer, TCreatePartitionResult* result) {
    return static_cast<VTabletWriterV2*>(writer)->on_partitions_created(result);
}

Status VTabletWriterV2::_incremental_open_streams(
        const std::vector<TOlapTablePartition>& partitions) {
    // do what we did in prepare() for partitions. indexes which don't change when we create new partition is orthogonal to partitions.
    std::unordered_set<int64_t> known_indexes;
    std::unordered_set<int64_t> new_backends;
    for (const auto& t_partition : partitions) {
        VOlapTablePartition* partition = nullptr;
        RETURN_IF_ERROR(_vpartition->generate_partition_from(t_partition, partition));
        for (const auto& index : partition->indexes) {
            for (const auto& tablet_id : index.tablets) {
                auto nodes = _location->find_tablet(tablet_id)->node_ids;
                for (auto& node : nodes) {
                    PTabletID tablet;
                    tablet.set_partition_id(partition->id);
                    tablet.set_index_id(index.index_id);
                    tablet.set_tablet_id(tablet_id);
                    if (!_streams_for_node.contains(node)) {
                        new_backends.insert(node);
                    }
                    _tablets_for_node[node].emplace(tablet_id, tablet);
                    if (known_indexes.contains(index.index_id)) [[likely]] {
                        continue;
                    }
                    _indexes_from_node[node].emplace_back(tablet);
                    known_indexes.insert(index.index_id);
                }
            }
        }
    }
    for (int64_t node_id : new_backends) {
        auto load_streams = ExecEnv::GetInstance()->load_stream_stub_pool()->get_or_create(
                _load_id, _backend_id, node_id, _stream_per_node, _num_local_sink, _state);
        RETURN_IF_ERROR(_open_streams_to_backend(node_id, *load_streams));
        _streams_for_node[node_id] = load_streams;
    }
    return Status::OK();
}

Status VTabletWriterV2::_init_row_distribution() {
    _row_distribution.init({.state = _state,
                            .block_convertor = _block_convertor.get(),
                            .tablet_finder = _tablet_finder.get(),
                            .vpartition = _vpartition,
                            .add_partition_request_timer = _add_partition_request_timer,
                            .txn_id = _txn_id,
                            .pool = _pool,
                            .location = _location,
                            .vec_output_expr_ctxs = &_vec_output_expr_ctxs,
                            .schema = _schema,
                            .caller = (void*)this,
                            .create_partition_callback = &vectorized::on_partitions_created});

    return _row_distribution.open(_output_row_desc);
}

Status VTabletWriterV2::init_properties(ObjectPool* pool) {
    _pool = pool;
    return Status::OK();
}

Status VTabletWriterV2::_init(RuntimeState* state, RuntimeProfile* profile) {
    auto& table_sink = _t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    signal::set_signal_task_id(_load_id);
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
    RETURN_IF_ERROR(_vpartition->init());

    _state = state;
    _profile = profile;

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();
    _backend_id = state->backend_id();
    _stream_per_node = state->load_stream_per_node();
    _total_streams = state->total_load_streams();
    _num_local_sink = state->num_local_sink();
    LOG(INFO) << "init olap tablet sink, load_id: " << print_id(_load_id)
              << ", num senders: " << _num_senders << ", stream per node: " << _stream_per_node
              << ", total_streams " << _total_streams << ", num_local_sink: " << _num_local_sink;
    DCHECK(_stream_per_node > 0) << "load stream per node should be greator than 0";
    DCHECK(_total_streams > 0) << "total load streams should be greator than 0";
    DCHECK(_num_local_sink > 0) << "num local sink should be greator than 0";
    _is_high_priority =
            (state->execution_timeout() <= config::load_task_high_priority_threshold_second);
    DBUG_EXECUTE_IF("VTabletWriterV2._init.is_high_priority", { _is_high_priority = true; });
    _mem_tracker =
            std::make_shared<MemTracker>("VTabletWriterV2:" + std::to_string(state->load_job_id()));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    DBUG_EXECUTE_IF("VTabletWriterV2._init._output_tuple_desc_null",
                    { _output_tuple_desc = nullptr; });
    if (_output_tuple_desc == nullptr) {
        return Status::InternalError("unknown destination tuple descriptor, id = {}",
                                     _tuple_desc_id);
    }
    DBUG_EXECUTE_IF("VTabletWriterV2._init._vec_output_expr_ctxs_not_equal_output_tuple_slot", {
        return Status::InvalidArgument(
                "output_tuple_slot_num {} should be equal to output_expr_num {}",
                _output_tuple_desc->slots().size() + 1, _vec_output_expr_ctxs.size());
    });
    if (_vec_output_expr_ctxs.size() > 0 &&
        _output_tuple_desc->slots().size() != _vec_output_expr_ctxs.size()) {
        LOG(WARNING) << "output tuple slot num should be equal to num of output exprs, "
                     << "output_tuple_slot_num " << _output_tuple_desc->slots().size()
                     << " output_expr_num " << _vec_output_expr_ctxs.size();
        return Status::InvalidArgument(
                "output_tuple_slot_num {} should be equal to output_expr_num {}",
                _output_tuple_desc->slots().size(), _vec_output_expr_ctxs.size());
    }

    _block_convertor = std::make_unique<OlapTableBlockConvertor>(_output_tuple_desc);
    _block_convertor->init_autoinc_info(_schema->db_id(), _schema->table_id(),
                                        _state->batch_size());
    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsProduced", TUnit::UNIT);
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

    if (config::share_delta_writers) {
        _delta_writer_for_tablet = ExecEnv::GetInstance()->delta_writer_v2_pool()->get_or_create(
                _load_id, _num_local_sink);
    } else {
        _delta_writer_for_tablet = std::make_shared<DeltaWriterV2Map>(_load_id);
    }
    return Status::OK();
}

Status VTabletWriterV2::open(RuntimeState* state, RuntimeProfile* profile) {
    RETURN_IF_ERROR(_init(state, profile));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    RETURN_IF_ERROR(_build_tablet_node_mapping());
    RETURN_IF_ERROR(_open_streams(_backend_id));
    RETURN_IF_ERROR(_init_row_distribution());

    return Status::OK();
}

Status VTabletWriterV2::_open_streams(int64_t src_id) {
    for (auto& [dst_id, _] : _tablets_for_node) {
        auto streams = ExecEnv::GetInstance()->load_stream_stub_pool()->get_or_create(
                _load_id, src_id, dst_id, _stream_per_node, _num_local_sink, _state);
        RETURN_IF_ERROR(_open_streams_to_backend(dst_id, *streams));
        _streams_for_node[dst_id] = streams;
    }
    return Status::OK();
}

Status VTabletWriterV2::_open_streams_to_backend(int64_t dst_id, LoadStreams& streams) {
    const auto* node_info = _nodes_info->find_node(dst_id);
    DBUG_EXECUTE_IF("VTabletWriterV2._open_streams_to_backend.node_info_null",
                    { node_info = nullptr; });
    if (node_info == nullptr) {
        return Status::InternalError("Unknown node {} in tablet location", dst_id);
    }
    // get tablet schema from each backend only in the 1st stream
    for (auto& stream : streams.streams() | std::ranges::views::take(1)) {
        const std::vector<PTabletID>& tablets_for_schema = _indexes_from_node[node_info->id];
        RETURN_IF_ERROR(stream->open(_state->exec_env()->brpc_internal_client_cache(), *node_info,
                                     _txn_id, *_schema, tablets_for_schema, _total_streams,
                                     _state->enable_profile()));
    }
    // for the rest streams, open without getting tablet schema
    for (auto& stream : streams.streams() | std::ranges::views::drop(1)) {
        RETURN_IF_ERROR(stream->open(_state->exec_env()->brpc_internal_client_cache(), *node_info,
                                     _txn_id, *_schema, {}, _total_streams,
                                     _state->enable_profile()));
    }
    return Status::OK();
}

Status VTabletWriterV2::_build_tablet_node_mapping() {
    std::unordered_set<int64_t> known_indexes;
    for (const auto& partition : _vpartition->get_partitions()) {
        for (const auto& index : partition->indexes) {
            for (const auto& tablet_id : index.tablets) {
                auto tablet_location = _location->find_tablet(tablet_id);
                DBUG_EXECUTE_IF("VTabletWriterV2._build_tablet_node_mapping.tablet_location_null",
                                { tablet_location = nullptr; });
                if (tablet_location == nullptr) {
                    return Status::InternalError("unknown tablet location, tablet id = {}",
                                                 tablet_id);
                }
                for (auto& node : tablet_location->node_ids) {
                    PTabletID tablet;
                    tablet.set_partition_id(partition->id);
                    tablet.set_index_id(index.index_id);
                    tablet.set_tablet_id(tablet_id);
                    _tablets_for_node[node].emplace(tablet_id, tablet);
                    if (known_indexes.contains(index.index_id)) [[likely]] {
                        continue;
                    }
                    _indexes_from_node[node].emplace_back(tablet);
                    known_indexes.insert(index.index_id);
                }
            }
        }
    }
    return Status::OK();
}

void VTabletWriterV2::_generate_rows_for_tablet(std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                                RowsForTablet& rows_for_tablet) {
    for (int index_idx = 0; index_idx < row_part_tablet_ids.size(); index_idx++) {
        auto& row_ids = row_part_tablet_ids[index_idx].row_ids;
        auto& partition_ids = row_part_tablet_ids[index_idx].partition_ids;
        auto& tablet_ids = row_part_tablet_ids[index_idx].tablet_ids;

        for (int i = 0; i < row_ids.size(); i++) {
            auto& tablet_id = tablet_ids[i];
            auto it = rows_for_tablet.find(tablet_id);
            if (it == rows_for_tablet.end()) {
                Rows rows;
                rows.partition_id = partition_ids[i];
                rows.index_id = _schema->indexes()[index_idx]->index_id;
                rows.row_idxes.reserve(row_ids.size());
                auto [tmp_it, _] = rows_for_tablet.insert({tablet_id, rows});
                it = tmp_it;
            }
            it->second.row_idxes.push_back(row_ids[i]);
            _number_output_rows++;
        }
    }
}

Status VTabletWriterV2::_select_streams(int64_t tablet_id, int64_t partition_id, int64_t index_id,
                                        Streams& streams) {
    const auto* location = _location->find_tablet(tablet_id);
    DBUG_EXECUTE_IF("VTabletWriterV2._select_streams.location_null", { location = nullptr; });
    if (location == nullptr) {
        return Status::InternalError("unknown tablet location, tablet id = {}", tablet_id);
    }
    for (const auto& node_id : location->node_ids) {
        PTabletID tablet;
        tablet.set_partition_id(partition_id);
        tablet.set_index_id(index_id);
        tablet.set_tablet_id(tablet_id);
        _tablets_for_node[node_id].emplace(tablet_id, tablet);
        streams.emplace_back(_streams_for_node.at(node_id)->streams().at(_stream_index));
        RETURN_IF_ERROR(streams[0]->wait_for_schema(partition_id, index_id, tablet_id));
    }
    _stream_index = (_stream_index + 1) % _stream_per_node;
    return Status::OK();
}

Status VTabletWriterV2::append_block(Block& input_block) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();

    if (_state->query_options().dry_run_query) {
        return status;
    }

    // check out of limit
    RETURN_IF_ERROR(_send_new_partition_batch());

    auto input_rows = input_block.rows();
    auto input_bytes = input_block.bytes();
    if (UNLIKELY(input_rows == 0)) {
        return status;
    }
    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += input_rows;
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    _state->update_num_rows_load_total(input_rows);
    _state->update_num_bytes_load_total(input_bytes);
    DorisMetrics::instance()->load_rows->increment(input_rows);
    DorisMetrics::instance()->load_bytes->increment(input_bytes);

    bool has_filtered_rows = false;
    int64_t filtered_rows = 0;

    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    _row_distribution_watch.start();

    std::shared_ptr<vectorized::Block> block;
    RETURN_IF_ERROR(_row_distribution.generate_rows_distribution(
            input_block, block, filtered_rows, has_filtered_rows, _row_part_tablet_ids,
            _number_input_rows));
    RowsForTablet rows_for_tablet;
    _generate_rows_for_tablet(_row_part_tablet_ids, rows_for_tablet);

    _row_distribution_watch.stop();

    // For each tablet, send its input_rows from block to delta writer
    for (const auto& [tablet_id, rows] : rows_for_tablet) {
        Streams streams;
        RETURN_IF_ERROR(_select_streams(tablet_id, rows.partition_id, rows.index_id, streams));
        RETURN_IF_ERROR(_write_memtable(block, tablet_id, rows, streams));
    }

    return Status::OK();
}

Status VTabletWriterV2::_write_memtable(std::shared_ptr<vectorized::Block> block, int64_t tablet_id,
                                        const Rows& rows, const Streams& streams) {
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
        for (const auto& index : _schema->indexes()) {
            if (index->index_id == rows.index_id) {
                req.slots = &index->slots;
                req.schema_hash = index->schema_hash;
                break;
            }
        }
        return DeltaWriterV2::open(&req, streams, _state);
    });
    {
        SCOPED_TIMER(_wait_mem_limit_timer);
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_memtable_flush();
    }
    SCOPED_TIMER(_write_memtable_timer);
    auto st = delta_writer->write(block.get(), rows.row_idxes, false);
    return st;
}

Status VTabletWriterV2::_cancel(Status status) {
    LOG(INFO) << "canceled olap table sink. load_id=" << print_id(_load_id)
              << ", txn_id=" << _txn_id << ", due to error: " << status;
    if (_delta_writer_for_tablet) {
        _delta_writer_for_tablet->cancel(status);
        _delta_writer_for_tablet.reset();
    }
    for (const auto& [_, streams] : _streams_for_node) {
        streams->release();
        streams->cancel(status);
    }
    return Status::OK();
}

Status VTabletWriterV2::_send_new_partition_batch() {
    if (_row_distribution.need_deal_batching()) { // maybe try_close more than 1 time
        RETURN_IF_ERROR(_row_distribution.automatic_create_partition());

        Block tmp_block = _row_distribution._batching_block->to_block(); // Borrow out, for lval ref

        // these order is only.
        //  1. clear batching stats(and flag goes true) so that we won't make a new batching process in dealing batched block.
        //  2. deal batched block
        //  3. now reuse the column of lval block. cuz append_block doesn't real adjust it. it generate a new block from that.
        _row_distribution.clear_batching_stats();
        RETURN_IF_ERROR(this->append_block(tmp_block));
        _row_distribution._batching_block->set_mutable_columns(
                tmp_block.mutate_columns()); // Recovery back
        _row_distribution._batching_block->clear_column_data();
        _row_distribution._deal_batched = false;
    }
    return Status::OK();
}

Status VTabletWriterV2::close(Status exec_status) {
    std::lock_guard<std::mutex> close_lock(_close_mutex);
    if (_is_closed) {
        return _close_status;
    }
    SCOPED_TIMER(_close_timer);
    Status status = exec_status;

    if (status.ok()) {
        SCOPED_TIMER(_profile->total_time_counter());
        _row_distribution._deal_batched = true;
        status = _send_new_partition_batch();
    }

    DBUG_EXECUTE_IF("VTabletWriterV2.close.cancel",
                    { status = Status::InternalError("load cancel"); });
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

        // defer stream release to prevent memory leak
        Defer defer([&] {
            for (const auto& [_, streams] : _streams_for_node) {
                streams->release();
            }
            _streams_for_node.clear();
        });

        {
            SCOPED_TIMER(_close_writer_timer);
            // close all delta writers if this is the last user
            RETURN_IF_ERROR(_delta_writer_for_tablet->close(_profile));
            _delta_writer_for_tablet.reset();
        }

        {
            // send CLOSE_LOAD to all streams, return ERROR if any
            for (const auto& [_, streams] : _streams_for_node) {
                RETURN_IF_ERROR(_close_load(streams->streams()));
            }
        }

        {
            SCOPED_TIMER(_close_load_timer);
            for (const auto& [_, streams] : _streams_for_node) {
                for (const auto& stream : streams->streams()) {
                    RETURN_IF_ERROR(stream->close_wait());
                }
            }
        }

        std::vector<TTabletCommitInfo> tablet_commit_infos;
        for (const auto& [node_id, streams] : _streams_for_node) {
            for (const auto& stream : streams->streams()) {
                for (auto tablet_id : stream->success_tablets()) {
                    TTabletCommitInfo commit_info;
                    commit_info.tabletId = tablet_id;
                    commit_info.backendId = node_id;
                    tablet_commit_infos.emplace_back(std::move(commit_info));
                }
            }
        }
        _state->tablet_commit_infos().insert(_state->tablet_commit_infos().end(),
                                             std::make_move_iterator(tablet_commit_infos.begin()),
                                             std::make_move_iterator(tablet_commit_infos.end()));

        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + _state->num_rows_load_filtered() +
                                      _state->num_rows_load_unselected();
        _state->set_num_rows_load_total(num_rows_load_total);
        _state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() +
                                              _tablet_finder->num_filtered_rows());
        _state->update_num_rows_load_unselected(
                _tablet_finder->num_immutable_partition_filtered_rows());

        LOG(INFO) << "finished to close olap table sink. load_id=" << print_id(_load_id)
                  << ", txn_id=" << _txn_id;
    } else {
        RETURN_IF_ERROR(_cancel(status));
    }

    _is_closed = true;
    _close_status = status;
    return status;
}

Status VTabletWriterV2::_close_load(const Streams& streams) {
    auto node_id = streams[0]->dst_id();
    std::vector<PTabletID> tablets_to_commit;
    for (auto [tablet_id, tablet] : _tablets_for_node[node_id]) {
        if (_tablet_finder->partition_ids().contains(tablet.partition_id())) {
            tablets_to_commit.push_back(tablet);
        }
    }
    for (const auto& stream : streams) {
        RETURN_IF_ERROR(stream->close_load(tablets_to_commit));
    }
    return Status::OK();
}

} // namespace doris::vectorized
