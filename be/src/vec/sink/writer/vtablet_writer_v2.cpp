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

#include <cstdint>
#include <mutex>
#include <ranges>
#include <string>
#include <unordered_map>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "olap/delta_writer_v2.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/sink/delta_writer_v2_pool.h"
// NOLINTNEXTLINE(unused-includes)
#include "vec/sink/load_stream_map_pool.h"
#include "vec/sink/load_stream_stub.h" // IWYU pragma: keep
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

VTabletWriterV2::VTabletWriterV2(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                                 std::shared_ptr<pipeline::Dependency> dep,
                                 std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep), _t_sink(t_sink) {
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
                    if (!_load_stream_map->contains(node)) {
                        new_backends.insert(node);
                    }
                    _tablets_for_node[node].emplace(tablet_id, tablet);
                    if (known_indexes.contains(index.index_id)) [[likely]] {
                        continue;
                    }
                    _indexes_from_node[node].emplace_back(tablet);
                    known_indexes.insert(index.index_id);
                    VLOG_DEBUG << "incremental open stream (" << partition->id << ", " << tablet_id
                               << ")";
                }
            }
        }
    }
    for (int64_t dst_id : new_backends) {
        auto streams = _load_stream_map->get_or_create(dst_id, true);
        RETURN_IF_ERROR(_open_streams_to_backend(dst_id, *streams));
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

Status VTabletWriterV2::_init(RuntimeState* state, RuntimeProfile* profile) {
    _pool = state->obj_pool();
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
    _schema->set_timestamp_ms(state->timestamp_ms());
    _schema->set_timezone(state->timezone());
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
    auto output_tuple_desc_slots_size = _output_tuple_desc->slots().size();
    DBUG_EXECUTE_IF("VTabletWriterV2._init._vec_output_expr_ctxs_not_equal_output_tuple_slot",
                    { output_tuple_desc_slots_size++; });
    if (!_vec_output_expr_ctxs.empty() &&
        _vec_output_expr_ctxs.size() != output_tuple_desc_slots_size) {
        LOG(WARNING) << "output tuple slot num should be equal to num of output exprs, "
                     << "output_tuple_slot_num " << _output_tuple_desc->slots().size()
                     << " output_expr_num " << _vec_output_expr_ctxs.size();
        return Status::InvalidArgument(
                "output_tuple_slot_num {} should be equal to output_expr_num {}",
                _output_tuple_desc->slots().size(), _vec_output_expr_ctxs.size());
    }

    _block_convertor = std::make_unique<OlapTableBlockConvertor>(_output_tuple_desc);
    // if partition_type is TABLET_SINK_SHUFFLE_PARTITIONED, we handle the processing of auto_increment column
    // on exchange node rather than on TabletWriter
    _block_convertor->init_autoinc_info(
            _schema->db_id(), _schema->table_id(), _state->batch_size(),
            _schema->is_partial_update() && !_schema->auto_increment_coulumn().empty(),
            _schema->auto_increment_column_unique_id());
    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsProduced", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER_WITH_LEVEL(_profile, "SendDataTime", 1);
    _wait_mem_limit_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "WaitMemLimitTime", "SendDataTime", 1);
    _row_distribution_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RowDistributionTime", "SendDataTime", 1);
    _write_memtable_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "WriteMemTableTime", "SendDataTime", 1);
    _validate_data_timer = ADD_TIMER_WITH_LEVEL(_profile, "ValidateDataTime", 1);
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
    _load_stream_map = ExecEnv::GetInstance()->load_stream_map_pool()->get_or_create(
            _load_id, _backend_id, _stream_per_node, _num_local_sink);
    return Status::OK();
}

Status VTabletWriterV2::open(RuntimeState* state, RuntimeProfile* profile) {
    RETURN_IF_ERROR(_init(state, profile));
    LOG(INFO) << "opening olap table sink, load_id=" << print_id(_load_id) << ", txn_id=" << _txn_id
              << ", sink_id=" << _sender_id;
    _timeout_watch.start();
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    RETURN_IF_ERROR(_build_tablet_node_mapping());
    RETURN_IF_ERROR(_open_streams());
    RETURN_IF_ERROR(_init_row_distribution());

    return Status::OK();
}

Status VTabletWriterV2::_open_streams() {
    bool fault_injection_skip_be = true;
    for (auto& [dst_id, _] : _tablets_for_node) {
        auto streams = _load_stream_map->get_or_create(dst_id);
        DBUG_EXECUTE_IF("VTabletWriterV2._open_streams.skip_one_backend", {
            if (fault_injection_skip_be) {
                fault_injection_skip_be = false;
                continue;
            }
        });
        RETURN_IF_ERROR(_open_streams_to_backend(dst_id, *streams));
    }
    return Status::OK();
}

Status VTabletWriterV2::_open_streams_to_backend(int64_t dst_id, Streams& streams) {
    const auto* node_info = _nodes_info->find_node(dst_id);
    DBUG_EXECUTE_IF("VTabletWriterV2._open_streams_to_backend.node_info_null",
                    { node_info = nullptr; });
    if (node_info == nullptr) {
        return Status::InternalError("Unknown node {} in tablet location", dst_id);
    }
    auto idle_timeout_ms = _state->execution_timeout() * 1000;
    std::vector<PTabletID>& tablets_for_schema = _indexes_from_node[node_info->id];
    DBUG_EXECUTE_IF("VTabletWriterV2._open_streams_to_backend.no_schema_when_open_streams",
                    { tablets_for_schema.clear(); });
    int fault_injection_skip_cnt = 0;
    for (auto& stream : streams) {
        DBUG_EXECUTE_IF("VTabletWriterV2._open_streams_to_backend.one_stream_open_failure", {
            if (fault_injection_skip_cnt < 1) {
                fault_injection_skip_cnt++;
                continue;
            }
        });
        auto st = stream->open(_state->exec_env()->brpc_streaming_client_cache(), *node_info,
                               _txn_id, *_schema, tablets_for_schema, _total_streams,
                               idle_timeout_ms, _state->enable_profile());
        if (st.ok()) {
            // get tablet schema from each backend only in the 1st stream
            tablets_for_schema.clear();
        } else {
            LOG(WARNING) << "failed to open stream to backend " << dst_id
                         << ", load_id=" << print_id(_load_id);
        }
    }
    return Status::OK();
}

Status VTabletWriterV2::_build_tablet_node_mapping() {
    std::unordered_set<int64_t> known_indexes;
    for (const auto& partition : _vpartition->get_partitions()) {
        for (const auto& index : partition->indexes) {
            for (const auto& tablet_id : index.tablets) {
                auto* tablet_location = _location->find_tablet(tablet_id);
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
                    constexpr int64_t DUMMY_TABLET_ID = 0;
                    if (tablet_id == DUMMY_TABLET_ID) [[unlikely]] {
                        // ignore fake tablet for auto partition
                        continue;
                    }
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
        VLOG_DEBUG << fmt::format("_select_streams P{} I{} T{}", partition_id, index_id, tablet_id);
        _tablets_for_node[node_id].emplace(tablet_id, tablet);
        auto stream = _load_stream_map->at(node_id)->at(_stream_index);
        for (int i = 1; i < _stream_per_node && !stream->is_inited(); i++) {
            stream = _load_stream_map->at(node_id)->at((_stream_index + i) % _stream_per_node);
        }
        streams.emplace_back(std::move(stream));
    }
    _stream_index = (_stream_index + 1) % _stream_per_node;
    Status st;
    for (auto& stream : streams) {
        st = stream->wait_for_schema(partition_id, index_id, tablet_id);
        if (st.ok()) {
            break;
        } else {
            LOG(WARNING) << "failed to get schema from stream " << stream << ", err=" << st;
        }
    }
    return st;
}

Status VTabletWriterV2::write(RuntimeState* state, Block& input_block) {
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
        RETURN_IF_ERROR(_write_memtable(block, tablet_id, rows));
    }

    return Status::OK();
}

Status VTabletWriterV2::_write_memtable(std::shared_ptr<vectorized::Block> block, int64_t tablet_id,
                                        const Rows& rows) {
    auto delta_writer = _delta_writer_for_tablet->get_or_create(tablet_id, [&]() {
        Streams streams;
        auto st = _select_streams(tablet_id, rows.partition_id, rows.index_id, streams);
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "select stream failed, " << st << ", load_id=" << print_id(_load_id);
            return std::unique_ptr<DeltaWriterV2>(nullptr);
        }
        WriteRequest req {
                .tablet_id = tablet_id,
                .txn_id = _txn_id,
                .index_id = rows.index_id,
                .partition_id = rows.partition_id,
                .load_id = _load_id,
                .tuple_desc = _schema->tuple_desc(),
                .table_schema_param = _schema,
                .is_high_priority = _is_high_priority,
                .write_file_cache = _write_file_cache,
                .storage_vault_id {},
        };
        bool index_not_found = true;
        for (const auto& index : _schema->indexes()) {
            if (index->index_id == rows.index_id) {
                req.slots = &index->slots;
                req.schema_hash = index->schema_hash;
                index_not_found = false;
                break;
            }
        }
        DBUG_EXECUTE_IF("VTabletWriterV2._write_memtable.index_not_found",
                        { index_not_found = true; });
        if (index_not_found) {
            LOG(WARNING) << "index " << rows.index_id
                         << " not found in schema, load_id=" << print_id(_load_id);
            return std::unique_ptr<DeltaWriterV2>(nullptr);
        }
        return DeltaWriterV2::create_unique(&req, streams, _state);
    });
    if (delta_writer == nullptr) {
        LOG(WARNING) << "failed to open DeltaWriter for tablet " << tablet_id
                     << ", load_id=" << print_id(_load_id);
        return Status::InternalError("failed to open DeltaWriter for tablet {}", tablet_id);
    }
    {
        SCOPED_TIMER(_wait_mem_limit_timer);
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_memtable_flush();
    }
    SCOPED_TIMER(_write_memtable_timer);
    auto st = delta_writer->write(block.get(), rows.row_idxes);
    return st;
}

void VTabletWriterV2::_cancel(Status status) {
    LOG(INFO) << "canceled olap table sink. load_id=" << print_id(_load_id)
              << ", txn_id=" << _txn_id << ", sink_id=" << _sender_id
              << ", due to error: " << status;
    if (_delta_writer_for_tablet) {
        _delta_writer_for_tablet->cancel(status);
        _delta_writer_for_tablet.reset();
    }
    if (_load_stream_map) {
        _load_stream_map->for_each([status](int64_t dst_id, const Streams& streams) {
            for (auto& stream : streams) {
                stream->cancel(status);
            }
        });
        _load_stream_map->release();
    }
}

Status VTabletWriterV2::_send_new_partition_batch() {
    if (_row_distribution.need_deal_batching()) { // maybe try_close more than 1 time
        RETURN_IF_ERROR(_row_distribution.automatic_create_partition());

        Block tmp_block = _row_distribution._batching_block->to_block(); // Borrow out, for lval ref

        // these order is only.
        //  1. clear batching stats(and flag goes true) so that we won't make a new batching process in dealing batched block.
        //  2. deal batched block
        //  3. now reuse the column of lval block. cuz write doesn't real adjust it. it generate a new block from that.
        _row_distribution.clear_batching_stats();
        RETURN_IF_ERROR(this->write(_state, tmp_block));
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
    LOG(INFO) << "closing olap table sink, load_id=" << print_id(_load_id) << ", txn_id=" << _txn_id
              << ", sink_id=" << _sender_id << ", status=" << exec_status.to_string();
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

        // close DeltaWriters
        {
            std::unordered_map<int64_t, int32_t> segments_for_tablet;
            SCOPED_TIMER(_close_writer_timer);
            // close all delta writers if this is the last user
            auto st = _delta_writer_for_tablet->close(segments_for_tablet, _profile);
            _delta_writer_for_tablet.reset();
            if (!st.ok()) {
                _cancel(st);
                return st;
            }
            // only the last sink closing delta writers will have segment num
            if (!segments_for_tablet.empty()) {
                _load_stream_map->save_segments_for_tablet(segments_for_tablet);
            }
        }

        _calc_tablets_to_commit();
        const bool is_last_sink = _load_stream_map->release();
        LOG(INFO) << "sink " << _sender_id << " released streams, is_last=" << is_last_sink
                  << ", load_id=" << print_id(_load_id);

        // send CLOSE_LOAD on all non-incremental streams if this is the last sink
        if (is_last_sink) {
            _load_stream_map->close_load(false);
        }

        // close_wait on all non-incremental streams, even if this is not the last sink.
        // because some per-instance data structures are now shared among all sinks
        // due to sharing delta writers and load stream stubs.
        _close_wait(false);

        // send CLOSE_LOAD on all incremental streams if this is the last sink.
        // this must happen after all non-incremental streams are closed,
        // so we can ensure all sinks are in close phase before closing incremental streams.
        if (is_last_sink) {
            _load_stream_map->close_load(true);
        }

        // close_wait on all incremental streams, even if this is not the last sink.
        _close_wait(true);

        // calculate and submit commit info
        if (is_last_sink) {
            DBUG_EXECUTE_IF("VTabletWriterV2.close.add_failed_tablet", {
                auto streams = _load_stream_map->at(_tablets_for_node.begin()->first);
                int64_t tablet_id = -1;
                for (auto& stream : *streams) {
                    const auto& tablets = stream->success_tablets();
                    if (tablets.size() > 0) {
                        tablet_id = tablets[0];
                        break;
                    }
                }
                if (tablet_id != -1) {
                    LOG(INFO) << "fault injection: adding failed tablet_id: " << tablet_id;
                    streams->front()->add_failed_tablet(tablet_id,
                                                        Status::InternalError("fault injection"));
                } else {
                    LOG(INFO) << "fault injection: failed to inject failed tablet_id";
                }
            });

            std::vector<TTabletCommitInfo> tablet_commit_infos;
            RETURN_IF_ERROR(
                    _create_commit_info(tablet_commit_infos, _load_stream_map, _num_replicas));
            _state->tablet_commit_infos().insert(
                    _state->tablet_commit_infos().end(),
                    std::make_move_iterator(tablet_commit_infos.begin()),
                    std::make_move_iterator(tablet_commit_infos.end()));
        }

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
        _cancel(status);
    }

    _is_closed = true;
    _close_status = status;
    return status;
}

void VTabletWriterV2::_close_wait(bool incremental) {
    SCOPED_TIMER(_close_load_timer);
    auto st = _load_stream_map->for_each_st(
            [this, incremental](int64_t dst_id, const Streams& streams) -> Status {
                Status status = Status::OK();
                for (auto& stream : streams) {
                    if (stream->is_incremental() != incremental) {
                        continue;
                    }
                    int64_t remain_ms = static_cast<int64_t>(_state->execution_timeout()) * 1000 -
                                        _timeout_watch.elapsed_time() / 1000 / 1000;
                    DBUG_EXECUTE_IF("VTabletWriterV2._close_wait.load_timeout", { remain_ms = 0; });
                    if (remain_ms <= 0) {
                        LOG(WARNING) << "load timed out before close waiting, load_id="
                                     << print_id(_load_id);
                        return Status::TimedOut("load timed out before close waiting");
                    }
                    auto st = stream->close_wait(_state, remain_ms);
                    if (!st.ok() && status.ok()) {
                        status = st;
                    }
                }
                return status;
            });
    if (!st.ok()) {
        LOG(WARNING) << "close_wait failed: " << st << ", load_id=" << print_id(_load_id);
    }
}

void VTabletWriterV2::_calc_tablets_to_commit() {
    LOG(INFO) << "saving close load info, load_id=" << print_id(_load_id) << ", txn_id=" << _txn_id
              << ", sink_id=" << _sender_id;
    for (const auto& [dst_id, tablets] : _tablets_for_node) {
        std::vector<PTabletID> tablets_to_commit;
        std::vector<int64_t> partition_ids;
        for (const auto& [tablet_id, tablet] : tablets) {
            if (_tablet_finder->partition_ids().contains(tablet.partition_id())) {
                if (VLOG_DEBUG_IS_ON) {
                    partition_ids.push_back(tablet.partition_id());
                }
                PTabletID t(tablet);
                tablets_to_commit.push_back(t);
            }
        }
        if (VLOG_DEBUG_IS_ON) {
            std::string msg("close load partitions: ");
            msg.reserve(partition_ids.size() * 7);
            for (auto v : partition_ids) {
                msg.append(std::to_string(v) + ", ");
            }
            LOG(WARNING) << msg;
        }
        _load_stream_map->save_tablets_to_commit(dst_id, tablets_to_commit);
    }
}

Status VTabletWriterV2::_create_commit_info(std::vector<TTabletCommitInfo>& tablet_commit_infos,
                                            std::shared_ptr<LoadStreamMap> load_stream_map,
                                            int num_replicas) {
    std::unordered_map<int64_t, int> failed_tablets;
    std::unordered_map<int64_t, Status> failed_reason;
    load_stream_map->for_each([&](int64_t dst_id, const Streams& streams) {
        std::unordered_set<int64_t> known_tablets;
        for (const auto& stream : streams) {
            LOG(INFO) << "stream " << stream->stream_id()
                      << " success tablets: " << stream->success_tablets().size()
                      << ", failed tablets: " << stream->failed_tablets().size();
            for (auto [tablet_id, reason] : stream->failed_tablets()) {
                if (known_tablets.contains(tablet_id)) {
                    continue;
                }
                known_tablets.insert(tablet_id);
                failed_tablets[tablet_id]++;
                failed_reason[tablet_id] = reason;
            }
            for (auto tablet_id : stream->success_tablets()) {
                if (known_tablets.contains(tablet_id)) {
                    continue;
                }
                known_tablets.insert(tablet_id);
                TTabletCommitInfo commit_info;
                commit_info.tabletId = tablet_id;
                commit_info.backendId = dst_id;
                tablet_commit_infos.emplace_back(std::move(commit_info));
            }
        }
    });

    for (auto [tablet_id, replicas] : failed_tablets) {
        if (replicas > (num_replicas - 1) / 2) {
            LOG(INFO) << "tablet " << tablet_id
                      << " failed on majority backends: " << failed_reason[tablet_id];
            return Status::InternalError("tablet {} failed on majority backends: {}", tablet_id,
                                         failed_reason[tablet_id]);
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized
