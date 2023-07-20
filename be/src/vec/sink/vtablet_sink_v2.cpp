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

#include <brpc/http_header.h>
#include <brpc/http_method.h>
#include <brpc/uri.h>
#include <bthread/bthread.h>
#include <butil/iobuf_inl.h>
#include <fmt/format.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/common.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <sys/param.h>
#include <sys/types.h>

#include <algorithm>
#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "io/fs/stream_sink_file_writer.h"
#include "olap/delta_writer_v2.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/binary_cast.hpp"
#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/telemetry/telemetry.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
class TExpr;

namespace stream_load {

int StreamSinkHandler::on_received_messages(brpc::StreamId id, butil::IOBuf* const messages[],
                                            size_t size) {
    int64_t backend_id = _sink->_node_id_for_stream->at(id);

    for (size_t i = 0; i < size; i++) {
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
        PWriteStreamSinkResponse response;
        response.ParseFromZeroCopyStream(&wrapper);

        Status st = Status::create(response.status());

        std::stringstream ss;
        ss << "received write stream sink response from backend " << backend_id
           << ", status: " << st << ", success tablet ids:";
        for (auto tablet_id : response.success_tablet_ids()) {
            ss << " " << tablet_id;
        }
        ss << ", failed tablet ids:";
        for (auto tablet_id : response.failed_tablet_ids()) {
            ss << " " << tablet_id;
        }
        LOG(INFO) << ss.str();

        int replica = _sink->_num_replicas;

        {
            std::lock_guard<bthread::Mutex> l(_sink->_tablet_success_map_mutex);
            for (auto tablet_id : response.success_tablet_ids()) {
                if (_sink->_tablet_success_map.count(tablet_id) == 0) {
                    _sink->_tablet_success_map.insert({tablet_id, {}});
                }
                _sink->_tablet_success_map[tablet_id].push_back(backend_id);
            }
        }
        {
            std::lock_guard<bthread::Mutex> l(_sink->_tablet_failure_map_mutex);
            for (auto tablet_id : response.failed_tablet_ids()) {
                if (_sink->_tablet_failure_map.count(tablet_id) == 0) {
                    _sink->_tablet_failure_map.insert({tablet_id, {}});
                }
                _sink->_tablet_failure_map[tablet_id].push_back(backend_id);
                if (_sink->_tablet_failure_map[tablet_id].size() * 2 >= replica) {
                    // TODO: _sink->cancel();
                }
            }
        }

        _sink->_pending_reports.fetch_add(-1);
    }
    return 0;
}

void StreamSinkHandler::on_closed(brpc::StreamId id) {}

VOlapTableSinkV2::VOlapTableSinkV2(ObjectPool* pool, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& texprs, Status* status)
        : _pool(pool), _input_row_desc(row_desc) {
    // From the thrift expressions create the real exprs.
    *status = vectorized::VExpr::create_expr_trees(texprs, _output_vexpr_ctxs);
    _name = "VOlapTableSinkV2";
    _transfer_large_data_by_brpc = config::transfer_large_data_by_brpc;
    _flying_memtable_counter = std::make_shared<std::atomic<int32_t>>(0);
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
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new DorisNodesInfo(table_sink.nodes_info));
    if (table_sink.__isset.write_single_replica && table_sink.write_single_replica) {
        _write_single_replica = true;
        _slave_location = _pool->add(new OlapTableLocationParam(table_sink.slave_location));
        if (!config::enable_single_replica_load) {
            return Status::InternalError("single replica load is disabled on BE.");
        }
    }

    if (table_sink.__isset.load_channel_timeout_s) {
        _load_channel_timeout_s = table_sink.load_channel_timeout_s;
    } else {
        _load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    }
    if (table_sink.__isset.send_batch_parallelism && table_sink.send_batch_parallelism > 1) {
        _send_batch_parallelism = table_sink.send_batch_parallelism;
    }
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
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));
    _mem_tracker =
            std::make_shared<MemTracker>("OlapTableSink:" + std::to_string(state->load_job_id()));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }

    _block_convertor = std::make_unique<OlapTableBlockConvertor>(_output_tuple_desc);
    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _wait_mem_limit_timer = ADD_CHILD_TIMER(_profile, "WaitMemLimitTime", "SendDataTime");
    _row_distribution_timer = ADD_CHILD_TIMER(_profile, "RowDistributionTime", "SendDataTime");
    _filter_timer = ADD_CHILD_TIMER(_profile, "FilterTime", "SendDataTime");
    _where_clause_timer = ADD_CHILD_TIMER(_profile, "WhereClauseTime", "SendDataTime");
    _append_node_channel_timer = ADD_CHILD_TIMER(_profile, "AppendNodeChannelTime", "SendDataTime");
    _validate_data_timer = ADD_TIMER(_profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _non_blocking_send_timer = ADD_TIMER(_profile, "NonBlockingSendTime");
    _non_blocking_send_work_timer =
            ADD_CHILD_TIMER(_profile, "NonBlockingSendWorkTime", "NonBlockingSendTime");
    _serialize_batch_timer =
            ADD_CHILD_TIMER(_profile, "SerializeBatchTime", "NonBlockingSendWorkTime");
    _total_add_batch_exec_timer = ADD_TIMER(_profile, "TotalAddBatchExecTime");
    _max_add_batch_exec_timer = ADD_TIMER(_profile, "MaxAddBatchExecTime");
    _total_wait_exec_timer = ADD_TIMER(_profile, "TotalWaitExecTime");
    _max_wait_exec_timer = ADD_TIMER(_profile, "MaxWaitExecTime");
    _add_batch_number = ADD_COUNTER(_profile, "NumberBatchAdded", TUnit::UNIT);
    _num_node_channels = ADD_COUNTER(_profile, "NumberNodeChannels", TUnit::UNIT);
    _load_mem_limit = state->get_load_mem_limit();

    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc));
    return Status::OK();
}

Status VOlapTableSinkV2::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    _stream_pool_for_node = std::make_shared<StreamPoolForNode>();
    _node_id_for_stream = std::make_shared<NodeIdForStream>();
    _delta_writer_for_tablet = std::make_shared<DeltaWriterForTablet>();
    RETURN_IF_ERROR(_init_stream_pools());

    return Status::OK();
}

Status VOlapTableSinkV2::_init_stream_pools() {
    for (auto& entry : _nodes_info->nodes_info()) {
        _stream_pool_for_node->insert({entry.first, StreamPool {}});
        StreamPool& stream_pool = _stream_pool_for_node->at(entry.first);
        RETURN_IF_ERROR(_init_stream_pool(entry.second, stream_pool));
        for (auto stream : stream_pool) {
            _node_id_for_stream->insert({stream, entry.first});
        }
    }
    return Status::OK();
}

Status VOlapTableSinkV2::_init_stream_pool(const NodeInfo& node_info, StreamPool& stream_pool) {
    DCHECK_GT(config::stream_cnt_per_sink, 0);
    stream_pool.reserve(config::stream_cnt_per_sink);
    for (int i = 0; i < config::stream_cnt_per_sink; ++i) {
        brpc::StreamOptions opt;
        opt.max_buf_size = 20 << 20; // 20MB
        opt.idle_timeout_ms = 30000;
        opt.messages_in_batch = 128;
        opt.handler = new StreamSinkHandler(this);
        brpc::StreamId stream;
        brpc::Controller cntl;
        if (StreamCreate(&stream, cntl, &opt) != 0) {
            LOG(ERROR) << "Failed to create stream";
            return Status::RpcError("Failed to create stream");
        }
        LOG(INFO) << "Created stream " << stream << " for backend " << node_info.id << " ("
                  << node_info.host << ":" << node_info.brpc_port << ")";
        const auto& stub = _state->exec_env()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.brpc_port);
        POpenStreamSinkRequest request;
        request.set_allocated_id(&_load_id);
        request.set_num_senders(_num_senders);
        request.set_num_stream_per_sender(config::stream_cnt_per_sink);
        request.set_txn_id(_txn_id);
        request.set_backend_id(node_info.id);
        request.set_allocated_schema(_schema->to_protobuf());
        for (const auto& partition : _vpartition->get_partitions()) {
            for (const auto& index : partition->indexes) {
                auto tablet_id = index.tablets[0];
                auto nodes = _location->find_tablet(tablet_id)->node_ids;
                if (std::find(nodes.begin(), nodes.end(), node_info.id) != nodes.end()) {
                    auto req = request.add_tablets();
                    req->set_tablet_id(tablet_id);
                    req->set_index_id(index.index_id);
                }
            }
        }
        POpenStreamSinkResponse response;
        stub->open_stream_sink(&cntl, &request, &response, nullptr);
        for (const auto& resp : response.tablet_schemas()) {
            auto tablet_schema = std::make_shared<TabletSchema>();
            tablet_schema->init_from_pb(resp.tablet_schema());
            _tablet_schema_for_index.insert({resp.index_id(), tablet_schema});
            _enable_unique_mow_for_index.insert(
                    {resp.index_id(), resp.enable_unique_key_merge_on_write()});
            // TODO: this is a debug log
            LOG(INFO) << "Got tablet schema from backend " << node_info.id
                      << ": num_short_key_columns = " << tablet_schema->num_short_key_columns()
                      << ", num_rows_per_row_block = " << tablet_schema->num_rows_per_row_block()
                      << ", enable_unique_key_merge_on_write = "
                      << resp.enable_unique_key_merge_on_write();
        }
        request.release_id();
        request.release_schema();
        if (cntl.Failed()) {
            LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
            return Status::RpcError("Failed to connect stream");
        }
        stream_pool.push_back(stream);
    }
    return Status::OK();
}

void VOlapTableSinkV2::_generate_rows_for_tablet(RowsForTablet& rows_for_tablet,
                                                 const VOlapTablePartition* partition,
                                                 uint32_t tablet_index, int row_idx,
                                                 size_t row_cnt) {
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
        _number_output_rows += row_cnt;
    }
}

Status VOlapTableSinkV2::_select_streams(int64_t tablet_id, std::vector<brpc::StreamId>& streams) {
    auto location = _location->find_tablet(tablet_id);
    if (location == nullptr) {
        LOG(WARNING) << "unknown tablet, tablet_id=" << tablet_id;
        return Status::InternalError("unknown tablet");
    }
    for (auto& node_id : location->node_ids) {
        streams.push_back(_stream_pool_for_node->at(node_id)[_stream_index]);
    }
    _stream_index = (_stream_index + 1) % config::stream_cnt_per_sink;
    return Status::OK();
}

Status VOlapTableSinkV2::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();

    auto rows = input_block->rows();
    auto bytes = input_block->bytes();
    if (UNLIKELY(rows == 0)) {
        return status;
    }
    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += rows;
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(rows);
    state->update_num_bytes_load_total(bytes);
    DorisMetrics::instance()->load_rows->increment(rows);
    DorisMetrics::instance()->load_bytes->increment(bytes);

    {
        SCOPED_TIMER(_wait_mem_limit_timer);
        while (_flying_memtable_counter->load() >= config::max_flying_memtable_per_sink) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            state, input_block, block, _output_vexpr_ctxs, rows, eos, has_filtered_rows));

    // clear and release the references of columns
    input_block->clear();

    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    RowsForTablet rows_for_tablet;
    _tablet_finder->clear_for_new_batch();
    _row_distribution_watch.start();
    auto num_rows = block->rows();
    for (int i = 0; i < num_rows; ++i) {
        if (UNLIKELY(has_filtered_rows) && _block_convertor->filter_bitmap().Get(i)) {
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
        _generate_rows_for_tablet(rows_for_tablet, partition, tablet_index, i, 1);
    }
    _row_distribution_watch.stop();

    // For each tablet, send its rows from block to delta writer
    for (const auto& [tablet_id, rows] : rows_for_tablet) {
        std::vector<brpc::StreamId> streams;
        RETURN_IF_ERROR(_select_streams(tablet_id, streams));
        RETURN_IF_ERROR(_write_memtable(block, tablet_id, rows, streams));
    }

    return Status::OK();
}

Status VOlapTableSinkV2::_write_memtable(std::shared_ptr<vectorized::Block> block,
                                         int64_t tablet_id, const Rows& rows,
                                         const std::vector<brpc::StreamId>& streams) {
    DeltaWriterV2* delta_writer = nullptr;
    {
        auto it = _delta_writer_for_tablet->find(tablet_id);
        if (it == _delta_writer_for_tablet->end()) {
            VLOG_DEBUG << "Creating DeltaWriterV2 for Tablet(tablet id: " << tablet_id
                       << ", index id: " << rows.index_id << ")";
            DeltaWriterV2::WriteRequest wrequest;
            wrequest.partition_id = rows.partition_id;
            wrequest.index_id = rows.index_id;
            wrequest.tablet_id = tablet_id;
            wrequest.txn_id = _txn_id;
            wrequest.load_id = _load_id;
            wrequest.tuple_desc = _output_tuple_desc;
            wrequest.is_high_priority = _is_high_priority;
            wrequest.table_schema_param = _schema.get();
            wrequest.tablet_schema = _tablet_schema_for_index[rows.index_id];
            wrequest.enable_unique_key_merge_on_write = _enable_unique_mow_for_index[rows.index_id];
            wrequest.sender_id = _sender_id;
            for (auto& index : _schema->indexes()) {
                if (index->index_id == rows.index_id) {
                    wrequest.slots = &index->slots;
                    wrequest.schema_hash = index->schema_hash;
                    break;
                }
            }
            DeltaWriterV2::open(&wrequest, &delta_writer, _profile, _load_id);
            // _table_sink_v2_mgr->register_writer(std::shared_ptr<DeltaWriterV2>(delta_writer));
            for (auto stream : streams) {
                delta_writer->add_stream(stream);
            }
            delta_writer->register_flying_memtable_counter(_flying_memtable_counter);
            _delta_writer_for_tablet->insert(
                    {tablet_id, std::unique_ptr<DeltaWriterV2>(delta_writer)});
        } else {
            VLOG_DEBUG << "Reusing DeltaWriterV2 for Tablet(tablet id: " << tablet_id
                       << ", index id: " << rows.index_id << ")";
            delta_writer = it->second.get();
        }
    }
    // _table_sink_v2_mgr->handle_memtable_flush();
    auto st = delta_writer->write(block.get(), rows.row_idxes, false);
    return st;
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
        COUNTER_SET(_filter_timer, _filter_ns);
        COUNTER_SET(_validate_data_timer, _block_convertor->validate_data_ns());

        // close all delta writers
        if (_delta_writer_for_tablet.use_count() == 1) {
            for (const auto& entry : *_delta_writer_for_tablet) {
                entry.second->close();
            }
            for (const auto& entry : *_delta_writer_for_tablet) {
                entry.second->close_wait();
            }
        }
        _delta_writer_for_tablet.reset();

        for (const auto& entry : *_stream_pool_for_node) {
            for (auto stream_id : entry.second) {
                RETURN_IF_ERROR(_close_load(stream_id));
            }
        }

        while (_pending_reports.load() > 0) {
            // TODO: use a better wait
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            LOG(INFO) << "sinkv2 close_wait, pending reports: " << _pending_reports.load();
        }

        // close streams
        if (_stream_pool_for_node.use_count() == 1) {
            for (const auto& entry : *_stream_pool_for_node) {
                for (auto stream_id : entry.second) {
                    brpc::StreamClose(stream_id);
                }
            }
        }
        _stream_pool_for_node.reset();

        std::vector<TTabletCommitInfo> tablet_commit_infos;
        for (auto& [tablet_id, backends] : _tablet_success_map) {
            for (int64_t be_id : backends) {
                TTabletCommitInfo commit_info;
                commit_info.tabletId = tablet_id;
                commit_info.backendId = be_id;
                tablet_commit_infos.emplace_back(std::move(commit_info));
            }
        }
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(tablet_commit_infos.begin()),
                                            std::make_move_iterator(tablet_commit_infos.end()));

        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + state->num_rows_load_filtered() +
                                      state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_block_convertor->num_filtered_rows() +
                                             _tablet_finder->num_filtered_rows());
        state->update_num_rows_load_unselected(
                _tablet_finder->num_immutable_partition_filtered_rows());

        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "finished to close olap table sink. load_id=" << print_id(_load_id)
           << ", txn_id=" << _txn_id;
        LOG(INFO) << ss.str();
    } else {
        // TODO: cancel
        LOG(INFO) << "finished to close olap table sink. load_id=" << print_id(_load_id)
                  << ", txn_id=" << _txn_id
                  << ", canceled all node channels due to error: " << status;
    }

    _close_status = status;
    DataSink::close(state, exec_status);
    return status;
}

Status VOlapTableSinkV2::_close_load(brpc::StreamId stream) {
    butil::IOBuf buf;
    PStreamHeader header;
    header.set_sender_id(_sender_id);
    header.set_allocated_load_id(&_load_id);
    header.set_opcode(doris::PStreamHeader::CLOSE_LOAD);
    size_t header_len = header.ByteSizeLong();
    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());
    _pending_reports.fetch_add(1);
    io::StreamSinkFileWriter::send_with_retry(stream, buf);
    header.release_load_id();
    return Status::OK();
}

} // namespace stream_load
} // namespace doris
