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
        ss << "received response from backend " << backend_id << ", status: " << st
           << ", success tablet ids:";
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
                    _sink->_cancel(Status::Cancelled(
                            "Failed to meet num replicas requirements for tablet {}", tablet_id));
                    break;
                }
            }
        }

        if (response.has_load_stream_profile()) {
            TRuntimeProfileTree tprofile;
            const uint8_t* buf =
                    reinterpret_cast<const uint8_t*>(response.load_stream_profile().data());
            uint32_t len = response.load_stream_profile().size();
            auto status = deserialize_thrift_msg(buf, &len, false, &tprofile);
            if (status.ok()) {
                _sink->_state->load_channel_profile()->update(tprofile);
            } else {
                LOG(WARNING) << "load channel TRuntimeProfileTree deserialize failed, errmsg="
                             << status;
            }
        }

        _sink->_pending_reports.fetch_add(-1);
    }
    return 0;
}

void StreamSinkHandler::on_closed(brpc::StreamId id) {}

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
    _close_stream_timer = ADD_CHILD_TIMER(_profile, "CloseStreamTime", "CloseWaitTime");

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

    _stream_pool_for_node = std::make_shared<StreamPoolForNode>();
    _node_id_for_stream = std::make_shared<NodeIdForStream>();
    _delta_writer_for_tablet = std::make_shared<DeltaWriterForTablet>();
    _build_tablet_node_mapping();
    RETURN_IF_ERROR(_init_stream_pools());

    return Status::OK();
}

Status VOlapTableSinkV2::_init_stream_pools() {
    for (auto& [node_id, _] : _tablets_for_node) {
        auto node_info = _nodes_info->find_node(node_id);
        if (node_info == nullptr) {
            return Status::InternalError("Unknown node {} in tablet location", node_id);
        }
        _stream_pool_for_node->insert({node_id, StreamPool {}});
        StreamPool& stream_pool = _stream_pool_for_node->at(node_id);
        RETURN_IF_ERROR(_init_stream_pool(*node_info, stream_pool));
        for (auto stream : stream_pool) {
            _node_id_for_stream->insert({stream, node_id});
        }
    }
    return Status::OK();
}

Status VOlapTableSinkV2::_init_stream_pool(const NodeInfo& node_info, StreamPool& stream_pool) {
    DCHECK_GT(config::num_streams_per_sink, 0);
    stream_pool.reserve(config::num_streams_per_sink);
    for (int i = 0; i < config::num_streams_per_sink; ++i) {
        brpc::StreamOptions opt;
        opt.max_buf_size = 20 << 20; // 20MB
        opt.idle_timeout_ms = 30000;
        opt.messages_in_batch = 128;
        opt.handler = new StreamSinkHandler(this);
        brpc::StreamId stream;
        brpc::Controller cntl;
        if (int ret = StreamCreate(&stream, cntl, &opt)) {
            return Status::RpcError("Failed to create stream, code = {}", ret);
        }
        LOG(INFO) << "Created stream " << stream << " for backend " << node_info.id << " ("
                  << node_info.host << ":" << node_info.brpc_port << ")";
        std::string host_port = get_host_port(node_info.host, node_info.brpc_port);
        // use "pooled" connection to avoid conflicts between streaming rpc and regular rpc,
        // see: https://github.com/apache/brpc/issues/392
        const auto& stub =
                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(
                        host_port, "baidu_std", "pooled");
        POpenStreamSinkRequest request;
        *request.mutable_load_id() = _load_id;
        request.set_src_id(_sender_id);
        request.set_txn_id(_txn_id);
        request.set_enable_profile(_state->enable_profile());
        _schema->to_protobuf(request.mutable_schema());
        if (i == 0) {
            // get tablet schema from each backend only in the 1st stream
            for (auto& tablet : _indexes_from_node[node_info.id]) {
                auto req = request.add_tablets();
                *req = tablet;
            }
        }
        POpenStreamSinkResponse response;
        cntl.set_timeout_ms(config::open_stream_sink_timeout_ms);
        stub->open_stream_sink(&cntl, &request, &response, nullptr);
        for (const auto& resp : response.tablet_schemas()) {
            auto tablet_schema = std::make_shared<TabletSchema>();
            tablet_schema->init_from_pb(resp.tablet_schema());
            _tablet_schema_for_index[resp.index_id()] = tablet_schema;
            _enable_unique_mow_for_index[resp.index_id()] = resp.enable_unique_key_merge_on_write();
        }
        if (cntl.Failed()) {
            return Status::InternalError("Failed to connect to backend {}: {}", node_info.id,
                                         cntl.ErrorText());
        }
        stream_pool.push_back(stream);
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

Status VOlapTableSinkV2::_select_streams(int64_t tablet_id, std::vector<brpc::StreamId>& streams) {
    auto location = _location->find_tablet(tablet_id);
    if (location == nullptr) {
        return Status::InternalError("unknown tablet location, tablet id = {}", tablet_id);
    }
    for (auto& node_id : location->node_ids) {
        streams.push_back(_stream_pool_for_node->at(node_id)[_stream_index]);
    }
    _stream_index = (_stream_index + 1) % config::num_streams_per_sink;
    return Status::OK();
}

Status VOlapTableSinkV2::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();

    LOG(INFO) << "upstream id = " << state->backend_id();

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
            state, input_block, block, _output_vexpr_ctxs, input_rows, eos, has_filtered_rows));

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
        _generate_rows_for_tablet(rows_for_tablet, partition, tablet_index, i);
    }
    _row_distribution_watch.stop();

    // For each tablet, send its input_rows from block to delta writer
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
            WriteRequest req;
            req.partition_id = rows.partition_id;
            req.index_id = rows.index_id;
            req.tablet_id = tablet_id;
            req.txn_id = _txn_id;
            req.load_id = _load_id;
            req.tuple_desc = _output_tuple_desc;
            req.is_high_priority = _is_high_priority;
            req.table_schema_param = _schema.get();
            req.tablet_schema = _tablet_schema_for_index[rows.index_id];
            req.enable_unique_key_merge_on_write = _enable_unique_mow_for_index[rows.index_id];
            req.sender_id = _sender_id;
            req.streams = streams;
            for (auto& index : _schema->indexes()) {
                if (index->index_id == rows.index_id) {
                    req.slots = &index->slots;
                    req.schema_hash = index->schema_hash;
                    break;
                }
            }
            DeltaWriterV2::open(&req, &delta_writer, _profile);
            _delta_writer_for_tablet->insert(
                    {tablet_id, std::unique_ptr<DeltaWriterV2>(delta_writer)});
        } else {
            VLOG_DEBUG << "Reusing DeltaWriterV2 for Tablet(tablet id: " << tablet_id
                       << ", index id: " << rows.index_id << ")";
            delta_writer = it->second.get();
        }
    }
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

    if (_delta_writer_for_tablet.use_count() == 1) {
        std::for_each(std::begin(*_delta_writer_for_tablet), std::end(*_delta_writer_for_tablet),
                      [&status](auto&& entry) { entry.second->cancel_with_status(status); });
    }
    _delta_writer_for_tablet.reset();
    if (_stream_pool_for_node.use_count() == 1) {
        std::for_each(std::begin(*_node_id_for_stream), std::end(*_node_id_for_stream),
                      [](auto&& entry) { brpc::StreamClose(entry.first); });
    }
    _stream_pool_for_node.reset();
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
            // close all delta writers
            if (_delta_writer_for_tablet.use_count() == 1) {
                std::for_each(std::begin(*_delta_writer_for_tablet),
                              std::end(*_delta_writer_for_tablet),
                              [](auto&& entry) { entry.second->close(); });
                std::for_each(std::begin(*_delta_writer_for_tablet),
                              std::end(*_delta_writer_for_tablet),
                              [](auto&& entry) { entry.second->close_wait(); });
            }
            _delta_writer_for_tablet.reset();
        }

        {
            // send CLOSE_LOAD to all streams, return ERROR if any
            RETURN_IF_ERROR(std::transform_reduce(
                    std::begin(*_node_id_for_stream), std::end(*_node_id_for_stream), Status::OK(),
                    [](Status& left, Status&& right) { return left.ok() ? right : left; },
                    [this](auto&& entry) { return _close_load(entry.first); }));
        }

        {
            SCOPED_TIMER(_close_load_timer);
            while (_pending_reports.load() > 0) {
                // TODO: use a better wait
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                LOG(INFO) << "sinkv2 close_wait, pending reports: " << _pending_reports.load();
            }
        }

        {
            SCOPED_TIMER(_close_stream_timer);
            // close streams
            if (_stream_pool_for_node.use_count() == 1) {
                std::for_each(std::begin(*_node_id_for_stream), std::end(*_node_id_for_stream),
                              [](auto&& entry) { brpc::StreamClose(entry.first); });
            }
            _stream_pool_for_node.reset();
        }

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

        LOG(INFO) << "finished to close olap table sink. load_id=" << print_id(_load_id)
                  << ", txn_id=" << _txn_id;
    } else {
        _cancel(status);
    }

    _close_status = status;
    DataSink::close(state, exec_status);
    return status;
}

Status VOlapTableSinkV2::_close_load(brpc::StreamId stream) {
    butil::IOBuf buf;
    PStreamHeader header;
    *header.mutable_load_id() = _load_id;
    header.set_src_id(_sender_id);
    header.set_opcode(doris::PStreamHeader::CLOSE_LOAD);
    auto node_id = _node_id_for_stream.get()->at(stream);
    for (auto tablet : _tablets_for_node[node_id]) {
        if (_tablet_finder->partition_ids().contains(tablet.partition_id())) {
            *header.add_tablets_to_commit() = tablet;
        }
    }
    size_t header_len = header.ByteSizeLong();
    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());
    _pending_reports.fetch_add(1);
    io::StreamSinkFileWriter::send_with_retry(stream, buf);
    return Status::OK();
}

} // namespace stream_load
} // namespace doris
