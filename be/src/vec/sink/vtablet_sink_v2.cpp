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
#include "olap/delta_writer.h"
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

namespace doris {
class TExpr;

namespace stream_load {

int StreamSinkHandler::on_received_messages(brpc::StreamId id, butil::IOBuf* const messages[],
                                            size_t size) {
    for (size_t i = 0; i < size; i++) {
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
        PWriteStreamSinkResponse response;
        response.ParseFromZeroCopyStream(&wrapper);

        // TODO: get replica num
        int replica = 1;

        auto key = std::make_pair(response.index_id(), response.tablet_id());

        if (response.success()) {
            if (_sink->tablet_success_map.count(key) == 0) {
                _sink->tablet_success_map.insert({key, {}});
            }
            _sink->tablet_success_map[key].push_back(response.backend_id());
        } else {
            LOG(WARNING) << "stream sink failed: " << response.error_msg();
            if (_sink->tablet_error_map.count(key) == 0) {
                _sink->tablet_error_map.insert({key, {}});
            }
            _sink->tablet_error_map[key].push_back(response.backend_id());
            if (_sink->tablet_error_map[key].size() * 2 >= replica) {
                // TODO: cancel load
            }
        }
    }
    return 0;
}

void StreamSinkHandler::on_closed(brpc::StreamId id) {
    _sink->all_stream_done_cv.notify_one();
}

VOlapTableSinkV2::VOlapTableSinkV2(ObjectPool* pool, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& texprs, Status* status)
        : _pool(pool), _input_row_desc(row_desc), _filter_bitmap(1024) {
    // From the thrift expressions create the real exprs.
    *status = vectorized::VExpr::create_expr_trees(texprs, _output_vexpr_ctxs);
    _name = "VOlapTableSinkV2";
    _transfer_large_data_by_brpc = config::transfer_large_data_by_brpc;
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
    if (table_sink.partition.distributed_columns.empty()) {
        if (table_sink.__isset.load_to_single_tablet && table_sink.load_to_single_tablet) {
            findTabletMode = FindTabletMode::FIND_TABLET_EVERY_SINK;
        } else {
            findTabletMode = FindTabletMode::FIND_TABLET_EVERY_BATCH;
        }
    }
    _vpartition = _pool->add(new doris::VOlapTablePartitionParam(_schema, table_sink.partition));
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

    _stream_pool = std::make_shared<StreamPool>();
    _delta_writer_for_tablet = std::make_shared<DeltaWriterForTablet>();
    _delta_writer_for_tablet_mutex = std::make_shared<bthread::Mutex>();
    RETURN_IF_ERROR(_init_stream_pool(*_stream_pool));

    return Status::OK();
}

Status VOlapTableSinkV2::_init_stream_pool(StreamPool& stream_pool) {
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
        LOG(INFO) << "Created stream " << stream;
        // randomly choose a BE to be the primary BE
        const auto& node_info = _nodes_info->nodes_info().begin()->second;
        const auto& stub = _state->exec_env()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.brpc_port);
        POpenStreamSinkRequest request;
        request.set_allocated_id(&_load_id);
        request.set_backend_id(node_info.id);
        POpenStreamSinkResponse response;
        stub->open_stream_sink(&cntl, &request, &response, nullptr);
        request.release_id();
        if (cntl.Failed()) {
            LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
            // TODO: uncomment return
            // return Status::RpcError("Failed to connect stream");
        }
        stream_pool.push_back(stream);
    }
    return Status::OK();
}

Status VOlapTableSinkV2::find_tablet(RuntimeState* state, vectorized::Block* block, int row_index,
                                     const VOlapTablePartition** partition, uint32_t& tablet_index,
                                     bool& stop_processing, bool& is_continue) {
    Status status = Status::OK();
    *partition = nullptr;
    tablet_index = 0;
    BlockRow block_row;
    block_row = {block, row_index};
    if (!_vpartition->find_partition(&block_row, partition)) {
        RETURN_IF_ERROR(state->append_error_msg_to_file(
                []() -> std::string { return ""; },
                [&]() -> std::string {
                    fmt::memory_buffer buf;
                    fmt::format_to(buf, "no partition for this tuple. tuple={}",
                                   block->dump_data(row_index, 1));
                    return fmt::to_string(buf);
                },
                &stop_processing));
        _number_filtered_rows++;
        if (stop_processing) {
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
        is_continue = true;
        return status;
    }
    if (!(*partition)->is_mutable) {
        _number_immutable_partition_filtered_rows++;
        is_continue = true;
        return status;
    }
    if ((*partition)->num_buckets <= 0) {
        std::stringstream ss;
        ss << "num_buckets must be greater than 0, num_buckets=" << (*partition)->num_buckets;
        return Status::InternalError(ss.str());
    }
    _partition_ids.emplace((*partition)->id);
    if (findTabletMode != FindTabletMode::FIND_TABLET_EVERY_ROW) {
        if (_partition_to_tablet_map.find((*partition)->id) == _partition_to_tablet_map.end()) {
            tablet_index = _vpartition->find_tablet(&block_row, **partition);
            _partition_to_tablet_map.emplace((*partition)->id, tablet_index);
        } else {
            tablet_index = _partition_to_tablet_map[(*partition)->id];
        }
    } else {
        tablet_index = _vpartition->find_tablet(&block_row, **partition);
    }

    return status;
}

void VOlapTableSinkV2::_generate_rows_for_tablet(RowsForTablet& rows_for_tablet,
                                                 const VOlapTablePartition* partition,
                                                 uint32_t tablet_index, int row_idx,
                                                 size_t row_cnt) {
    // Generate channel payload for sinking data to each tablet
    for (const auto& index : partition->indexes) {
        auto tablet_id = index.tablets[tablet_index];
        auto key = TabletKey {partition->id, index.index_id, tablet_id};
        if (rows_for_tablet.count(key) == 0) {
            rows_for_tablet.insert({key, std::vector<int32_t>()});
        }
        rows_for_tablet[key].push_back(row_idx);
        _number_output_rows += row_cnt;
    }
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

    auto block = vectorized::Block::create_shared(input_block->get_columns_with_type_and_name());
    if (!_output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
                _output_vexpr_ctxs, *input_block, block.get()));
    }

    // remove ownership of columns from input_block
    input_block->clear();

    auto num_rows = block->rows();
    int filtered_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(block->rows());
        bool stop_processing = false;
        RETURN_IF_ERROR(_validate_data(state, block.get(), &_filter_bitmap, &filtered_rows,
                                       &stop_processing));
        _number_filtered_rows += filtered_rows;
        if (stop_processing) {
            // should be returned after updating "_number_filtered_rows", to make sure that load job can be cancelled
            // because of "data unqualified"
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
        _convert_to_dest_desc_block(block.get());
    }

    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    RowsForTablet rows_for_tablet;
    if (findTabletMode == FIND_TABLET_EVERY_BATCH) {
        // Recaculate is needed
        _partition_to_tablet_map.clear();
    }
    _row_distribution_watch.start();
    for (int i = 0; i < num_rows; ++i) {
        if (UNLIKELY(filtered_rows) > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        bool is_continue = false;
        uint32_t tablet_index = 0;
        RETURN_IF_ERROR(find_tablet(state, block.get(), i, &partition, tablet_index,
                                    stop_processing, is_continue));
        if (is_continue) {
            continue;
        }
        _generate_rows_for_tablet(rows_for_tablet, partition, tablet_index, i, 1);
    }
    _row_distribution_watch.stop();

    // For each tablet, send its rows from block to delta writer
    for (const auto& entry : rows_for_tablet) {
        bthread_t th;
        auto closure = new WriteMemtableTaskClosure {};
        closure->sink = this;
        closure->block = block;
        closure->partition_id = entry.first.partition_id;
        closure->index_id = entry.first.index_id;
        closure->tablet_id = entry.first.tablet_id;
        closure->row_idxes = entry.second;
        auto cnt = _flying_task_count.fetch_add(1) + 1;
        LOG(INFO) << "Creating WriteMemtableTask for Tablet(tablet id: " << closure->tablet_id
                  << ", index id: " << closure->index_id << "), flying task count: " << cnt;
        bthread_start_background(&th, nullptr, _write_memtable_task, closure);
        _write_memtable_threads.push_back(th);
    }

    return Status::OK();
}

void* VOlapTableSinkV2::_write_memtable_task(void* closure) {
    auto ctx = static_cast<WriteMemtableTaskClosure*>(closure);
    VOlapTableSinkV2* sink = ctx->sink;
    DeltaWriter* delta_writer = nullptr;
    {
        std::lock_guard<bthread::Mutex> l(*sink->_delta_writer_for_tablet_mutex);
        auto key = std::make_pair(ctx->tablet_id, ctx->index_id);
        auto it = sink->_delta_writer_for_tablet->find(key);
        if (it == sink->_delta_writer_for_tablet->end()) {
            LOG(INFO) << "Creating DeltaWriter for Tablet(tablet id: " << ctx->tablet_id
                      << ", index id: " << ctx->index_id << ")";
            WriteRequest wrequest;
            wrequest.partition_id = ctx->partition_id;
            wrequest.index_id = ctx->index_id;
            wrequest.tablet_id = ctx->tablet_id;
            wrequest.write_type = WriteType::LOAD;
            wrequest.txn_id = sink->_txn_id;
            wrequest.load_id = sink->_load_id;
            wrequest.tuple_desc = sink->_output_tuple_desc;
            wrequest.is_high_priority = sink->_is_high_priority;
            wrequest.table_schema_param = sink->_schema.get();
            for (auto& index : sink->_schema->indexes()) {
                if (index->index_id == ctx->index_id) {
                    wrequest.slots = &index->slots;
                    wrequest.schema_hash = index->schema_hash;
                    break;
                }
            }
            const brpc::StreamId& stream = sink->_stream_pool->at(sink->_stream_pool_index);
            sink->_stream_pool_index = (sink->_stream_pool_index + 1) % sink->_stream_pool->size();
            DeltaWriter::open(&wrequest, &delta_writer, sink->_profile, sink->_load_id);
            delta_writer->add_stream(stream);
            sink->_delta_writer_for_tablet->insert({key, std::unique_ptr<DeltaWriter>(delta_writer)});
        } else {
            LOG(INFO) << "Reusing DeltaWriter for Tablet(tablet id: " << ctx->tablet_id
                      << ", index id: " << ctx->index_id << ")";
            delta_writer = it->second.get();
        }
    }
    auto st = delta_writer->write(ctx->block.get(), ctx->row_idxes, false);
    auto cnt = sink->_flying_task_count.fetch_sub(1) - 1;
    LOG(INFO) << "Finished writing Tablet(tablet id: " << ctx->tablet_id
              << ", index id: " << ctx->index_id << "), flying task count: " << cnt;
    delete ctx;
    DCHECK_EQ(st, Status::OK()) << "DeltaWriter::write failed";
    return nullptr;
}

Status VOlapTableSinkV2::close(RuntimeState* state, Status exec_status) {
    LOG(INFO) << "Closing VOlapTableSinkV2, flying task count: " << _flying_task_count;
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
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_row_distribution_timer, (int64_t)_row_distribution_watch.elapsed_time());
        COUNTER_SET(_filter_timer, _filter_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);

        // join all write memtable tasks
        for (auto& th : _write_memtable_threads) {
            if (th) {
                bthread_join(th, nullptr);
            }
        }

        // close all delta writers
        if (_delta_writer_for_tablet.use_count() == 1) {
            for (const auto& entry : *_delta_writer_for_tablet) {
                entry.second->close();
            }
            for (const auto& entry : *_delta_writer_for_tablet) {
                entry.second->close_wait(PSlaveTabletNodes {}, false);
            }
        }
        _delta_writer_for_tablet.reset();

        // close streams
        if (_stream_pool.use_count() == 1) {
            for (const auto& stream_id : *_stream_pool) {
                brpc::StreamClose(stream_id);
            }
        }
        _stream_pool.reset();

        // TODO: wait all stream replies
        //{
        //    std::unique_lock lock(all_stream_done_mutex);
        //    all_stream_done_cv.wait(lock);
        //}

        std::vector<TTabletCommitInfo> tablet_commit_infos;
        for (auto& entry : tablet_success_map) {
            for (int64_t be_id : entry.second) {
                TTabletCommitInfo commit_info;
                commit_info.tabletId = entry.first.first;
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
        state->update_num_rows_load_filtered(_number_filtered_rows);
        state->update_num_rows_load_unselected(_number_immutable_partition_filtered_rows);

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

        for (auto& th : _write_memtable_threads) {
            if (th) {
                bthread_stop(th);
            }
        }
    }

    _close_status = status;
    DataSink::close(state, exec_status);
    return status;
}

template <bool is_min>
DecimalV2Value VOlapTableSinkV2::_get_decimalv2_min_or_max(const TypeDescriptor& type) {
    std::map<std::pair<int, int>, DecimalV2Value>* pmap = nullptr;
    if constexpr (is_min) {
        pmap = &_min_decimalv2_val;
    } else {
        pmap = &_max_decimalv2_val;
    }

    // found
    auto iter = pmap->find({type.precision, type.scale});
    if (iter != pmap->end()) {
        return iter->second;
    }

    // save min or max DecimalV2Value for next time
    DecimalV2Value value;
    if constexpr (is_min) {
        value.to_min_decimal(type.precision, type.scale);
    } else {
        value.to_max_decimal(type.precision, type.scale);
    }
    pmap->emplace(std::pair<int, int> {type.precision, type.scale}, value);
    return value;
}

template <typename DecimalType, bool IsMin>
DecimalType VOlapTableSinkV2::_get_decimalv3_min_or_max(const TypeDescriptor& type) {
    std::map<int, typename DecimalType::NativeType>* pmap = nullptr;
    if constexpr (std::is_same_v<DecimalType, vectorized::Decimal32>) {
        pmap = IsMin ? &_min_decimal32_val : &_max_decimal32_val;
    } else if constexpr (std::is_same_v<DecimalType, vectorized::Decimal64>) {
        pmap = IsMin ? &_min_decimal64_val : &_max_decimal64_val;
    } else {
        pmap = IsMin ? &_min_decimal128_val : &_max_decimal128_val;
    }

    // found
    auto iter = pmap->find(type.precision);
    if (iter != pmap->end()) {
        return iter->second;
    }

    typename DecimalType::NativeType value;
    if constexpr (IsMin) {
        value = vectorized::min_decimal_value<DecimalType>(type.precision);
    } else {
        value = vectorized::max_decimal_value<DecimalType>(type.precision);
    }
    pmap->emplace(type.precision, value);
    return value;
}

Status VOlapTableSinkV2::_validate_column(RuntimeState* state, const TypeDescriptor& type,
                                          bool is_nullable, vectorized::ColumnPtr column,
                                          size_t slot_index, Bitmap* filter_bitmap,
                                          bool* stop_processing, fmt::memory_buffer& error_prefix,
                                          vectorized::IColumn::Permutation* rows) {
    DCHECK((rows == nullptr) || (rows->size() == column->size()));
    fmt::memory_buffer error_msg;
    auto set_invalid_and_append_error_msg = [&](int row) {
        filter_bitmap->Set(row, true);
        auto ret = state->append_error_msg_to_file([]() -> std::string { return ""; },
                                                   [&error_prefix, &error_msg]() -> std::string {
                                                       return fmt::to_string(error_prefix) +
                                                              fmt::to_string(error_msg);
                                                   },
                                                   stop_processing);
        error_msg.clear();
        return ret;
    };

    auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
    auto& real_column_ptr = column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());
    auto null_map = column_ptr == nullptr ? nullptr : column_ptr->get_null_map_data().data();
    auto need_to_validate = [&null_map, &filter_bitmap](size_t j, size_t row) {
        return !filter_bitmap->Get(row) && (null_map == nullptr || null_map[j] == 0);
    };

    ssize_t last_invalid_row = -1;
    switch (type.type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());

        size_t limit = config::string_type_length_soft_limit_bytes;
        // when type.len is negative, std::min will return overflow value, so we need to check it
        if (type.len > 0) {
            limit = std::min(config::string_type_length_soft_limit_bytes, type.len);
        }
        for (size_t j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (need_to_validate(j, row)) {
                auto str_val = column_string->get_data_at(j);
                bool invalid = str_val.size > limit;
                if (invalid) {
                    last_invalid_row = row;
                    if (str_val.size > type.len) {
                        fmt::format_to(error_msg, "{}",
                                       "the length of input is too long than schema. ");
                        fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                       str_val.to_prefix(32));
                        fmt::format_to(error_msg, "schema length: {}; ", type.len);
                        fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                    } else if (str_val.size > limit) {
                        fmt::format_to(error_msg, "{}",
                                       "the length of input string is too long than vec schema. ");
                        fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                       str_val.to_prefix(32));
                        fmt::format_to(error_msg, "schema length: {}; ", type.len);
                        fmt::format_to(error_msg, "limit length: {}; ", limit);
                        fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                    }
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                }
            }
        }
        break;
    }
    case TYPE_JSONB: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());
        for (size_t j = 0; j < column->size(); ++j) {
            if (!filter_bitmap->Get(j)) {
                if (is_nullable && column_ptr && column_ptr->is_null_at(j)) {
                    continue;
                }
                auto str_val = column_string->get_data_at(j);
                bool invalid = str_val.size == 0;
                if (invalid) {
                    error_msg.clear();
                    fmt::format_to(error_msg, "{}", "jsonb with size 0 is invalid");
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                }
            }
        }
        break;
    }
    case TYPE_DECIMALV2: {
        auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                        real_column_ptr.get()));
        const auto& max_decimalv2 = _get_decimalv2_min_or_max<false>(type);
        const auto& min_decimalv2 = _get_decimalv2_min_or_max<true>(type);
        for (size_t j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (need_to_validate(j, row)) {
                auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                        column_decimal->get_data()[j]);
                bool invalid = false;

                if (dec_val.greater_than_scale(type.scale)) {
                    auto code = dec_val.round(&dec_val, type.scale, HALF_UP);
                    column_decimal->get_data()[j] = dec_val.value();

                    if (code != E_DEC_OK) {
                        fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                       dec_val.to_string());
                        invalid = true;
                    }
                }
                if (dec_val > max_decimalv2 || dec_val < min_decimalv2) {
                    fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");
                    fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                    fmt::format_to(error_msg, ", precision={}, scale={}", type.precision,
                                   type.scale);
                    fmt::format_to(error_msg, ", min={}, max={}; ", min_decimalv2.to_string(),
                                   max_decimalv2.to_string());
                    invalid = true;
                }

                if (invalid) {
                    last_invalid_row = row;
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                }
            }
        }
        break;
    }
    case TYPE_DECIMAL32: {
#define CHECK_VALIDATION_FOR_DECIMALV3(ColumnDecimalType, DecimalType)                             \
    auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::ColumnDecimalType>*>(   \
            assert_cast<const vectorized::ColumnDecimal<vectorized::ColumnDecimalType>*>(          \
                    real_column_ptr.get()));                                                       \
    const auto& max_decimal = _get_decimalv3_min_or_max<vectorized::DecimalType, false>(type);     \
    const auto& min_decimal = _get_decimalv3_min_or_max<vectorized::DecimalType, true>(type);      \
    for (size_t j = 0; j < column->size(); ++j) {                                                  \
        auto row = rows ? (*rows)[j] : j;                                                          \
        if (row == last_invalid_row) {                                                             \
            continue;                                                                              \
        }                                                                                          \
        if (need_to_validate(j, row)) {                                                            \
            auto dec_val = column_decimal->get_data()[j];                                          \
            bool invalid = false;                                                                  \
            if (dec_val > max_decimal || dec_val < min_decimal) {                                  \
                fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");      \
                fmt::format_to(error_msg, ", value={}", dec_val);                                  \
                fmt::format_to(error_msg, ", precision={}, scale={}", type.precision, type.scale); \
                fmt::format_to(error_msg, ", min={}, max={}; ", min_decimal, max_decimal);         \
                invalid = true;                                                                    \
            }                                                                                      \
            if (invalid) {                                                                         \
                last_invalid_row = row;                                                            \
                RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));                            \
            }                                                                                      \
        }                                                                                          \
    }
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal32, Decimal32);
        break;
    }
    case TYPE_DECIMAL64: {
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal64, Decimal64);
        break;
    }
    case TYPE_DECIMAL128I: {
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal128I, Decimal128);
        break;
    }
    case TYPE_ARRAY: {
        const auto column_array =
                assert_cast<const vectorized::ColumnArray*>(real_column_ptr.get());
        DCHECK(type.children.size() == 1);
        auto nested_type = type.children[0];
        const auto& offsets = column_array->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "ARRAY type failed: ");
        RETURN_IF_ERROR(_validate_column(state, nested_type, type.contains_nulls[0],
                                         column_array->get_data_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        break;
    }
    case TYPE_MAP: {
        const auto column_map = assert_cast<const vectorized::ColumnMap*>(real_column_ptr.get());
        DCHECK(type.children.size() == 2);
        auto key_type = type.children[0];
        auto val_type = type.children[1];
        const auto& offsets = column_map->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "MAP type failed: ");
        RETURN_IF_ERROR(_validate_column(state, key_type, type.contains_nulls[0],
                                         column_map->get_keys_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        RETURN_IF_ERROR(_validate_column(state, val_type, type.contains_nulls[1],
                                         column_map->get_values_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        break;
    }
    case TYPE_STRUCT: {
        const auto column_struct =
                assert_cast<const vectorized::ColumnStruct*>(real_column_ptr.get());
        DCHECK(type.children.size() == column_struct->tuple_size());
        fmt::format_to(error_prefix, "STRUCT type failed: ");
        for (size_t sc = 0; sc < column_struct->tuple_size(); ++sc) {
            RETURN_IF_ERROR(_validate_column(state, type.children[sc], type.contains_nulls[sc],
                                             column_struct->get_column_ptr(sc), slot_index,
                                             filter_bitmap, stop_processing, error_prefix));
        }
        break;
    }
    default:
        break;
    }

    // Dispose the column should do not contain the NULL value
    // Only two case:
    // 1. column is nullable but the desc is not nullable
    // 2. desc->type is BITMAP
    if ((!is_nullable || type == TYPE_OBJECT) && column_ptr) {
        for (int j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (null_map[j] && !filter_bitmap->Get(row)) {
                fmt::format_to(error_msg, "null value for not null column, type={}",
                               type.debug_string());
                last_invalid_row = row;
                RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
            }
        }
    }

    return Status::OK();
}

Status VOlapTableSinkV2::_validate_data(RuntimeState* state, vectorized::Block* block,
                                        Bitmap* filter_bitmap, int* filtered_rows,
                                        bool* stop_processing) {
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        fmt::memory_buffer error_prefix;
        fmt::format_to(error_prefix, "column_name[{}], ", desc->col_name());
        RETURN_IF_ERROR(_validate_column(state, desc->type(), desc->is_nullable(), column, i,
                                         filter_bitmap, stop_processing, error_prefix));
    }

    *filtered_rows = 0;
    for (int i = 0; i < block->rows(); ++i) {
        *filtered_rows += filter_bitmap->Get(i);
    }
    return Status::OK();
}

void VOlapTableSinkV2::_convert_to_dest_desc_block(doris::vectorized::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size() && i < block->columns(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        if (desc->is_nullable() != block->get_by_position(i).type->is_nullable()) {
            if (desc->is_nullable()) {
                block->get_by_position(i).type =
                        vectorized::make_nullable(block->get_by_position(i).type);
                block->get_by_position(i).column =
                        vectorized::make_nullable(block->get_by_position(i).column);
            } else {
                block->get_by_position(i).type = assert_cast<const vectorized::DataTypeNullable&>(
                                                         *block->get_by_position(i).type)
                                                         .get_nested_type();
                block->get_by_position(i).column = assert_cast<const vectorized::ColumnNullable&>(
                                                           *block->get_by_position(i).column)
                                                           .get_nested_column_ptr();
            }
        }
    }
}

} // namespace stream_load
} // namespace doris
