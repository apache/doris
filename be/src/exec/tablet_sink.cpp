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

#include "exec/tablet_sink.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"

#include "olap/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/uid_util.h"
#include "service/brpc.h"

namespace doris {
namespace stream_load {

NodeChannel::NodeChannel(OlapTableSink* parent, int64_t index_id,
                         int64_t node_id, int32_t schema_hash)
        : _parent(parent), _index_id(index_id),
        _node_id(node_id), _schema_hash(schema_hash) {
}

NodeChannel::~NodeChannel() {
    if (_open_closure != nullptr) {
        if (_open_closure->unref()) {
            delete _open_closure;
        }
        _open_closure = nullptr;
    }
    if (_add_batch_closure != nullptr) {
        if (_add_batch_closure->unref()) {
            delete _add_batch_closure;
        }
        _add_batch_closure = nullptr;
    }
    _add_batch_request.release_id();
}

Status NodeChannel::init(RuntimeState* state) {
    _tuple_desc = _parent->_output_tuple_desc;
    _node_info = _parent->_nodes_info->find_node(_node_id);
    if (_node_info == nullptr) {
        std::stringstream ss;
        ss << "unknown node id, id=" << _node_id;
        return Status::InternalError(ss.str());
    }
    RowDescriptor row_desc(_tuple_desc, false);
    _batch.reset(new RowBatch(row_desc, state->batch_size(), _parent->_mem_tracker));

    _stub = state->exec_env()->brpc_stub_cache()->get_stub(
            _node_info->host, _node_info->brpc_port);
    if (_stub == nullptr) {
        LOG(WARNING) << "Get rpc stub failed, host=" << _node_info->host
            << ", port=" << _node_info->brpc_port;
        return Status::InternalError("get rpc stub failed");
    }

    // Initialize _add_batch_request
    _add_batch_request.set_allocated_id(&_parent->_load_id);
    _add_batch_request.set_index_id(_index_id);
    _add_batch_request.set_sender_id(_parent->_sender_id);

    _rpc_timeout_ms = state->query_options().query_timeout * 1000;
    return Status::OK();
}

void NodeChannel::open() {
    PTabletWriterOpenRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    for (auto& tablet : _all_tablets) {
        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(tablet.partition_id);
        ptablet->set_tablet_id(tablet.tablet_id);
    }
    request.set_num_senders(_parent->_num_senders);
    request.set_need_gen_rollup(_parent->_need_gen_rollup);
    request.set_load_mem_limit(_parent->_load_mem_limit);
    request.set_load_channel_timeout_s(_parent->_load_channel_timeout_s);

    _open_closure = new RefCountClosure<PTabletWriterOpenResult>();
    _open_closure->ref();

    // This ref is for RPC's reference
    _open_closure->ref();
    _open_closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    _stub->tablet_writer_open(&_open_closure->cntl,
                              &request,
                              &_open_closure->result,
                              _open_closure);
    request.release_id();
    request.release_schema();
}

Status NodeChannel::open_wait() {
    _open_closure->join();
    if (_open_closure->cntl.Failed()) {
        LOG(WARNING) << "failed to open tablet writer, error="
            << berror(_open_closure->cntl.ErrorCode())
            << ", error_text=" << _open_closure->cntl.ErrorText();
        return Status::InternalError("failed to open tablet writer");
    }
    Status status(_open_closure->result.status());
    if (_open_closure->unref()) {
        delete _open_closure;
    }
    _open_closure = nullptr;

    // add batch closure
    _add_batch_closure = new RefCountClosure<PTabletWriterAddBatchResult>();
    _add_batch_closure->ref();

    return status;
}

Status NodeChannel::add_row(Tuple* input_tuple, int64_t tablet_id) {
    auto row_no = _batch->add_row();
    if (row_no == RowBatch::INVALID_ROW_INDEX) {
        RETURN_IF_ERROR(_send_cur_batch());
        row_no = _batch->add_row();
    }
    DCHECK_NE(row_no, RowBatch::INVALID_ROW_INDEX);
    auto tuple = input_tuple->deep_copy(*_tuple_desc, _batch->tuple_data_pool());
    _batch->get_row(row_no)->set_tuple(0, tuple);
    _batch->commit_last_row();
    _add_batch_request.add_tablet_ids(tablet_id);
    return Status::OK();
}

Status NodeChannel::close(RuntimeState* state) {
    auto st = _close(state);
    _batch.reset();
    return st;
}

Status NodeChannel::_close(RuntimeState* state) {
    RETURN_IF_ERROR(_wait_in_flight_packet());
    return _send_cur_batch(true);
}

Status NodeChannel::close_wait(RuntimeState* state) {
    RETURN_IF_ERROR(_wait_in_flight_packet());
    Status status(_add_batch_closure->result.status());
    if (status.ok()) {
        for (auto& tablet : _add_batch_closure->result.tablet_vec()) {
            TTabletCommitInfo commit_info;
            commit_info.tabletId = tablet.tablet_id();
            commit_info.backendId = _node_id;
            state->tablet_commit_infos().emplace_back(std::move(commit_info));
        }
    }
    // clear batch after sendt
    _batch.reset();
    return status;
}

void NodeChannel::cancel() {
    // Do we need to wait last rpc finished???
    PTabletWriterCancelRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_sender_id(_parent->_sender_id);

    auto closure = new RefCountClosure<PTabletWriterCancelResult>();

    closure->ref();
    closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    _stub->tablet_writer_cancel(&closure->cntl,
                                &request,
                                &closure->result,
                                closure);
    request.release_id();

    // reset batch
    _batch.reset();
}

Status NodeChannel::_wait_in_flight_packet() {
    if (!_has_in_flight_packet) {
        return Status::OK();
    }

    SCOPED_RAW_TIMER(_parent->mutable_wait_in_flight_packet_ns());
    _add_batch_closure->join();
    _has_in_flight_packet = false;
    if (_add_batch_closure->cntl.Failed()) {
        LOG(WARNING) << "failed to send batch, error="
            << berror(_add_batch_closure->cntl.ErrorCode())
            << ", error_text=" << _add_batch_closure->cntl.ErrorText();
        return Status::InternalError("failed to send batch");
    }

    if (_add_batch_closure->result.has_execution_time_us()) {
        _parent->update_node_add_batch_counter(_node_id,
                _add_batch_closure->result.execution_time_us(),
                _add_batch_closure->result.wait_lock_time_us());
    }
    return {_add_batch_closure->result.status()};
}

Status NodeChannel::_send_cur_batch(bool eos) {
    RETURN_IF_ERROR(_wait_in_flight_packet());

    // tablet_ids has already set when add row
    _add_batch_request.set_eos(eos);
    _add_batch_request.set_packet_seq(_next_packet_seq);
    if (_batch->num_rows() > 0) {
        SCOPED_RAW_TIMER(_parent->mutable_serialize_batch_ns());
        _batch->serialize(_add_batch_request.mutable_row_batch());
    }

    _add_batch_closure->ref();
    _add_batch_closure->cntl.Reset();
    _add_batch_closure->cntl.set_timeout_ms(_rpc_timeout_ms);

    if (eos) {
        for (auto pid : _parent->_partition_ids) {
            _add_batch_request.add_partition_ids(pid);
        }
    }

    _stub->tablet_writer_add_batch(&_add_batch_closure->cntl,
                                   &_add_batch_request,
                                   &_add_batch_closure->result,
                                   _add_batch_closure);
    _add_batch_request.clear_tablet_ids();
    _add_batch_request.clear_row_batch();
    _add_batch_request.clear_partition_ids();

    _has_in_flight_packet = true;
    _next_packet_seq++;

    _batch->reset();
    return Status::OK();
}

IndexChannel::~IndexChannel() {
}

Status IndexChannel::init(RuntimeState* state,
                          const std::vector<TTabletWithPartition>& tablets) {
    for (auto& tablet : tablets) {
        auto location = _parent->_location->find_tablet(tablet.tablet_id);
        if (location == nullptr) {
            LOG(WARNING) << "unknow tablet, tablet_id=" << tablet.tablet_id;
            return Status::InternalError("unknown tablet");
        }
        std::vector<NodeChannel*> channels;
        for (auto& node_id : location->node_ids) {
            NodeChannel* channel = nullptr;
            auto it = _node_channels.find(node_id);
            if (it == std::end(_node_channels)) {
                channel = _parent->_pool->add(
                        new NodeChannel(_parent, _index_id, node_id, _schema_hash));
                _node_channels.emplace(node_id, channel);
            } else {
                channel = it->second;
            }
            channel->add_tablet(tablet);
            channels.push_back(channel);
        }
        _channels_by_tablet.emplace(tablet.tablet_id, std::move(channels));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
    }
    return Status::OK();
}

Status IndexChannel::open() {
    for (auto& it : _node_channels) {
        it.second->open();
    }
    for (auto& it : _node_channels) {
        auto channel = it.second;
        auto st = channel->open_wait();
        if (!st.ok()) {
            LOG(WARNING) << "tablet open failed, load_id=" << _parent->_load_id
                << ", node=" << channel->node_info()->host
                << ":" << channel->node_info()->brpc_port
                << ", errmsg=" << st.get_error_msg();
            if (_handle_failed_node(channel)) {
                LOG(WARNING) << "open failed, load_id=" << _parent->_load_id;
                return st;
            }
        }
    }
    return Status::OK();
}

Status IndexChannel::add_row(Tuple* tuple, int64_t tablet_id) {
    auto it = _channels_by_tablet.find(tablet_id);
    DCHECK(it != std::end(_channels_by_tablet)) << "unknown tablet, tablet_id=" << tablet_id;
    for (auto channel : it->second) {
        if (channel->already_failed()) {
            continue;
        }
        auto st = channel->add_row(tuple, tablet_id);
        if (!st.ok()) {
            LOG(WARNING) << "NodeChannel add row failed, load_id=" << _parent->_load_id
                << ", tablet_id=" << tablet_id
                << ", node=" << channel->node_info()->host
                << ":" << channel->node_info()->brpc_port
                << ", errmsg=" << st.get_error_msg();
            if (_handle_failed_node(channel)) {
                LOG(WARNING) << "add row failed, load_id=" << _parent->_load_id;
                return st;
            }
        }
    }
    return Status::OK();
}

Status IndexChannel::close(RuntimeState* state) {
    std::vector<NodeChannel*> need_wait_channels;
    need_wait_channels.reserve(_node_channels.size());

    Status close_status;
    for (auto& it : _node_channels) {
        auto channel = it.second;
        if (channel->already_failed() || !close_status.ok()) {
            channel->cancel();
            continue;
        }
        auto st = channel->close(state);
        if (st.ok()) {
            need_wait_channels.push_back(channel);
        } else {
            LOG(WARNING) << "close node channel failed, load_id=" << _parent->_load_id
                << ", node=" << channel->node_info()->host
                << ":" << channel->node_info()->brpc_port
                << ", errmsg=" << st.get_error_msg();
            if (_handle_failed_node(channel)) {
                LOG(WARNING) << "close failed, load_id=" << _parent->_load_id;
                close_status = st;
            }
        }
    }

    if (close_status.ok()) {
        for (auto channel : need_wait_channels) {
            auto st = channel->close_wait(state);
            if (!st.ok()) {
                LOG(WARNING) << "close_wait node channel failed, load_id=" << _parent->_load_id
                    << ", node=" << channel->node_info()->host
                    << ":" << channel->node_info()->brpc_port
                    << ", errmsg=" << st.get_error_msg();
                if (_handle_failed_node(channel)) {
                    LOG(WARNING) << "close_wait failed, load_id=" << _parent->_load_id;
                    return st;
                }
            }
        }
    }
    return close_status;
}

void IndexChannel::cancel() {
    for (auto& it : _node_channels) {
        it.second->cancel();
    }
}

bool IndexChannel::_handle_failed_node(NodeChannel* channel) {
    DCHECK(!channel->already_failed());
    channel->set_failed();
    _num_failed_channels++;
    return _num_failed_channels >= ((_parent->_num_repicas + 1) / 2);
}

OlapTableSink::OlapTableSink(ObjectPool* pool,
                             const RowDescriptor& row_desc,
                             const std::vector<TExpr>& texprs,
                             Status* status)
        : _pool(pool), _input_row_desc(row_desc), _filter_bitmap(1024) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs);
    }
}

OlapTableSink::~OlapTableSink() {
}

Status OlapTableSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _db_id = table_sink.db_id;
    _table_id = table_sink.table_id;
    _num_repicas = table_sink.num_replicas;
    _need_gen_rollup = table_sink.need_gen_rollup;
    _db_name = table_sink.db_name;
    _table_name = table_sink.table_name;
    _tuple_desc_id = table_sink.tuple_id;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _partition = _pool->add(new OlapTablePartitionParam(_schema, table_sink.partition));
    RETURN_IF_ERROR(_partition->init());
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new DorisNodesInfo(table_sink.nodes_info));

    if (table_sink.__isset.load_channel_timeout_s) {
        _load_channel_timeout_s = table_sink.load_channel_timeout_s;
    } else {
        _load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    }

    return Status::OK();
}

Status OlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile(_pool, "OlapTableSink"));
    _mem_tracker = _pool->add(new MemTracker(-1, "OlapTableSink", state->instance_mem_tracker()));

    SCOPED_TIMER(_profile->total_time_counter());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state,
                                  _input_row_desc, _expr_mem_tracker.get()));

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }
    if (!_output_expr_ctxs.empty()) {
        if (_output_expr_ctxs.size() != _output_tuple_desc->slots().size()) {
            LOG(WARNING) << "number of exprs is not same with slots, num_exprs="
                << _output_expr_ctxs.size()
                << ", num_slots=" << _output_tuple_desc->slots().size();
            return Status::InternalError("number of exprs is not same with slots");
        }
        for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
            if (!is_type_compatible(_output_expr_ctxs[i]->root()->type().type,
                                    _output_tuple_desc->slots()[i]->type().type)) {
                LOG(WARNING) << "type of exprs is not match slot's, expr_type="
                    << _output_expr_ctxs[i]->root()->type().type
                    << ", slot_type=" << _output_tuple_desc->slots()[i]->type().type
                    << ", slot_name=" << _output_tuple_desc->slots()[i]->col_name();
                return Status::InternalError("expr's type is not same with slot's");
            }
        }
    }

    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));
    _output_batch.reset(new RowBatch(*_output_row_desc, state->batch_size(), _mem_tracker));

    _max_decimal_val.resize(_output_tuple_desc->slots().size());
    _min_decimal_val.resize(_output_tuple_desc->slots().size());

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
        case TYPE_DECIMAL:
            _max_decimal_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimal_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            _need_validate_data = true;
            break;
        case TYPE_DECIMALV2:
            _max_decimalv2_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimalv2_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            _need_validate_data = true;
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_HLL:
            _need_validate_data = true;
            break;
        default:
            break;
        }
    }

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _convert_batch_timer = ADD_TIMER(_profile, "ConvertBatchTime");
    _validate_data_timer = ADD_TIMER(_profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseTime");
    _wait_in_flight_packet_timer = ADD_TIMER(_profile, "WaitInFlightPacketTime");
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _load_mem_limit = state->get_load_mem_limit();

    // open all channels
    auto& partitions = _partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<TTabletWithPartition> tablets;
        auto index = _schema->indexes()[i];
        for (auto part : partitions) {
            for (auto tablet : part->indexes[i].tablets) {
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
        }
        auto channel = _pool->add(new IndexChannel(this, index->index_id, index->schema_hash));
        RETURN_IF_ERROR(channel->init(state, tablets));
        _channels.emplace_back(channel);
    }

    return Status::OK();
}

Status OlapTableSink::open(RuntimeState* state) {
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    for (auto channel : _channels) {
        RETURN_IF_ERROR(channel->open());
    }
    return Status::OK();
}

Status OlapTableSink::send(RuntimeState* state, RowBatch* input_batch) {
    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += input_batch->num_rows();
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(input_batch->num_rows());
    RowBatch* batch = input_batch;
    if (!_output_expr_ctxs.empty()) {
        SCOPED_RAW_TIMER(&_convert_batch_ns);
        _output_batch->reset();
        _convert_batch(state, input_batch, _output_batch.get());
        batch = _output_batch.get();
    }

    int num_invalid_rows = 0;
    if (_need_validate_data) {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(batch->num_rows());
        num_invalid_rows = _validate_data(state, batch, &_filter_bitmap);
        _number_filtered_rows += num_invalid_rows;
    }
    SCOPED_RAW_TIMER(&_send_data_ns);
    for (int i = 0; i < batch->num_rows(); ++i) {
        Tuple* tuple = batch->get_row(i)->get_tuple(0);
        if (num_invalid_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const OlapTablePartition* partition = nullptr;
        uint32_t dist_hash = 0;
        if (!_partition->find_tablet(tuple, &partition, &dist_hash)) {
            std::stringstream ss;
            ss << "no partition for this tuple. tuple="
                << Tuple::to_string(tuple, *_output_tuple_desc);
#if BE_TEST
            LOG(INFO) << ss.str();
#else
            state->append_error_msg_to_file("", ss.str());
#endif
            _number_filtered_rows++;
            continue;
        }
        _partition_ids.emplace(partition->id);
        uint32_t tablet_index = dist_hash % partition->num_buckets;
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            RETURN_IF_ERROR(_channels[j]->add_row(tuple, tablet_id));
            _number_output_rows++;
        }
    }
    return Status::OK();
}

Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    Status status = close_status;
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        {
            SCOPED_TIMER(_close_timer);
            for (auto channel : _channels) {
                status = channel->close(state);
                if (!status.ok()) {
                    LOG(WARNING) << "close channel failed, load_id=" << print_id(_load_id)
                        << ", txn_id=" << _txn_id;
                }
            }
        }
        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_convert_batch_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);
        COUNTER_SET(_wait_in_flight_packet_timer, _wait_in_flight_packet_ns);
        COUNTER_SET(_serialize_batch_timer, _serialize_batch_ns);
        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + state->num_rows_load_filtered() + state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_number_filtered_rows);

        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "finished to close olap table sink. load_id=" << print_id(_load_id)
                << ", txn_id=" << _txn_id << ", node add batch time(ms)/wait lock time(ms)/num: ";
        for (auto const& pair : _node_add_batch_counter_map) {
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_ns / 1000) << ")("
               << (pair.second.add_batch_wait_lock_time_ns / 1000) << ")("
               << pair.second.add_batch_num << ")} ";
        }
        LOG(INFO) << ss.str();

    } else {
        for (auto channel : _channels) {
            channel->cancel();
        }
    }
    Expr::close(_output_expr_ctxs, state);
    _output_batch.reset();
    return status;
}

void OlapTableSink::_convert_batch(RuntimeState* state, RowBatch* input_batch, RowBatch* output_batch) {
    DCHECK_GE(output_batch->capacity(), input_batch->num_rows());
    int commit_rows = 0;
    for (int i = 0; i < input_batch->num_rows(); ++i) {
        auto src_row = input_batch->get_row(i);
        Tuple* dst_tuple = (Tuple*)output_batch->tuple_data_pool()->allocate(
            _output_tuple_desc->byte_size());
        bool ignore_this_row = false;
        for (int j = 0; j < _output_expr_ctxs.size(); ++j) {
            auto src_val = _output_expr_ctxs[j]->get_value(src_row);
            auto slot_desc = _output_tuple_desc->slots()[j];
            // The following logic is similar to BaseScanner::fill_dest_tuple
            // Todo(kks): we should unify it
            if (src_val == nullptr) {
                // Only when the expr return value is null, we will check the error message.
                std::string expr_error = _output_expr_ctxs[j]->get_error_msg();
                if (!expr_error.empty()) {
                    state->append_error_msg_to_file(slot_desc->col_name(), expr_error);
                    _number_filtered_rows++;
                    ignore_this_row = true;
                    // The ctx is reused, so must clear the error state and message.
                    _output_expr_ctxs[j]->clear_error_msg();
                    break;
                }
                if (!slot_desc->is_nullable()) {
                    std::stringstream ss;
                    ss << "null value for not null column, column=" << slot_desc->col_name();
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    _number_filtered_rows++;
                    ignore_this_row = true;
                    break;
                }
                dst_tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            }
            if (slot_desc->is_nullable()) {
                dst_tuple->set_not_null(slot_desc->null_indicator_offset());
            }
            void* slot = dst_tuple->get_slot(slot_desc->tuple_offset());
            RawValue::write(src_val, slot, slot_desc->type(), _output_batch->tuple_data_pool());
        }

        if (!ignore_this_row) {
            output_batch->get_row(commit_rows)->set_tuple(0, dst_tuple);
            commit_rows++;
        }
    }
    output_batch->commit_rows(commit_rows);
}

int OlapTableSink::_validate_data(RuntimeState* state, RowBatch* batch, Bitmap* filter_bitmap) {
    int filtered_rows = 0;
    for (int row_no = 0; row_no < batch->num_rows(); ++row_no) {
        Tuple* tuple = batch->get_row(row_no)->get_tuple(0);
        bool row_valid = true;
        for (int i = 0; row_valid && i < _output_tuple_desc->slots().size(); ++i) {
            SlotDescriptor* desc = _output_tuple_desc->slots()[i];
            if (desc->is_nullable() && tuple->is_null(desc->null_indicator_offset())) {
                continue;
            }
            void* slot = tuple->get_slot(desc->tuple_offset());
            switch (desc->type().type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                // Fixed length string
                StringValue* str_val = (StringValue*)slot;
                if (str_val->len > desc->type().len) {
                    std::stringstream ss;
                    ss << "the length of input is too long than schema. "
                        << "column_name: " << desc->col_name() << "; "
                        << "input_str: [" << std::string(str_val->ptr, str_val->len) << "] "
                        << "schema length: " << desc->type().len << "; "
                        << "actual length: " << str_val->len << "; ";
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif

                    filtered_rows++;
                    row_valid = false;
                    filter_bitmap->Set(row_no, true);
                    continue;
                }
                // padding 0 to CHAR field
                if (desc->type().type == TYPE_CHAR
                        && str_val->len < desc->type().len) {
                    auto new_ptr =  (char*)batch->tuple_data_pool()->allocate(desc->type().len);
                    memcpy(new_ptr, str_val->ptr, str_val->len);
                    memset(new_ptr + str_val->len, 0, desc->type().len - str_val->len);

                    str_val->ptr = new_ptr;
                    str_val->len = desc->type().len;
                }
                break;
            }
            case TYPE_DECIMAL: {
                DecimalValue* dec_val = (DecimalValue*)slot;
                if (dec_val->scale() > desc->type().scale) {
                    int code = dec_val->round(dec_val, desc->type().scale, HALF_UP);
                    if (code != E_DEC_OK) {
                        std::stringstream ss;
                        ss << "round one decimal failed.value=" << dec_val->to_string();
#if BE_TEST
                        LOG(INFO) << ss.str();
#else
                        state->append_error_msg_to_file("", ss.str());
#endif

                        filtered_rows++;
                        row_valid = false;
                        filter_bitmap->Set(row_no, true);
                        continue;
                    }
                }
                if (*dec_val > _max_decimal_val[i] || *dec_val < _min_decimal_val[i]) {
                    std::stringstream ss;
                    ss << "decimal value is not valid for defination, column=" << desc->col_name()
                        << ", value=" << dec_val->to_string()
                        << ", precision=" << desc->type().precision
                        << ", scale=" << desc->type().scale;
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    filtered_rows++;
                    row_valid = false;
                    filter_bitmap->Set(row_no, true);
                    continue;
                }
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value dec_val(reinterpret_cast<const PackedInt128*>(slot)->value);
                if (dec_val.greater_than_scale(desc->type().scale)) {
                    int code = dec_val.round(&dec_val, desc->type().scale, HALF_UP);
                    reinterpret_cast<PackedInt128*>(slot)->value = dec_val.value();
                    if (code != E_DEC_OK) {
                        std::stringstream ss;
                        ss << "round one decimal failed.value=" << dec_val.to_string();
#if BE_TEST
                        LOG(INFO) << ss.str();
#else
                        state->append_error_msg_to_file("", ss.str());
#endif

                        filtered_rows++;
                        row_valid = false;
                        filter_bitmap->Set(row_no, true);
                        continue;
                    }
                }
                if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                    std::stringstream ss;
                    ss << "decimal value is not valid for defination, column=" << desc->col_name()
                        << ", value=" << dec_val.to_string()
                        << ", precision=" << desc->type().precision
                        << ", scale=" << desc->type().scale;
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    filtered_rows++;
                    row_valid = false;
                    filter_bitmap->Set(row_no, true);
                    continue;
                }
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                static DateTimeValue s_min_value = DateTimeValue(19000101000000UL);
                // static DateTimeValue s_max_value = DateTimeValue(99991231235959UL);
                DateTimeValue* date_val = (DateTimeValue*)slot;
                if (*date_val < s_min_value) {
                    std::stringstream ss;
                    ss << "datetime value is not valid, column=" << desc->col_name()
                        << ", value=" << date_val->debug_string();
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    filtered_rows++;
                    row_valid = false;
                    filter_bitmap->Set(row_no, true);
                    continue;
                }
                break;
            }
            case TYPE_HLL: {
                Slice* hll_val = (Slice*)slot;
                if (!HyperLogLog::is_valid(*hll_val)) {
                    std::stringstream ss;
                    ss << "Content of HLL type column is invalid"
                        << "column_name: " << desc->col_name() << "; ";
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    filtered_rows++;
                    row_valid = false;
                    filter_bitmap->Set(row_no, true);
                    continue;
                }
                break;
            }
            default:
                break;
            }
        }
    }
    return filtered_rows;
}

}
}
