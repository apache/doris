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

#include "vec/sink/vtablet_sink.h"

#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/doris_metrics.h"
#include "util/proto_util.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace stream_load {

VNodeChannel::VNodeChannel(OlapTableSink* parent, IndexChannel* index_channel, int64_t node_id)
        : NodeChannel(parent, index_channel, node_id) {
    _is_vectorized = true;
}

VNodeChannel::~VNodeChannel() {
    if (_add_block_closure != nullptr) {
        delete _add_block_closure;
        _add_block_closure = nullptr;
    }
    _cur_add_block_request.release_id();
}

void VNodeChannel::clear_all_blocks() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    std::queue<AddBlockReq> empty;
    std::swap(_pending_blocks, empty);
    _cur_mutable_block.reset();
}

// if "_cancelled" is set to true,
// no need to set _cancel_msg because the error will be
// returned directly via "TabletSink::prepare()" method.
Status VNodeChannel::init(RuntimeState* state) {
    RETURN_IF_ERROR(NodeChannel::init(state));

    _cur_mutable_block.reset(new vectorized::MutableBlock({_tuple_desc}));

    // Initialize _cur_add_block_request
    _cur_add_block_request.set_allocated_id(&_parent->_load_id);
    _cur_add_block_request.set_index_id(_index_channel->_index_id);
    _cur_add_block_request.set_sender_id(_parent->_sender_id);
    _cur_add_block_request.set_backend_id(_node_id);
    _cur_add_block_request.set_eos(false);

    _name = fmt::format("VNodeChannel[{}-{}]", _index_channel->_index_id, _node_id);

    return Status::OK();
}

Status VNodeChannel::open_wait() {
    Status status = NodeChannel::open_wait();
    if (!status.ok()) {
        return status;
    }

    // add block closure
    _add_block_closure = ReusableClosure<PTabletWriterAddBlockResult>::create();
    _add_block_closure->addFailedHandler([this](bool is_last_rpc) {
        std::lock_guard<std::mutex> l(this->_closed_lock);
        if (this->_is_closed) {
            // if the node channel is closed, no need to call `mark_as_failed`,
            // and notice that _index_channel may already be destroyed.
            return;
        }
        // If rpc failed, mark all tablets on this node channel as failed
        _index_channel->mark_as_failed(this->node_id(), this->host(),
                                       _add_block_closure->cntl.ErrorText(), -1);
        Status st = _index_channel->check_intolerable_failure();
        if (!st.ok()) {
            _cancel_with_msg(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
        } else if (is_last_rpc) {
            // if this is last rpc, will must set _add_batches_finished. otherwise, node channel's close_wait
            // will be blocked.
            _add_batches_finished = true;
        }
    });

    _add_block_closure->addSuccessHandler([this](const PTabletWriterAddBlockResult& result,
                                                 bool is_last_rpc) {
        std::lock_guard<std::mutex> l(this->_closed_lock);
        if (this->_is_closed) {
            // if the node channel is closed, no need to call the following logic,
            // and notice that _index_channel may already be destroyed.
            return;
        }
        Status status(result.status());
        if (status.ok()) {
            // if has error tablet, handle them first
            for (auto& error : result.tablet_errors()) {
                _index_channel->mark_as_failed(this->node_id(), this->host(), error.msg(),
                                               error.tablet_id());
            }

            Status st = _index_channel->check_intolerable_failure();
            if (!st.ok()) {
                _cancel_with_msg(st.get_error_msg());
            } else if (is_last_rpc) {
                for (auto& tablet : result.tablet_vec()) {
                    TTabletCommitInfo commit_info;
                    commit_info.tabletId = tablet.tablet_id();
                    commit_info.backendId = _node_id;
                    _tablet_commit_infos.emplace_back(std::move(commit_info));
                }
                _add_batches_finished = true;
            }
        } else {
            _cancel_with_msg(fmt::format("{}, add batch req success but status isn't ok, err: {}",
                                         channel_info(), status.get_error_msg()));
        }

        if (result.has_execution_time_us()) {
            _add_batch_counter.add_batch_execution_time_us += result.execution_time_us();
            _add_batch_counter.add_batch_wait_execution_time_us += result.wait_execution_time_us();
            _add_batch_counter.add_batch_num++;
        }
    });
    return status;
}

Status VNodeChannel::add_row(const BlockRow& block_row, int64_t tablet_id) {
    // If add_row() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("add row failed. {}", _cancel_msg);
        } else {
            return st.clone_and_prepend("already stopped, can't add row. cancelled/eos: ");
        }
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_row() and return OK, because we will check _cancelled in next add_row() or mark_close().
    while (!_cancelled &&
           (_pending_batches_bytes > _max_pending_batches_bytes ||
            _parent->_mem_tracker->any_limit_exceeded()) &&
           _pending_batches_num > 0) {
        SCOPED_ATOMIC_TIMER(&_mem_exceeded_block_ns);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    _cur_mutable_block->add_row(block_row.first, block_row.second);
    _cur_add_block_request.add_tablet_ids(tablet_id);

    if (_cur_mutable_block->rows() == _batch_size ||
        _cur_mutable_block->bytes() > config::doris_scanner_row_bytes) {
        {
            SCOPED_ATOMIC_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            //To simplify the add_row logic, postpone adding block into req until the time of sending req
            _pending_batches_bytes += _cur_mutable_block->allocated_bytes();
            _pending_blocks.emplace(std::move(_cur_mutable_block), _cur_add_block_request);
            _pending_batches_num++;
        }

        _cur_mutable_block.reset(new vectorized::MutableBlock({_tuple_desc}));
        _cur_add_block_request.clear_tablet_ids();
    }

    return Status::OK();
}

int VNodeChannel::try_send_and_fetch_status(RuntimeState* state,
                                            std::unique_ptr<ThreadPoolToken>& thread_pool_token) {
    auto st = none_of({_cancelled, _send_finished});
    if (!st.ok()) {
        return 0;
    }

    if (!_add_block_closure->try_set_in_flight()) {
        return _send_finished ? 0 : 1;
    }

    // We are sure that try_send_batch is not running
    if (_pending_batches_num > 0) {
        auto s = thread_pool_token->submit_func(
                std::bind(&VNodeChannel::try_send_block, this, state));
        if (!s.ok()) {
            _cancel_with_msg("submit send_batch task to send_batch_thread_pool failed");
            // clear in flight
            _add_block_closure->clear_in_flight();
        }
        // in_flight is cleared in closure::Run
    } else {
        // clear in flight
        _add_block_closure->clear_in_flight();
    }
    return _send_finished ? 0 : 1;
}

void VNodeChannel::try_send_block(RuntimeState* state) {
    SCOPED_ATTACH_TASK_THREAD(state, _node_channel_tracker);
    SCOPED_ATOMIC_TIMER(&_actual_consume_ns);
    AddBlockReq send_block;
    {
        debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        DCHECK(!_pending_blocks.empty());
        send_block = std::move(_pending_blocks.front());
        _pending_blocks.pop();
        _pending_batches_num--;
        _pending_batches_bytes -= send_block.first->allocated_bytes();
    }

    auto mutable_block = std::move(send_block.first);
    auto request = std::move(send_block.second); // doesn't need to be saved in heap

    // tablet_ids has already set when add row
    request.set_packet_seq(_next_packet_seq);
    auto block = mutable_block->to_block();
    if (block.rows() > 0) {
        SCOPED_ATOMIC_TIMER(&_serialize_batch_ns);
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        Status st = block.serialize(request.mutable_block(), &uncompressed_bytes, &compressed_bytes,
                                    _parent->_transfer_large_data_by_brpc);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
            _add_block_closure->clear_in_flight();
            return;
        }
        if (compressed_bytes >= double(config::brpc_max_body_size) * 0.95f) {
            LOG(WARNING) << "send block too large, this rpc may failed. send size: "
                         << compressed_bytes << ", threshold: " << config::brpc_max_body_size
                         << ", " << channel_info();
        }
    }

    int remain_ms = _rpc_timeout_ms - _timeout_watch.elapsed_time() / NANOS_PER_MILLIS;
    if (UNLIKELY(remain_ms < config::min_load_rpc_timeout_ms)) {
        if (remain_ms <= 0 && !request.eos()) {
            cancel(fmt::format("{}, err: timeout", channel_info()));
            _add_block_closure->clear_in_flight();
            return;
        } else {
            remain_ms = config::min_load_rpc_timeout_ms;
        }
    }

    _add_block_closure->reset();
    _add_block_closure->cntl.set_timeout_ms(remain_ms);
    if (config::tablet_writer_ignore_eovercrowded) {
        _add_block_closure->cntl.ignore_eovercrowded();
    }

    if (request.eos()) {
        for (auto pid : _parent->_partition_ids) {
            request.add_partition_ids(pid);
        }

        // eos request must be the last request
        _add_block_closure->end_mark();
        _send_finished = true;
        CHECK(_pending_batches_num == 0) << _pending_batches_num;
    }

    if (_parent->_transfer_large_data_by_brpc && request.has_block() &&
        request.block().has_column_values() && request.ByteSizeLong() > MIN_HTTP_BRPC_SIZE) {
        Status st = request_embed_attachment_contain_block<
                PTabletWriterAddBlockRequest, ReusableClosure<PTabletWriterAddBlockResult>>(
                &request, _add_block_closure);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
            _add_block_closure->clear_in_flight();
            return;
        }
        std::string brpc_url = fmt::format("http://{}:{}", _node_info.host, _node_info.brpc_port);
        std::shared_ptr<PBackendService_Stub> _brpc_http_stub =
                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
                                                                                          "http");
        _add_block_closure->cntl.http_request().uri() =
                brpc_url + "/PInternalServiceImpl/tablet_writer_add_block_by_http";
        _add_block_closure->cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        _add_block_closure->cntl.http_request().set_content_type("application/json");
        _brpc_http_stub->tablet_writer_add_block_by_http(
                &_add_block_closure->cntl, NULL, &_add_block_closure->result, _add_block_closure);
    } else {
        _add_block_closure->cntl.http_request().Clear();
        _stub->tablet_writer_add_block(&_add_block_closure->cntl, &request,
                                       &_add_block_closure->result, _add_block_closure);
    }

    _next_packet_seq++;
}

void VNodeChannel::_close_check() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    CHECK(_pending_blocks.empty()) << name();
    CHECK(_cur_mutable_block == nullptr) << name();
}

void VNodeChannel::mark_close() {
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        return;
    }

    _cur_add_block_request.set_eos(true);
    {
        debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        _pending_blocks.emplace(std::move(_cur_mutable_block), _cur_add_block_request);
        _pending_batches_num++;
        DCHECK(_pending_blocks.back().second.eos());
        _close_time_ms = UnixMillis();
        LOG(INFO) << channel_info()
                  << " mark closed, left pending batch size: " << _pending_blocks.size();
    }

    _eos_is_produced = true;
}

VOlapTableSink::VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& texprs, Status* status)
        : OlapTableSink(pool, row_desc, texprs, status) {
    _is_vectorized = true;
    // From the thrift expressions create the real exprs.
    vectorized::VExpr::create_expr_trees(pool, texprs, &_output_vexpr_ctxs);
    _name = "VOlapTableSink";
}

VOlapTableSink::~VOlapTableSink() {
    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    for (const auto& index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<NodeChannel>& ch) { ch->clear_all_blocks(); });
    }
}

Status VOlapTableSink::init(const TDataSink& sink) {
    RETURN_IF_ERROR(OlapTableSink::init(sink));
    _vpartition = _pool->add(new VOlapTablePartitionParam(_schema, sink.olap_table_sink.partition));
    return _vpartition->init();
}

Status VOlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OlapTableSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc,
                                               _expr_mem_tracker));
    return Status::OK();
}

Status VOlapTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapTableSink::open");
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return OlapTableSink::open(state);
}

size_t VOlapTableSink::get_pending_bytes() const {
    size_t mem_consumption = 0;
    for (auto& indexChannel : _channels) {
        mem_consumption += indexChannel->get_pending_bytes();
    }
    return mem_consumption;
}
Status VOlapTableSink::send(RuntimeState* state, vectorized::Block* input_block) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VOlapTableSink::send");
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
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

    vectorized::Block block(input_block->get_columns_with_type_and_name());
    if (!_output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        block = vectorized::VExprContext::get_output_block_after_execute_exprs(
                _output_vexpr_ctxs, *input_block, status);
        if (UNLIKELY(block.rows() == 0)) {
            return status;
        }
    }

    auto num_rows = block.rows();
    int filtered_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(block.rows());
        bool stop_processing = false;
        RETURN_IF_ERROR(
                _validate_data(state, &block, &_filter_bitmap, &filtered_rows, &stop_processing));
        _number_filtered_rows += filtered_rows;
        if (stop_processing) {
            // should be returned after updating "_number_filtered_rows", to make sure that load job can be cancelled
            // because of "data unqualified"
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
        _convert_to_dest_desc_block(&block);
    }

    BlockRow block_row;
    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    if (findTabletMode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
        _partition_to_tablet_map.clear();
    }

    //if pending bytes is more than 500M, wait
    //constexpr size_t MAX_PENDING_BYTES = 500 * 1024 * 1024;
    //while (get_pending_bytes() > MAX_PENDING_BYTES) {
    //    std::this_thread::sleep_for(std::chrono::microseconds(500));
    //}

    for (int i = 0; i < num_rows; ++i) {
        if (filtered_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        uint32_t tablet_index = 0;
        block_row = {&block, i};
        if (!_vpartition->find_partition(&block_row, &partition)) {
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple=[]");
                        return fmt::to_string(buf);
                    },
                    &stop_processing));
            _number_filtered_rows++;
            if (stop_processing) {
                return Status::EndOfFile("Encountered unqualified data, stop processing");
            }
            continue;
        }
        _partition_ids.emplace(partition->id);
        if (findTabletMode != FindTabletMode::FIND_TABLET_EVERY_ROW) {
            if (_partition_to_tablet_map.find(partition->id) == _partition_to_tablet_map.end()) {
                tablet_index = _vpartition->find_tablet(&block_row, *partition);
                _partition_to_tablet_map.emplace(partition->id, tablet_index);
            } else {
                tablet_index = _partition_to_tablet_map[partition->id];
            }
        } else {
            tablet_index = _vpartition->find_tablet(&block_row, *partition);
        }
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            _channels[j]->add_row(block_row, tablet_id);
            _number_output_rows++;
        }
    }

    // check intolerable failure
    for (const auto& index_channel : _channels) {
        RETURN_IF_ERROR(index_channel->check_intolerable_failure());
    }
    return Status::OK();
}

Status VOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) return _close_status;
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapTableSink::close");
    vectorized::VExpr::close(_output_vexpr_ctxs, state);
    return OlapTableSink::close(state, exec_status);
}

Status VOlapTableSink::_validate_data(RuntimeState* state, vectorized::Block* block,
                                      Bitmap* filter_bitmap, int* filtered_rows,
                                      bool* stop_processing) {
    const auto num_rows = block->rows();
    fmt::memory_buffer error_msg;
    auto set_invalid_and_append_error_msg = [&](int row) {
        filter_bitmap->Set(row, true);
        return state->append_error_msg_to_file(
                []() -> std::string { return ""; },
                [&error_msg]() -> std::string { return fmt::to_string(error_msg); },
                stop_processing);
    };

    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
        auto& real_column_ptr =
                column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());

        switch (desc->type().type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            const auto column_string =
                    assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());

            size_t limit = std::min(config::string_type_length_soft_limit_bytes, desc->type().len);
            for (int j = 0; j < num_rows; ++j) {
                if (!filter_bitmap->Get(j)) {
                    auto str_val = column_string->get_data_at(j);
                    bool invalid = str_val.size > limit;
                    if (invalid) {
                        error_msg.clear();
                        if (str_val.size > desc->type().len) {
                            fmt::format_to(error_msg, "{}",
                                           "the length of input is too long than schema. ");
                            fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                            fmt::format_to(error_msg, "input str: [{}] ", str_val.to_prefix(10));
                            fmt::format_to(error_msg, "schema length: {}; ", desc->type().len);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        } else if (str_val.size > limit) {
                            fmt::format_to(
                                    error_msg, "{}",
                                    "the length of input string is too long than vec schema. ");
                            fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                            fmt::format_to(error_msg, "input str: [{}] ", str_val.to_prefix(10));
                            fmt::format_to(error_msg, "schema length: {}; ", desc->type().len);
                            fmt::format_to(error_msg, "limit length: {}; ", limit);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        }
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

            for (int j = 0; j < num_rows; ++j) {
                if (!filter_bitmap->Get(j)) {
                    auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                            column_decimal->get_data()[j]);
                    error_msg.clear();
                    bool invalid = false;

                    if (dec_val.greater_than_scale(desc->type().scale)) {
                        auto code = dec_val.round(&dec_val, desc->type().scale, HALF_UP);
                        column_decimal->get_data()[j] =
                                binary_cast<DecimalV2Value, vectorized::Int128>(dec_val);

                        if (code != E_DEC_OK) {
                            fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                           dec_val.to_string());
                            invalid = true;
                        }
                    }
                    if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                        fmt::format_to(error_msg,
                                       "decimal value is not valid for definition, column={}",
                                       desc->col_name());
                        fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                        fmt::format_to(error_msg, ", precision={}, scale={}; ",
                                       desc->type().precision, desc->type().scale);
                        invalid = true;
                    }

                    if (invalid) {
                        RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                    }
                }
            }
            break;
        }
        default:
            break;
        }

        // Dispose the column should do not contain the NULL value
        // Only tow case:
        // 1. column is nullable but the desc is not nullable
        // 2. desc->type is BITMAP
        if ((!desc->is_nullable() || desc->type() == TYPE_OBJECT) && column_ptr) {
            const auto& null_map = column_ptr->get_null_map_data();
            for (int j = 0; j < null_map.size(); ++j) {
                if (null_map[j] && !filter_bitmap->Get(j)) {
                    error_msg.clear();
                    fmt::format_to(error_msg, "null value for not null column, column={}; ",
                                   desc->col_name());
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                }
            }
        }
    }

    *filtered_rows = 0;
    for (int i = 0; i < num_rows; ++i) {
        *filtered_rows += filter_bitmap->Get(i);
    }
    return Status::OK();
}

void VOlapTableSink::_convert_to_dest_desc_block(doris::vectorized::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
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
