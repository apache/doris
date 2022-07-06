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

#include <fmt/format.h>

#include <sstream>
#include <string>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/hll.h"
#include "olap/olap_define.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/defer_op.h"
#include "util/monotime.h"
#include "util/proto_util.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
namespace stream_load {

NodeChannel::NodeChannel(OlapTableSink* parent, IndexChannel* index_channel, int64_t node_id)
        : _parent(parent), _index_channel(index_channel), _node_id(node_id) {
}

NodeChannel::~NodeChannel() noexcept {
    if (_open_closure != nullptr) {
        if (_open_closure->unref()) {
            delete _open_closure;
        }
        _open_closure = nullptr;
    }
    if (_add_batch_closure != nullptr) {
        // it's safe to delete, but may take some time to wait until brpc joined
        delete _add_batch_closure;
        _add_batch_closure = nullptr;
    }
    _cur_add_batch_request.release_id();
}

// if "_cancelled" is set to true,
// no need to set _cancel_msg because the error will be
// returned directly via "TabletSink::prepare()" method.
Status NodeChannel::init(RuntimeState* state) {
    _tuple_desc = _parent->_output_tuple_desc;
    _state = state;
    auto node = _parent->_nodes_info->find_node(_node_id);
    if (node == nullptr) {
        std::stringstream ss;
        ss << "unknown node id, id=" << _node_id;
        _cancelled = true;
        return Status::InternalError(ss.str());
    }

    _node_info = *node;

    _row_desc.reset(new RowDescriptor(_tuple_desc, false));
    _batch_size = state->batch_size();
    _cur_batch.reset(new RowBatch(*_row_desc, _batch_size, _parent->_mem_tracker.get()));

    _stub = state->exec_env()->brpc_internal_client_cache()->get_client(_node_info.host,
                                                                        _node_info.brpc_port);
    if (_stub == nullptr) {
        LOG(WARNING) << "Get rpc stub failed, host=" << _node_info.host
                     << ", port=" << _node_info.brpc_port;
        _cancelled = true;
        return Status::InternalError("get rpc stub failed");
    }

    // Initialize _cur_add_batch_request
    _cur_add_batch_request.set_allocated_id(&_parent->_load_id);
    _cur_add_batch_request.set_index_id(_index_channel->_index_id);
    _cur_add_batch_request.set_sender_id(_parent->_sender_id);
    _cur_add_batch_request.set_backend_id(_node_id);
    _cur_add_batch_request.set_eos(false);

    _rpc_timeout_ms = state->query_options().query_timeout * 1000;
    _timeout_watch.start();
    _max_pending_batches_bytes = _parent->_load_mem_limit / 20; //TODO: session variable percent

    _load_info = "load_id=" + print_id(_parent->_load_id) +
                 ", txn_id=" + std::to_string(_parent->_txn_id);
    _name = fmt::format("NodeChannel[{}-{}]", _index_channel->_index_id, _node_id);
    return Status::OK();
}

void NodeChannel::open() {
    PTabletWriterOpenRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_channel->_index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    for (auto& tablet : _all_tablets) {
        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(tablet.partition_id);
        ptablet->set_tablet_id(tablet.tablet_id);
    }
    request.set_num_senders(_parent->_num_senders);
    request.set_need_gen_rollup(false); // Useless but it is a required field in pb
    request.set_load_mem_limit(_parent->_load_mem_limit);
    request.set_load_channel_timeout_s(_parent->_load_channel_timeout_s);
    request.set_is_high_priority(_parent->_is_high_priority);
    request.set_sender_ip(BackendOptions::get_localhost());

    _open_closure = new RefCountClosure<PTabletWriterOpenResult>();
    _open_closure->ref();

    // This ref is for RPC's reference
    _open_closure->ref();
    _open_closure->cntl.set_timeout_ms(config::tablet_writer_open_rpc_timeout_sec * 1000);
    if (config::tablet_writer_ignore_eovercrowded) {
        _open_closure->cntl.ignore_eovercrowded();
    }
    _stub->tablet_writer_open(&_open_closure->cntl, &request, &_open_closure->result,
                              _open_closure);
    request.release_id();
    request.release_schema();
}

void NodeChannel::_cancel_with_msg(const std::string& msg) {
    LOG(WARNING) << msg;
    {
        std::lock_guard<SpinLock> l(_cancel_msg_lock);
        if (_cancel_msg == "") {
            _cancel_msg = msg;
        }
    }
    _cancelled = true;
}

Status NodeChannel::open_wait() {
    _open_closure->join();
    if (_open_closure->cntl.Failed()) {
        if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                    _stub, _node_info.host, _node_info.brpc_port)) {
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    _open_closure->cntl.remote_side());
        }
        std::stringstream ss;
        ss << "failed to open tablet writer, error=" << berror(_open_closure->cntl.ErrorCode())
           << ", error_text=" << _open_closure->cntl.ErrorText();
        _cancelled = true;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    Status status(_open_closure->result.status());
    if (_open_closure->unref()) {
        delete _open_closure;
    }
    _open_closure = nullptr;

    if (!status.ok()) {
        _cancelled = true;
        return status;
    }

    // add batch closure
    _add_batch_closure = ReusableClosure<PTabletWriterAddBatchResult>::create();
    _add_batch_closure->addFailedHandler([this](bool is_last_rpc) {
        std::lock_guard<std::mutex> l(this->_closed_lock);
        if (this->_is_closed) {
            // if the node channel is closed, no need to call `mark_as_failed`,
            // and notice that _index_channel may already be destroyed.
            return;
        }
        // If rpc failed, mark all tablets on this node channel as failed
        _index_channel->mark_as_failed(this->node_id(), this->host(),
                                       _add_batch_closure->cntl.ErrorText(), -1);
        Status st = _index_channel->check_intolerable_failure();
        if (!st.ok()) {
            _cancel_with_msg(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
        } else if (is_last_rpc) {
            // if this is last rpc, will must set _add_batches_finished. otherwise, node channel's close_wait
            // will be blocked.
            _add_batches_finished = true;
        }
    });

    _add_batch_closure->addSuccessHandler([this](const PTabletWriterAddBatchResult& result,
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

Status NodeChannel::add_row(Tuple* input_tuple, int64_t tablet_id) {
    // If add_row() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("add row failed. " + _cancel_msg);
        } else {
            return st.clone_and_prepend("already stopped, can't add row. cancelled/eos: ");
        }
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_row() and return OK, because we will check _cancelled in next add_row() or mark_close().
    while (!_cancelled && _pending_batches_bytes > _max_pending_batches_bytes) {
        SCOPED_ATOMIC_TIMER(&_mem_exceeded_block_ns);
        SleepFor(MonoDelta::FromMilliseconds(10));
    }

    auto row_no = _cur_batch->add_row();
    if (row_no == RowBatch::INVALID_ROW_INDEX) {
        {
            SCOPED_ATOMIC_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            _pending_batches_bytes += _cur_batch->tuple_data_pool()->total_reserved_bytes();
            //To simplify the add_row logic, postpone adding batch into req until the time of sending req
            _pending_batches.emplace(std::move(_cur_batch), _cur_add_batch_request);
            _pending_batches_num++;
        }

        _cur_batch.reset(new RowBatch(*_row_desc, _batch_size, _parent->_mem_tracker.get()));
        _cur_add_batch_request.clear_tablet_ids();

        row_no = _cur_batch->add_row();
    }
    DCHECK_NE(row_no, RowBatch::INVALID_ROW_INDEX);
    auto tuple = input_tuple->deep_copy(*_tuple_desc, _cur_batch->tuple_data_pool());

    _cur_batch->get_row(row_no)->set_tuple(0, tuple);
    _cur_batch->commit_last_row();
    _cur_add_batch_request.add_tablet_ids(tablet_id);
    return Status::OK();
}

// Used for vectorized engine.
// TODO(cmy): deprecated, need refactor
Status NodeChannel::add_row(BlockRow& block_row, int64_t tablet_id) {
    // If add_row() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("add row failed. " + _cancel_msg);
        } else {
            return st.clone_and_prepend("already stopped, can't add row. cancelled/eos: ");
        }
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_row() and return OK, because we will check _cancelled in next add_row() or mark_close().
    while (!_cancelled && _pending_batches_bytes > _max_pending_batches_bytes) {
        SCOPED_ATOMIC_TIMER(&_mem_exceeded_block_ns);
        SleepFor(MonoDelta::FromMilliseconds(10));
    }
    constexpr size_t BATCH_SIZE_FOR_SEND = 2 * 1024 * 1024; //2M
    auto row_no = _cur_batch->add_row();
    if (row_no == RowBatch::INVALID_ROW_INDEX ||
        _cur_batch->tuple_data_pool()->total_allocated_bytes() > BATCH_SIZE_FOR_SEND) {
        {
            SCOPED_ATOMIC_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            _pending_batches_bytes += _cur_batch->tuple_data_pool()->total_reserved_bytes();
            //To simplify the add_row logic, postpone adding batch into req until the time of sending req
            _pending_batches.emplace(std::move(_cur_batch), _cur_add_batch_request);
            _pending_batches_num++;
        }

        _cur_batch.reset(new RowBatch(*_row_desc, _batch_size, _parent->_mem_tracker.get()));
        _cur_add_batch_request.clear_tablet_ids();

        row_no = _cur_batch->add_row();
    }
    DCHECK_NE(row_no, RowBatch::INVALID_ROW_INDEX);

    _cur_batch->get_row(row_no)->set_tuple(
            0, block_row.first->deep_copy_tuple(*_tuple_desc, _cur_batch->tuple_data_pool(),
                                                block_row.second, 0, true));
    _cur_batch->commit_last_row();
    _cur_add_batch_request.add_tablet_ids(tablet_id);
    return Status::OK();
}

void NodeChannel::mark_close() {
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        return;
    }

    _cur_add_batch_request.set_eos(true);
    {
        debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        _pending_batches_bytes += _cur_batch->tuple_data_pool()->total_reserved_bytes();
        _pending_batches.emplace(std::move(_cur_batch), _cur_add_batch_request);
        _pending_batches_num++;
        DCHECK(_pending_batches.back().second.eos());
        _close_time_ms = UnixMillis();
        LOG(INFO) << channel_info()
                  << " mark closed, left pending batch size: " << _pending_batches.size();
    }

    _eos_is_produced = true;
    return;
}

Status NodeChannel::close_wait(RuntimeState* state) {
    // set _is_closed to true finally
    Defer set_closed {[&]() {
        std::lock_guard<std::mutex> l(_closed_lock);
        _is_closed = true;
    }};

    auto st = none_of({_cancelled, !_eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("wait close failed. " + _cancel_msg);
        } else {
            return st.clone_and_prepend(
                    "already stopped, skip waiting for close. cancelled/!eos: ");
        }
    }

    // waiting for finished, it may take a long time, so we couldn't set a timeout
    while (!_add_batches_finished && !_cancelled) {
        SleepFor(MonoDelta::FromMilliseconds(1));
    }
    _close_time_ms = UnixMillis() - _close_time_ms;

    if (_add_batches_finished) {
        {
            std::lock_guard<std::mutex> lg(_pending_batches_lock);
            CHECK(_pending_batches.empty()) << name();
            CHECK(_cur_batch == nullptr) << name();
        }
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(_tablet_commit_infos.begin()),
                                            std::make_move_iterator(_tablet_commit_infos.end()));

        _index_channel->set_error_tablet_in_state(state);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "close wait failed coz rpc error";
    {
        std::lock_guard<SpinLock> l(_cancel_msg_lock);
        if (_cancel_msg != "") {
            ss << ". " << _cancel_msg;
        }
    }
    return Status::InternalError(ss.str());
}

void NodeChannel::cancel(const std::string& cancel_msg) {
    // set _is_closed to true finally
    Defer set_closed {[&]() {
        std::lock_guard<std::mutex> l(_closed_lock);
        _is_closed = true;
    }};
    // we don't need to wait last rpc finished, cause closure's release/reset will join.
    // But do we need brpc::StartCancel(call_id)?
    _cancel_with_msg(cancel_msg);

    PTabletWriterCancelRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_channel->_index_id);
    request.set_sender_id(_parent->_sender_id);

    auto closure = new RefCountClosure<PTabletWriterCancelResult>();

    closure->ref();
    int remain_ms = _rpc_timeout_ms - _timeout_watch.elapsed_time() / NANOS_PER_MILLIS;
    if (UNLIKELY(remain_ms < config::min_load_rpc_timeout_ms)) {
        remain_ms = config::min_load_rpc_timeout_ms;
    }
    closure->cntl.set_timeout_ms(remain_ms);
    if (config::tablet_writer_ignore_eovercrowded) {
        closure->cntl.ignore_eovercrowded();
    }
    _stub->tablet_writer_cancel(&closure->cntl, &request, &closure->result, closure);
    request.release_id();
}

int NodeChannel::try_send_and_fetch_status(std::unique_ptr<ThreadPoolToken>& thread_pool_token) {
    auto st = none_of({_cancelled, _send_finished});
    if (!st.ok()) {
        return 0;
    }

    if (!_add_batch_closure->try_set_in_flight()) {
        return _send_finished ? 0 : 1;
    }

    // We are sure that try_send_batch is not running
    if (_pending_batches_num > 0) {
        auto s = thread_pool_token->submit_func(
                std::bind(&NodeChannel::try_send_batch, this));
        if (!s.ok()) {
            _cancel_with_msg("submit send_batch task to send_batch_thread_pool failed");
            // clear in flight
            _add_batch_closure->clear_in_flight();
        }
        // in_flight is cleared in closure::Run
    } else {
        // clear in flight
        _add_batch_closure->clear_in_flight();
    }
    return _send_finished ? 0 : 1;
}

void NodeChannel::try_send_batch() {
    SCOPED_ATOMIC_TIMER(&_actual_consume_ns);
    AddBatchReq send_batch;
    {
        debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        DCHECK(!_pending_batches.empty());
        send_batch = std::move(_pending_batches.front());
        _pending_batches.pop();
        _pending_batches_num--;
        _pending_batches_bytes -= send_batch.first->tuple_data_pool()->total_reserved_bytes();
    }

    auto row_batch = std::move(send_batch.first);
    auto request = std::move(send_batch.second); // doesn't need to be saved in heap

    // tablet_ids has already set when add row
    request.set_packet_seq(_next_packet_seq);
    if (row_batch->num_rows() > 0) {
        SCOPED_ATOMIC_TIMER(&_serialize_batch_ns);
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        Status st = row_batch->serialize(request.mutable_row_batch(), &uncompressed_bytes,
                                         &compressed_bytes, _parent->_transfer_large_data_by_brpc);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
            _add_batch_closure->clear_in_flight();
            return;
        }
        if (compressed_bytes >= double(config::brpc_max_body_size) * 0.95f) {
            LOG(WARNING) << "send batch too large, this rpc may failed. send size: "
                         << compressed_bytes << ", threshold: " << config::brpc_max_body_size
                         << ", " << channel_info();
        }
    }

    int remain_ms = _rpc_timeout_ms - _timeout_watch.elapsed_time() / NANOS_PER_MILLIS;
    if (UNLIKELY(remain_ms < config::min_load_rpc_timeout_ms)) {
        if (remain_ms <= 0 && !request.eos()) {
            cancel(fmt::format("{}, err: timeout", channel_info()));
            _add_batch_closure->clear_in_flight();
            return;
        } else {
            remain_ms = config::min_load_rpc_timeout_ms;
        }
    }

    // After calling reset(), make sure that the rpc will be called finally.
    // Otherwise, when calling _add_batch_closure->join(), it will be blocked forever.
    // and _add_batch_closure->join() will be called in ~NodeChannel().
    _add_batch_closure->reset();
    _add_batch_closure->cntl.set_timeout_ms(remain_ms);
    if (config::tablet_writer_ignore_eovercrowded) {
        _add_batch_closure->cntl.ignore_eovercrowded();
    }

    if (request.eos()) {
        for (auto pid : _parent->_partition_ids) {
            request.add_partition_ids(pid);
        }

        // eos request must be the last request
        _add_batch_closure->end_mark();
        _send_finished = true;
        CHECK(_pending_batches_num == 0) << _pending_batches_num;
    }

    if (_parent->_transfer_large_data_by_brpc && request.has_row_batch() &&
        request.row_batch().has_tuple_data() && request.ByteSizeLong() > MIN_HTTP_BRPC_SIZE) {
        Status st = request_embed_attachment_contain_tuple<
                PTabletWriterAddBatchRequest, ReusableClosure<PTabletWriterAddBatchResult>>(
                &request, _add_batch_closure);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.get_error_msg()));
            _add_batch_closure->clear_in_flight();
            return;
        }
        std::string brpc_url = fmt::format("http://{}:{}", _node_info.host, _node_info.brpc_port);
        std::shared_ptr<PBackendService_Stub> _brpc_http_stub =
                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
                                                                                          "http");
        _add_batch_closure->cntl.http_request().uri() =
                brpc_url + "/PInternalServiceImpl/tablet_writer_add_batch_by_http";
        _add_batch_closure->cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        _add_batch_closure->cntl.http_request().set_content_type("application/json");
        _brpc_http_stub->tablet_writer_add_batch_by_http(
                &_add_batch_closure->cntl, NULL, &_add_batch_closure->result, _add_batch_closure);
    } else {
        _add_batch_closure->cntl.http_request().Clear();
        _stub->tablet_writer_add_batch(&_add_batch_closure->cntl, &request,
                                       &_add_batch_closure->result, _add_batch_closure);
    }
    _next_packet_seq++;
}

Status NodeChannel::none_of(std::initializer_list<bool> vars) {
    bool none = std::none_of(vars.begin(), vars.end(), [](bool var) { return var; });
    Status st = Status::OK();
    if (!none) {
        std::string vars_str;
        std::for_each(vars.begin(), vars.end(),
                      [&vars_str](bool var) -> void { vars_str += (var ? "1/" : "0/"); });
        if (!vars_str.empty()) {
            vars_str.pop_back(); // 0/1/0/ -> 0/1/0
        }
        st = Status::InternalError(vars_str);
    }

    return st;
}

void NodeChannel::clear_all_batches() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    std::queue<AddBatchReq> empty;
    std::swap(_pending_batches, empty);
    _cur_batch.reset();
}

IndexChannel::~IndexChannel() {}

Status IndexChannel::init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets) {
    for (auto& tablet : tablets) {
        auto location = _parent->_location->find_tablet(tablet.tablet_id);
        if (location == nullptr) {
            LOG(WARNING) << "unknown tablet, tablet_id=" << tablet.tablet_id;
            return Status::InternalError("unknown tablet");
        }
        std::vector<std::shared_ptr<NodeChannel>> channels;
        for (auto& node_id : location->node_ids) {
            std::shared_ptr<NodeChannel> channel;
            auto it = _node_channels.find(node_id);
            if (it == _node_channels.end()) {
                // NodeChannel is not added to the _parent->_pool.
                // Because the deconstruction of NodeChannel may take a long time to wait rpc finish.
                // but the ObjectPool will hold a spin lock to delete objects.
                channel = std::make_shared<NodeChannel>(_parent, this, node_id);
                _node_channels.emplace(node_id, channel);
            } else {
                channel = it->second;
            }
            channel->add_tablet(tablet);
            channels.push_back(channel);
            _tablets_by_channel[node_id].insert(tablet.tablet_id);
        }
        _channels_by_tablet.emplace(tablet.tablet_id, std::move(channels));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
    }
    return Status::OK();
}

void IndexChannel::add_row(Tuple* tuple, int64_t tablet_id) {
    auto it = _channels_by_tablet.find(tablet_id);
    DCHECK(it != _channels_by_tablet.end()) << "unknown tablet, tablet_id=" << tablet_id;
    for (auto channel : it->second) {
        // if this node channel is already failed, this add_row will be skipped
        auto st = channel->add_row(tuple, tablet_id);
        if (!st.ok()) {
            mark_as_failed(channel->node_id(), channel->host(), st.get_error_msg(), tablet_id);
            // continue add row to other node, the error will be checked for every batch outside
        }
    }
}

// Used for vectorized engine.
// TODO(cmy): deprecated, need refactor
void IndexChannel::add_row(BlockRow& block_row, int64_t tablet_id) {
    auto it = _channels_by_tablet.find(tablet_id);
    DCHECK(it != _channels_by_tablet.end()) << "unknown tablet, tablet_id=" << tablet_id;
    for (auto channel : it->second) {
        // if this node channel is already failed, this add_row will be skipped
        auto st = channel->add_row(block_row, tablet_id);
        if (!st.ok()) {
            mark_as_failed(channel->node_id(), channel->host(), st.get_error_msg(), tablet_id);
        }
    }
}

void IndexChannel::mark_as_failed(int64_t node_id, const std::string& host, const std::string& err,
                                  int64_t tablet_id) {
    const auto& it = _tablets_by_channel.find(node_id);
    if (it == _tablets_by_channel.end()) {
        return;
    }

    {
        std::lock_guard<SpinLock> l(_fail_lock);
        if (tablet_id == -1) {
            for (const auto the_tablet_id : it->second) {
                _failed_channels[the_tablet_id].insert(node_id);
                _failed_channels_msgs.emplace(the_tablet_id, err + ", host: " + host);
                if (_failed_channels[the_tablet_id].size() >= ((_parent->_num_replicas + 1) / 2)) {
                    _intolerable_failure_status =
                            Status::InternalError(_failed_channels_msgs[the_tablet_id]);
                }
            }
        } else {
            _failed_channels[tablet_id].insert(node_id);
            _failed_channels_msgs.emplace(tablet_id, err + ", host: " + host);
            if (_failed_channels[tablet_id].size() >= ((_parent->_num_replicas + 1) / 2)) {
                _intolerable_failure_status =
                        Status::InternalError(_failed_channels_msgs[tablet_id]);
            }
        }
    }
}

Status IndexChannel::check_intolerable_failure() {
    std::lock_guard<SpinLock> l(_fail_lock);
    return _intolerable_failure_status;
}

void IndexChannel::set_error_tablet_in_state(RuntimeState* state) {
    std::vector<TErrorTabletInfo>& error_tablet_infos = state->error_tablet_infos();

    std::lock_guard<SpinLock> l(_fail_lock);
    for (const auto& it : _failed_channels_msgs) {
        TErrorTabletInfo error_info;
        error_info.__set_tabletId(it.first);
        error_info.__set_msg(it.second);
        error_tablet_infos.emplace_back(error_info);
    }
}

OlapTableSink::OlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& texprs, Status* status)
        : _pool(pool),
          _input_row_desc(row_desc),
          _filter_bitmap(1024),
          _stop_background_threads_latch(1) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs);
    }
    _name = "OlapTableSink";
    _transfer_large_data_by_brpc = config::transfer_large_data_by_brpc;
}

OlapTableSink::~OlapTableSink() {
    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<NodeChannel>& ch) { ch->clear_all_batches(); });
    }
}

Status OlapTableSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _num_replicas = table_sink.num_replicas;
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
    return Status::OK();
}

Status OlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();
    _is_high_priority = (state->query_options().query_timeout <=
                         config::load_task_high_priority_threshold_second);

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));
    _mem_tracker =
            MemTracker::CreateTracker(-1, "OlapTableSink:" + std::to_string(state->load_job_id()),
                                      state->instance_mem_tracker(), true, false);

    SCOPED_TIMER(_profile->total_time_counter());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _input_row_desc, _expr_mem_tracker));

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
    _output_batch.reset(new RowBatch(*_output_row_desc, state->batch_size(), _mem_tracker.get()));

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
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
        case TYPE_OBJECT:
        case TYPE_STRING:
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
    _wait_mem_limit_timer = ADD_CHILD_TIMER(_profile, "WaitMemLimitTime", "SendDataTime");
    _convert_batch_timer = ADD_TIMER(_profile, "ConvertBatchTime");
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
    _add_batch_number = ADD_COUNTER(_profile, "NumberBatchAdded", TUnit::UNIT);
    _num_node_channels = ADD_COUNTER(_profile, "NumberNodeChannels", TUnit::UNIT);
    _load_mem_limit = state->get_load_mem_limit();

    // open all channels
    const auto& partitions = _partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<TTabletWithPartition> tablets;
        auto index = _schema->indexes()[i];
        for (const auto& part : partitions) {
            for (const auto& tablet : part->indexes[i].tablets) {
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
        }
        auto channel = std::make_shared<IndexChannel>(this, index->index_id);
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

    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<NodeChannel>& ch) { ch->open(); });
    }

    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel](
                                                     const std::shared_ptr<NodeChannel>& ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                // The open() phase is mainly to generate DeltaWriter instances on the nodes corresponding to each node channel.
                // This phase will not fail due to a single tablet.
                // Therefore, if the open() phase fails, all tablets corresponding to the node need to be marked as failed.
                index_channel->mark_as_failed(ch->node_id(), ch->host(),
                                              fmt::format("{}, open failed, err: {}",
                                                          ch->channel_info(), st.get_error_msg()),
                                              -1);
            }
        });

        RETURN_IF_ERROR(index_channel->check_intolerable_failure());
    }
    int32_t send_batch_parallelism =
            MIN(_send_batch_parallelism, config::max_send_batch_parallelism_per_job);
    _send_batch_thread_pool_token = state->exec_env()->send_batch_thread_pool()->new_token(
            ThreadPool::ExecutionMode::CONCURRENT, send_batch_parallelism);
    RETURN_IF_ERROR(Thread::create(
            "OlapTableSink", "send_batch_process", [this]() { this->_send_batch_process(); },
            &_sender_thread));

    return Status::OK();
}

Status OlapTableSink::send(RuntimeState* state, RowBatch* input_batch) {
    SCOPED_TIMER(_profile->total_time_counter());
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    int64_t num_rows = input_batch->num_rows();
    int64_t num_bytes = input_batch->total_byte_size();
    _number_input_rows += num_rows;
    state->update_num_rows_load_total(num_rows);
    state->update_num_bytes_load_total(num_bytes);
    DorisMetrics::instance()->load_rows->increment(num_rows);
    DorisMetrics::instance()->load_bytes->increment(num_bytes);
    RowBatch* batch = input_batch;
    if (!_output_expr_ctxs.empty()) {
        SCOPED_RAW_TIMER(&_convert_batch_ns);
        _output_batch->reset();
        RETURN_IF_ERROR(_convert_batch(state, input_batch, _output_batch.get()));
        batch = _output_batch.get();
    }

    int filtered_rows = 0;
    if (_need_validate_data) {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(batch->num_rows());
        bool stop_processing = false;
        RETURN_IF_ERROR(
                _validate_data(state, batch, &_filter_bitmap, &filtered_rows, &stop_processing));
        _number_filtered_rows += filtered_rows;
        if (stop_processing) {
            // should be returned after updating "_number_filtered_rows", to make sure that load job can be cancelled
            // because of "data unqualified"
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
    }

    SCOPED_RAW_TIMER(&_send_data_ns);
    bool stop_processing = false;
    if (findTabletMode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
        _partition_to_tablet_map.clear();
    }
    for (int i = 0; i < batch->num_rows(); ++i) {
        Tuple* tuple = batch->get_row(i)->get_tuple(0);
        if (filtered_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const OlapTablePartition* partition = nullptr;
        if (!_partition->find_partition(tuple, &partition)) {
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple={}",
                                       Tuple::to_string(tuple, *_output_tuple_desc));
                        return fmt::to_string(buf);
                    },
                    &stop_processing));
            _number_filtered_rows++;
            if (stop_processing) {
                return Status::EndOfFile("Encountered unqualified data, stop processing");
            }
            continue;
        }
        uint32_t tablet_index = 0;
        if (findTabletMode != FindTabletMode::FIND_TABLET_EVERY_ROW) {
            if (_partition_to_tablet_map.find(partition->id) == _partition_to_tablet_map.end()) {
                tablet_index = _partition->find_tablet(tuple, *partition);
                _partition_to_tablet_map.emplace(partition->id, tablet_index);
            } else {
                tablet_index = _partition_to_tablet_map[partition->id];
            }
        } else {
            tablet_index = _partition->find_tablet(tuple, *partition);
        }
        _partition_ids.emplace(partition->id);
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            _channels[j]->add_row(tuple, tablet_id);
            _number_output_rows++;
        }
    }

    // check intolerable failure
    for (auto index_channel : _channels) {
        RETURN_IF_ERROR(index_channel->check_intolerable_failure());
    }
    return Status::OK();
}

Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    if (_closed) {
        /// The close method may be called twice.
        /// In the open_internal() method of plan_fragment_executor, close is called once.
        /// If an error occurs in this call, it will be called again in fragment_mgr.
        /// So here we use a flag to prevent repeated close operations.
        return _close_status;
    }
    Status status = close_status;
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, mem_exceeded_block_ns = 0, queue_push_lock_ns = 0,
                actual_consume_ns = 0, total_add_batch_exec_time_ns = 0,
                max_add_batch_exec_time_ns = 0, total_add_batch_num = 0, num_node_channels = 0;
        {
            SCOPED_TIMER(_close_timer);
            for (auto index_channel : _channels) {
                index_channel->for_each_node_channel(
                        [](const std::shared_ptr<NodeChannel>& ch) { ch->mark_close(); });
                num_node_channels += index_channel->num_node_channels();
            }

            for (auto index_channel : _channels) {
                int64_t add_batch_exec_time = 0;
                index_channel->for_each_node_channel(
                        [&index_channel, &state, &node_add_batch_counter_map, &serialize_batch_ns,
                         &mem_exceeded_block_ns, &queue_push_lock_ns, &actual_consume_ns,
                         &total_add_batch_exec_time_ns, &add_batch_exec_time,
                         &total_add_batch_num](const std::shared_ptr<NodeChannel>& ch) {
                            auto s = ch->close_wait(state);
                            if (!s.ok()) {
                                index_channel->mark_as_failed(ch->node_id(), ch->host(),
                                                              s.get_error_msg(), -1);
                                LOG(WARNING)
                                        << ch->channel_info()
                                        << ", close channel failed, err: " << s.get_error_msg();
                            }
                            ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns,
                                            &mem_exceeded_block_ns, &queue_push_lock_ns,
                                            &actual_consume_ns, &total_add_batch_exec_time_ns,
                                            &add_batch_exec_time, &total_add_batch_num);
                        });

                if (add_batch_exec_time > max_add_batch_exec_time_ns) {
                    max_add_batch_exec_time_ns = add_batch_exec_time;
                }

                // check if index has intolerable failure
                Status index_st = index_channel->check_intolerable_failure();
                if (!index_st.ok()) {
                    status = index_st;
                }
            } // end for index channels
        }
        // TODO need to be improved
        LOG(INFO) << "total mem_exceeded_block_ns=" << mem_exceeded_block_ns
                  << ", total queue_push_lock_ns=" << queue_push_lock_ns
                  << ", total actual_consume_ns=" << actual_consume_ns
                  << ", load id=" << print_id(_load_id);

        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_wait_mem_limit_timer, mem_exceeded_block_ns);
        COUNTER_SET(_convert_batch_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);
        COUNTER_SET(_serialize_batch_timer, serialize_batch_ns);
        COUNTER_SET(_non_blocking_send_work_timer, actual_consume_ns);
        COUNTER_SET(_total_add_batch_exec_timer, total_add_batch_exec_time_ns);
        COUNTER_SET(_max_add_batch_exec_timer, max_add_batch_exec_time_ns);
        COUNTER_SET(_add_batch_number, total_add_batch_num);
        COUNTER_SET(_num_node_channels, num_node_channels);
        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + state->num_rows_load_filtered() +
                                      state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_number_filtered_rows);

        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "finished to close olap table sink. load_id=" << print_id(_load_id)
           << ", txn_id=" << _txn_id
           << ", node add batch time(ms)/wait execution time(ms)/close time(ms)/num: ";
        for (auto const& pair : node_add_batch_counter_map) {
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000)
               << ")(" << (pair.second.add_batch_wait_execution_time_us / 1000) << ")("
               << pair.second.close_wait_time_ms << ")(" << pair.second.add_batch_num << ")} ";
        }
        LOG(INFO) << ss.str();
    } else {
        for (auto channel : _channels) {
            channel->for_each_node_channel([&status](const std::shared_ptr<NodeChannel>& ch) {
                ch->cancel(status.get_error_msg());
            });
        }
    }

    // Sender join() must put after node channels mark_close/cancel.
    // But there is no specific sequence required between sender join() & close_wait().
    _stop_background_threads_latch.count_down();
    if (_sender_thread) {
        _sender_thread->join();
        // We have to wait all task in _send_batch_thread_pool_token finished,
        // because it is difficult to handle concurrent problem if we just
        // shutdown it.
        _send_batch_thread_pool_token->wait();
    }

    Expr::close(_output_expr_ctxs, state);
    _output_batch.reset();

    _close_status = status;
    DataSink::close(state, close_status);
    return status;
}

Status OlapTableSink::_convert_batch(RuntimeState* state, RowBatch* input_batch,
                                     RowBatch* output_batch) {
    DCHECK_GE(output_batch->capacity(), input_batch->num_rows());
    int commit_rows = 0;
    bool stop_processing = false;
    for (int i = 0; i < input_batch->num_rows(); ++i) {
        auto src_row = input_batch->get_row(i);
        Tuple* dst_tuple =
                (Tuple*)output_batch->tuple_data_pool()->allocate(_output_tuple_desc->byte_size());
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
                    RETURN_IF_ERROR(state->append_error_msg_to_file(
                            [&]() -> std::string { return slot_desc->col_name(); },
                            [&]() -> std::string { return expr_error; }, &stop_processing));
                    _number_filtered_rows++;
                    ignore_this_row = true;
                    // The ctx is reused, so must clear the error state and message.
                    _output_expr_ctxs[j]->clear_error_msg();
                    break;
                }
                if (!slot_desc->is_nullable()) {
                    RETURN_IF_ERROR(state->append_error_msg_to_file(
                            []() -> std::string { return ""; },
                            [&]() -> std::string {
                                fmt::memory_buffer buf;
                                fmt::format_to(
                                        buf, "null value for not null column, column={}, type={}",
                                        slot_desc->col_name(), slot_desc->type().debug_string());
                                return fmt::to_string(buf);
                            },
                            &stop_processing));
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
        } // end for output expr

        if (!ignore_this_row) {
            output_batch->get_row(commit_rows)->set_tuple(0, dst_tuple);
            commit_rows++;
        }

        if (stop_processing) {
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
    }
    output_batch->commit_rows(commit_rows);
    return Status::OK();
}

Status OlapTableSink::_validate_data(RuntimeState* state, RowBatch* batch, Bitmap* filter_bitmap,
                                     int* filtered_rows, bool* stop_processing) {
    for (int row_no = 0; row_no < batch->num_rows(); ++row_no) {
        Tuple* tuple = batch->get_row(row_no)->get_tuple(0);
        bool row_valid = true;
        fmt::memory_buffer error_msg; // error message
        for (int i = 0; row_valid && i < _output_tuple_desc->slots().size(); ++i) {
            SlotDescriptor* desc = _output_tuple_desc->slots()[i];
            if (desc->is_nullable() && tuple->is_null(desc->null_indicator_offset())) {
                if (desc->type().type == TYPE_OBJECT) {
                    fmt::format_to(error_msg,
                                   "null is not allowed for bitmap column, column_name: {}; ",
                                   desc->col_name());
                    row_valid = false;
                }
                continue;
            }
            void* slot = tuple->get_slot(desc->tuple_offset());
            switch (desc->type().type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                // Fixed length string
                StringValue* str_val = (StringValue*)slot;
                if (str_val->len > desc->type().len) {
                    fmt::format_to(error_msg, "{}",
                                   "the length of input is too long than schema. ");
                    fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                    fmt::format_to(error_msg, "input str: [{}] ",
                                   std::string(str_val->ptr, str_val->len));
                    fmt::format_to(error_msg, "schema length: {}; ", desc->type().len);
                    fmt::format_to(error_msg, "actual length: {}; ", str_val->len);
                    row_valid = false;
                    continue;
                }
                // padding 0 to CHAR field
                if (desc->type().type == TYPE_CHAR && str_val->len < desc->type().len) {
                    auto new_ptr = (char*)batch->tuple_data_pool()->allocate(desc->type().len);
                    memcpy(new_ptr, str_val->ptr, str_val->len);
                    memset(new_ptr + str_val->len, 0, desc->type().len - str_val->len);

                    str_val->ptr = new_ptr;
                    str_val->len = desc->type().len;
                }
                break;
            }
            case TYPE_STRING: {
                StringValue* str_val = (StringValue*)slot;
                if (str_val->len > config::string_type_length_soft_limit_bytes) {
                    fmt::format_to(error_msg, "{}",
                                   "the length of input is too long than schema. ");
                    fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                    fmt::format_to(error_msg, "first 128 bytes of input str: [{}] ",
                                   std::string(str_val->ptr, 128));
                    fmt::format_to(error_msg, "schema length: {}; ",
                                   config::string_type_length_soft_limit_bytes);
                    fmt::format_to(error_msg, "actual length: {}; ", str_val->len);
                    row_valid = false;
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
                        fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                       dec_val.to_string());
                        row_valid = false;
                        continue;
                    }
                }
                if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                    fmt::format_to(error_msg,
                                   "decimal value is not valid for definition, column={}",
                                   desc->col_name());
                    fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                    fmt::format_to(error_msg, ", precision={}, scale={}; ", desc->type().precision,
                                   desc->type().scale);
                    row_valid = false;
                    continue;
                }
                break;
            }
            case TYPE_HLL: {
                Slice* hll_val = (Slice*)slot;
                if (!HyperLogLog::is_valid(*hll_val)) {
                    fmt::format_to(error_msg,
                                   "Content of HLL type column is invalid. column name: {}; ",
                                   desc->col_name());
                    row_valid = false;
                    continue;
                }
                break;
            }
            default:
                break;
            }
        }

        if (!row_valid) {
            (*filtered_rows)++;
            filter_bitmap->Set(row_no, true);
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string { return fmt::to_string(error_msg); }, stop_processing));
        }
    }
    return Status::OK();
}

void OlapTableSink::_send_batch_process() {
    SCOPED_TIMER(_non_blocking_send_timer);
    do {
        int running_channels_num = 0;
        for (auto index_channel : _channels) {
            index_channel->for_each_node_channel([&running_channels_num, this](const std::shared_ptr<NodeChannel>& ch) {
                running_channels_num +=
                        ch->try_send_and_fetch_status(this->_send_batch_thread_pool_token);
            });
        }

        if (running_channels_num == 0) {
            LOG(INFO) << "all node channels are stopped(maybe finished/offending/cancelled), "
                         "sender thread exit. "
                      << print_id(_load_id);
            return;
        }
    } while (!_stop_background_threads_latch.wait_for(
            MonoDelta::FromMilliseconds(config::olap_table_sink_send_interval_ms)));
}

} // namespace stream_load
} // namespace doris
