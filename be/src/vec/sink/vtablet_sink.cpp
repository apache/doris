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

#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include "exec/tablet_info.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/hll.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/proto_util.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace stream_load {

Status IndexChannel::init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets) {
    SCOPED_CONSUME_MEM_TRACKER(_index_channel_tracker.get());
    for (auto& tablet : tablets) {
        auto location = _parent->_location->find_tablet(tablet.tablet_id);
        if (location == nullptr) {
            LOG(WARNING) << "unknown tablet, tablet_id=" << tablet.tablet_id;
            return Status::InternalError("unknown tablet");
        }
        std::vector<std::shared_ptr<VNodeChannel>> channels;
        for (auto& node_id : location->node_ids) {
            std::shared_ptr<VNodeChannel> channel;
            auto it = _node_channels.find(node_id);
            if (it == _node_channels.end()) {
                // NodeChannel is not added to the _parent->_pool.
                // Because the deconstruction of NodeChannel may take a long time to wait rpc finish.
                // but the ObjectPool will hold a spin lock to delete objects.
                channel = std::make_shared<VNodeChannel>(_parent, this, node_id);
                _node_channels.emplace(node_id, channel);
            } else {
                channel = it->second;
            }
            channel->add_tablet(tablet);
            if (_parent->_write_single_replica) {
                auto slave_location = _parent->_slave_location->find_tablet(tablet.tablet_id);
                if (slave_location != nullptr) {
                    channel->add_slave_tablet_nodes(tablet.tablet_id, slave_location->node_ids);
                }
            }
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

void IndexChannel::mark_as_failed(int64_t node_id, const std::string& host, const std::string& err,
                                  int64_t tablet_id) {
    VLOG_PROGRESS << "mark node_id:" << node_id << " tablet_id: " << tablet_id
                  << " as failed, err: " << err;
    const auto& it = _tablets_by_channel.find(node_id);
    if (it == _tablets_by_channel.end()) {
        return;
    }

    {
        std::lock_guard<doris::SpinLock> l(_fail_lock);
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
    std::lock_guard<doris::SpinLock> l(_fail_lock);
    return _intolerable_failure_status;
}

void IndexChannel::set_error_tablet_in_state(RuntimeState* state) {
    std::vector<TErrorTabletInfo>& error_tablet_infos = state->error_tablet_infos();

    std::lock_guard<doris::SpinLock> l(_fail_lock);
    for (const auto& it : _failed_channels_msgs) {
        TErrorTabletInfo error_info;
        error_info.__set_tabletId(it.first);
        error_info.__set_msg(it.second);
        error_tablet_infos.emplace_back(error_info);
    }
}

void IndexChannel::set_tablets_received_rows(
        const std::vector<std::pair<int64_t, int64_t>>& tablets_received_rows, int64_t node_id) {
    for (const auto& [tablet_id, rows_num] : tablets_received_rows) {
        _tablets_received_rows[tablet_id].emplace_back(node_id, rows_num);
    }
}

Status IndexChannel::check_tablet_received_rows_consistency() {
    for (auto& tablet : _tablets_received_rows) {
        for (size_t i = 0; i < tablet.second.size(); i++) {
            VLOG_NOTICE << "check_tablet_received_rows_consistency, load_id: " << _parent->_load_id
                        << ", txn_id: " << std::to_string(_parent->_txn_id)
                        << ", tablet_id: " << tablet.first
                        << ", node_id: " << tablet.second[i].first
                        << ", rows_num: " << tablet.second[i].second;
            if (i == 0) {
                continue;
            }
            if (tablet.second[i].second != tablet.second[0].second) {
                LOG(WARNING) << "rows num doest't match, load_id: " << _parent->_load_id
                             << ", txn_id: " << std::to_string(_parent->_txn_id)
                             << ", tablt_id: " << tablet.first
                             << ", node_id: " << tablet.second[i].first
                             << ", rows_num: " << tablet.second[i].second
                             << ", node_id: " << tablet.second[0].first
                             << ", rows_num: " << tablet.second[0].second;
                return Status::InternalError("rows num written by multi replicas doest't match");
            }
        }
    }
    return Status::OK();
}

VNodeChannel::VNodeChannel(VOlapTableSink* parent, IndexChannel* index_channel, int64_t node_id)
        : _parent(parent), _index_channel(index_channel), _node_id(node_id) {
    _node_channel_tracker = std::make_shared<MemTracker>(fmt::format(
            "NodeChannel:indexID={}:threadId={}", std::to_string(_index_channel->_index_id),
            thread_context()->get_thread_id()));
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
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    _tuple_desc = _parent->_output_tuple_desc;
    _state = state;
    auto node = _parent->_nodes_info->find_node(_node_id);
    if (node == nullptr) {
        _cancelled = true;
        return Status::InternalError("unknown node id, id={}", _node_id);
    }

    _node_info = *node;

    _load_info = "load_id=" + print_id(_parent->_load_id) +
                 ", txn_id=" + std::to_string(_parent->_txn_id);

    _row_desc.reset(new RowDescriptor(_tuple_desc, false));
    _batch_size = state->batch_size();

    _stub = state->exec_env()->brpc_internal_client_cache()->get_client(_node_info.host,
                                                                        _node_info.brpc_port);
    if (_stub == nullptr) {
        LOG(WARNING) << "Get rpc stub failed, host=" << _node_info.host
                     << ", port=" << _node_info.brpc_port << ", " << channel_info();
        _cancelled = true;
        return Status::InternalError("get rpc stub failed");
    }

    _rpc_timeout_ms = state->query_options().query_timeout * 1000;
    _timeout_watch.start();

    _cur_mutable_block.reset(new vectorized::MutableBlock({_tuple_desc}));

    // Initialize _cur_add_block_request
    _cur_add_block_request.set_allocated_id(&_parent->_load_id);
    _cur_add_block_request.set_index_id(_index_channel->_index_id);
    _cur_add_block_request.set_sender_id(_parent->_sender_id);
    _cur_add_block_request.set_backend_id(_node_id);
    _cur_add_block_request.set_eos(false);

    _name = fmt::format("VNodeChannel[{}-{}]", _index_channel->_index_id, _node_id);
    // The node channel will send _batch_size rows of data each rpc. When the
    // number of tablets is large, the number of data rows received by each
    // tablet is small, TabletsChannel need to traverse each tablet for import.
    // so the import performance is poor. Therefore, we set _batch_size to
    // a relatively large value to improve the import performance.
    _batch_size = std::max(_batch_size, 8192);

    return Status::OK();
}

void VNodeChannel::open() {
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
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
    request.set_is_vectorized(true);

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

Status VNodeChannel::open_wait() {
    _open_closure->join();
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
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
        LOG(WARNING) << ss.str() << " " << channel_info();
        return Status::InternalError("failed to open tablet writer, error={}, error_text={}",
                                     berror(_open_closure->cntl.ErrorCode()),
                                     _open_closure->cntl.ErrorText());
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

    // add block closure
    _add_block_closure = ReusableClosure<PTabletWriterAddBlockResult>::create();
    _add_block_closure->addFailedHandler([this](bool is_last_rpc) {
        SCOPED_ATTACH_TASK(_state);
        std::lock_guard<std::mutex> l(this->_closed_lock);
        if (this->_is_closed) {
            // if the node channel is closed, no need to call `mark_as_failed`,
            // and notice that _index_channel may already be destroyed.
            return;
        }
        // If rpc failed, mark all tablets on this node channel as failed
        _index_channel->mark_as_failed(this->node_id(), this->host(),
                                       fmt::format("rpc failed, error coed:{}, error text:{}",
                                                   _add_block_closure->cntl.ErrorCode(),
                                                   _add_block_closure->cntl.ErrorText()),
                                       -1);
        Status st = _index_channel->check_intolerable_failure();
        if (!st.ok()) {
            _cancel_with_msg(fmt::format("{}, err: {}", channel_info(), st.to_string()));
        } else if (is_last_rpc) {
            // if this is last rpc, will must set _add_batches_finished. otherwise, node channel's close_wait
            // will be blocked.
            _add_batches_finished = true;
        }
    });

    _add_block_closure->addSuccessHandler([this](const PTabletWriterAddBlockResult& result,
                                                 bool is_last_rpc) {
        SCOPED_ATTACH_TASK(_state);
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
                _index_channel->mark_as_failed(this->node_id(), this->host(),
                                               "tablet error: " + error.msg(), error.tablet_id());
            }

            Status st = _index_channel->check_intolerable_failure();
            if (!st.ok()) {
                _cancel_with_msg(st.to_string());
            } else if (is_last_rpc) {
                for (auto& tablet : result.tablet_vec()) {
                    TTabletCommitInfo commit_info;
                    commit_info.tabletId = tablet.tablet_id();
                    commit_info.backendId = _node_id;
                    _tablet_commit_infos.emplace_back(std::move(commit_info));
                    if (tablet.has_received_rows()) {
                        _tablets_received_rows.emplace_back(tablet.tablet_id(),
                                                            tablet.received_rows());
                    }
                    VLOG_CRITICAL << "master replica commit info: tabletId=" << tablet.tablet_id()
                                  << ", backendId=" << _node_id
                                  << ", master node id: " << this->node_id()
                                  << ", host: " << this->host() << ", txn_id=" << _parent->_txn_id;
                }
                if (_parent->_write_single_replica) {
                    for (auto& tablet_slave_node_ids : result.success_slave_tablet_node_ids()) {
                        for (auto slave_node_id : tablet_slave_node_ids.second.slave_node_ids()) {
                            TTabletCommitInfo commit_info;
                            commit_info.tabletId = tablet_slave_node_ids.first;
                            commit_info.backendId = slave_node_id;
                            _tablet_commit_infos.emplace_back(std::move(commit_info));
                            VLOG_CRITICAL << "slave replica commit info: tabletId="
                                          << tablet_slave_node_ids.first
                                          << ", backendId=" << slave_node_id
                                          << ", master node id: " << this->node_id()
                                          << ", host: " << this->host()
                                          << ", txn_id=" << _parent->_txn_id;
                        }
                    }
                }
                _add_batches_finished = true;
            }
        } else {
            _cancel_with_msg(fmt::format("{}, add batch req success but status isn't ok, err: {}",
                                         channel_info(), status.to_string()));
        }

        if (result.has_execution_time_us()) {
            _add_batch_counter.add_batch_execution_time_us += result.execution_time_us();
            _add_batch_counter.add_batch_wait_execution_time_us += result.wait_execution_time_us();
            _add_batch_counter.add_batch_num++;
        }
    });
    return status;
}

Status VNodeChannel::add_block(vectorized::Block* block,
                               const std::pair<std::unique_ptr<vectorized::IColumn::Selector>,
                                               std::vector<int64_t>>& payload) {
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    // If add_block() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("add row failed. {}", _cancel_msg);
        } else {
            return std::move(st.prepend("already stopped, can't add row. cancelled/eos: "));
        }
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_block() and return OK, because we will check _cancelled in next add_block() or mark_close().
    while (!_cancelled && _pending_batches_num > 0 &&
           _pending_batches_bytes > _max_pending_batches_bytes) {
        SCOPED_ATOMIC_TIMER(&_mem_exceeded_block_ns);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    block->append_block_by_selector(_cur_mutable_block->mutable_columns(), *(payload.first));
    for (auto tablet_id : payload.second) {
        _cur_add_block_request.add_tablet_ids(tablet_id);
    }

    if (_cur_mutable_block->rows() >= _batch_size ||
        _cur_mutable_block->bytes() > config::doris_scanner_row_bytes) {
        {
            SCOPED_ATOMIC_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            // To simplify the add_row logic, postpone adding block into req until the time of sending req
            _pending_batches_bytes += _cur_mutable_block->allocated_bytes();
            _pending_blocks.emplace(std::move(_cur_mutable_block), _cur_add_block_request);
            _pending_batches_num++;
            VLOG_DEBUG << "VOlapTableSink:" << _parent << " VNodeChannel:" << this
                       << " pending_batches_bytes:" << _pending_batches_bytes
                       << " jobid:" << std::to_string(_state->load_job_id())
                       << " loadinfo:" << _load_info;
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

void VNodeChannel::_cancel_with_msg(const std::string& msg) {
    LOG(WARNING) << "cancel node channel " << channel_info() << ", error message: " << msg;
    {
        std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
        if (_cancel_msg == "") {
            _cancel_msg = msg;
        }
    }
    _cancelled = true;
}

Status VNodeChannel::none_of(std::initializer_list<bool> vars) {
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

void VNodeChannel::try_send_block(RuntimeState* state) {
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker);
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
        Status st = block.serialize(state->be_exec_version(), request.mutable_block(),
                                    &uncompressed_bytes, &compressed_bytes,
                                    state->fragement_transmission_compression_type(),
                                    _parent->_transfer_large_data_by_brpc);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.to_string()));
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

        request.set_write_single_replica(false);
        if (_parent->_write_single_replica) {
            request.set_write_single_replica(true);
            for (std::unordered_map<int64_t, std::vector<int64_t>>::iterator iter =
                         _slave_tablet_nodes.begin();
                 iter != _slave_tablet_nodes.end(); iter++) {
                PSlaveTabletNodes slave_tablet_nodes;
                for (auto node_id : iter->second) {
                    auto node = _parent->_nodes_info->find_node(node_id);
                    if (node == nullptr) {
                        return;
                    }
                    PNodeInfo* pnode = slave_tablet_nodes.add_slave_nodes();
                    pnode->set_id(node->id);
                    pnode->set_option(node->option);
                    pnode->set_host(node->host);
                    pnode->set_async_internal_port(node->brpc_port);
                }
                request.mutable_slave_tablet_nodes()->insert({iter->first, slave_tablet_nodes});
            }
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
            cancel(fmt::format("{}, err: {}", channel_info(), st.to_string()));
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

        {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            _brpc_http_stub->tablet_writer_add_block_by_http(&_add_block_closure->cntl, NULL,
                                                             &_add_block_closure->result,
                                                             _add_block_closure);
        }
    } else {
        _add_block_closure->cntl.http_request().Clear();
        {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            _stub->tablet_writer_add_block(&_add_block_closure->cntl, &request,
                                           &_add_block_closure->result, _add_block_closure);
        }
    }

    _next_packet_seq++;
}

void VNodeChannel::cancel(const std::string& cancel_msg) {
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
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

Status VNodeChannel::close_wait(RuntimeState* state) {
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    // set _is_closed to true finally
    Defer set_closed {[&]() {
        std::lock_guard<std::mutex> l(_closed_lock);
        _is_closed = true;
    }};

    auto st = none_of({_cancelled, !_eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
            return Status::InternalError("wait close failed. {}", _cancel_msg);
        } else {
            return std::move(
                    st.prepend("already stopped, skip waiting for close. cancelled/!eos: "));
        }
    }

    // waiting for finished, it may take a long time, so we couldn't set a timeout
    while (!_add_batches_finished && !_cancelled) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    _close_time_ms = UnixMillis() - _close_time_ms;

    if (_add_batches_finished) {
        _close_check();
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(_tablet_commit_infos.begin()),
                                            std::make_move_iterator(_tablet_commit_infos.end()));

        _index_channel->set_error_tablet_in_state(state);
        _index_channel->set_tablets_received_rows(_tablets_received_rows, _node_id);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "close wait failed coz rpc error";
    {
        std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
        if (_cancel_msg != "") {
            ss << ". " << _cancel_msg;
        }
    }
    return Status::InternalError(ss.str());
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
        : _pool(pool),
          _input_row_desc(row_desc),
          _filter_bitmap(1024),
          _stop_background_threads_latch(1) {
    // From the thrift expressions create the real exprs.
    *status = vectorized::VExpr::create_expr_trees(pool, texprs, &_output_vexpr_ctxs);
    _name = "VOlapTableSink";
    _transfer_large_data_by_brpc = config::transfer_large_data_by_brpc;
}

VOlapTableSink::~VOlapTableSink() {
    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    for (const auto& index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<VNodeChannel>& ch) { ch->clear_all_blocks(); });
    }
}

Status VOlapTableSink::init(const TDataSink& t_sink) {
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

Status VOlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();
    _is_high_priority = (state->query_options().query_timeout <=
                         config::load_task_high_priority_threshold_second);

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

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
        // For DECIMAL32,DECIMAL64,DECIMAL128, we have done precision and scale conversion so just
        // skip data validation here.
        case TYPE_DECIMALV2:
            _max_decimalv2_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimalv2_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            _need_validate_data = true;
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATEV2:
        case TYPE_DATETIMEV2:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_STRING:
        case TYPE_ARRAY:
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
    const auto& partitions = _vpartition->get_partitions();
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
        if (UNLIKELY(tablets.empty())) {
            LOG(WARNING) << "load job:" << state->load_job_id() << " index: " << index->index_id
                         << " would open 0 tablet";
        }
        _channels.emplace_back(new IndexChannel(this, index->index_id));
        RETURN_IF_ERROR(_channels.back()->init(state, tablets));
    }
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc));
    return Status::OK();
}

Status VOlapTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOlapTableSink::open");
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<VNodeChannel>& ch) { ch->open(); });
    }

    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel](
                                                     const std::shared_ptr<VNodeChannel>& ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                // The open() phase is mainly to generate DeltaWriter instances on the nodes corresponding to each node channel.
                // This phase will not fail due to a single tablet.
                // Therefore, if the open() phase fails, all tablets corresponding to the node need to be marked as failed.
                index_channel->mark_as_failed(
                        ch->node_id(), ch->host(),
                        fmt::format("{}, open failed, err: {}", ch->channel_info(), st.to_string()),
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
            "OlapTableSink", "send_batch_process",
            [this, state]() { this->_send_batch_process(state); }, &_sender_thread));

    return Status::OK();
}

void VOlapTableSink::_send_batch_process(RuntimeState* state) {
    SCOPED_TIMER(_non_blocking_send_timer);
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    do {
        int running_channels_num = 0;
        for (auto index_channel : _channels) {
            index_channel->for_each_node_channel([&running_channels_num, this,
                                                  state](const std::shared_ptr<VNodeChannel>& ch) {
                running_channels_num +=
                        ch->try_send_and_fetch_status(state, this->_send_batch_thread_pool_token);
            });
        }

        if (running_channels_num == 0) {
            LOG(INFO) << "all node channels are stopped(maybe finished/offending/cancelled), "
                         "sender thread exit. "
                      << print_id(_load_id);
            return;
        }
    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::olap_table_sink_send_interval_ms)));
}

size_t VOlapTableSink::get_pending_bytes() const {
    size_t mem_consumption = 0;
    for (auto& indexChannel : _channels) {
        mem_consumption += indexChannel->get_pending_bytes();
    }
    return mem_consumption;
}

Status VOlapTableSink::find_tablet(RuntimeState* state, vectorized::Block* block, int row_index,
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

Status VOlapTableSink::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VOlapTableSink::send");
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

    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    if (findTabletMode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
        _partition_to_tablet_map.clear();
    }

    std::vector<std::unordered_map<
            VNodeChannel*,
            std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>>>
            channel_to_payload;
    channel_to_payload.resize(_channels.size());
    for (int i = 0; i < num_rows; ++i) {
        if (filtered_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        uint32_t tablet_index = 0;
        bool is_continue = false;
        RETURN_IF_ERROR(find_tablet(state, &block, i, &partition, tablet_index, stop_processing,
                                    is_continue));
        if (is_continue) {
            continue;
        }
        for (int j = 0; j < partition->indexes.size(); ++j) {
            auto tid = partition->indexes[j].tablets[tablet_index];
            auto it = _channels[j]->_channels_by_tablet.find(tid);
            DCHECK(it != _channels[j]->_channels_by_tablet.end())
                    << "unknown tablet, tablet_id=" << tablet_index;
            for (const auto& channel : it->second) {
                if (channel_to_payload[j].count(channel.get()) < 1) {
                    channel_to_payload[j].insert(
                            {channel.get(),
                             std::pair<std::unique_ptr<vectorized::IColumn::Selector>,
                                       std::vector<int64_t>> {
                                     std::unique_ptr<vectorized::IColumn::Selector>(
                                             new vectorized::IColumn::Selector()),
                                     std::vector<int64_t>()}});
                }
                channel_to_payload[j][channel.get()].first->push_back(i);
                channel_to_payload[j][channel.get()].second.push_back(tid);
            }
            _number_output_rows++;
        }
    }
    for (size_t i = 0; i < _channels.size(); i++) {
        for (const auto& entry : channel_to_payload[i]) {
            // if this node channel is already failed, this add_row will be skipped
            auto st = entry.first->add_block(&block, entry.second);
            if (!st.ok()) {
                _channels[i]->mark_as_failed(entry.first->node_id(), entry.first->host(),
                                             st.to_string());
            }
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
    Status status = exec_status;
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
                        [](const std::shared_ptr<VNodeChannel>& ch) { ch->mark_close(); });
                num_node_channels += index_channel->num_node_channels();
            }

            for (auto index_channel : _channels) {
                int64_t add_batch_exec_time = 0;
                index_channel->for_each_node_channel(
                        [&index_channel, &state, &node_add_batch_counter_map, &serialize_batch_ns,
                         &mem_exceeded_block_ns, &queue_push_lock_ns, &actual_consume_ns,
                         &total_add_batch_exec_time_ns, &add_batch_exec_time,
                         &total_add_batch_num](const std::shared_ptr<VNodeChannel>& ch) {
                            auto s = ch->close_wait(state);
                            if (!s.ok()) {
                                auto err_msg = s.to_string();
                                index_channel->mark_as_failed(ch->node_id(), ch->host(), err_msg,
                                                              -1);
                                // cancel the node channel in best effort
                                ch->cancel(err_msg);
                                LOG(WARNING) << ch->channel_info()
                                             << ", close channel failed, err: " << err_msg;
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
                } else if (Status st = index_channel->check_tablet_received_rows_consistency();
                           !st.ok()) {
                    status = st;
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
            channel->for_each_node_channel([&status](const std::shared_ptr<VNodeChannel>& ch) {
                ch->cancel(status.to_string());
            });
        }
        LOG(INFO) << "finished to close olap table sink. load_id=" << print_id(_load_id)
                  << ", txn_id=" << _txn_id
                  << ", canceled all node channels due to error: " << status;
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

    _close_status = status;
    DataSink::close(state, exec_status);
    return status;
}

Status VOlapTableSink::_validate_column(RuntimeState* state, const TypeDescriptor& type,
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
                if (dec_val > _max_decimalv2_val[slot_index] ||
                    dec_val < _min_decimalv2_val[slot_index]) {
                    fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");
                    fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                    fmt::format_to(error_msg, ", precision={}, scale={}; ", type.precision,
                                   type.scale);
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
    case TYPE_ARRAY: {
        const auto column_array =
                assert_cast<const vectorized::ColumnArray*>(real_column_ptr.get());
        DCHECK(type.children.size() == 1);
        auto nested_type = type.children[0];
        if (nested_type.type == TYPE_ARRAY || nested_type.type == TYPE_CHAR ||
            nested_type.type == TYPE_VARCHAR || nested_type.type == TYPE_STRING) {
            const auto& offsets = column_array->get_offsets();
            vectorized::IColumn::Permutation permutation(offsets.back());
            for (size_t r = 0; r < offsets.size(); ++r) {
                for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                    permutation[c] = rows ? (*rows)[r] : r;
                }
            }
            fmt::format_to(error_prefix, "ARRAY type failed: ");
            RETURN_IF_ERROR(_validate_column(
                    state, nested_type, nested_type.contains_null, column_array->get_data_ptr(),
                    slot_index, filter_bitmap, stop_processing, error_prefix, &permutation));
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

Status VOlapTableSink::_validate_data(RuntimeState* state, vectorized::Block* block,
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
