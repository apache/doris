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

#include "vtablet_writer.h"

#include <brpc/http_method.h>
#include <bthread/bthread.h>
#include <fmt/format.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>
#include <sys/param.h>

#include <algorithm>
#include <exception>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/sink/volap_table_sink.h"
#include "vec/sink/vrow_distribution.h"

#ifdef DEBUG
#include <unordered_set>
#endif

#include "bvar/bvar.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
class TExpr;

namespace vectorized {

bvar::Adder<int64_t> g_sink_write_bytes;
bvar::PerSecond<bvar::Adder<int64_t>> g_sink_write_bytes_per_second("sink_throughput_byte",
                                                                    &g_sink_write_bytes, 60);
bvar::Adder<int64_t> g_sink_write_rows;
bvar::PerSecond<bvar::Adder<int64_t>> g_sink_write_rows_per_second("sink_throughput_row",
                                                                   &g_sink_write_rows, 60);

Status IndexChannel::init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets) {
    SCOPED_CONSUME_MEM_TRACKER(_index_channel_tracker.get());
    for (const auto& tablet : tablets) {
        // First find the location BEs of this tablet
        auto* tablet_locations = _parent->_location->find_tablet(tablet.tablet_id);
        if (tablet_locations == nullptr) {
            return Status::InternalError("unknown tablet, tablet_id={}", tablet.tablet_id);
        }
        std::vector<std::shared_ptr<VNodeChannel>> channels;
        // For tablet, deal with its' all replica (in some node).
        for (auto& replica_node_id : tablet_locations->node_ids) {
            std::shared_ptr<VNodeChannel> channel;
            auto it = _node_channels.find(replica_node_id);
            // when we prepare for TableSink or incremental open tablet, we need init
            if (it == _node_channels.end()) {
                // NodeChannel is not added to the _parent->_pool.
                // Because the deconstruction of NodeChannel may take a long time to wait rpc finish.
                // but the ObjectPool will hold a spin lock to delete objects.
                channel = std::make_shared<VNodeChannel>(_parent, this, replica_node_id);
                _node_channels.emplace(replica_node_id, channel);
            } else {
                channel = it->second;
            }
            channel->add_tablet(tablet);
            if (_parent->_write_single_replica) {
                auto* slave_location = _parent->_slave_location->find_tablet(tablet.tablet_id);
                if (slave_location != nullptr) {
                    channel->add_slave_tablet_nodes(tablet.tablet_id, slave_location->node_ids);
                }
            }
            channels.push_back(channel);
            _tablets_by_channel[replica_node_id].insert(tablet.tablet_id);
        }
        _channels_by_tablet.emplace(tablet.tablet_id, std::move(channels));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
    }
    if (_where_clause != nullptr) {
        RETURN_IF_ERROR(_where_clause->prepare(state, *_parent->_output_row_desc));
        RETURN_IF_ERROR(_where_clause->open(state));
    }

    return Status::OK();
}

void IndexChannel::mark_as_failed(const VNodeChannel* node_channel, const std::string& err,
                                  int64_t tablet_id) {
    DCHECK(node_channel != nullptr);
    LOG(INFO) << "mark node_id:" << node_channel->channel_info() << " tablet_id: " << tablet_id
              << " as failed, err: " << err;
    auto node_id = node_channel->node_id();
    const auto& it = _tablets_by_channel.find(node_id);
    if (it == _tablets_by_channel.end()) {
        return;
    }

    {
        std::lock_guard<doris::SpinLock> l(_fail_lock);
        if (tablet_id == -1) {
            for (const auto the_tablet_id : it->second) {
                _failed_channels[the_tablet_id].insert(node_id);
                _failed_channels_msgs.emplace(the_tablet_id,
                                              err + ", host: " + node_channel->host());
                if (_failed_channels[the_tablet_id].size() >= ((_parent->_num_replicas + 1) / 2)) {
                    _intolerable_failure_status = Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                            _failed_channels_msgs[the_tablet_id]);
                }
            }
        } else {
            _failed_channels[tablet_id].insert(node_id);
            _failed_channels_msgs.emplace(tablet_id, err + ", host: " + node_channel->host());
            if (_failed_channels[tablet_id].size() >= ((_parent->_num_replicas + 1) / 2)) {
                _intolerable_failure_status = Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                        _failed_channels_msgs[tablet_id]);
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

void IndexChannel::set_tablets_filtered_rows(
        const std::vector<std::pair<int64_t, int64_t>>& tablets_filtered_rows, int64_t node_id) {
    for (const auto& [tablet_id, rows_num] : tablets_filtered_rows) {
        _tablets_filtered_rows[tablet_id].emplace_back(node_id, rows_num);
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
                return Status::InternalError(
                        "rows num written by multi replicas doest't match, load_id={}, txn_id={}, "
                        "tablt_id={}, node_id={}, rows_num={}, node_id={}, rows_num={}",
                        print_id(_parent->_load_id), _parent->_txn_id, tablet.first,
                        tablet.second[i].first, tablet.second[i].second, tablet.second[0].first,
                        tablet.second[0].second);
            }
        }
    }
    return Status::OK();
}

Status IndexChannel::check_tablet_filtered_rows_consistency() {
    for (auto& tablet : _tablets_filtered_rows) {
        for (size_t i = 0; i < tablet.second.size(); i++) {
            VLOG_NOTICE << "check_tablet_filtered_rows_consistency, load_id: " << _parent->_load_id
                        << ", txn_id: " << std::to_string(_parent->_txn_id)
                        << ", tablet_id: " << tablet.first
                        << ", node_id: " << tablet.second[i].first
                        << ", rows_num: " << tablet.second[i].second;
            if (i == 0) {
                continue;
            }
            if (tablet.second[i].second != tablet.second[0].second) {
                return Status::InternalError(
                        "rows num filtered by multi replicas doest't match, load_id={}, txn_id={}, "
                        "tablt_id={}, node_id={}, rows_num={}, node_id={}, rows_num={}",
                        print_id(_parent->_load_id), _parent->_txn_id, tablet.first,
                        tablet.second[i].first, tablet.second[i].second, tablet.second[0].first,
                        tablet.second[0].second);
            }
        }
    }
    return Status::OK();
}

static Status none_of(std::initializer_list<bool> vars) {
    bool none = std::none_of(vars.begin(), vars.end(), [](bool var) { return var; });
    Status st = Status::OK();
    if (!none) {
        std::string vars_str;
        std::for_each(vars.begin(), vars.end(),
                      [&vars_str](bool var) -> void { vars_str += (var ? "1/" : "0/"); });
        if (!vars_str.empty()) {
            vars_str.pop_back(); // 0/1/0/ -> 0/1/0
        }
        st = Status::Uninitialized(vars_str);
    }

    return st;
}

VNodeChannel::VNodeChannel(VTabletWriter* parent, IndexChannel* index_channel, int64_t node_id,
                           bool is_incremental)
        : _parent(parent),
          _index_channel(index_channel),
          _node_id(node_id),
          _is_incremental(is_incremental) {
    _cur_add_block_request = std::make_shared<PTabletWriterAddBlockRequest>();
    _node_channel_tracker = std::make_shared<MemTracker>(
            fmt::format("NodeChannel:indexID={}:threadId={}",
                        std::to_string(_index_channel->_index_id), ThreadContext::get_thread_id()));
}

VNodeChannel::~VNodeChannel() = default;

void VNodeChannel::clear_all_blocks() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    std::queue<AddBlockReq> empty;
    std::swap(_pending_blocks, empty);
    _cur_mutable_block.reset();
}

// we don't need to send tablet_writer_cancel rpc request when
// init failed, so set _is_closed to true.
// if "_cancelled" is set to true,
// no need to set _cancel_msg because the error will be
// returned directly via "TabletSink::prepare()" method.
Status VNodeChannel::init(RuntimeState* state) {
    if (_inited) {
        return Status::OK();
    }

    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    _task_exec_ctx = state->get_task_execution_context();
    _tuple_desc = _parent->_output_tuple_desc;
    _state = state;
    // get corresponding BE node.
    const auto* node = _parent->_nodes_info->find_node(_node_id);
    if (node == nullptr) {
        _cancelled = true;
        _is_closed = true;
        return Status::InternalError("unknown node id, id={}", _node_id);
    }
    _node_info = *node;

    _load_info = "load_id=" + print_id(_parent->_load_id) +
                 ", txn_id=" + std::to_string(_parent->_txn_id);

    _row_desc = std::make_unique<RowDescriptor>(_tuple_desc, false);
    _batch_size = state->batch_size();

    _stub = state->exec_env()->brpc_internal_client_cache()->get_client(_node_info.host,
                                                                        _node_info.brpc_port);
    if (_stub == nullptr) {
        _cancelled = true;
        _is_closed = true;
        return Status::InternalError("Get rpc stub failed, host={}, port={}, info={}",
                                     _node_info.host, _node_info.brpc_port, channel_info());
    }

    _rpc_timeout_ms = state->execution_timeout() * 1000;
    _timeout_watch.start();

    // Initialize _cur_add_block_request
    if (!_cur_add_block_request->has_id()) {
        *(_cur_add_block_request->mutable_id()) = _parent->_load_id;
    }
    _cur_add_block_request->set_index_id(_index_channel->_index_id);
    _cur_add_block_request->set_sender_id(_parent->_sender_id);
    _cur_add_block_request->set_backend_id(_node_id);
    _cur_add_block_request->set_eos(false);

    // add block closure
    // Has to using value to capture _task_exec_ctx because tablet writer may destroyed during callback.
    _send_block_callback = WriteBlockCallback<PTabletWriterAddBlockResult>::create_shared();
    _send_block_callback->addFailedHandler([&, task_exec_ctx = _task_exec_ctx](bool is_last_rpc) {
        auto ctx_lock = task_exec_ctx.lock();
        if (ctx_lock == nullptr) {
            return;
        }
        _add_block_failed_callback(is_last_rpc);
    });

    _send_block_callback->addSuccessHandler(
            [&, task_exec_ctx = _task_exec_ctx](const PTabletWriterAddBlockResult& result,
                                                bool is_last_rpc) {
                auto ctx_lock = task_exec_ctx.lock();
                if (ctx_lock == nullptr) {
                    return;
                }
                _add_block_success_callback(result, is_last_rpc);
            });

    _name = fmt::format("VNodeChannel[{}-{}]", _index_channel->_index_id, _node_id);
    // The node channel will send _batch_size rows of data each rpc. When the
    // number of tablets is large, the number of data rows received by each
    // tablet is small, TabletsChannel need to traverse each tablet for import.
    // so the import performance is poor. Therefore, we set _batch_size to
    // a relatively large value to improve the import performance.
    _batch_size = std::max(_batch_size, 8192);

    _inited = true;
    return Status::OK();
}

void VNodeChannel::_open_internal(bool is_incremental) {
    if (_tablets_wait_open.empty()) {
        return;
    }
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    auto request = std::make_shared<PTabletWriterOpenRequest>();
    request->set_allocated_id(&_parent->_load_id);
    request->set_index_id(_index_channel->_index_id);
    request->set_txn_id(_parent->_txn_id);
    request->set_allocated_schema(_parent->_schema->to_protobuf());

    std::set<int64_t> deduper;
    for (auto& tablet : _tablets_wait_open) {
        if (deduper.contains(tablet.tablet_id)) {
            continue;
        }
        auto* ptablet = request->add_tablets();
        ptablet->set_partition_id(tablet.partition_id);
        ptablet->set_tablet_id(tablet.tablet_id);
        deduper.insert(tablet.tablet_id);
        _all_tablets.push_back(std::move(tablet));
    }
    _tablets_wait_open.clear();

    request->set_num_senders(_parent->_num_senders);
    request->set_need_gen_rollup(false); // Useless but it is a required field in pb
    request->set_load_mem_limit(_parent->_load_mem_limit);
    request->set_load_channel_timeout_s(_parent->_load_channel_timeout_s);
    request->set_is_high_priority(_parent->_is_high_priority);
    request->set_sender_ip(BackendOptions::get_localhost());
    request->set_is_vectorized(true);
    request->set_backend_id(_node_id);
    request->set_enable_profile(_state->enable_profile());
    request->set_is_incremental(is_incremental);

    auto open_callback = DummyBrpcCallback<PTabletWriterOpenResult>::create_shared();
    auto open_closure = AutoReleaseClosure<
            PTabletWriterOpenRequest,
            DummyBrpcCallback<PTabletWriterOpenResult>>::create_unique(request, open_callback);
    open_callback->cntl_->set_timeout_ms(config::tablet_writer_open_rpc_timeout_sec * 1000);
    if (config::tablet_writer_ignore_eovercrowded) {
        open_callback->cntl_->ignore_eovercrowded();
    }
    // the real transmission here. the corresponding BE's load mgr will open load channel for it.
    _stub->tablet_writer_open(open_closure->cntl_.get(), open_closure->request_.get(),
                              open_closure->response_.get(), open_closure.get());
    open_closure.release();
    _open_callbacks.push_back(open_callback);

    static_cast<void>(request->release_id());
    static_cast<void>(request->release_schema());
}

void VNodeChannel::open() {
    _open_internal(false);
}

void VNodeChannel::incremental_open() {
    _open_internal(true);
}

Status VNodeChannel::open_wait() {
    Status status;
    for (auto& open_callback : _open_callbacks) {
        // because of incremental open, we will wait multi times. so skip the closures which have been checked and set to nullptr in previous rounds
        if (open_callback == nullptr) {
            continue;
        }

        open_callback->join();
        SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
        if (open_callback->cntl_->Failed()) {
            if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                        _stub, _node_info.host, _node_info.brpc_port)) {
                ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                        open_callback->cntl_->remote_side());
            }
            _cancelled = true;
            auto error_code = open_callback->cntl_->ErrorCode();
            auto error_text = open_callback->cntl_->ErrorText();
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                    "failed to open tablet writer, error={}, error_text={}, info={}",
                    berror(error_code), error_text, channel_info());
        }
        status = Status::create(open_callback->response_->status());

        if (!status.ok()) {
            _cancelled = true;
            return status;
        }
    }

    return status;
}

Status VNodeChannel::add_block(vectorized::Block* block, const Payload* payload) {
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    if (payload->second.empty()) {
        return Status::OK();
    }
    // If add_block() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        if (_cancelled) {
            std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>("add row failed. {}",
                                                                   _cancel_msg);
        } else {
            return std::move(st.prepend("already stopped, can't add row. cancelled/eos: "));
        }
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_block() and return OK, because we will check _cancelled in next add_block() or mark_close().
    while (!_cancelled && !_state->is_cancelled() && _pending_batches_num > 0 &&
           _pending_batches_bytes > _max_pending_batches_bytes) {
        SCOPED_RAW_TIMER(&_stat.mem_exceeded_block_ns);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (UNLIKELY(!_cur_mutable_block)) {
        _cur_mutable_block = vectorized::MutableBlock::create_unique(block->clone_empty());
    }

    SCOPED_RAW_TIMER(&_stat.append_node_channel_ns);
    RETURN_IF_ERROR(
            block->append_to_block_by_selector(_cur_mutable_block.get(), *(payload->first)));
    for (auto tablet_id : payload->second) {
        _cur_add_block_request->add_tablet_ids(tablet_id);
    }

    if (_cur_mutable_block->rows() >= _batch_size ||
        _cur_mutable_block->bytes() > config::doris_scanner_row_bytes) {
        {
            SCOPED_ATOMIC_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            // To simplify the add_row logic, postpone adding block into req until the time of sending req
            _pending_batches_bytes += _cur_mutable_block->allocated_bytes();
            _cur_add_block_request->set_eos(
                    false); // for multi-add, only when marking close we set it eos.
            // Copy the request to tmp request to add to pend block queue
            auto tmp_add_block_request = std::make_shared<PTabletWriterAddBlockRequest>();
            *tmp_add_block_request = *_cur_add_block_request;
            _pending_blocks.emplace(std::move(_cur_mutable_block), tmp_add_block_request);
            _pending_batches_num++;
            VLOG_DEBUG << "VTabletWriter:" << _parent << " VNodeChannel:" << this
                       << " pending_batches_bytes:" << _pending_batches_bytes
                       << " jobid:" << std::to_string(_state->load_job_id())
                       << " loadinfo:" << _load_info;
        }
        _cur_mutable_block = vectorized::MutableBlock::create_unique(block->clone_empty());
        _cur_add_block_request->clear_tablet_ids();
    }

    return Status::OK();
}

int VNodeChannel::try_send_and_fetch_status(RuntimeState* state,
                                            std::unique_ptr<ThreadPoolToken>& thread_pool_token) {
    DBUG_EXECUTE_IF("VNodeChannel.try_send_and_fetch_status_full_gc",
                    { MemInfo::process_full_gc(); });

    if (_cancelled || _send_finished) { // not run
        return 0;
    }

    // set closure for sending block.
    if (!_send_block_callback->try_set_in_flight()) {
        // There is packet in flight, skip.
        return _send_finished ? 0 : 1;
    }

    // We are sure that try_send_batch is not running
    if (_pending_batches_num > 0) {
        auto s = thread_pool_token->submit_func([this, state] { try_send_pending_block(state); });
        if (!s.ok()) {
            _cancel_with_msg("submit send_batch task to send_batch_thread_pool failed");
            // sending finished. clear in flight
            _send_block_callback->clear_in_flight();
        }
        // in_flight is cleared in closure::Run
    } else {
        // sending finished. clear in flight
        _send_block_callback->clear_in_flight();
    }
    return _send_finished ? 0 : 1;
}

void VNodeChannel::_cancel_with_msg(const std::string& msg) {
    LOG(WARNING) << "cancel node channel " << channel_info() << ", error message: " << msg;
    {
        std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
        if (_cancel_msg.empty()) {
            _cancel_msg = msg;
        }
    }
    _cancelled = true;
}

void VNodeChannel::try_send_pending_block(RuntimeState* state) {
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker);
    SCOPED_ATOMIC_TIMER(&_actual_consume_ns);
    signal::set_signal_task_id(_parent->_load_id);
    AddBlockReq send_block;
    {
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
    request->set_packet_seq(_next_packet_seq);
    auto block = mutable_block->to_block();
    CHECK(block.rows() == request->tablet_ids_size())
            << "block rows: " << block.rows()
            << ", tablet_ids_size: " << request->tablet_ids_size();
    if (block.rows() > 0) {
        SCOPED_ATOMIC_TIMER(&_serialize_batch_ns);
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        Status st = block.serialize(state->be_exec_version(), request->mutable_block(),
                                    &uncompressed_bytes, &compressed_bytes,
                                    state->fragement_transmission_compression_type(),
                                    _parent->_transfer_large_data_by_brpc);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.to_string()));
            _send_block_callback->clear_in_flight();
            return;
        }
        if (compressed_bytes >= double(config::brpc_max_body_size) * 0.95F) {
            LOG(WARNING) << "send block too large, this rpc may failed. send size: "
                         << compressed_bytes << ", threshold: " << config::brpc_max_body_size
                         << ", " << channel_info();
        }
    }

    int remain_ms = _rpc_timeout_ms - _timeout_watch.elapsed_time() / NANOS_PER_MILLIS;
    if (UNLIKELY(remain_ms < config::min_load_rpc_timeout_ms)) {
        if (remain_ms <= 0 && !request->eos()) {
            cancel(fmt::format("{}, err: timeout", channel_info()));
            _send_block_callback->clear_in_flight();
            return;
        } else {
            remain_ms = config::min_load_rpc_timeout_ms;
        }
    }

    _send_block_callback->reset();
    _send_block_callback->cntl_->set_timeout_ms(remain_ms);
    if (config::tablet_writer_ignore_eovercrowded) {
        _send_block_callback->cntl_->ignore_eovercrowded();
    }

    if (request->eos()) {
        for (auto pid : _parent->_tablet_finder->partition_ids()) {
            request->add_partition_ids(pid);
        }

        request->set_write_single_replica(_parent->_write_single_replica);
        if (_parent->_write_single_replica) {
            for (auto& _slave_tablet_node : _slave_tablet_nodes) {
                PSlaveTabletNodes slave_tablet_nodes;
                for (auto node_id : _slave_tablet_node.second) {
                    const auto* node = _parent->_nodes_info->find_node(node_id);
                    if (node == nullptr) {
                        return;
                    }
                    PNodeInfo* pnode = slave_tablet_nodes.add_slave_nodes();
                    pnode->set_id(node->id);
                    pnode->set_option(node->option);
                    pnode->set_host(node->host);
                    pnode->set_async_internal_port(node->brpc_port);
                }
                request->mutable_slave_tablet_nodes()->insert(
                        {_slave_tablet_node.first, slave_tablet_nodes});
            }
        }

        // eos request must be the last request-> it's a signal makeing callback function to set _add_batch_finished true.
        _send_block_callback->end_mark();
        _send_finished = true;
        CHECK(_pending_batches_num == 0) << _pending_batches_num;
    }

    auto send_block_closure = AutoReleaseClosure<
            PTabletWriterAddBlockRequest,
            WriteBlockCallback<PTabletWriterAddBlockResult>>::create_unique(request,
                                                                            _send_block_callback);
    if (_parent->_transfer_large_data_by_brpc && request->has_block() &&
        request->block().has_column_values() && request->ByteSizeLong() > MIN_HTTP_BRPC_SIZE) {
        Status st = request_embed_attachment_contain_blockv2(send_block_closure->request_.get(),
                                                             send_block_closure);
        if (!st.ok()) {
            cancel(fmt::format("{}, err: {}", channel_info(), st.to_string()));
            _send_block_callback->clear_in_flight();
            return;
        }

        //format an ipv6 address
        std::string brpc_url = get_brpc_http_url(_node_info.host, _node_info.brpc_port);
        std::shared_ptr<PBackendService_Stub> _brpc_http_stub =
                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
                                                                                          "http");
        _send_block_callback->cntl_->http_request().uri() =
                brpc_url + "/PInternalServiceImpl/tablet_writer_add_block_by_http";
        _send_block_callback->cntl_->http_request().set_method(brpc::HTTP_METHOD_POST);
        _send_block_callback->cntl_->http_request().set_content_type("application/json");

        {
            _brpc_http_stub->tablet_writer_add_block_by_http(
                    send_block_closure->cntl_.get(), nullptr, send_block_closure->response_.get(),
                    send_block_closure.get());
            send_block_closure.release();
        }
    } else {
        _send_block_callback->cntl_->http_request().Clear();
        {
            _stub->tablet_writer_add_block(
                    send_block_closure->cntl_.get(), send_block_closure->request_.get(),
                    send_block_closure->response_.get(), send_block_closure.get());
            send_block_closure.release();
        }
    }

    _next_packet_seq++;
}

void VNodeChannel::_add_block_success_callback(const PTabletWriterAddBlockResult& result,
                                               bool is_last_rpc) {
    std::lock_guard<std::mutex> l(this->_closed_lock);
    if (this->_is_closed) {
        // if the node channel is closed, no need to call the following logic,
        // and notice that _index_channel may already be destroyed.
        return;
    }
    SCOPED_ATTACH_TASK(_state);
    Status status(Status::create(result.status()));
    if (status.ok()) {
        // if has error tablet, handle them first
        for (const auto& error : result.tablet_errors()) {
            _index_channel->mark_as_failed(this, "tablet error: " + error.msg(), error.tablet_id());
        }

        Status st = _index_channel->check_intolerable_failure();
        if (!st.ok()) {
            _cancel_with_msg(st.to_string());
        } else if (is_last_rpc) {
            for (const auto& tablet : result.tablet_vec()) {
                TTabletCommitInfo commit_info;
                commit_info.tabletId = tablet.tablet_id();
                commit_info.backendId = _node_id;
                _tablet_commit_infos.emplace_back(std::move(commit_info));
                if (tablet.has_received_rows()) {
                    _tablets_received_rows.emplace_back(tablet.tablet_id(), tablet.received_rows());
                }
                if (tablet.has_num_rows_filtered()) {
                    _tablets_filtered_rows.emplace_back(tablet.tablet_id(),
                                                        tablet.num_rows_filtered());
                }
                VLOG_CRITICAL << "master replica commit info: tabletId=" << tablet.tablet_id()
                              << ", backendId=" << _node_id
                              << ", master node id: " << this->node_id()
                              << ", host: " << this->host() << ", txn_id=" << _parent->_txn_id;
            }
            if (_parent->_write_single_replica) {
                for (const auto& tablet_slave_node_ids : result.success_slave_tablet_node_ids()) {
                    for (auto slave_node_id : tablet_slave_node_ids.second.slave_node_ids()) {
                        TTabletCommitInfo commit_info;
                        commit_info.tabletId = tablet_slave_node_ids.first;
                        commit_info.backendId = slave_node_id;
                        _tablet_commit_infos.emplace_back(std::move(commit_info));
                        VLOG_CRITICAL
                                << "slave replica commit info: tabletId="
                                << tablet_slave_node_ids.first << ", backendId=" << slave_node_id
                                << ", master node id: " << this->node_id()
                                << ", host: " << this->host() << ", txn_id=" << _parent->_txn_id;
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
    if (result.has_load_channel_profile()) {
        TRuntimeProfileTree tprofile;
        const auto* buf = (const uint8_t*)result.load_channel_profile().data();
        uint32_t len = result.load_channel_profile().size();
        auto st = deserialize_thrift_msg(buf, &len, false, &tprofile);
        if (st.ok()) {
            _state->load_channel_profile()->update(tprofile);
        } else {
            LOG(WARNING) << "load channel TRuntimeProfileTree deserialize failed, errmsg=" << st;
        }
    }
}

void VNodeChannel::_add_block_failed_callback(bool is_last_rpc) {
    std::lock_guard<std::mutex> l(this->_closed_lock);
    if (this->_is_closed) {
        // if the node channel is closed, no need to call `mark_as_failed`,
        // and notice that _index_channel may already be destroyed.
        return;
    }
    SCOPED_ATTACH_TASK(_state);
    // If rpc failed, mark all tablets on this node channel as failed
    _index_channel->mark_as_failed(this,
                                   fmt::format("rpc failed, error coed:{}, error text:{}",
                                               _send_block_callback->cntl_->ErrorCode(),
                                               _send_block_callback->cntl_->ErrorText()),
                                   -1);
    Status st = _index_channel->check_intolerable_failure();
    if (!st.ok()) {
        _cancel_with_msg(fmt::format("{}, err: {}", channel_info(), st.to_string()));
    } else if (is_last_rpc) {
        // if this is last rpc, will must set _add_batches_finished. otherwise, node channel's close_wait
        // will be blocked.
        _add_batches_finished = true;
    }
}

// When _cancelled is true, we still need to send a tablet_writer_cancel
// rpc request to truly release the load channel
void VNodeChannel::cancel(const std::string& cancel_msg) {
    if (_is_closed) {
        // skip the channels that have been canceled or close_wait.
        return;
    }
    SCOPED_CONSUME_MEM_TRACKER(_node_channel_tracker.get());
    // set _is_closed to true finally
    Defer set_closed {[&]() {
        std::lock_guard<std::mutex> l(_closed_lock);
        _is_closed = true;
    }};
    // we don't need to wait last rpc finished, cause closure's release/reset will join.
    // But do we need brpc::StartCancel(call_id)?
    _cancel_with_msg(cancel_msg);
    // if not inited, _stub will be nullptr, skip sending cancel rpc
    if (!_inited) {
        return;
    }

    auto request = std::make_shared<PTabletWriterCancelRequest>();
    request->set_allocated_id(&_parent->_load_id);
    request->set_index_id(_index_channel->_index_id);
    request->set_sender_id(_parent->_sender_id);

    auto cancel_callback = DummyBrpcCallback<PTabletWriterCancelResult>::create_shared();
    auto closure = AutoReleaseClosure<
            PTabletWriterCancelRequest,
            DummyBrpcCallback<PTabletWriterCancelResult>>::create_unique(request, cancel_callback);

    int remain_ms = _rpc_timeout_ms - _timeout_watch.elapsed_time() / NANOS_PER_MILLIS;
    if (UNLIKELY(remain_ms < config::min_load_rpc_timeout_ms)) {
        remain_ms = config::min_load_rpc_timeout_ms;
    }
    cancel_callback->cntl_->set_timeout_ms(remain_ms);
    if (config::tablet_writer_ignore_eovercrowded) {
        closure->cntl_->ignore_eovercrowded();
    }
    _stub->tablet_writer_cancel(closure->cntl_.get(), closure->request_.get(),
                                closure->response_.get(), closure.get());
    closure.release();
    static_cast<void>(request->release_id());
}

Status VNodeChannel::close_wait(RuntimeState* state) {
    DBUG_EXECUTE_IF("VNodeChannel.close_wait_full_gc", { MemInfo::process_full_gc(); });
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
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>("wait close failed. {}",
                                                                   _cancel_msg);
        } else {
            return std::move(
                    st.prepend("already stopped, skip waiting for close. cancelled/!eos: "));
        }
    }

    // waiting for finished, it may take a long time, so we couldn't set a timeout
    // For pipeline engine, the close is called in async writer's process block method,
    // so that it will not block pipeline thread.
    while (!_add_batches_finished && !_cancelled && !state->is_cancelled()) {
        bthread_usleep(1000);
    }
    _close_time_ms = UnixMillis() - _close_time_ms;

    if (_cancelled || state->is_cancelled()) {
        cancel(state->cancel_reason());
    }

    if (_add_batches_finished) {
        _close_check();
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(_tablet_commit_infos.begin()),
                                            std::make_move_iterator(_tablet_commit_infos.end()));

        _index_channel->set_error_tablet_in_state(state);
        _index_channel->set_tablets_received_rows(_tablets_received_rows, _node_id);
        _index_channel->set_tablets_filtered_rows(_tablets_filtered_rows, _node_id);
        return Status::OK();
    }

    return Status::Error<ErrorCode::INTERNAL_ERROR, false>(get_cancel_msg());
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

    _cur_add_block_request->set_eos(true);
    {
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        if (!_cur_mutable_block) [[unlikely]] {
            // add a dummy block
            _cur_mutable_block = vectorized::MutableBlock::create_unique();
        }
        auto tmp_add_block_request =
                std::make_shared<PTabletWriterAddBlockRequest>(*_cur_add_block_request);
        // when prepare to close, add block to queue so that try_send_pending_block thread will send it.
        _pending_blocks.emplace(std::move(_cur_mutable_block), tmp_add_block_request);
        _pending_batches_num++;
        DCHECK(_pending_blocks.back().second->eos());
        _close_time_ms = UnixMillis();
        LOG(INFO) << channel_info()
                  << " mark closed, left pending batch size: " << _pending_blocks.size();
    }

    _eos_is_produced = true;
}

VTabletWriter::VTabletWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs)
        : AsyncResultWriter(output_exprs), _t_sink(t_sink) {
    _transfer_large_data_by_brpc = config::transfer_large_data_by_brpc;
}

void VTabletWriter::_send_batch_process() {
    SCOPED_TIMER(_non_blocking_send_timer);
    SCOPED_ATTACH_TASK(_state);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

    int sleep_time = int(config::olap_table_sink_send_interval_microseconds *
                         (_vpartition->is_auto_partition()
                                  ? config::olap_table_sink_send_interval_auto_partition_factor
                                  : 1));

    while (true) {
        // incremental open will temporarily make channels into abnormal state. stop checking when this.
        std::unique_lock<std::mutex> l(_stop_check_channel);

        int running_channels_num = 0;
        int opened_nodes = 0;
        for (const auto& index_channel : _channels) {
            index_channel->for_each_node_channel([&running_channels_num,
                                                  this](const std::shared_ptr<VNodeChannel>& ch) {
                // if this channel all completed(cancelled), got 0. else 1.
                running_channels_num +=
                        ch->try_send_and_fetch_status(_state, this->_send_batch_thread_pool_token);
            });
            opened_nodes += index_channel->num_node_channels();
        }

        // auto partition table may have no node channel temporarily. wait to open.
        if (opened_nodes != 0 && running_channels_num == 0) {
            LOG(INFO) << "All node channels are stopped(maybe finished/offending/cancelled), "
                         "sender thread exit. "
                      << print_id(_load_id);
            return;
        }

        // for auto partition tables, there's a situation: we haven't open any node channel but decide to cancel the task.
        // then the judge in front will never be true because opened_nodes won't increase. so we have to specially check wether we called close.
        // we must RECHECK opened_nodes below, after got closed signal, because it may changed. Think of this:
        //      checked opened_nodes = 0 ---> new block arrived ---> task finished, close() was called ---> we got _try_close here
        // if we don't check again, we may lose the last package.
        if (_try_close) {
            opened_nodes = 0;
            std::ranges::for_each(_channels,
                                  [&opened_nodes](const std::shared_ptr<IndexChannel>& ich) {
                                      opened_nodes += ich->num_node_channels();
                                  });
            if (opened_nodes == 0) {
                LOG(INFO) << "No node channel have ever opened but now we have to close. sender "
                             "thread exit. "
                          << print_id(_load_id);
                return;
            }
        }
        bthread_usleep(sleep_time);
    }
}

static void* periodic_send_batch(void* writer) {
    auto* tablet_writer = (VTabletWriter*)(writer);
    tablet_writer->_send_batch_process();
    return nullptr;
}

Status VTabletWriter::open(doris::RuntimeState* state, doris::RuntimeProfile* profile) {
    RETURN_IF_ERROR(_init(state, profile));
    signal::set_signal_task_id(_load_id);
    SCOPED_TIMER(profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    fmt::memory_buffer buf;
    for (const auto& index_channel : _channels) {
        fmt::format_to(buf, "index id:{}", index_channel->_index_id);
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<VNodeChannel>& ch) { ch->open(); });
    }
    VLOG_DEBUG << "list of open index id = " << fmt::to_string(buf);

    for (const auto& index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel](
                                                     const std::shared_ptr<VNodeChannel>& ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                // The open() phase is mainly to generate DeltaWriter instances on the nodes corresponding to each node channel.
                // This phase will not fail due to a single tablet.
                // Therefore, if the open() phase fails, all tablets corresponding to the node need to be marked as failed.
                index_channel->mark_as_failed(
                        ch.get(),
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

    // start to send batch continually. this must be called after _init
    if (bthread_start_background(&_sender_thread, nullptr, periodic_send_batch, (void*)this) != 0) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("bthread_start_backgroud failed");
    }
    return Status::OK();
}

Status VTabletWriter::on_partitions_created(TCreatePartitionResult* result) {
    // add new tablet locations. it will use by address. so add to pool
    auto* new_locations = _pool->add(new std::vector<TTabletLocation>(result->tablets));
    _location->add_locations(*new_locations);
    if (_write_single_replica) {
        _slave_location->add_locations(*new_locations);
    }

    // update new node info
    _nodes_info->add_nodes(result->nodes);

    // incremental open node channel
    RETURN_IF_ERROR(_incremental_open_node_channel(result->partitions));

    return Status::OK();
}

static Status on_partitions_created(void* writer, TCreatePartitionResult* result) {
    return static_cast<VTabletWriter*>(writer)->on_partitions_created(result);
}

Status VTabletWriter::_init_row_distribution() {
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
                            .caller = this,
                            .create_partition_callback = &vectorized::on_partitions_created});

    return _row_distribution.open(_output_row_desc);
}

Status VTabletWriter::_init(RuntimeState* state, RuntimeProfile* profile) {
    DCHECK(_t_sink.__isset.olap_table_sink);
    _pool = state->obj_pool();
    auto& table_sink = _t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _num_replicas = table_sink.num_replicas;
    _tuple_desc_id = table_sink.tuple_id;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _schema->set_timestamp_ms(state->timestamp_ms());
    _schema->set_timezone(state->timezone());
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
    RETURN_IF_ERROR(_vpartition->init());

    _state = state;
    _profile = profile;

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();
    _is_high_priority =
            (state->execution_timeout() <= config::load_task_high_priority_threshold_second);
    DBUG_EXECUTE_IF("VTabletWriter._init.is_high_priority", { _is_high_priority = true; });
    // profile must add to state's object pool
    _mem_tracker =
            std::make_shared<MemTracker>("OlapTableSink:" + std::to_string(state->load_job_id()));
    SCOPED_TIMER(profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }

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
    _block_convertor->init_autoinc_info(
            _schema->db_id(), _schema->table_id(), _state->batch_size(),
            _schema->is_partial_update() && !_schema->auto_increment_coulumn().empty(),
            _schema->auto_increment_column_unique_id());
    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));

    // add all counter
    _input_rows_counter = ADD_COUNTER(profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(profile, "RowsProduced", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(profile, "SendDataTime");
    _wait_mem_limit_timer = ADD_CHILD_TIMER(profile, "WaitMemLimitTime", "SendDataTime");
    _row_distribution_timer = ADD_CHILD_TIMER(profile, "RowDistributionTime", "SendDataTime");
    _filter_timer = ADD_CHILD_TIMER(profile, "FilterTime", "SendDataTime");
    _where_clause_timer = ADD_CHILD_TIMER(profile, "WhereClauseTime", "SendDataTime");
    _append_node_channel_timer = ADD_CHILD_TIMER(profile, "AppendNodeChannelTime", "SendDataTime");
    _add_partition_request_timer =
            ADD_CHILD_TIMER(profile, "AddPartitionRequestTime", "SendDataTime");
    _validate_data_timer = ADD_TIMER(profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(profile, "OpenTime");
    _close_timer = ADD_TIMER(profile, "CloseWaitTime");
    _non_blocking_send_timer = ADD_TIMER(profile, "NonBlockingSendTime");
    _non_blocking_send_work_timer =
            ADD_CHILD_TIMER(profile, "NonBlockingSendWorkTime", "NonBlockingSendTime");
    _serialize_batch_timer =
            ADD_CHILD_TIMER(profile, "SerializeBatchTime", "NonBlockingSendWorkTime");
    _total_add_batch_exec_timer = ADD_TIMER(profile, "TotalAddBatchExecTime");
    _max_add_batch_exec_timer = ADD_TIMER(profile, "MaxAddBatchExecTime");
    _total_wait_exec_timer = ADD_TIMER(profile, "TotalWaitExecTime");
    _max_wait_exec_timer = ADD_TIMER(profile, "MaxWaitExecTime");
    _add_batch_number = ADD_COUNTER(profile, "NumberBatchAdded", TUnit::UNIT);
    _num_node_channels = ADD_COUNTER(profile, "NumberNodeChannels", TUnit::UNIT);
    _load_mem_limit = state->get_load_mem_limit();

#ifdef DEBUG
    // check: tablet ids should be unique
    {
        std::unordered_set<int64_t> tablet_ids;
        const auto& partitions = _vpartition->get_partitions();
        for (int i = 0; i < _schema->indexes().size(); ++i) {
            for (const auto& partition : partitions) {
                for (const auto& tablet : partition->indexes[i].tablets) {
                    CHECK(tablet_ids.count(tablet) == 0) << "found duplicate tablet id: " << tablet;
                    tablet_ids.insert(tablet);
                }
            }
        }
    }
#endif

    // open all channels
    const auto& partitions = _vpartition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<TTabletWithPartition> tablets;
        auto* index = _schema->indexes()[i];
        for (const auto& part : partitions) {
            for (const auto& tablet : part->indexes[i].tablets) {
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
        }
        if (tablets.empty() && !_vpartition->is_auto_partition()) {
            LOG(WARNING) << "load job:" << state->load_job_id() << " index: " << index->index_id
                         << " would open 0 tablet";
        }
        _channels.emplace_back(new IndexChannel(this, index->index_id, index->where_clause));
        _index_id_to_channel[index->index_id] = _channels.back();
        RETURN_IF_ERROR(_channels.back()->init(state, tablets));
    }

    RETURN_IF_ERROR(_init_row_distribution());

    _inited = true;
    return Status::OK();
}

Status VTabletWriter::_incremental_open_node_channel(
        const std::vector<TOlapTablePartition>& partitions) {
    // do what we did in prepare() for partitions. indexes which don't change when we create new partition is orthogonal to partitions.
    std::unique_lock<std::mutex> _l(_stop_check_channel);
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        const OlapTableIndexSchema* index = _schema->indexes()[i];
        std::vector<TTabletWithPartition> tablets;
        for (const auto& t_part : partitions) {
            VOlapTablePartition* part = nullptr;
            RETURN_IF_ERROR(_vpartition->generate_partition_from(t_part, part));
            for (const auto& tablet : part->indexes[i].tablets) {
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
            DCHECK(!tablets.empty()) << "incremental open got nothing!";
        }
        // update and reinit for existing channels.
        std::shared_ptr<IndexChannel> channel = _index_id_to_channel[index->index_id];
        DCHECK(channel != nullptr);
        RETURN_IF_ERROR(channel->init(_state, tablets)); // add tablets into it
    }

    fmt::memory_buffer buf;
    for (auto& channel : _channels) {
        // incremental open new partition's tablet on storage side
        channel->for_each_node_channel(
                [](const std::shared_ptr<VNodeChannel>& ch) { ch->incremental_open(); });
        fmt::format_to(buf, "index id:{}", channel->_index_id);
        VLOG_DEBUG << "list of open index id = " << fmt::to_string(buf);

        channel->for_each_node_channel([&channel](const std::shared_ptr<VNodeChannel>& ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                // The open() phase is mainly to generate DeltaWriter instances on the nodes corresponding to each node channel.
                // This phase will not fail due to a single tablet.
                // Therefore, if the open() phase fails, all tablets corresponding to the node need to be marked as failed.
                channel->mark_as_failed(
                        ch.get(),
                        fmt::format("{}, open failed, err: {}", ch->channel_info(), st.to_string()),
                        -1);
            }
        });

        RETURN_IF_ERROR(channel->check_intolerable_failure());
    }

    return Status::OK();
}

static Status cancel_channel_and_check_intolerable_failure(
        Status status, const std::string& err_msg, const std::shared_ptr<IndexChannel> ich,
        const std::shared_ptr<VNodeChannel> nch) {
    LOG(WARNING) << nch->channel_info() << ", close channel failed, err: " << err_msg;
    ich->mark_as_failed(nch.get(), err_msg, -1);
    // cancel the node channel in best effort
    nch->cancel(err_msg);

    // check if index has intolerable failure
    Status index_st = ich->check_intolerable_failure();
    if (!index_st.ok()) {
        status = index_st;
    } else if (Status st = ich->check_tablet_received_rows_consistency(); !st.ok()) {
        status = st;
    } else if (Status st = ich->check_tablet_filtered_rows_consistency(); !st.ok()) {
        status = st;
    }
    return status;
}

void VTabletWriter::_cancel_all_channel(Status status) {
    for (const auto& index_channel : _channels) {
        index_channel->for_each_node_channel([&status](const std::shared_ptr<VNodeChannel>& ch) {
            ch->cancel(status.to_string());
        });
    }
    LOG(INFO) << fmt::format(
            "close olap table sink. load_id={}, txn_id={}, canceled all node channels due to "
            "error: {}",
            print_id(_load_id), _txn_id, status);
}

Status VTabletWriter::_send_new_partition_batch() {
    if (_row_distribution.need_deal_batching()) { // maybe try_close more than 1 time
        RETURN_IF_ERROR(_row_distribution.automatic_create_partition());

        Block tmp_block = _row_distribution._batching_block->to_block(); // Borrow out, for lval ref

        // these order is only.
        //  1. clear batching stats(and flag goes true) so that we won't make a new batching process in dealing batched block.
        //  2. deal batched block
        //  3. now reuse the column of lval block. cuz write doesn't real adjust it. it generate a new block from that.
        _row_distribution.clear_batching_stats();
        RETURN_IF_ERROR(this->write(tmp_block));
        _row_distribution._batching_block->set_mutable_columns(
                tmp_block.mutate_columns()); // Recovery back
        _row_distribution._batching_block->clear_column_data();
        _row_distribution._deal_batched = false;
    }
    return Status::OK();
}

void VTabletWriter::_do_try_close(RuntimeState* state, const Status& exec_status) {
    SCOPED_TIMER(_close_timer);
    Status status = exec_status;

    // must before set _try_close
    if (status.ok()) {
        SCOPED_TIMER(_profile->total_time_counter());
        _row_distribution._deal_batched = true;
        status = _send_new_partition_batch();
    }

    _try_close = true; // will stop periodic thread
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        {
            for (const auto& index_channel : _channels) {
                if (!status.ok()) {
                    break;
                }
                index_channel->for_each_node_channel(
                        [&index_channel, &status](const std::shared_ptr<VNodeChannel>& ch) {
                            if (!status.ok() || ch->is_closed()) {
                                return;
                            }
                            // only first try close, all node channels will mark_close()
                            ch->mark_close();
                            if (ch->is_cancelled()) {
                                status = cancel_channel_and_check_intolerable_failure(
                                        status, ch->get_cancel_msg(), index_channel, ch);
                            }
                        });
            } // end for index channels
        }
    }

    if (!status.ok()) {
        _cancel_all_channel(status);
        _close_status = status;
        _close_wait = true;
    }
}

Status VTabletWriter::close(Status exec_status) {
    if (!_inited) {
        DCHECK(!exec_status.ok());
        _cancel_all_channel(exec_status);
        _close_status = exec_status;
        return _close_status;
    }

    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(_profile->total_time_counter());

    // will make the last batch of request-> close_wait will wait this finished.
    _do_try_close(_state, exec_status);

    // If _close_status is not ok, all nodes have been canceled in try_close.
    if (_close_status.ok()) {
        auto status = Status::OK();
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0;
        int64_t queue_push_lock_ns = 0;
        int64_t actual_consume_ns = 0;
        int64_t total_add_batch_exec_time_ns = 0;
        int64_t max_add_batch_exec_time_ns = 0;
        int64_t total_wait_exec_time_ns = 0;
        int64_t max_wait_exec_time_ns = 0;
        int64_t total_add_batch_num = 0;
        int64_t num_node_channels = 0;
        VNodeChannelStat channel_stat;

        for (const auto& index_channel : _channels) {
            if (!status.ok()) {
                break;
            }
            int64_t add_batch_exec_time = 0;
            int64_t wait_exec_time = 0;
            index_channel->for_each_node_channel(
                    [this, &index_channel, &status, &node_add_batch_counter_map,
                     &serialize_batch_ns, &channel_stat, &queue_push_lock_ns, &actual_consume_ns,
                     &total_add_batch_exec_time_ns, &add_batch_exec_time, &total_wait_exec_time_ns,
                     &wait_exec_time,
                     &total_add_batch_num](const std::shared_ptr<VNodeChannel>& ch) {
                        if (!status.ok() || ch->is_closed()) {
                            return;
                        }
                        // in pipeline, all node channels are done or canceled, will not block.
                        // no pipeline, close may block waiting.
                        auto s = ch->close_wait(_state);
                        if (!s.ok()) {
                            status = cancel_channel_and_check_intolerable_failure(
                                    status, s.to_string(), index_channel, ch);
                        }
                        ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns,
                                        &channel_stat, &queue_push_lock_ns, &actual_consume_ns,
                                        &total_add_batch_exec_time_ns, &add_batch_exec_time,
                                        &total_wait_exec_time_ns, &wait_exec_time,
                                        &total_add_batch_num);
                    });

            // Due to the non-determinism of compaction, the rowsets of each replica may be different from each other on different
            // BE nodes. The number of rows filtered in SegmentWriter depends on the historical rowsets located in the correspoding
            // BE node. So we check the number of rows filtered on each succeccful BE to ensure the consistency of the current load
            if (status.ok() && !_write_single_replica && _schema->is_strict_mode() &&
                _schema->is_partial_update()) {
                if (Status st = index_channel->check_tablet_filtered_rows_consistency(); !st.ok()) {
                    status = st;
                } else {
                    _state->set_num_rows_filtered_in_strict_mode_partial_update(
                            index_channel->num_rows_filtered());
                }
            }

            num_node_channels += index_channel->num_node_channels();
            if (add_batch_exec_time > max_add_batch_exec_time_ns) {
                max_add_batch_exec_time_ns = add_batch_exec_time;
            }
            if (wait_exec_time > max_wait_exec_time_ns) {
                max_wait_exec_time_ns = wait_exec_time;
            }
        } // end for index channels

        if (status.ok()) {
            // TODO need to be improved
            LOG(INFO) << "total mem_exceeded_block_ns=" << channel_stat.mem_exceeded_block_ns
                      << ", total queue_push_lock_ns=" << queue_push_lock_ns
                      << ", total actual_consume_ns=" << actual_consume_ns
                      << ", load id=" << print_id(_load_id);

            COUNTER_SET(_input_rows_counter, _number_input_rows);
            COUNTER_SET(_output_rows_counter, _number_output_rows);
            COUNTER_SET(_filtered_rows_counter,
                        _block_convertor->num_filtered_rows() +
                                _tablet_finder->num_filtered_rows() +
                                _state->num_rows_filtered_in_strict_mode_partial_update());
            COUNTER_SET(_send_data_timer, _send_data_ns);
            COUNTER_SET(_row_distribution_timer, (int64_t)_row_distribution_watch.elapsed_time());
            COUNTER_SET(_filter_timer, _filter_ns);
            COUNTER_SET(_append_node_channel_timer, channel_stat.append_node_channel_ns);
            COUNTER_SET(_where_clause_timer, channel_stat.where_clause_ns);
            COUNTER_SET(_wait_mem_limit_timer, channel_stat.mem_exceeded_block_ns);
            COUNTER_SET(_validate_data_timer, _block_convertor->validate_data_ns());
            COUNTER_SET(_serialize_batch_timer, serialize_batch_ns);
            COUNTER_SET(_non_blocking_send_work_timer, actual_consume_ns);
            COUNTER_SET(_total_add_batch_exec_timer, total_add_batch_exec_time_ns);
            COUNTER_SET(_max_add_batch_exec_timer, max_add_batch_exec_time_ns);
            COUNTER_SET(_total_wait_exec_timer, total_wait_exec_time_ns);
            COUNTER_SET(_max_wait_exec_timer, max_wait_exec_time_ns);
            COUNTER_SET(_add_batch_number, total_add_batch_num);
            COUNTER_SET(_num_node_channels, num_node_channels);

            // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
            int64_t num_rows_load_total = _number_input_rows + _state->num_rows_load_filtered() +
                                          _state->num_rows_load_unselected();
            _state->set_num_rows_load_total(num_rows_load_total);
            _state->update_num_rows_load_filtered(
                    _block_convertor->num_filtered_rows() + _tablet_finder->num_filtered_rows() +
                    _state->num_rows_filtered_in_strict_mode_partial_update());
            _state->update_num_rows_load_unselected(
                    _tablet_finder->num_immutable_partition_filtered_rows());

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
            _cancel_all_channel(status);
        }
        _close_status = status;
    }

    // Sender join() must put after node channels mark_close/cancel.
    // But there is no specific sequence required between sender join() & close_wait().
    if (_sender_thread) {
        bthread_join(_sender_thread, nullptr);
        // We have to wait all task in _send_batch_thread_pool_token finished,
        // because it is difficult to handle concurrent problem if we just
        // shutdown it.
        _send_batch_thread_pool_token->wait();
    }

    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    for (const auto& index_channel : _channels) {
        index_channel->for_each_node_channel(
                [](const std::shared_ptr<VNodeChannel>& ch) { ch->clear_all_blocks(); });
    }
    return _close_status;
}

void VTabletWriter::_generate_one_index_channel_payload(
        RowPartTabletIds& row_part_tablet_id, int32_t index_idx,
        ChannelDistributionPayload& channel_payload) {
    auto& row_ids = row_part_tablet_id.row_ids;
    auto& tablet_ids = row_part_tablet_id.tablet_ids;

    size_t row_cnt = row_ids.size();

    for (int i = 0; i < row_ids.size(); i++) {
        // (tablet_id, VNodeChannel) where this tablet locate
        auto it = _channels[index_idx]->_channels_by_tablet.find(tablet_ids[i]);
        DCHECK(it != _channels[index_idx]->_channels_by_tablet.end())
                << "unknown tablet, tablet_id=" << tablet_ids[i];

        std::vector<std::shared_ptr<VNodeChannel>>& tablet_locations = it->second;
        for (const auto& locate_node : tablet_locations) {
            auto payload_it = channel_payload.find(locate_node.get()); // <VNodeChannel*, Payload>
            if (payload_it == channel_payload.end()) {
                auto [tmp_it, _] = channel_payload.emplace(
                        locate_node.get(),
                        Payload {std::make_unique<vectorized::IColumn::Selector>(),
                                 std::vector<int64_t>()});
                payload_it = tmp_it;
                payload_it->second.first->reserve(row_cnt);
                payload_it->second.second.reserve(row_cnt);
            }
            payload_it->second.first->push_back(row_ids[i]);
            payload_it->second.second.push_back(tablet_ids[i]);
        }
    }
}

void VTabletWriter::_generate_index_channels_payloads(
        std::vector<RowPartTabletIds>& row_part_tablet_ids,
        ChannelDistributionPayloadVec& payload) {
    for (int i = 0; i < _schema->indexes().size(); i++) {
        _generate_one_index_channel_payload(row_part_tablet_ids[i], i, payload[i]);
    }
}

Status VTabletWriter::write(doris::vectorized::Block& input_block) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();

    if (_state->query_options().dry_run_query) {
        return status;
    }

    // check out of limit
    RETURN_IF_ERROR(_send_new_partition_batch());

    auto rows = input_block.rows();
    auto bytes = input_block.bytes();
    if (UNLIKELY(rows == 0)) {
        return status;
    }
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_RAW_TIMER(&_send_data_ns);

    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    int64_t filtered_rows = 0;
    _number_input_rows += rows;
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    _state->update_num_rows_load_total(rows);
    _state->update_num_bytes_load_total(bytes);
    DorisMetrics::instance()->load_rows->increment(rows);
    DorisMetrics::instance()->load_bytes->increment(bytes);

    _row_distribution_watch.start();
    RETURN_IF_ERROR(_row_distribution.generate_rows_distribution(
            input_block, block, filtered_rows, has_filtered_rows, _row_part_tablet_ids,
            _number_input_rows));

    ChannelDistributionPayloadVec channel_to_payload;

    channel_to_payload.resize(_channels.size());
    _generate_index_channels_payloads(_row_part_tablet_ids, channel_to_payload);
    _row_distribution_watch.stop();

    // Add block to node channel
    for (size_t i = 0; i < _channels.size(); i++) {
        for (const auto& entry : channel_to_payload[i]) {
            // if this node channel is already failed, this add_row will be skipped
            // entry.second is a [row -> tablet] mapping
            auto st = entry.first->add_block(block.get(), &entry.second);
            if (!st.ok()) {
                _channels[i]->mark_as_failed(entry.first, st.to_string());
            }
        }
    }

    // check intolerable failure
    for (const auto& index_channel : _channels) {
        RETURN_IF_ERROR(index_channel->check_intolerable_failure());
    }

    g_sink_write_bytes << bytes;
    g_sink_write_rows << rows;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
