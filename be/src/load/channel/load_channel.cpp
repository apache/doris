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

#include "load/channel/load_channel.h"

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>

#include "cloud/cloud_tablets_channel.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "load/channel/tablets_channel.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "storage/storage_engine.h"
#include "util/debug_points.h"

namespace doris {

bvar::Adder<int64_t> g_loadchannel_cnt("loadchannel_cnt");

static void copy_final_tablet_result(PTabletWriterAddBlockResult* response,
                                     const PTabletWriterAddBlockResult& result, bool fanout) {
    response->mutable_status()->CopyFrom(result.status());
    response->mutable_tablet_vec()->CopyFrom(result.tablet_vec());
    response->mutable_tablet_errors()->CopyFrom(result.tablet_errors());
    response->set_final_tablet_result_fanout(fanout);
}

LoadChannel::LoadChannel(const UniqueId& load_id, int64_t timeout_s, bool is_high_priority,
                         std::string sender_ip, int64_t backend_id, bool enable_profile,
                         int64_t wg_id)
        : _load_id(load_id),
          _timeout_s(timeout_s),
          _is_high_priority(is_high_priority),
          _sender_ip(std::move(sender_ip)),
          _backend_id(backend_id),
          _enable_profile(enable_profile) {
    std::shared_ptr<QueryContext> query_context =
            ExecEnv::GetInstance()->fragment_mgr()->get_query_ctx(_load_id.to_thrift());

    if (query_context != nullptr) {
        _resource_ctx = query_context->resource_ctx();
    } else {
        _resource_ctx = ResourceContext::create_shared();
        _resource_ctx->task_controller()->set_task_id(_load_id.to_thrift());
        // when memtable on sink is not enabled, load can not find queryctx
        std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD,
                fmt::format("(FromLoadChannel)Load#Id={}", _load_id.to_string()));
        _resource_ctx->memory_context()->set_mem_tracker(mem_tracker);
        WorkloadGroupPtr wg_ptr = nullptr;
        if (wg_id > 0) {
            std::vector<uint64_t> id_set;
            id_set.push_back(wg_id);
            wg_ptr = ExecEnv::GetInstance()->workload_group_mgr()->get_group(id_set);
            _resource_ctx->set_workload_group(wg_ptr);
        }
    }

    g_loadchannel_cnt << 1;
    // _last_updated_time should be set before being inserted to
    // _load_channels in load_channel_mgr, or it may be erased
    // immediately by gc thread.
    _last_updated_time.store(time(nullptr));
    if (enable_profile) {
        _init_profile();
    }
}

LoadChannel::~LoadChannel() {
    _cancel_final_tablet_result_waiters(Status::Cancelled("Load channel removed"));
    g_loadchannel_cnt << -1;
    std::stringstream rows_str;
    for (const auto& entry : _tablets_channels_rows) {
        rows_str << ", index id: " << entry.first << ", total_received_rows: " << entry.second.first
                 << ", num_rows_filtered: " << entry.second.second;
    }
    LOG(INFO) << "load channel removed"
              << " load_id=" << _load_id << ", is high priority=" << _is_high_priority
              << ", sender_ip=" << _sender_ip << rows_str.str();
}

void LoadChannel::_init_profile() {
    DCHECK(_enable_profile);
    _profile = std::make_unique<RuntimeProfile>("LoadChannels");
    _mgr_add_batch_timer = ADD_TIMER(_profile, "LoadChannelMgrAddBatchTime");
    _handle_mem_limit_timer = ADD_TIMER(_profile, "HandleMemLimitTime");
    _self_profile =
            _profile->create_child(fmt::format("LoadChannel load_id={} (host={}, backend_id={})",
                                               _load_id.to_string(), _sender_ip, _backend_id),
                                   true, true);
    _add_batch_number_counter = ADD_COUNTER(_self_profile, "NumberBatchAdded", TUnit::UNIT);
    _add_batch_timer = ADD_TIMER(_self_profile, "AddBatchTime");
    _handle_eos_timer = ADD_CHILD_TIMER(_self_profile, "HandleEosTime", "AddBatchTime");
    _add_batch_times = ADD_COUNTER(_self_profile, "AddBatchTimes", TUnit::UNIT);
}

Status LoadChannel::open(const PTabletWriterOpenRequest& params) {
    if (config::is_cloud_mode() && params.txn_expiration() <= 0) {
        return Status::InternalError(
                "The txn expiration of PTabletWriterOpenRequest is invalid, value={}",
                params.txn_expiration());
    }
    if (_resource_ctx->workload_group() != nullptr) {
        RETURN_IF_ERROR(_resource_ctx->workload_group()->add_resource_ctx(
                _resource_ctx->task_controller()->task_id(), _resource_ctx));
    }
    SCOPED_ATTACH_TASK(_resource_ctx);

    int64_t index_id = params.index_id();
    std::shared_ptr<BaseTabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it != _tablets_channels.end()) {
            channel = it->second;
        } else {
            // just for VLOG
            if (_txn_id == 0) [[unlikely]] {
                _txn_id = params.txn_id();
            }
            // create a new tablets channel
            TabletsChannelKey key(params.id(), index_id);
            BaseStorageEngine& engine = ExecEnv::GetInstance()->storage_engine();
            if (config::is_cloud_mode()) {
                channel = std::make_shared<CloudTabletsChannel>(engine.to_cloud(), key, _load_id,
                                                                _is_high_priority, _self_profile);
            } else {
                channel = std::make_shared<TabletsChannel>(engine.to_local(), key, _load_id,
                                                           _is_high_priority, _self_profile);
            }
            {
                std::lock_guard<std::mutex> lt(_tablets_channels_lock);
                _tablets_channels.insert({index_id, channel});
            }
        }
    }

    if (params.is_incremental()) {
        // incremental open would ensure not to open tablet repeatedly
        RETURN_IF_ERROR(channel->incremental_open(params));
    } else {
        RETURN_IF_ERROR(channel->open(params));
    }

    _opened = true;
    _last_updated_time.store(time(nullptr));
    return Status::OK();
}

Status LoadChannel::_get_tablets_channel(std::shared_ptr<BaseTabletsChannel>& channel,
                                         bool& is_finished, const int64_t index_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _tablets_channels.find(index_id);
    if (it == _tablets_channels.end()) {
        if (_finished_channel_ids.find(index_id) != _finished_channel_ids.end()) {
            // this channel is already finished, just return OK
            is_finished = true;
            return Status::OK();
        }
        std::stringstream ss;
        ss << "load channel " << _load_id << " add batch with unknown index id: " << index_id;
        return Status::InternalError(ss.str());
    }

    is_finished = false;
    channel = it->second;
    return Status::OK();
}

Status LoadChannel::add_batch(const PTabletWriterAddBlockRequest& request,
                              PTabletWriterAddBlockResult* response,
                              google::protobuf::Closure** done) {
    DBUG_EXECUTE_IF("LoadChannel.add_batch.failed",
                    { return Status::InternalError("fault injection"); });
    SCOPED_TIMER(_add_batch_timer);
    if (_enable_profile) {
        COUNTER_UPDATE(_add_batch_times, 1);
    }
    SCOPED_ATTACH_TASK(_resource_ctx);
    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<BaseTabletsChannel> channel;
    bool is_finished = false;
    Status st = _get_tablets_channel(channel, is_finished, index_id);
    if (!st.ok() || is_finished) {
        if (st.ok() && request.eos() && request.need_final_tablet_result()) {
            _defer_or_copy_final_tablet_result(index_id, request.sender_id(), response, done);
        }
        return st;
    }

    // 2. add block to tablets channel
    if (request.has_block()) {
        RETURN_IF_ERROR(channel->add_batch(request, response));
        if (_enable_profile) {
            _add_batch_number_counter->update(1);
        }
    }

    // 3. handle eos
    // if channel is incremental, maybe hang on close until all close request arrived.
    if (request.has_eos() && request.eos()) {
        bool finished = false;
        st = _handle_eos(channel.get(), request, response, &finished);
        _report_profile(response);
        if (finished && need_final_tablet_result()) {
            st.to_protobuf(response->mutable_status());
            _publish_final_tablet_result(index_id, request.sender_id(), *response);
            if (request.need_final_tablet_result()) {
                _defer_or_copy_final_tablet_result(index_id, request.sender_id(), response, done);
            }
        } else if (request.need_final_tablet_result() && st.ok()) {
            st.to_protobuf(response->mutable_status());
            _defer_or_copy_final_tablet_result(index_id, request.sender_id(), response, done);
        }
        if (!st.ok()) {
            return st;
        }
    } else if (_enable_profile && _add_batch_number_counter->value() % 100 == 1) {
        _report_profile(response);
    }
    _last_updated_time.store(time(nullptr));
    return st;
}

Status LoadChannel::_handle_eos(BaseTabletsChannel* channel,
                                const PTabletWriterAddBlockRequest& request,
                                PTabletWriterAddBlockResult* response, bool* finished) {
    if (_enable_profile) {
        _self_profile->add_info_string("EosHost", fmt::format("{}", request.backend_id()));
    }
    auto index_id = request.index_id();

    RETURN_IF_ERROR(channel->close(this, request, response, finished));

    // for init node, we close waiting(hang on) all close request and let them return together.
    // Cross-AZ final-result fan-out replaces this busy-wait barrier and completes after the final
    // tablets-channel close result is published.
    if (request.has_hang_wait() && request.hang_wait() && !request.need_final_tablet_result()) {
        DCHECK(!channel->is_incremental_channel());
        VLOG_DEBUG << fmt::format("txn {}: reciever index {} close waiting by sender {}", _txn_id,
                                  request.index_id(), request.sender_id());
        int count = 0;
        while (!channel->is_finished()) {
            bthread_usleep(1000);
            count++;
        }
        // now maybe finished or cancelled.
        VLOG_TRACE << "reciever close wait finished!" << request.sender_id();
        if (count >= 1000 * _timeout_s) { // maybe config::streaming_load_rpc_max_alive_time_sec
            return Status::InternalError("Tablets channel didn't wait all close");
        }
    }

    if (*finished) {
        std::lock_guard<std::mutex> l(_lock);
        {
            std::lock_guard<std::mutex> lt(_tablets_channels_lock);
            _tablets_channels_rows.insert(std::make_pair(
                    index_id,
                    std::make_pair(channel->total_received_rows(), channel->num_rows_filtered())));
            _tablets_channels.erase(index_id);
        }
        LOG(INFO) << "txn " << _txn_id << " closed tablets_channel " << index_id;
        _finished_channel_ids.emplace(index_id);
    }
    return Status::OK();
}

void LoadChannel::_reserve_final_tablet_result(int64_t index_id) {
    std::lock_guard<std::mutex> lock(_final_tablet_result_lock);
    _need_final_tablet_result.store(true);
    if (_cancelled.load() && _final_tablet_result_cancel_status.ok()) {
        _final_tablet_result_cancel_status = Status::Cancelled("Load channel cancelled");
    }
    _final_tablet_results.try_emplace(index_id);
}

void LoadChannel::_defer_or_copy_final_tablet_result(int64_t index_id, int32_t sender_id,
                                                     PTabletWriterAddBlockResult* response,
                                                     google::protobuf::Closure** done) {
    std::shared_ptr<PTabletWriterAddBlockResult> tablet_result;
    bool fanout = false;
    {
        std::lock_guard<std::mutex> lock(_final_tablet_result_lock);
        _need_final_tablet_result.store(true);
        auto& state = _final_tablet_results[index_id];
        if (state.tablet_result != nullptr) {
            tablet_result = state.tablet_result;
            fanout = sender_id != state.owner_sender_id;
        } else if (!_final_tablet_result_cancel_status.ok()) {
            state.tablet_result = std::make_shared<PTabletWriterAddBlockResult>();
            _final_tablet_result_cancel_status.to_protobuf(state.tablet_result->mutable_status());
            tablet_result = state.tablet_result;
        } else {
            DORIS_CHECK(done != nullptr && *done != nullptr);
            for (const auto& error : response->tablet_errors()) {
                state.tablet_errors.try_emplace(error.tablet_id(), error);
            }
            state.waiters.push_back({response, *done});
            *done = nullptr;
            return;
        }
    }
    copy_final_tablet_result(response, *tablet_result, fanout);
}

void LoadChannel::_publish_final_tablet_result(int64_t index_id, int32_t sender_id,
                                               const PTabletWriterAddBlockResult& result) {
    std::shared_ptr<PTabletWriterAddBlockResult> tablet_result;
    std::vector<FinalTabletResultWaiter> waiters;
    {
        std::lock_guard<std::mutex> lock(_final_tablet_result_lock);
        _need_final_tablet_result.store(true);
        auto state_it = _final_tablet_results.find(index_id);
        if (state_it == _final_tablet_results.end()) {
            state_it = _final_tablet_results.emplace(index_id, FinalTabletResultState()).first;
        }
        auto& state = state_it->second;
        if (state.tablet_result != nullptr) {
            return;
        }
        state.tablet_result = std::make_shared<PTabletWriterAddBlockResult>();
        state.owner_sender_id = sender_id;
        if (_final_tablet_result_cancel_status.ok()) {
            state.tablet_result->mutable_status()->CopyFrom(result.status());
            state.tablet_result->mutable_tablet_vec()->CopyFrom(result.tablet_vec());
            for (const auto& error : result.tablet_errors()) {
                state.tablet_errors.try_emplace(error.tablet_id(), error);
            }
            for (const auto& [_, error] : state.tablet_errors) {
                state.tablet_result->add_tablet_errors()->CopyFrom(error);
            }
        } else {
            _final_tablet_result_cancel_status.to_protobuf(state.tablet_result->mutable_status());
        }
        tablet_result = state.tablet_result;
        waiters.swap(state.waiters);
    }
    for (auto& waiter : waiters) {
        copy_final_tablet_result(waiter.response, *tablet_result, true);
        waiter.done->Run();
    }
}

void LoadChannel::_cancel_final_tablet_result_waiters(const Status& reason) {
    if (!_need_final_tablet_result.load()) {
        return;
    }
    std::vector<std::pair<std::shared_ptr<PTabletWriterAddBlockResult>,
                          std::vector<FinalTabletResultWaiter>>>
            completions;
    {
        std::lock_guard<std::mutex> lock(_final_tablet_result_lock);
        if (_final_tablet_result_cancel_status.ok()) {
            _final_tablet_result_cancel_status = reason;
        }
        for (auto& [_, state] : _final_tablet_results) {
            if (state.tablet_result != nullptr) {
                continue;
            }
            state.tablet_result = std::make_shared<PTabletWriterAddBlockResult>();
            _final_tablet_result_cancel_status.to_protobuf(state.tablet_result->mutable_status());
            completions.emplace_back(state.tablet_result, std::move(state.waiters));
        }
    }
    for (auto& [result, waiters] : completions) {
        for (auto& waiter : waiters) {
            copy_final_tablet_result(waiter.response, *result, false);
            waiter.done->Run();
        }
    }
}

void LoadChannel::_report_profile(PTabletWriterAddBlockResult* response) {
    if (!_enable_profile) {
        return;
    }

    // TabletSink and LoadChannel in BE are M: N relationship,
    // Every once in a while LoadChannel will randomly return its own runtime profile to a TabletSink,
    // so usually all LoadChannel runtime profiles are saved on each TabletSink,
    // and the timeliness of the same LoadChannel profile saved on different TabletSinks is different,
    // and each TabletSink will periodically send fe reports all the LoadChannel profiles saved by itself,
    // and ensures to update the latest LoadChannel profile according to the timestamp.
    _self_profile->set_timestamp(_last_updated_time);

    {
        std::lock_guard<std::mutex> l(_tablets_channels_lock);
        for (auto& it : _tablets_channels) {
            it.second->refresh_profile();
        }
    }

    TRuntimeProfileTree tprofile;
    ThriftSerializer ser(false, 4096);
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    std::lock_guard<std::mutex> l(_profile_serialize_lock);
    _profile->to_thrift(&tprofile);
    auto st = ser.serialize(&tprofile, &len, &buf);
    if (st.ok()) {
        response->set_load_channel_profile(std::string((const char*)buf, len));
    } else {
        LOG(WARNING) << "load channel TRuntimeProfileTree serialize failed, errmsg=" << st;
    }
}

bool LoadChannel::is_finished() {
    if (!_opened) {
        return false;
    }
    std::lock_guard<std::mutex> l(_lock);
    return _tablets_channels.empty();
}

bool LoadChannel::copy_final_tablet_results(std::unordered_map<int64_t, FinalTabletResult>* results,
                                            size_t max_bytes, bool* oversized,
                                            size_t* result_bytes) const {
    std::lock_guard<std::mutex> lock(_final_tablet_result_lock);
    results->clear();
    *oversized = false;
    *result_bytes = 0;
    for (const auto& [_, state] : _final_tablet_results) {
        if (state.tablet_result == nullptr) {
            return false;
        }
        const size_t bytes = state.tablet_result->SpaceUsedLong();
        if (bytes > max_bytes - *result_bytes) {
            *oversized = true;
            return true;
        }
        *result_bytes += bytes;
    }
    for (const auto& [index_id, state] : _final_tablet_results) {
        results->emplace(index_id, FinalTabletResult {*state.tablet_result, state.owner_sender_id});
    }
    return true;
}

Status LoadChannel::cancel(const Status& reason) {
    _cancelled.store(true);
    _cancel_final_tablet_result_waiters(reason);
    {
        std::lock_guard<std::mutex> l(_lock);
        for (auto& it : _tablets_channels) {
            static_cast<void>(it.second->cancel());
        }
    }
    return Status::OK();
}

} // namespace doris
