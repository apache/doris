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

#include "runtime/load_channel.h"

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include "bvar/bvar.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/tablets_channel.h"

namespace doris {

bvar::Adder<int64_t> g_loadchannel_cnt("loadchannel_cnt");

LoadChannel::LoadChannel(const UniqueId& load_id, int64_t timeout_s, bool is_high_priority,
                         const std::string& sender_ip, int64_t backend_id, bool enable_profile)
        : _load_id(load_id),
          _timeout_s(timeout_s),
          _is_high_priority(is_high_priority),
          _sender_ip(sender_ip),
          _backend_id(backend_id),
          _enable_profile(enable_profile) {
    g_loadchannel_cnt << 1;
    // _last_updated_time should be set before being inserted to
    // _load_channels in load_channel_mgr, or it may be erased
    // immediately by gc thread.
    _last_updated_time.store(time(nullptr));
    _init_profile();
}

LoadChannel::~LoadChannel() {
    g_loadchannel_cnt << -1;
    LOG(INFO) << "load channel removed"
              << " load_id=" << _load_id << ", is high priority=" << _is_high_priority
              << ", sender_ip=" << _sender_ip;
}

void LoadChannel::_init_profile() {
    _profile = std::make_unique<RuntimeProfile>("LoadChannels");
    _mgr_add_batch_timer = ADD_TIMER(_profile, "LoadChannelMgrAddBatchTime");
    _handle_mem_limit_timer = ADD_TIMER(_profile, "HandleMemLimitTime");
    _self_profile =
            _profile->create_child(fmt::format("LoadChannel load_id={} (host={}, backend_id={})",
                                               _load_id.to_string(), _sender_ip, _backend_id),
                                   true, true);
    _add_batch_number_counter = ADD_COUNTER(_self_profile, "NumberBatchAdded", TUnit::UNIT);
    _peak_memory_usage_counter = ADD_COUNTER(_self_profile, "PeakMemoryUsage", TUnit::BYTES);
    _add_batch_timer = ADD_TIMER(_self_profile, "AddBatchTime");
    _handle_eos_timer = ADD_CHILD_TIMER(_self_profile, "HandleEosTime", "AddBatchTime");
    _add_batch_times = ADD_COUNTER(_self_profile, "AddBatchTimes", TUnit::UNIT);
}

Status LoadChannel::open(const PTabletWriterOpenRequest& params) {
    int64_t index_id = params.index_id();
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it != _tablets_channels.end()) {
            channel = it->second;
        } else {
            // create a new tablets channel
            TabletsChannelKey key(params.id(), index_id);
            channel = std::make_shared<TabletsChannel>(key, _load_id, _is_high_priority,
                                                       _self_profile);
            {
                std::lock_guard<SpinLock> l(_tablets_channels_lock);
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

Status LoadChannel::_get_tablets_channel(std::shared_ptr<TabletsChannel>& channel,
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
                              PTabletWriterAddBlockResult* response) {
    SCOPED_TIMER(_add_batch_timer);
    COUNTER_UPDATE(_add_batch_times, 1);
    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<TabletsChannel> channel;
    bool is_finished = false;
    Status st = _get_tablets_channel(channel, is_finished, index_id);
    if (!st.ok() || is_finished) {
        return st;
    }

    // 2. add block to tablets channel
    if (request.has_block()) {
        RETURN_IF_ERROR(channel->add_batch(request, response));
        _add_batch_number_counter->update(1);
    }

    // 3. handle eos
    if (request.has_eos() && request.eos()) {
        st = _handle_eos(channel, request, response);
        _report_profile(response);
        if (!st.ok()) {
            return st;
        }
    } else if (_add_batch_number_counter->value() % 100 == 1) {
        _report_profile(response);
    }
    _last_updated_time.store(time(nullptr));
    return st;
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
        std::lock_guard<SpinLock> l(_tablets_channels_lock);
        for (auto& it : _tablets_channels) {
            it.second->refresh_profile();
        }
    }

    TRuntimeProfileTree tprofile;
    ThriftSerializer ser(false, 4096);
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    std::lock_guard<SpinLock> l(_profile_serialize_lock);
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

Status LoadChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& it : _tablets_channels) {
        static_cast<void>(it.second->cancel());
    }
    return Status::OK();
}

} // namespace doris
