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

#include "runtime/tablet_writer_mgr.h"

#include <cstdint>
#include <unordered_map>
#include <utility>

#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/bitmap.h"
#include "util/stopwatch.hpp"
#include "olap/delta_writer.h"
#include "olap/lru_cache.h"

namespace doris {

// channel that process all data for this load
class TabletsChannel {
public:
    TabletsChannel(const TabletsChannelKey& key) : _key(key), _closed_senders(64) { }
    ~TabletsChannel();

    Status open(const PTabletWriterOpenRequest& params);

    Status add_batch(const PTabletWriterAddBatchRequest& batch);

    Status close(int sender_id, bool* finished,
        const google::protobuf::RepeatedField<int64_t>& partition_ids,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    time_t last_updated_time() {
        return _last_updated_time;
    }

private:
    // open all writer
    Status _open_all_writers(const PTabletWriterOpenRequest& params);

private:
    // id of this load channel, just for 
    TabletsChannelKey _key;

    // make execute sequece
    std::mutex _lock;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    // row_desc used to construct
    RowDescriptor* _row_desc = nullptr;
    bool _opened = false;

    // next sequence we expect
    int _num_remaining_senders = 0;
    std::vector<int64_t> _next_seqs;
    Bitmap _closed_senders;
    Status _close_status;

    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, DeltaWriter*> _tablet_writers;

    std::unordered_set<int64_t> _partition_ids;

    // TODO(zc): to add this tracker to somewhere
    MemTracker _mem_tracker;

    //use to erase timeout TabletsChannel in _tablets_channels
    time_t _last_updated_time;
};

TabletsChannel::~TabletsChannel() {
    for (auto& it : _tablet_writers) {
        delete it.second;
    }
    delete _row_desc;
    delete _schema;
}

Status TabletsChannel::open(const PTabletWriterOpenRequest& params) {
    std::lock_guard<std::mutex> l(_lock);
    if (_opened) {
        // Normal case, already open by other sender
        return Status::OK();
    }
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(params.schema()));
    _tuple_desc = _schema->tuple_desc();
    _row_desc = new RowDescriptor(_tuple_desc, false);

    _num_remaining_senders = params.num_senders();
    _next_seqs.resize(_num_remaining_senders, 0);
    _closed_senders.Reset(_num_remaining_senders);

    RETURN_IF_ERROR(_open_all_writers(params));
    
    _opened = true;
    _last_updated_time = time(nullptr);
    return Status::OK();
}

Status TabletsChannel::add_batch(const PTabletWriterAddBatchRequest& params) {
    DCHECK(params.tablet_ids_size() == params.row_batch().num_rows());
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_opened);
    auto next_seq = _next_seqs[params.sender_id()];
    // check packet
    if (params.packet_seq() < next_seq) {
        LOG(INFO) << "packet has already recept before, expect_seq=" << next_seq
            << ", recept_seq=" << params.packet_seq();
        return Status::OK();
    } else if (params.packet_seq() > next_seq) {
        LOG(WARNING) << "lost data packet, expect_seq=" << next_seq
            << ", recept_seq=" << params.packet_seq();
        return Status::InternalError("lost data packet");
    }

    RowBatch row_batch(*_row_desc, params.row_batch(), &_mem_tracker);

    // iterator all data
    for (int i = 0; i < params.tablet_ids_size(); ++i) {
        auto tablet_id = params.tablet_ids(i);
        auto it = _tablet_writers.find(tablet_id);
        if (it == std::end(_tablet_writers)) {
            std::stringstream ss;
            ss << "unknown tablet to append data, tablet=" << tablet_id;
            return Status::InternalError(ss.str());
        }
        auto st = it->second->write(row_batch.get_row(i)->get_tuple(0));
        if (st != OLAP_SUCCESS) {
            LOG(WARNING) << "tablet writer writer failed, tablet_id=" << it->first
                << ", transaction_id=" << _txn_id;
            return Status::InternalError("tablet writer write failed");
        }
    }
    _next_seqs[params.sender_id()]++;
    _last_updated_time = time(nullptr);
    return Status::OK();
}

Status TabletsChannel::close(int sender_id, bool* finished,
        const google::protobuf::RepeatedField<int64_t>& partition_ids,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    std::lock_guard<std::mutex> l(_lock);
    if (_closed_senders.Get(sender_id)) {
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }
    for (auto pid : partition_ids) {
        _partition_ids.emplace(pid);
    }
    _closed_senders.Set(sender_id, true);
    _num_remaining_senders--;
    *finished = (_num_remaining_senders == 0);
    if (*finished) {
        // All senders are closed
        for (auto& it : _tablet_writers) {
            if (_partition_ids.count(it.second->partition_id()) > 0) {
                auto st = it.second->close(tablet_vec);
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << it.first
                        << ", transaction_id=" << _txn_id;
                    _close_status = Status::InternalError("close tablet writer failed");
                    return _close_status;
                }
            } else {
                auto st = it.second->cancel();
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << it.first
                        << ", transaction_id=" << _txn_id;
                }
            }
        }
    }
    return Status::OK();
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* columns = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            columns = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (columns == nullptr) {
        std::stringstream ss;
        ss << "unknown index id, key=" << _key;
        return Status::InternalError(ss.str());
    }
    for (auto& tablet : params.tablets()) {
        WriteRequest request;
        request.tablet_id = tablet.tablet_id();
        request.schema_hash = schema_hash;
        request.write_type = LOAD;
        request.txn_id = _txn_id;
        request.partition_id = tablet.partition_id();
        request.load_id = params.id();
        request.need_gen_rollup = params.need_gen_rollup();
        request.tuple_desc = _tuple_desc;

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&request, &writer);
        if (st != OLAP_SUCCESS) {
            LOG(WARNING) << "open delta writer failed, tablet_id=" << tablet.tablet_id()
                << ", txn_id=" << _txn_id
                << ", partition_id=" << tablet.partition_id()
                << ", status=" << st;
            return Status::InternalError("open tablet writer failed");
        }
        _tablet_writers.emplace(tablet.tablet_id(), writer);
    }
    DCHECK(_tablet_writers.size() == params.tablets_size());
    return Status::OK();
}

TabletWriterMgr::TabletWriterMgr(ExecEnv* exec_env) :_exec_env(exec_env) {
    _tablets_channels.init(2011);
    _lastest_success_channel = new_lru_cache(1024);
}

TabletWriterMgr::~TabletWriterMgr() {
    delete _lastest_success_channel;
}

Status TabletWriterMgr::open(const PTabletWriterOpenRequest& params) {
    TabletsChannelKey key(params.id(), params.index_id());
    LOG(INFO) << "open tablets writer channel: " << key;
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto val = _tablets_channels.seek(key);
        if (val != nullptr) {
            channel = *val;
        } else {
            // create a new 
            channel.reset(new TabletsChannel(key));
            _tablets_channels.insert(key, channel);
        }
    }
    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {
}

Status TabletWriterMgr::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    TabletsChannelKey key(request.id(), request.index_id());
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto value = _tablets_channels.seek(key);
        if (value == nullptr) {
            auto handle = _lastest_success_channel->lookup(key.to_string());
            // success only when eos be true
            if (handle != nullptr && request.has_eos() && request.eos()) {
                _lastest_success_channel->release(handle);
                return Status::OK();
            }
            std::stringstream ss;
            ss << "TabletWriter add batch with unknown id, key=" << key;
            return Status::InternalError(ss.str());
        }
        channel = *value;
    }
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request));
    }
    Status st;
    if (request.has_eos() && request.eos()) {
        bool finished = false;
        st = channel->close(request.sender_id(), &finished, request.partition_ids(), tablet_vec);
        if (!st.ok()) {
            LOG(WARNING) << "channle close failed, key=" << key
                << ", sender_id=" << request.sender_id()
                << ", err_msg=" << st.get_error_msg();
        }
        if (finished) {
            std::lock_guard<std::mutex> l(_lock);
            _tablets_channels.erase(key);
            if (st.ok()) {
                auto handle = _lastest_success_channel->insert(
                    key.to_string(), nullptr, 1, dummy_deleter);
                _lastest_success_channel->release(handle);
            }
        }
    }
    return st;
}

Status TabletWriterMgr::cancel(const PTabletWriterCancelRequest& params) {
    TabletsChannelKey key(params.id(), params.index_id());
    {
        std::lock_guard<std::mutex> l(_lock);
        _tablets_channels.erase(key);
    }
    return Status::OK();
}

Status TabletWriterMgr::start_bg_worker() {
    _tablets_channel_clean_thread = std::thread(
        [this] {
            #ifdef GOOGLE_PROFILER
                ProfilerRegisterThread();
            #endif

            uint32_t interval = 60;
            while (true) {
                _start_tablets_channel_clean();
                sleep(interval);
            }
        });
    _tablets_channel_clean_thread.detach();
    return Status::OK();
}

Status TabletWriterMgr::_start_tablets_channel_clean() {
    const int32_t max_alive_time = config::streaming_load_rpc_max_alive_time_sec;
    time_t now = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        std::vector<TabletsChannelKey> need_delete_keys;

        for (auto& kv : _tablets_channels) {
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= max_alive_time) {
                need_delete_keys.emplace_back(kv.first);
            }
        }

        for(auto& key: need_delete_keys) {
            _tablets_channels.erase(key);
            LOG(INFO) << "erase timeout tablets channel: " << key;
        }
    }
    return Status::OK();
}

std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

}
