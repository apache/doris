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

#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "util/bitmap.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace google {
namespace protobuf {
template <typename Element>
class RepeatedField;
template <typename Key, typename T>
class Map;
template <typename T>
class RepeatedPtrField;
} // namespace protobuf
} // namespace google

namespace doris {
class PSlaveTabletNodes;
class PSuccessSlaveTabletNodeIds;
class PTabletError;
class PTabletInfo;
class PTabletWriterOpenRequest;
class PUniqueId;
class TupleDescriptor;
class OpenPartitionRequest;

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept {}

    bool operator==(const TabletsChannelKey& rhs) const noexcept {
        return index_id == rhs.index_id && id == rhs.id;
    }

    std::string to_string() const;
};

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key);

class DeltaWriter;
class MemTableWriter;
class OlapTableSchemaParam;
class LoadChannel;

// Write channel for a particular (load, index).
class TabletsChannel {
public:
    TabletsChannel(const TabletsChannelKey& key, const UniqueId& load_id, bool is_high_priority,
                   RuntimeProfile* profile);

    ~TabletsChannel();

    Status open(const PTabletWriterOpenRequest& request);
    // open + open writers
    Status incremental_open(const PTabletWriterOpenRequest& params);

    // no-op when this channel has been closed or cancelled
    Status add_batch(const PTabletWriterAddBlockRequest& request,
                     PTabletWriterAddBlockResult* response);

    // Mark sender with 'sender_id' as closed.
    // If all senders are closed, close this channel, set '*finished' to true, update 'tablet_vec'
    // to include all tablets written in this channel.
    // no-op when this channel has been closed or cancelled
    Status
    close(LoadChannel* parent, int sender_id, int64_t backend_id, bool* finished,
          const google::protobuf::RepeatedField<int64_t>& partition_ids,
          google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
          google::protobuf::RepeatedPtrField<PTabletError>* tablet_error,
          const google::protobuf::Map<int64_t, PSlaveTabletNodes>& slave_tablet_nodes,
          google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids,
          const bool write_single_replica);

    // no-op when this channel has been closed or cancelled
    Status cancel();

    void refresh_profile();

private:
    template <typename Request>
    Status _get_current_seq(int64_t& cur_seq, const Request& request);

    // open all writer
    Status _open_all_writers(const PTabletWriterOpenRequest& request);

    // deal with DeltaWriter commit_txn(), add tablet to list for return.
    void _commit_txn(DeltaWriter* writer,
                     google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
                     google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
                     PSlaveTabletNodes slave_tablet_nodes, const bool write_single_replica);

    void _add_broken_tablet(int64_t tablet_id);
    void _add_error_tablet(google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
                           int64_t tablet_id, Status error) const;
    bool _is_broken_tablet(int64_t tablet_id);
    void _init_profile(RuntimeProfile* profile);

    // id of this load channel
    TabletsChannelKey _key;

    // make execute sequence
    std::mutex _lock;

    SpinLock _tablet_writers_lock;

    enum State {
        kInitialized,
        kOpened,
        kFinished // closed or cancelled
    };
    State _state;

    UniqueId _load_id;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;

    TupleDescriptor* _tuple_desc = nullptr;

    // next sequence we expect
    int _num_remaining_senders = 0;
    std::vector<int64_t> _next_seqs;
    Bitmap _closed_senders;
    // status to return when operate on an already closed/cancelled channel
    // currently it's OK.
    Status _close_status;

    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, DeltaWriter*> _tablet_writers;
    // broken tablet ids.
    // If a tablet write fails, it's id will be added to this set.
    // So that following batch will not handle this tablet anymore.
    std::unordered_set<int64_t> _broken_tablets;

    std::shared_mutex _broken_tablets_lock;

    std::unordered_set<int64_t> _reducing_tablets;

    std::unordered_set<int64_t> _partition_ids;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    bool _is_high_priority = false;

    bool _write_single_replica = false;

    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _add_batch_number_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _write_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _flush_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_write_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_flush_memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _slave_replica_timer = nullptr;
    RuntimeProfile::Counter* _add_batch_timer = nullptr;
    RuntimeProfile::Counter* _write_block_timer = nullptr;
};

template <typename Request>
Status TabletsChannel::_get_current_seq(int64_t& cur_seq, const Request& request) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state != kOpened) {
        return _state == kFinished ? _close_status
                                   : Status::InternalError("TabletsChannel {} state: {}",
                                                           _key.to_string(), _state);
    }
    cur_seq = _next_seqs[request.sender_id()];
    // check packet
    if (request.packet_seq() > cur_seq) {
        LOG(WARNING) << "lost data packet, expect_seq=" << cur_seq
                     << ", recept_seq=" << request.packet_seq();
        return Status::InternalError("lost data packet");
    }
    return Status::OK();
}

} // namespace doris
