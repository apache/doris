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

namespace google::protobuf {
template <typename Element>
class RepeatedField;
template <typename Key, typename T>
class Map;
template <typename T>
class RepeatedPtrField;
} // namespace google::protobuf

namespace doris {
class PSlaveTabletNodes;
class PSuccessSlaveTabletNodeIds;
class PTabletError;
class PTabletInfo;
class PTabletWriterOpenRequest;
class PTabletWriterAddBlockRequest;
class PTabletWriterAddBlockResult;
class PUniqueId;
class TupleDescriptor;
class OpenPartitionRequest;
class StorageEngine;

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept {
        return index_id == rhs.index_id && id == rhs.id;
    }

    std::string to_string() const;
};

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key);

class BaseDeltaWriter;
class MemTableWriter;
class OlapTableSchemaParam;
class LoadChannel;

// Write channel for a particular (load, index).
class BaseTabletsChannel {
public:
    BaseTabletsChannel(const TabletsChannelKey& key, const UniqueId& load_id, bool is_high_priority,
                       RuntimeProfile* profile);

    virtual ~BaseTabletsChannel();

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
    virtual Status close(LoadChannel* parent, const PTabletWriterAddBlockRequest& req,
                         PTabletWriterAddBlockResult* res, bool* finished) = 0;

    // no-op when this channel has been closed or cancelled
    virtual Status cancel();

    void refresh_profile();

protected:
    Status _get_current_seq(int64_t& cur_seq, const PTabletWriterAddBlockRequest& request);

    // open all writer
    Status _open_all_writers(const PTabletWriterOpenRequest& request);

    void _add_broken_tablet(int64_t tablet_id);
    // thread-unsafe, add a shared lock for `_tablet_writers_lock` if needed
    bool _is_broken_tablet(int64_t tablet_id) const;
    void _add_error_tablet(google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
                           int64_t tablet_id, Status error) const;
    void _build_tablet_to_rowidxs(
            const PTabletWriterAddBlockRequest& request,
            std::unordered_map<int64_t /* tablet_id */, std::vector<uint32_t> /* row index */>*
                    tablet_to_rowidxs);
    virtual void _init_profile(RuntimeProfile* profile);

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
    std::unique_ptr<OlapTableSchemaParam> _schema;

    TupleDescriptor* _tuple_desc = nullptr;

    // next sequence we expect
    int _num_remaining_senders = 0;
    std::vector<int64_t> _next_seqs;
    Bitmap _closed_senders;
    // status to return when operate on an already closed/cancelled channel
    // currently it's OK.
    Status _close_status;

    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::unique_ptr<BaseDeltaWriter>> _tablet_writers;
    // broken tablet ids.
    // If a tablet write fails, it's id will be added to this set.
    // So that following batch will not handle this tablet anymore.
    std::unordered_set<int64_t> _broken_tablets;

    std::shared_mutex _broken_tablets_lock;

    std::unordered_set<int64_t> _reducing_tablets;

    std::unordered_set<int64_t> _partition_ids;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    bool _is_high_priority = false;

    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _add_batch_number_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _write_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _flush_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_write_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _max_tablet_flush_memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _add_batch_timer = nullptr;
    RuntimeProfile::Counter* _write_block_timer = nullptr;
    RuntimeProfile::Counter* _incremental_open_timer = nullptr;
};

class DeltaWriter;

// `StorageEngine` mixin for `BaseTabletsChannel`
class TabletsChannel final : public BaseTabletsChannel {
public:
    TabletsChannel(StorageEngine& engine, const TabletsChannelKey& key, const UniqueId& load_id,
                   bool is_high_priority, RuntimeProfile* profile);

    ~TabletsChannel() override;

    Status close(LoadChannel* parent, const PTabletWriterAddBlockRequest& req,
                 PTabletWriterAddBlockResult* res, bool* finished) override;

    Status cancel() override;

private:
    void _init_profile(RuntimeProfile* profile) override;

    // deal with DeltaWriter commit_txn(), add tablet to list for return.
    void _commit_txn(DeltaWriter* writer, const PTabletWriterAddBlockRequest& req,
                     PTabletWriterAddBlockResult* res);

    StorageEngine& _engine;
    bool _write_single_replica = false;
    RuntimeProfile::Counter* _slave_replica_timer = nullptr;
};

} // namespace doris
