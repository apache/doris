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

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <utility>

#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "util/bitmap.h"
#include "util/thread_pool.hpp"

#include "gen_cpp/Types_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"

namespace doris {

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_)
        : id(pid), index_id(index_id_) { }

    ~TabletsChannelKey() noexcept { }

    bool operator==(const TabletsChannelKey& rhs) const noexcept {
        return index_id == rhs.index_id && id == rhs.id;
    }

    std::string to_string() const;
};

struct TabletsChannelKeyHasher {
    std::size_t operator()(const TabletsChannelKey& key) const {
        size_t seed = key.id.hash();
        return doris::HashUtil::hash(&key.index_id, sizeof(key.index_id), seed);
    }
};

class DeltaWriter;
class MemTable;
class MemTableFlushExecutor;
class OlapTableSchemaParam;

// channel that process all data for this load
class TabletsChannel {
public:
    TabletsChannel(const TabletsChannelKey& key, MemTableFlushExecutor* flush_executor);

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
    // id of this load channel
    TabletsChannelKey _key;
    MemTableFlushExecutor* _flush_executor;

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

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key);

} // end namespace
