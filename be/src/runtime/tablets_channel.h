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

#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/thread_context.h"
#include "util/bitmap.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"
#include "gutil/strings/substitute.h"

#include "vec/core/block.h"
#include "olap/delta_writer.h"

namespace doris {

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
class OlapTableSchemaParam;

// Write channel for a particular (load, index).
class TabletsChannel {
public:
    TabletsChannel(const TabletsChannelKey& key, bool is_high_priority, bool is_vec);

    ~TabletsChannel();

    Status open(const PTabletWriterOpenRequest& request);

    // no-op when this channel has been closed or cancelled
    template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
    Status add_batch(const TabletWriterAddRequest& request, TabletWriterAddResult* response);

    // Mark sender with 'sender_id' as closed.
    // If all senders are closed, close this channel, set '*finished' to true, update 'tablet_vec'
    // to include all tablets written in this channel.
    // no-op when this channel has been closed or cancelled
    Status close(int sender_id, int64_t backend_id, bool* finished,
                 const google::protobuf::RepeatedField<int64_t>& partition_ids,
                 google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    // no-op when this channel has been closed or cancelled
    Status cancel();

    // upper application may call this to try to reduce the mem usage of this channel.
    // eg. flush the largest memtable immediately.
    // return Status::OK if mem is reduced.
    // no-op when this channel has been closed or cancelled
    Status reduce_mem_usage(int64_t mem_limit);

    int64_t mem_consumption() const { return _mem_tracker->consumption(); }

private:
    template <typename Request>
    Status _get_current_seq(int64_t& cur_seq, const Request& request);

    // open all writer
    Status _open_all_writers(const PTabletWriterOpenRequest& request);

    // id of this load channel
    TabletsChannelKey _key;

    // make execute sequence
    std::mutex _lock;

    enum State {
        kInitialized,
        kOpened,
        kFinished // closed or cancelled
    };
    State _state;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;

    TupleDescriptor* _tuple_desc = nullptr;
    // row_desc used to construct
    RowDescriptor* _row_desc = nullptr;

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

    std::unordered_set<int64_t> _partition_ids;

    std::shared_ptr<MemTracker> _mem_tracker;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    bool _is_high_priority = false;

    bool _is_vec = false;
};

template <typename Request>
Status TabletsChannel::_get_current_seq(int64_t& cur_seq, const Request& request) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state != kOpened) {
        return _state == kFinished
                       ? _close_status
                       : Status::InternalError(strings::Substitute("TabletsChannel $0 state: $1",
                                                                   _key.to_string(), _state));
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

template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
Status TabletsChannel::add_batch(const TabletWriterAddRequest& request,
                                 TabletWriterAddResult* response) {
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    int64_t cur_seq = 0;

    auto status = _get_current_seq(cur_seq, request);
    if (UNLIKELY(!status.ok())) {
        return status;
    }

    if (request.packet_seq() < cur_seq) {
        LOG(INFO) << "packet has already recept before, expect_seq=" << cur_seq
                  << ", recept_seq=" << request.packet_seq();
        return Status::OK();
    }

    std::unordered_map<int64_t /* tablet_id */, std::vector<int> /* row index */> tablet_to_rowidxs;
    for (int i = 0; i < request.tablet_ids_size(); ++i) {
        int64_t tablet_id = request.tablet_ids(i);
        if (_broken_tablets.find(tablet_id) != _broken_tablets.end()) {
            // skip broken tablets
            continue;
        }
        auto it = tablet_to_rowidxs.find(tablet_id);
        if (it == tablet_to_rowidxs.end()) {
            tablet_to_rowidxs.emplace(tablet_id, std::initializer_list<int> {i});
        } else {
            it->second.emplace_back(i);
        }
    }

    auto get_send_data = [&]() {
        if constexpr (std::is_same_v<TabletWriterAddRequest, PTabletWriterAddBatchRequest>) {
            return RowBatch(*_row_desc, request.row_batch());
        } else {
            return vectorized::Block(request.block());
        }
    };

    auto send_data = get_send_data();
    google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors =
            response->mutable_tablet_errors();
    for (const auto& tablet_to_rowidxs_it : tablet_to_rowidxs) {
        auto tablet_writer_it = _tablet_writers.find(tablet_to_rowidxs_it.first);
        if (tablet_writer_it == _tablet_writers.end()) {
            return Status::InternalError(strings::Substitute(
                    "unknown tablet to append data, tablet=$0", tablet_to_rowidxs_it.first));
        }

        Status st = tablet_writer_it->second->write(&send_data, tablet_to_rowidxs_it.second);
        if (!st.ok()) {
            auto err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2",
                    tablet_to_rowidxs_it.first, _txn_id, st.code());
            LOG(WARNING) << err_msg;
            PTabletError* error = tablet_errors->Add();
            error->set_tablet_id(tablet_to_rowidxs_it.first);
            error->set_msg(err_msg);
            _broken_tablets.insert(tablet_to_rowidxs_it.first);
            // continue write to other tablet.
            // the error will return back to sender.
        }
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _next_seqs[request.sender_id()] = cur_seq + 1;
    }
    return Status::OK();
}
} // namespace doris