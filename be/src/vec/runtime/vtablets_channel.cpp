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

#include "vtablets_channel.h"
#include "exec/tablet_info.h"
#include "gutil/strings/substitute.h"
#include "vec/olap/vdelta_writer.h"
#include "olap/memtable.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/doris_metrics.h"

namespace doris {

namespace vectorized {

VTabletsChannel::VTabletsChannel(const TabletsChannelKey& key,
                                 const std::shared_ptr<MemTracker>& mem_tracker,
                                 bool is_high_priority)
        : TabletsChannel(key, mem_tracker, is_high_priority) {}

Status VTabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& request) {
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        std::stringstream ss;
        ss << "unknown index id, key=" << _key;
        return Status::InternalError(ss.str());
    }
    for (auto& tablet : request.tablets()) {
        WriteRequest wrequest;
        wrequest.tablet_id = tablet.tablet_id();
        wrequest.schema_hash = schema_hash;
        wrequest.write_type = WriteType::LOAD;
        wrequest.txn_id = _txn_id;
        wrequest.partition_id = tablet.partition_id();
        wrequest.load_id = request.id();
        wrequest.need_gen_rollup = request.need_gen_rollup();
        wrequest.slots = index_slots;
        wrequest.is_high_priority = _is_high_priority;

        VDeltaWriter* writer = nullptr;
        auto st = VDeltaWriter::open(&wrequest, _mem_tracker, &writer);
        if (st != OLAP_SUCCESS) {
            std::stringstream ss;
            ss << "open delta writer failed, tablet_id=" << tablet.tablet_id()
               << ", txn_id=" << _txn_id << ", partition_id=" << tablet.partition_id()
               << ", err=" << st;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _tablet_writers.emplace(tablet.tablet_id(), writer);
    }
    _s_tablet_writer_count += _tablet_writers.size();
    DCHECK_EQ(_tablet_writers.size(), request.tablets_size());
    return Status::OK();
}

Status VTabletsChannel::add_block(const PTabletWriterAddBlockRequest& request,
                             PTabletWriterAddBlockResult* response) {
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

    Block block(request.block());

    std::unordered_map<int64_t /* tablet_id */, std::vector<int> /* row index */> tablet_to_rowidxs;
    for (int i = 0; i < request.tablet_ids_size(); ++i) {
        int64_t tablet_id = request.tablet_ids(i);
        if (_broken_tablets.find(tablet_id) != _broken_tablets.end()) {
            // skip broken tablets
            continue;
        }
        auto it = tablet_to_rowidxs.find(tablet_id);
        if (it == tablet_to_rowidxs.end()) {
            tablet_to_rowidxs.emplace(tablet_id, std::initializer_list<int>{ i });
        } else {
            it->second.emplace_back(i);
        }
    }

    google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors = response->mutable_tablet_errors();
    for (const auto& tablet_to_rowidxs_it : tablet_to_rowidxs) {
        auto tablet_writer_it = _tablet_writers.find(tablet_to_rowidxs_it.first);
        if (tablet_writer_it == _tablet_writers.end()) {
            return Status::InternalError(
                    strings::Substitute("unknown tablet to append data, tablet=$0", tablet_to_rowidxs_it.first));
        }

        OLAPStatus st = tablet_writer_it->second->write_block(&block, tablet_to_rowidxs_it.second);
        if (st != OLAP_SUCCESS) {
            auto err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2",
                    tablet_to_rowidxs_it.first, _txn_id, st);
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

} // namespace vectorized

} // namespace doris