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

#include "runtime/tablets_channel.h"

#include "exec/tablet_info.h"
#include "gutil/strings/substitute.h"
#include "olap/delta_writer.h"
#include "olap/memtable.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key,
                               const std::shared_ptr<MemTracker>& mem_tracker)
        : _key(key), _state(kInitialized), _closed_senders(64) {
    _mem_tracker = MemTracker::CreateTracker(-1, "tablets channel", mem_tracker);
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_HOOK_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });
}

TabletsChannel::~TabletsChannel() {
    _s_tablet_writer_count -= _tablet_writers.size();
    for (auto& it : _tablet_writers) {
        delete it.second;
    }
    delete _row_desc;
    delete _schema;
}

Status TabletsChannel::open(const PTabletWriterOpenRequest& params) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kOpened) {
        // Normal case, already open by other sender
        return Status::OK();
    }
    LOG(INFO) << "open tablets channel: " << _key << ", tablets num: " << params.tablets().size()
              << ", timeout(s): " << params.load_channel_timeout_s();
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

    _state = kOpened;
    return Status::OK();
}

Status TabletsChannel::add_batch(const PTabletWriterAddBatchRequest& params) {
    DCHECK(params.tablet_ids_size() == params.row_batch().num_rows());
    int64_t cur_seq;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_state != kOpened) {
            return _state == kFinished
                ? _close_status
                : Status::InternalError(strings::Substitute("TabletsChannel $0 state: $1",
                            _key.to_string(), _state));
        }
        cur_seq = _next_seqs[params.sender_id()];
        // check packet
        if (params.packet_seq() < cur_seq) {
            LOG(INFO) << "packet has already recept before, expect_seq=" << cur_seq
                << ", recept_seq=" << params.packet_seq();
            return Status::OK();
        } else if (params.packet_seq() > cur_seq) {
            LOG(WARNING) << "lost data packet, expect_seq=" << cur_seq
                << ", recept_seq=" << params.packet_seq();
            return Status::InternalError("lost data packet");
        }
    }

    RowBatch row_batch(*_row_desc, params.row_batch(), _mem_tracker.get());

    // iterator all data
    for (int i = 0; i < params.tablet_ids_size(); ++i) {
        auto tablet_id = params.tablet_ids(i);
        auto it = _tablet_writers.find(tablet_id);
        if (it == std::end(_tablet_writers)) {
            return Status::InternalError(
                    strings::Substitute("unknown tablet to append data, tablet=$0", tablet_id));
        }
        auto st = it->second->write(row_batch.get_row(i)->get_tuple(0));
        if (st != OLAP_SUCCESS) {
            const std::string& err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2", it->first,
                    _txn_id, st);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _next_seqs[params.sender_id()] = cur_seq + 1;
    }
    return Status::OK();
}

Status TabletsChannel::close(int sender_id, bool* finished,
                             const google::protobuf::RepeatedField<int64_t>& partition_ids,
                             google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    if (_closed_senders.Get(sender_id)) {
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }
    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id;
    for (auto pid : partition_ids) {
        _partition_ids.emplace(pid);
    }
    _closed_senders.Set(sender_id, true);
    _num_remaining_senders--;
    *finished = (_num_remaining_senders == 0);
    if (*finished) {
        _state = kFinished;
        // All senders are closed
        // 1. close all delta writers
        std::vector<DeltaWriter*> need_wait_writers;
        for (auto& it : _tablet_writers) {
            if (_partition_ids.count(it.second->partition_id()) > 0) {
                auto st = it.second->close();
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << it.first
                                 << ", transaction_id=" << _txn_id << ", err=" << st;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                need_wait_writers.push_back(it.second);
            } else {
                auto st = it.second->cancel();
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << it.first
                                 << ", transaction_id=" << _txn_id;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
            }
        }

        // 2. wait delta writers and build the tablet vector
        for (auto writer : need_wait_writers) {
            // close may return failed, but no need to handle it here.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            writer->close_wait(tablet_vec);
        }
        // TODO(gaodayue) clear and destruct all delta writers to make sure all memory are freed
        // DCHECK_EQ(_mem_tracker->consumption(), 0);
    }
    return Status::OK();
}

Status TabletsChannel::reduce_mem_usage() {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        // TabletsChannel is closed without LoadChannel's lock,
        // therefore it's possible for reduce_mem_usage() to be called right after close()
        return _close_status;
    }

    // Flush all memtables
    for (auto& it : _tablet_writers) {
        it.second->flush_memtable_and_wait(false);
    }

    for (auto& it : _tablet_writers) {
        OLAPStatus st = it.second->wait_flush();
        if (st != OLAP_SUCCESS) {
            // flush failed, return error
            std::stringstream ss;
            ss << "failed to reduce mem consumption by flushing memtable. err: " << st;
            return Status::InternalError(ss.str());
        }
    }
    return Status::OK();
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
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
    for (auto& tablet : params.tablets()) {
        WriteRequest request;
        request.tablet_id = tablet.tablet_id();
        request.schema_hash = schema_hash;
        request.write_type = WriteType::LOAD;
        request.txn_id = _txn_id;
        request.partition_id = tablet.partition_id();
        request.load_id = params.id();
        request.need_gen_rollup = params.need_gen_rollup();
        request.tuple_desc = _tuple_desc;
        request.slots = index_slots;

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&request, _mem_tracker, &writer);
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
    DCHECK_EQ(_tablet_writers.size(), params.tablets_size());
    return Status::OK();
}

Status TabletsChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    for (auto& it : _tablet_writers) {
        it.second->cancel();
    }
    DCHECK_EQ(_mem_tracker->consumption(), 0);
    _state = kFinished;
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

} // namespace doris
