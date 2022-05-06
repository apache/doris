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
#include "olap/memtable.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key, bool is_high_priority, bool is_vec)
        : _key(key),
          _state(kInitialized),
          _closed_senders(64),
          _is_high_priority(is_high_priority),
          _is_vec(is_vec) {
    _mem_tracker = MemTracker::create_tracker(-1, "TabletsChannel:" + std::to_string(key.index_id));
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

Status TabletsChannel::open(const PTabletWriterOpenRequest& request) {
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kOpened) {
        // Normal case, already open by other sender
        return Status::OK();
    }
    LOG(INFO) << "open tablets channel: " << _key << ", tablets num: " << request.tablets().size()
              << ", timeout(s): " << request.load_channel_timeout_s();
    _txn_id = request.txn_id();
    _index_id = request.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(request.schema()));
    _tuple_desc = _schema->tuple_desc();
    _row_desc = new RowDescriptor(_tuple_desc, false);

    _num_remaining_senders = request.num_senders();
    _next_seqs.resize(_num_remaining_senders, 0);
    _closed_senders.Reset(_num_remaining_senders);

    RETURN_IF_ERROR(_open_all_writers(request));

    _state = kOpened;
    return Status::OK();
}

Status TabletsChannel::close(int sender_id, int64_t backend_id, bool* finished,
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
    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id
              << ", backend id: " << backend_id;
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
                if (!st.ok()) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << it.first
                                 << ", transaction_id=" << _txn_id << ", err=" << st;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                need_wait_writers.push_back(it.second);
            } else {
                auto st = it.second->cancel();
                if (!st.ok()) {
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
            writer->close_wait(tablet_vec, (_broken_tablets.find(writer->tablet_id()) !=
                                            _broken_tablets.end()));
        }
    }
    return Status::OK();
}

Status TabletsChannel::reduce_mem_usage(int64_t mem_limit) {
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        // TabletsChannel is closed without LoadChannel's lock,
        // therefore it's possible for reduce_mem_usage() to be called right after close()
        return _close_status;
    }

    // Sort the DeltaWriters by mem consumption in descend order.
    std::vector<DeltaWriter*> writers;
    for (auto& it : _tablet_writers) {
        writers.push_back(it.second);
    }
    std::sort(writers.begin(), writers.end(), [](const DeltaWriter* lhs, const DeltaWriter* rhs) {
        return lhs->mem_consumption() > rhs->mem_consumption();
    });

    // Decide which writes should be flushed to reduce mem consumption.
    // The main idea is to flush at least one third of the mem_limit.
    // This is mainly to solve the following scenarios.
    // Suppose there are N tablets in this TabletsChannel, and the mem limit is M.
    // If the data is evenly distributed, when each tablet memory accumulates to M/N,
    // the reduce memory operation will be triggered.
    // At this time, the value of M/N may be much smaller than the value of `write_buffer_size`.
    // If we flush all the tablets at this time, each tablet will generate a lot of small files.
    // So here we only flush part of the tablet, and the next time the reduce memory operation is triggered,
    // the tablet that has not been flushed before will accumulate more data, thereby reducing the number of flushes.
    int64_t mem_to_flushed = mem_limit / 3;
    int counter = 0;
    int64_t sum = 0;
    for (auto writer : writers) {
        if (writer->mem_consumption() <= 0) {
            break;
        }
        ++counter;
        sum += writer->mem_consumption();
        if (sum > mem_to_flushed) {
            break;
        }
    }
    VLOG_CRITICAL << "flush " << counter << " memtables to reduce memory: " << sum;
    for (int i = 0; i < counter; i++) {
        writers[i]->flush_memtable_and_wait(false);
    }

    for (int i = 0; i < counter; i++) {
        Status st = writers[i]->wait_flush();
        if (!st.ok()) {
            return Status::InternalError(fmt::format(
                    "failed to reduce mem consumption by flushing memtable. err: {}", st));
        }
    }
    return Status::OK();
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& request) {
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
        wrequest.tuple_desc = _tuple_desc;
        wrequest.slots = index_slots;
        wrequest.is_high_priority = _is_high_priority;

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&wrequest, &writer, _is_vec);
        if (!st.ok()) {
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

Status TabletsChannel::cancel() {
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    for (auto& it : _tablet_writers) {
        it.second->cancel();
    }
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