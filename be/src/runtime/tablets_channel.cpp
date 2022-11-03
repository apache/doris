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
#include "olap/delta_writer.h"
#include "olap/memtable.h"
#include "olap/storage_engine.h"
#include "runtime/load_channel.h"
#include "runtime/row_batch.h"
#include "runtime/thread_context.h"
#include "runtime/tuple_row.h"
#include "util/doris_metrics.h"
#include "util/time.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key, const UniqueId& load_id,
                               bool is_high_priority, bool is_vec)
        : _key(key),
          _state(kInitialized),
          _load_id(load_id),
          _closed_senders(64),
          _is_high_priority(is_high_priority),
          _is_vec(is_vec) {
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

Status TabletsChannel::close(
        LoadChannel* parent, int sender_id, int64_t backend_id, bool* finished,
        const google::protobuf::RepeatedField<int64_t>& partition_ids,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
        const google::protobuf::Map<int64_t, PSlaveTabletNodes>& slave_tablet_nodes,
        google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids,
        const bool write_single_replica) {
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
        std::set<DeltaWriter*> need_wait_writers;
        for (auto& it : _tablet_writers) {
            if (_partition_ids.count(it.second->partition_id()) > 0) {
                auto st = it.second->close();
                if (!st.ok()) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << it.first
                                 << ", transaction_id=" << _txn_id << ", err=" << st;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                need_wait_writers.insert(it.second);
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

        _write_single_replica = write_single_replica;

        // 2. wait delta writers and build the tablet vector
        for (auto writer : need_wait_writers) {
            PSlaveTabletNodes slave_nodes;
            if (write_single_replica) {
                slave_nodes = slave_tablet_nodes.at(writer->tablet_id());
            }
            // close may return failed, but no need to handle it here.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            _close_wait(writer, tablet_vec, tablet_errors, slave_nodes, write_single_replica);
        }

        if (write_single_replica) {
            // The operation waiting for all slave replicas to complete must end before the timeout,
            // so that there is enough time to collect completed replica. Otherwise, the task may
            // timeout and fail even though most of the replicas are completed. Here we set 0.9
            // times the timeout as the maximum waiting time.
            while (need_wait_writers.size() > 0 &&
                   (time(nullptr) - parent->last_updated_time()) < (parent->timeout() * 0.9)) {
                std::set<DeltaWriter*>::iterator it;
                for (it = need_wait_writers.begin(); it != need_wait_writers.end();) {
                    bool is_done = (*it)->check_slave_replicas_done(success_slave_tablet_node_ids);
                    if (is_done) {
                        need_wait_writers.erase(it++);
                    } else {
                        it++;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (auto writer : need_wait_writers) {
                writer->add_finished_slave_replicas(success_slave_tablet_node_ids);
            }
            StorageEngine::instance()->txn_manager()->clear_txn_tablet_delta_writer(_txn_id);
        }
    }
    return Status::OK();
}

void TabletsChannel::_close_wait(DeltaWriter* writer,
                                 google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
                                 google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
                                 PSlaveTabletNodes slave_tablet_nodes,
                                 const bool write_single_replica) {
    Status st = writer->close_wait(slave_tablet_nodes, write_single_replica);
    if (st.ok()) {
        if (_broken_tablets.find(writer->tablet_id()) == _broken_tablets.end()) {
            PTabletInfo* tablet_info = tablet_vec->Add();
            tablet_info->set_tablet_id(writer->tablet_id());
            tablet_info->set_schema_hash(writer->schema_hash());
        }
    } else {
        PTabletError* tablet_error = tablet_errors->Add();
        tablet_error->set_tablet_id(writer->tablet_id());
        tablet_error->set_msg(st.get_error_msg());
    }
}

int64_t TabletsChannel::mem_consumption() {
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_tablet_writers_lock);
        for (auto& it : _tablet_writers) {
            mem_usage += it.second->mem_consumption();
        }
    }
    return mem_usage;
}

template <typename TabletWriterAddResult>
Status TabletsChannel::reduce_mem_usage(TabletWriterAddResult* response) {
    if (_try_to_wait_flushing()) {
        // `_try_to_wait_flushing()` returns true means other thread already
        // reduced the mem usage, and current thread do not need to reduce again.
        LOG(INFO) << "Duplicate reduce mem usage on TabletsChannel, txn_id: " << _txn_id
                  << ", index_id: " << _index_id;
        return Status::OK();
    }

    std::vector<DeltaWriter*> writers_to_wait_flush;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_state == kFinished) {
            // TabletsChannel is closed without LoadChannel's lock,
            // therefore it's possible for reduce_mem_usage() to be called right after close()
            return _close_status;
        }

        // Sort the DeltaWriters by mem consumption in descend order.
        std::vector<DeltaWriter*> writers;
        for (auto& it : _tablet_writers) {
            it.second->save_mem_consumption_snapshot();
            writers.push_back(it.second);
        }
        int64_t total_memtable_consumption_in_flush = 0;
        for (auto writer : writers) {
            if (writer->get_memtable_consumption_inflush() > 0) {
                writers_to_wait_flush.push_back(writer);
                total_memtable_consumption_in_flush += writer->get_memtable_consumption_inflush();
            }
        }
        std::sort(writers.begin(), writers.end(),
                  [](const DeltaWriter* lhs, const DeltaWriter* rhs) {
                      return lhs->get_memtable_consumption_snapshot() >
                             rhs->get_memtable_consumption_snapshot();
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

        int64_t mem_to_flushed = mem_consumption() / 3;
        if (total_memtable_consumption_in_flush < mem_to_flushed) {
            mem_to_flushed -= total_memtable_consumption_in_flush;
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
            std::ostringstream ss;
            ss << "total size of memtables in flush: " << total_memtable_consumption_in_flush
               << " will flush " << counter << " more memtables to reduce memory: " << sum;
            if (counter > 0) {
                ss << ", the size of smallest memtable to flush is "
                   << writers[counter - 1]->get_memtable_consumption_snapshot() << " bytes";
            }
            LOG(INFO) << ss.str();
            google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors =
                    response->mutable_tablet_errors();
            // following loop flush memtable async, we'll do it with _lock
            for (int i = 0; i < counter; i++) {
                Status st = writers[i]->flush_memtable_and_wait(false);
                if (!st.ok()) {
                    auto err_msg = strings::Substitute(
                            "tablet writer failed to reduce mem consumption by flushing memtable, "
                            "tablet_id=$0, txn_id=$1, err=$2, errcode=$3, msg:$4",
                            writers[i]->tablet_id(), _txn_id, st.code(), st.precise_code(),
                            st.get_error_msg());
                    LOG(WARNING) << err_msg;
                    PTabletError* error = tablet_errors->Add();
                    error->set_tablet_id(writers[i]->tablet_id());
                    error->set_msg(err_msg);
                    _broken_tablets.insert(writers[i]->tablet_id());
                }
            }
            for (int i = 0; i < counter; i++) {
                if (_broken_tablets.find(writers[i]->tablet_id()) != _broken_tablets.end()) {
                    // skip broken tablets
                    continue;
                }
                writers_to_wait_flush.push_back(writers[i]);
            }
            _reducing_mem_usage = true;
        } else {
            LOG(INFO) << "total size of memtables in flush is big enough: "
                      << total_memtable_consumption_in_flush
                      << " bytes, will not flush more memtables";
        }
    }

    Status st = Status::OK();
    for (auto writer : writers_to_wait_flush) {
        st = writer->wait_flush();
        if (!st.ok()) {
            st = Status::InternalError(
                    "failed to reduce mem consumption by flushing memtable. err: {}",
                    st.to_string());
            break;
        }
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _reducing_mem_usage = false;
        _reduce_memory_cond.notify_all();
    }

    return st;
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
        wrequest.index_id = request.index_id();
        wrequest.tablet_id = tablet.tablet_id();
        wrequest.schema_hash = schema_hash;
        wrequest.write_type = WriteType::LOAD;
        wrequest.txn_id = _txn_id;
        wrequest.partition_id = tablet.partition_id();
        wrequest.load_id = request.id();
        wrequest.tuple_desc = _tuple_desc;
        wrequest.slots = index_slots;
        wrequest.is_high_priority = _is_high_priority;
        wrequest.ptable_schema_param = request.schema();

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&wrequest, &writer, _load_id, _is_vec);
        if (!st.ok()) {
            std::stringstream ss;
            ss << "open delta writer failed, tablet_id=" << tablet.tablet_id()
               << ", txn_id=" << _txn_id << ", partition_id=" << tablet.partition_id()
               << ", err=" << st;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        {
            std::lock_guard<SpinLock> l(_tablet_writers_lock);
            _tablet_writers.emplace(tablet.tablet_id(), writer);
        }
    }
    _s_tablet_writer_count += _tablet_writers.size();
    DCHECK_EQ(_tablet_writers.size(), request.tablets_size());
    return Status::OK();
}

bool TabletsChannel::_try_to_wait_flushing() {
    bool duplicate_work = false;
    std::unique_lock<std::mutex> l(_lock);
    // NOTE: we call `reduce_mem_usage()` because we think it's necessary
    // to reduce it's memory and should not write more data into this
    // tablets channel. If there's already some other thead doing the
    // reduce-memory work, the only choice for current thread is to wait
    // here.
    // If current thread do not wait, it has two options:
    // 1. continue to write data to current channel.
    // 2. pick another tablets channel to flush
    // The first choice might cause OOM, the second choice might pick a
    // channel that is not big enough.
    while (_reducing_mem_usage) {
        duplicate_work = true;
        _reduce_memory_cond.wait(l);
    }
    return duplicate_work;
}

Status TabletsChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    for (auto& it : _tablet_writers) {
        it.second->cancel();
    }
    _state = kFinished;
    if (_write_single_replica) {
        StorageEngine::instance()->txn_manager()->clear_txn_tablet_delta_writer(_txn_id);
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

template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
Status TabletsChannel::add_batch(const TabletWriterAddRequest& request,
                                 TabletWriterAddResult* response) {
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
            return Status::InternalError("unknown tablet to append data, tablet={}",
                                         tablet_to_rowidxs_it.first);
        }

        Status st = tablet_writer_it->second->write(&send_data, tablet_to_rowidxs_it.second);
        if (!st.ok()) {
            auto err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2"
                    ", errcode=$3, msg:$4",
                    tablet_to_rowidxs_it.first, _txn_id, st.code(), st.precise_code(),
                    st.get_error_msg());
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

template Status
TabletsChannel::add_batch<PTabletWriterAddBatchRequest, PTabletWriterAddBatchResult>(
        PTabletWriterAddBatchRequest const&, PTabletWriterAddBatchResult*);
template Status
TabletsChannel::add_batch<PTabletWriterAddBlockRequest, PTabletWriterAddBlockResult>(
        PTabletWriterAddBlockRequest const&, PTabletWriterAddBlockResult*);
template Status TabletsChannel::reduce_mem_usage<PTabletWriterAddBatchResult>(
        PTabletWriterAddBatchResult*);
template Status TabletsChannel::reduce_mem_usage<PTabletWriterAddBlockResult>(
        PTabletWriterAddBlockResult*);
} // namespace doris
