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
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key, const UniqueId& load_id,
                               bool is_high_priority)
        : _key(key),
          _state(kInitialized),
          _load_id(load_id),
          _closed_senders(64),
          _is_high_priority(is_high_priority) {
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
                    PTabletError* tablet_error = tablet_errors->Add();
                    tablet_error->set_tablet_id(it.first);
                    tablet_error->set_msg(st.to_string());
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                // to make sure tablet writer in `_broken_tablets` won't call `close_wait` method.
                // `close_wait` might create the rowset and commit txn directly, and the subsequent
                // publish version task will success, which can cause the replica inconsistency.
                if (_broken_tablets.find(it.second->tablet_id()) != _broken_tablets.end()) {
                    LOG(WARNING) << "SHOULD NOT HAPPEN, tablet writer is broken but not cancelled"
                                 << ", tablet_id=" << it.first << ", transaction_id=" << _txn_id;
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
                VLOG_PROGRESS << "cancel tablet writer successfully, tablet_id=" << it.first
                              << ", transaction_id=" << _txn_id;
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
        PTabletInfo* tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(writer->tablet_id());
        tablet_info->set_schema_hash(writer->schema_hash());
    } else {
        PTabletError* tablet_error = tablet_errors->Add();
        tablet_error->set_tablet_id(writer->tablet_id());
        tablet_error->set_msg(st.to_string());
        VLOG_PROGRESS << "close wait failed tablet " << writer->tablet_id() << " transaction_id "
                      << _txn_id << "err msg " << st;
    }
}

int64_t TabletsChannel::mem_consumption() {
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_tablet_writers_lock);
        _mem_consumptions.clear();
        for (auto& it : _tablet_writers) {
            int64_t writer_mem = it.second->mem_consumption();
            mem_usage += writer_mem;
            _mem_consumptions.emplace(writer_mem, it.first);
        }
    }
    return mem_usage;
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
        auto st = DeltaWriter::open(&wrequest, &writer, _load_id);
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
            VLOG_PROGRESS << "skip broken tablet tablet=" << tablet_id;
            continue;
        }
        auto it = tablet_to_rowidxs.find(tablet_id);
        if (it == tablet_to_rowidxs.end()) {
            tablet_to_rowidxs.emplace(tablet_id, std::initializer_list<int> {i});
        } else {
            it->second.emplace_back(i);
        }
    }

    auto get_send_data = [&]() { return vectorized::Block(request.block()); };

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
            auto err_msg =
                    fmt::format("tablet writer write failed, tablet_id={}, txn_id={}, err={}",
                                tablet_to_rowidxs_it.first, _txn_id, st);
            LOG(WARNING) << err_msg;
            PTabletError* error = tablet_errors->Add();
            error->set_tablet_id(tablet_to_rowidxs_it.first);
            error->set_msg(err_msg);
            tablet_writer_it->second->cancel_with_status(st);
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

void TabletsChannel::flush_memtable_async(int64_t tablet_id) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        // TabletsChannel is closed without LoadChannel's lock,
        // therefore it's possible for reduce_mem_usage() to be called right after close()
        LOG(INFO) << "TabletsChannel is closed when reduce mem usage, txn_id: " << _txn_id
                  << ", index_id: " << _index_id;
        return;
    }

    auto iter = _tablet_writers.find(tablet_id);
    if (iter == _tablet_writers.end()) {
        return;
    }

    if (!(_reducing_tablets.insert(tablet_id).second)) {
        return;
    }

    Status st = iter->second->flush_memtable_and_wait(false);
    if (!st.ok()) {
        auto err_msg = fmt::format(
                "tablet writer failed to reduce mem consumption by flushing memtable, "
                "tablet_id={}, txn_id={}, err={}",
                tablet_id, _txn_id, st);
        LOG(WARNING) << err_msg;
        iter->second->cancel_with_status(st);
        _broken_tablets.insert(iter->second->tablet_id());
    }
}

void TabletsChannel::wait_flush(int64_t tablet_id) {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_state == kFinished) {
            // TabletsChannel is closed without LoadChannel's lock,
            // therefore it's possible for reduce_mem_usage() to be called right after close()
            LOG(INFO) << "TabletsChannel is closed when reduce mem usage, txn_id: " << _txn_id
                      << ", index_id: " << _index_id;
            return;
        }
    }

    auto iter = _tablet_writers.find(tablet_id);
    if (iter == _tablet_writers.end()) {
        return;
    }
    Status st = iter->second->wait_flush();
    if (!st.ok()) {
        auto err_msg = fmt::format(
                "tablet writer failed to reduce mem consumption by flushing memtable, "
                "tablet_id={}, txn_id={}, err={}",
                tablet_id, _txn_id, st);
        LOG(WARNING) << err_msg;
        iter->second->cancel_with_status(st);
        _broken_tablets.insert(iter->second->tablet_id());
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _reducing_tablets.erase(tablet_id);
    }
}

template Status
TabletsChannel::add_batch<PTabletWriterAddBlockRequest, PTabletWriterAddBlockResult>(
        PTabletWriterAddBlockRequest const&, PTabletWriterAddBlockResult*);
} // namespace doris
