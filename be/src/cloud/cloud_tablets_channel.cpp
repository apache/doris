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

#include "cloud/cloud_tablets_channel.h"

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "olap/delta_writer.h"
#include "runtime/tablets_channel.h"

namespace doris {

CloudTabletsChannel::CloudTabletsChannel(CloudStorageEngine& engine, const TabletsChannelKey& key,
                                         const UniqueId& load_id, bool is_high_priority,
                                         RuntimeProfile* profile)
        : BaseTabletsChannel(key, load_id, is_high_priority, profile), _engine(engine) {}

CloudTabletsChannel::~CloudTabletsChannel() = default;

std::unique_ptr<BaseDeltaWriter> CloudTabletsChannel::create_delta_writer(
        const WriteRequest& request) {
    return std::make_unique<CloudDeltaWriter>(_engine, request, _profile, _load_id);
}

Status CloudTabletsChannel::add_batch(const PTabletWriterAddBlockRequest& request,
                                      PTabletWriterAddBlockResult* response) {
    // FIXME(plat1ko): Too many duplicate code with `TabletsChannel`
    SCOPED_TIMER(_add_batch_timer);
    int64_t cur_seq = 0;
    _add_batch_number_counter->update(1);

    auto status = _get_current_seq(cur_seq, request);
    if (UNLIKELY(!status.ok())) {
        return status;
    }

    if (request.packet_seq() < cur_seq) {
        LOG(INFO) << "packet has already recept before, expect_seq=" << cur_seq
                  << ", recept_seq=" << request.packet_seq();
        return Status::OK();
    }

    std::unordered_map<int64_t, std::vector<uint32_t>> tablet_to_rowidxs;
    _build_tablet_to_rowidxs(request, &tablet_to_rowidxs);

    std::unordered_set<int64_t> partition_ids;
    {
        // add_batch may concurrency with inc_open but not under _lock.
        // so need to protect it with _tablet_writers_lock.
        std::lock_guard<SpinLock> l(_tablet_writers_lock);
        for (auto& [tablet_id, _] : tablet_to_rowidxs) {
            auto tablet_writer_it = _tablet_writers.find(tablet_id);
            if (tablet_writer_it == _tablet_writers.end()) {
                return Status::InternalError("unknown tablet to append data, tablet={}", tablet_id);
            }
            partition_ids.insert(tablet_writer_it->second->partition_id());
        }
        if (!partition_ids.empty()) {
            RETURN_IF_ERROR(_init_writers_by_partition_ids(partition_ids));
        }
    }

    return _write_block_data(request, cur_seq, tablet_to_rowidxs, response);
}

Status CloudTabletsChannel::_init_writers_by_partition_ids(
        const std::unordered_set<int64_t>& partition_ids) {
    std::vector<CloudDeltaWriter*> writers;
    for (auto&& [tablet_id, base_writer] : _tablet_writers) {
        auto* writer = static_cast<CloudDeltaWriter*>(base_writer.get());
        if (partition_ids.contains(writer->partition_id()) && !writer->is_init()) {
            writers.push_back(writer);
        }
    }
    if (!writers.empty()) {
        RETURN_IF_ERROR(CloudDeltaWriter::batch_init(writers));
    }
    return Status::OK();
}

Status CloudTabletsChannel::close(LoadChannel* parent, const PTabletWriterAddBlockRequest& req,
                                  PTabletWriterAddBlockResult* res, bool* finished) {
    // FIXME(plat1ko): Too many duplicate code with `TabletsChannel`
    std::lock_guard l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }

    auto sender_id = req.sender_id();
    if (_closed_senders.Get(sender_id)) {
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }

    for (auto pid : req.partition_ids()) {
        _partition_ids.emplace(pid);
    }

    _closed_senders.Set(sender_id, true);
    _num_remaining_senders--;
    *finished = (_num_remaining_senders == 0);

    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id
              << ", backend id: " << req.backend_id()
              << " remaining sender: " << _num_remaining_senders;

    if (!*finished) {
        return Status::OK();
    }

    auto* tablet_errors = res->mutable_tablet_errors();
    auto* tablet_vec = res->mutable_tablet_vec();
    _state = kFinished;

    // All senders are closed
    // 1. close all delta writers. under _lock.
    std::vector<CloudDeltaWriter*> writers_to_commit;
    writers_to_commit.reserve(_tablet_writers.size());
    bool success = true;

    for (auto&& [tablet_id, base_writer] : _tablet_writers) {
        auto* writer = static_cast<CloudDeltaWriter*>(base_writer.get());
        // ATTN: the strict mode means strict filtering of column type conversions during import.
        // Sometimes all inputs are filtered, but the partition ID is still set, and the writer is
        // not initialized.
        if (_partition_ids.contains(writer->partition_id())) {
            if (!success) { // Already failed, cancel all remain writers
                static_cast<void>(writer->cancel());
                continue;
            }

            if (writer->is_init()) {
                auto st = writer->close();
                if (!st.ok()) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << tablet_id
                                 << ", txn_id=" << _txn_id << ", err=" << st;
                    PTabletError* tablet_error = tablet_errors->Add();
                    tablet_error->set_tablet_id(tablet_id);
                    tablet_error->set_msg(st.to_string());
                    success = false;
                    _close_status = std::move(st);
                    continue;
                }
            }

            // to make sure tablet writer in `_broken_tablets` won't call `close_wait` method.
            if (_is_broken_tablet(writer->tablet_id())) {
                LOG(WARNING) << "SHOULD NOT HAPPEN, tablet writer is broken but not cancelled"
                             << ", tablet_id=" << tablet_id << ", transaction_id=" << _txn_id;
                continue;
            }

            writers_to_commit.push_back(writer);
        } else {
            auto st = writer->cancel();
            if (!st.ok()) {
                LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << tablet_id
                             << ", txn_id=" << _txn_id;
                // just skip this tablet(writer) and continue to close others
                continue;
            }
        }
    }

    if (!success) {
        return _close_status;
    }

    // 2. wait delta writers
    using namespace std::chrono;
    auto build_start = steady_clock::now();
    for (auto* writer : writers_to_commit) {
        if (!writer->is_init()) {
            continue;
        }

        auto st = writer->build_rowset();
        if (!st.ok()) {
            LOG(WARNING) << "failed to close wait DeltaWriter. tablet_id=" << writer->tablet_id()
                         << ", err=" << st;
            PTabletError* tablet_error = tablet_errors->Add();
            tablet_error->set_tablet_id(writer->tablet_id());
            tablet_error->set_msg(st.to_string());
            _close_status = std::move(st);
            return _close_status;
        }
    }
    int64_t build_latency = duration_cast<milliseconds>(steady_clock::now() - build_start).count();

    // 3. commit rowsets to meta-service
    auto commit_start = steady_clock::now();
    std::vector<std::function<Status()>> tasks;
    tasks.reserve(writers_to_commit.size());
    for (auto* writer : writers_to_commit) {
        tasks.emplace_back([writer] { return writer->commit_rowset(); });
    }
    _close_status = cloud::bthread_fork_join(tasks, 10);
    if (!_close_status.ok()) {
        return _close_status;
    }

    int64_t commit_latency =
            duration_cast<milliseconds>(steady_clock::now() - commit_start).count();

    // 4. calculate delete bitmap for Unique Key MoW tables
    for (auto* writer : writers_to_commit) {
        auto st = writer->submit_calc_delete_bitmap_task();
        if (!st.ok()) {
            LOG(WARNING) << "failed to close wait DeltaWriter. tablet_id=" << writer->tablet_id()
                         << ", err=" << st;
            _add_error_tablet(tablet_errors, writer->tablet_id(), st);
            _close_status = std::move(st);
            return _close_status;
        }
    }

    // 5. wait for delete bitmap calculation complete if necessary
    for (auto* writer : writers_to_commit) {
        auto st = writer->wait_calc_delete_bitmap();
        if (!st.ok()) {
            LOG(WARNING) << "failed to close wait DeltaWriter. tablet_id=" << writer->tablet_id()
                         << ", err=" << st;
            _add_error_tablet(tablet_errors, writer->tablet_id(), st);
            _close_status = std::move(st);
            return _close_status;
        }
    }

    // 6. set txn related delete bitmap if necessary
    for (auto it = writers_to_commit.begin(); it != writers_to_commit.end();) {
        auto st = (*it)->set_txn_related_delete_bitmap();
        if (!st.ok()) {
            _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
            _close_status = std::move(st);
            return _close_status;
        }
        it++;
    }

    tablet_vec->Reserve(writers_to_commit.size());
    for (auto* writer : writers_to_commit) {
        PTabletInfo* tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(writer->tablet_id());
        // unused required field.
        tablet_info->set_schema_hash(0);
        tablet_info->set_received_rows(writer->total_received_rows());
        tablet_info->set_num_rows_filtered(writer->num_rows_filtered());
        // These stats may be larger than the actual value if the txn is aborted
        writer->update_tablet_stats();
    }
    res->set_build_rowset_latency_ms(build_latency);
    res->set_commit_rowset_latency_ms(commit_latency);
    return Status::OK();
}

} // namespace doris
