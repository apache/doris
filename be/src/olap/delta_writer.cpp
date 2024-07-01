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

#include "olap/delta_writer.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset_builder.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

BaseDeltaWriter::BaseDeltaWriter(const WriteRequest& req, RuntimeProfile* profile,
                                 const UniqueId& load_id)
        : _req(req), _memtable_writer(new MemTableWriter(req)) {
    _init_profile(profile);
}

DeltaWriter::DeltaWriter(StorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile,
                         const UniqueId& load_id)
        : BaseDeltaWriter(req, profile, load_id), _engine(engine) {
    _rowset_builder = std::make_unique<RowsetBuilder>(_engine, req, profile);
}

void BaseDeltaWriter::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("DeltaWriter {}", _req.tablet_id), true, true);
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _wait_flush_limit_timer = ADD_TIMER(_profile, "WaitFlushLimitTime");
}

void DeltaWriter::_init_profile(RuntimeProfile* profile) {
    BaseDeltaWriter::_init_profile(profile);
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
}

BaseDeltaWriter::~BaseDeltaWriter() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    static_cast<void>(_memtable_writer->cancel());

    if (_rowset_builder->tablet() != nullptr) {
        const FlushStatistic& stat = _memtable_writer->get_flush_token_stats();
        _rowset_builder->tablet()->flush_bytes->increment(stat.flush_size_bytes);
        _rowset_builder->tablet()->flush_finish_count->increment(stat.flush_finish_count);
    }
}

DeltaWriter::~DeltaWriter() = default;

Status BaseDeltaWriter::init() {
    if (_is_init) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_rowset_builder->init());
    RETURN_IF_ERROR(_memtable_writer->init(
            _rowset_builder->rowset_writer(), _rowset_builder->tablet_schema(),
            _rowset_builder->get_partial_update_info(), nullptr,
            _rowset_builder->tablet()->enable_unique_key_merge_on_write()));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs) {
    if (UNLIKELY(row_idxs.empty())) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    {
        SCOPED_TIMER(_wait_flush_limit_timer);
        while (_memtable_writer->flush_running_count() >=
               config::memtable_flush_running_count_limit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return _memtable_writer->write(block, row_idxs);
}

Status BaseDeltaWriter::wait_flush() {
    return _memtable_writer->wait_flush();
}

Status DeltaWriter::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriter, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }
    return _memtable_writer->close();
}

Status BaseDeltaWriter::build_rowset() {
    SCOPED_TIMER(_close_wait_timer);
    RETURN_IF_ERROR(_memtable_writer->close_wait(_profile));
    return _rowset_builder->build_rowset();
}

Status DeltaWriter::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before build_rowset() being called";
    return BaseDeltaWriter::build_rowset();
}

Status BaseDeltaWriter::submit_calc_delete_bitmap_task() {
    return _rowset_builder->submit_calc_delete_bitmap_task();
}

Status BaseDeltaWriter::wait_calc_delete_bitmap() {
    return _rowset_builder->wait_calc_delete_bitmap();
}

RowsetBuilder* DeltaWriter::rowset_builder() {
    return static_cast<RowsetBuilder*>(_rowset_builder.get());
}

Status DeltaWriter::commit_txn(const PSlaveTabletNodes& slave_tablet_nodes) {
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_commit_txn_timer);
    RETURN_IF_ERROR(rowset_builder()->commit_txn());

    for (auto&& node_info : slave_tablet_nodes.slave_nodes()) {
        _request_slave_tablet_pull_rowset(node_info);
    }
    return Status::OK();
}

bool DeltaWriter::check_slave_replicas_done(
        google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids) {
    std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
    if (_unfinished_slave_node.empty()) {
        success_slave_tablet_node_ids->insert({_req.tablet_id, _success_slave_node_ids});
        return true;
    }
    return false;
}

void DeltaWriter::add_finished_slave_replicas(
        google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids) {
    std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
    success_slave_tablet_node_ids->insert({_req.tablet_id, _success_slave_node_ids});
}

Status BaseDeltaWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status BaseDeltaWriter::cancel_with_status(const Status& st) {
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer->cancel_with_status(st));
    _is_cancelled = true;
    return Status::OK();
}

Status DeltaWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    return BaseDeltaWriter::cancel_with_status(st);
}

int64_t BaseDeltaWriter::mem_consumption(MemType mem) {
    return _memtable_writer->mem_consumption(mem);
}

void DeltaWriter::_request_slave_tablet_pull_rowset(const PNodeInfo& node_info) {
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                    node_info.host(), node_info.async_internal_port());
    if (stub == nullptr) {
        LOG(WARNING) << "failed to send pull rowset request to slave replica. get rpc stub failed, "
                        "slave host="
                     << node_info.host() << ", port=" << node_info.async_internal_port()
                     << ", tablet_id=" << _req.tablet_id << ", txn_id=" << _req.txn_id;
        return;
    }

    _engine.txn_manager()->add_txn_tablet_delta_writer(_req.txn_id, _req.tablet_id, this);
    {
        std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
        _unfinished_slave_node.insert(node_info.id());
    }

    std::vector<std::pair<int64_t, std::string>> indices_ids;
    auto cur_rowset = _rowset_builder->rowset();
    auto tablet_schema = cur_rowset->rowset_meta()->tablet_schema();
    if (!tablet_schema->skip_write_index_on_load()) {
        for (auto& column : tablet_schema->columns()) {
            const TabletIndex* index_meta = tablet_schema->get_inverted_index(*column);
            if (index_meta) {
                indices_ids.emplace_back(index_meta->index_id(), index_meta->get_index_suffix());
            }
        }
    }

    auto request = std::make_shared<PTabletWriteSlaveRequest>();
    auto* request_mutable_rs_meta = request->mutable_rowset_meta();
    *request_mutable_rs_meta = cur_rowset->rowset_meta()->get_rowset_pb();
    if (request_mutable_rs_meta != nullptr && request_mutable_rs_meta->has_partition_id() &&
        request_mutable_rs_meta->partition_id() == 0) {
        // TODO(dx): remove log after fix partition id eq 0 bug
        request_mutable_rs_meta->set_partition_id(_req.partition_id);
        LOG(WARNING) << "cant get partition id from local rs pb, get from _req, partition_id="
                     << _req.partition_id;
    }
    request->set_host(BackendOptions::get_localhost());
    request->set_http_port(config::webserver_port);
    const auto& tablet_path = cur_rowset->tablet_path();
    request->set_rowset_path(tablet_path);
    request->set_token(ExecEnv::GetInstance()->token());
    request->set_brpc_port(config::brpc_port);
    request->set_node_id(node_info.id());
    for (int segment_id = 0; segment_id < cur_rowset->rowset_meta()->num_segments(); segment_id++) {
        auto seg_path =
                local_segment_path(tablet_path, cur_rowset->rowset_id().to_string(), segment_id);
        int64_t segment_size = std::filesystem::file_size(seg_path);
        request->mutable_segments_size()->insert({segment_id, segment_size});
        auto index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(seg_path);
        if (!indices_ids.empty()) {
            if (tablet_schema->get_inverted_index_storage_format() ==
                InvertedIndexStorageFormatPB::V1) {
                for (auto index_meta : indices_ids) {
                    std::string inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    index_path_prefix, index_meta.first, index_meta.second);
                    int64_t size = std::filesystem::file_size(inverted_index_file);
                    PTabletWriteSlaveRequest::IndexSize index_size;
                    index_size.set_indexid(index_meta.first);
                    index_size.set_size(size);
                    index_size.set_suffix_path(index_meta.second);
                    // Fetch the map value for the current segment_id.
                    // If it doesn't exist, this will insert a new default-constructed IndexSizeMapValue
                    auto& index_size_map_value =
                            (*(request->mutable_inverted_indices_size()))[segment_id];
                    // Add the new index size to the map value.
                    *index_size_map_value.mutable_index_sizes()->Add() = std::move(index_size);
                }
            } else {
                std::string inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
                int64_t size = std::filesystem::file_size(inverted_index_file);
                PTabletWriteSlaveRequest::IndexSize index_size;
                // special id for non-V1 format
                index_size.set_indexid(0);
                index_size.set_size(size);
                index_size.set_suffix_path("");
                // Fetch the map value for the current segment_id.
                // If it doesn't exist, this will insert a new default-constructed IndexSizeMapValue
                auto& index_size_map_value =
                        (*(request->mutable_inverted_indices_size()))[segment_id];
                // Add the new index size to the map value.
                *index_size_map_value.mutable_index_sizes()->Add() = std::move(index_size);
            }
        }
    }

    auto pull_callback = DummyBrpcCallback<PTabletWriteSlaveResult>::create_shared();
    auto closure = AutoReleaseClosure<
            PTabletWriteSlaveRequest,
            DummyBrpcCallback<PTabletWriteSlaveResult>>::create_unique(request, pull_callback);
    closure->cntl_->set_timeout_ms(config::slave_replica_writer_rpc_timeout_sec * 1000);
    closure->cntl_->ignore_eovercrowded();
    stub->request_slave_tablet_pull_rowset(closure->cntl_.get(), closure->request_.get(),
                                           closure->response_.get(), closure.get());

    closure.release();
    pull_callback->join();
    if (pull_callback->cntl_->Failed()) {
        if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                    stub, node_info.host(), node_info.async_internal_port())) {
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    pull_callback->cntl_->remote_side());
        }
        LOG(WARNING) << "failed to send pull rowset request to slave replica, error="
                     << berror(pull_callback->cntl_->ErrorCode())
                     << ", error_text=" << pull_callback->cntl_->ErrorText()
                     << ". slave host: " << node_info.host() << ", tablet_id=" << _req.tablet_id
                     << ", txn_id=" << _req.txn_id;
        std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
        _unfinished_slave_node.erase(node_info.id());
    }
}

void DeltaWriter::finish_slave_tablet_pull_rowset(int64_t node_id, bool is_succeed) {
    std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
    if (is_succeed) {
        _success_slave_node_ids.add_slave_node_ids(node_id);
        VLOG_CRITICAL << "record successful slave replica for txn [" << _req.txn_id
                      << "], tablet_id=" << _req.tablet_id << ", node_id=" << node_id;
    }
    _unfinished_slave_node.erase(node_id);
}

int64_t BaseDeltaWriter::num_rows_filtered() const {
    auto rowset_writer = _rowset_builder->rowset_writer();
    return rowset_writer == nullptr ? 0 : rowset_writer->num_rows_filtered();
}

} // namespace doris
