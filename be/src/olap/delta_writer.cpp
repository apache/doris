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
#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/memtable_flush_executor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
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
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

Status DeltaWriter::open(WriteRequest* req, DeltaWriter** writer, RuntimeProfile* profile,
                         const UniqueId& load_id) {
    *writer = new DeltaWriter(req, StorageEngine::instance(), profile, load_id);
    return Status::OK();
}

DeltaWriter::DeltaWriter(WriteRequest* req, StorageEngine* storage_engine, RuntimeProfile* profile,
                         const UniqueId& load_id)
        : _req(*req), _rowset_builder(*req, profile), _memtable_writer(new MemTableWriter(*req)) {
    _init_profile(profile);
}

void DeltaWriter::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("DeltaWriter {}", _req.tablet_id), true, true);
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
}

DeltaWriter::~DeltaWriter() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    static_cast<void>(_memtable_writer->cancel());

    if (_rowset_builder.tablet() != nullptr) {
        const FlushStatistic& stat = _memtable_writer->get_flush_token_stats();
        _rowset_builder.tablet()->flush_bytes->increment(stat.flush_size_bytes);
        _rowset_builder.tablet()->flush_finish_count->increment(stat.flush_finish_count);
    }
}

Status DeltaWriter::init() {
    if (_is_init) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_rowset_builder.init());
    RETURN_IF_ERROR(
            _memtable_writer->init(_rowset_builder.rowset_writer(), _rowset_builder.tablet_schema(),
                                   _rowset_builder.get_partial_update_info(),
                                   _rowset_builder.tablet()->enable_unique_key_merge_on_write()));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status DeltaWriter::write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs,
                          bool is_append) {
    if (UNLIKELY(row_idxs.empty() && !is_append)) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    return _memtable_writer->write(block, row_idxs, is_append);
}
Status DeltaWriter::wait_flush() {
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

Status DeltaWriter::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before build_rowset() being called";

    SCOPED_TIMER(_close_wait_timer);
    RETURN_IF_ERROR(_memtable_writer->close_wait(_profile));
    return _rowset_builder.build_rowset();
}

Status DeltaWriter::submit_calc_delete_bitmap_task() {
    return _rowset_builder.submit_calc_delete_bitmap_task();
}

Status DeltaWriter::wait_calc_delete_bitmap() {
    return _rowset_builder.wait_calc_delete_bitmap();
}

Status DeltaWriter::commit_txn(const PSlaveTabletNodes& slave_tablet_nodes,
                               const bool write_single_replica) {
    if (config::cloud_mode) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_commit_txn_timer);
    RETURN_IF_ERROR(_rowset_builder.commit_txn());

    if (write_single_replica) {
        for (auto node_info : slave_tablet_nodes.slave_nodes()) {
            _request_slave_tablet_pull_rowset(node_info);
        }
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

Status DeltaWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer->cancel_with_status(st));
    _is_cancelled = true;
    return Status::OK();
}

int64_t DeltaWriter::mem_consumption(MemType mem) {
    return _memtable_writer->mem_consumption(mem);
}

void DeltaWriter::_request_slave_tablet_pull_rowset(PNodeInfo node_info) {
    if (config::cloud_mode) [[unlikely]] {
        return;
    }
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

    StorageEngine::instance()->txn_manager()->add_txn_tablet_delta_writer(_req.txn_id,
                                                                          _req.tablet_id, this);
    {
        std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
        _unfinished_slave_node.insert(node_info.id());
    }

    std::vector<int64_t> indices_ids;
    auto cur_rowset = _rowset_builder.rowset();
    auto tablet_schema = cur_rowset->rowset_meta()->tablet_schema();
    if (!tablet_schema->skip_write_index_on_load()) {
        for (auto& column : tablet_schema->columns()) {
            const TabletIndex* index_meta = tablet_schema->get_inverted_index(column.unique_id());
            if (index_meta) {
                indices_ids.emplace_back(index_meta->index_id());
            }
        }
    }

    PTabletWriteSlaveRequest request;
    RowsetMetaPB rowset_meta_pb = cur_rowset->rowset_meta()->get_rowset_pb();
    request.set_allocated_rowset_meta(&rowset_meta_pb);
    request.set_host(BackendOptions::get_localhost());
    request.set_http_port(config::webserver_port);
    string tablet_path = _rowset_builder.tablet()->tablet_path();
    request.set_rowset_path(tablet_path);
    request.set_token(ExecEnv::GetInstance()->token());
    request.set_brpc_port(config::brpc_port);
    request.set_node_id(node_info.id());
    for (int segment_id = 0; segment_id < cur_rowset->rowset_meta()->num_segments(); segment_id++) {
        std::stringstream segment_name;
        segment_name << cur_rowset->rowset_id() << "_" << segment_id << ".dat";
        int64_t segment_size = std::filesystem::file_size(tablet_path + "/" + segment_name.str());
        request.mutable_segments_size()->insert({segment_id, segment_size});

        if (!indices_ids.empty()) {
            for (auto index_id : indices_ids) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_name(
                        tablet_path + "/" + segment_name.str(), index_id);
                int64_t size = std::filesystem::file_size(inverted_index_file);
                PTabletWriteSlaveRequest::IndexSize index_size;
                index_size.set_indexid(index_id);
                index_size.set_size(size);
                // Fetch the map value for the current segment_id.
                // If it doesn't exist, this will insert a new default-constructed IndexSizeMapValue
                auto& index_size_map_value = (*request.mutable_inverted_indices_size())[segment_id];
                // Add the new index size to the map value.
                *index_size_map_value.mutable_index_sizes()->Add() = std::move(index_size);
            }
        }
    }
    RefCountClosure<PTabletWriteSlaveResult>* closure =
            new RefCountClosure<PTabletWriteSlaveResult>();
    closure->ref();
    closure->ref();
    closure->cntl.set_timeout_ms(config::slave_replica_writer_rpc_timeout_sec * 1000);
    closure->cntl.ignore_eovercrowded();
    stub->request_slave_tablet_pull_rowset(&closure->cntl, &request, &closure->result, closure);
    static_cast<void>(request.release_rowset_meta());

    closure->join();
    if (closure->cntl.Failed()) {
        if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                    stub, node_info.host(), node_info.async_internal_port())) {
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    closure->cntl.remote_side());
        }
        LOG(WARNING) << "failed to send pull rowset request to slave replica, error="
                     << berror(closure->cntl.ErrorCode())
                     << ", error_text=" << closure->cntl.ErrorText()
                     << ". slave host: " << node_info.host() << ", tablet_id=" << _req.tablet_id
                     << ", txn_id=" << _req.txn_id;
        std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
        _unfinished_slave_node.erase(node_info.id());
    }

    if (closure->unref()) {
        delete closure;
    }
    closure = nullptr;
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

int64_t DeltaWriter::num_rows_filtered() const {
    auto rowset_writer = _rowset_builder.rowset_writer();
    return rowset_writer == nullptr ? 0 : rowset_writer->num_rows_filtered();
}

} // namespace doris
