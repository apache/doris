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
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
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

Status DeltaWriter::open(WriteRequest* req, DeltaWriter** writer, RuntimeProfile* profile,
                         const UniqueId& load_id) {
    *writer = new DeltaWriter(req, StorageEngine::instance(), profile, load_id);
    return Status::OK();
}

DeltaWriter::DeltaWriter(WriteRequest* req, StorageEngine* storage_engine, RuntimeProfile* profile,
                         const UniqueId& load_id)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _memtable_writer(*req, profile),
          _tablet_schema(new TabletSchema),
          _delta_written_success(false),
          _storage_engine(storage_engine),
          _load_id(load_id) {
    _init_profile(profile);
}

void DeltaWriter::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("DeltaWriter {}", _req.tablet_id), true, true);
    _close_wait_timer = ADD_TIMER(_profile, "DeltaWriterCloseWaitTime");
}

DeltaWriter::~DeltaWriter() {
    if (_is_init && !_delta_written_success) {
        _garbage_collection();
    }

    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    _memtable_writer.cancel();

    if (_tablet != nullptr) {
        const FlushStatistic& stat = _memtable_writer.get_flush_token_stats();
        _tablet->flush_bytes->increment(stat.flush_size_bytes);
        _tablet->flush_finish_count->increment(stat.flush_finish_count);
    }

    if (_calc_delete_bitmap_token != nullptr) {
        _calc_delete_bitmap_token->cancel();
    }

    if (_tablet != nullptr) {
        _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                _rowset_writer->rowset_id().to_string());
    }
}

void DeltaWriter::_garbage_collection() {
    Status rollback_status = Status::OK();
    TxnManager* txn_mgr = _storage_engine->txn_manager();
    if (_tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, _tablet, _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

Status DeltaWriter::init() {
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_req.tablet_id);
    if (_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("fail to find tablet. tablet_id={}, schema_hash={}",
                                              _req.tablet_id, _req.schema_hash);
    }

    // get rowset ids snapshot
    if (_tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::shared_mutex> lck(_tablet->get_header_lock());
        _cur_max_version = _tablet->max_version_unlocked().second;
        // tablet is under alter process. The delete bitmap will be calculated after conversion.
        if (_tablet->tablet_state() == TABLET_NOTREADY &&
            SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
            // Disable 'partial_update' when the tablet is undergoing a 'schema changing process'
            if (_req.table_schema_param->is_partial_update()) {
                return Status::InternalError(
                        "Unable to do 'partial_update' when "
                        "the tablet is undergoing a 'schema changing process'");
            }
            _rowset_ids.clear();
        } else {
            _rowset_ids = _tablet->all_rs_id(_cur_max_version);
        }
    }

    // check tablet version number
    if (!config::disable_auto_compaction &&
        _tablet->exceed_version_limit(config::max_tablet_version_num - 100) &&
        !MemInfo::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
        //trigger compaction
        StorageEngine::instance()->submit_compaction_task(
                _tablet, CompactionType::CUMULATIVE_COMPACTION, true);
        if (_tablet->version_count() > config::max_tablet_version_num) {
            return Status::Error<TOO_MANY_VERSION>(
                    "failed to init delta writer. version count: {}, exceed limit: {}, tablet: {}",
                    _tablet->version_count(), config::max_tablet_version_num, _tablet->full_name());
        }
    }

    {
        std::shared_lock base_migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!base_migration_rlock.owns_lock()) {
            return Status::Error<TRY_LOCK_FAILED>("get lock failed");
        }
        std::lock_guard<std::mutex> push_lock(_tablet->get_push_lock());
        RETURN_IF_ERROR(_storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet,
                                                                    _req.txn_id, _req.load_id));
    }
    if (_tablet->enable_unique_key_merge_on_write() && _delete_bitmap == nullptr) {
        _delete_bitmap.reset(new DeleteBitmap(_tablet->tablet_id()));
    }
    // build tablet schema in request level
    _build_current_tablet_schema(_req.index_id, _req.table_schema_param, *_tablet->tablet_schema());
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _tablet->tablet_id();
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = std::make_shared<MowContext>(_cur_max_version, _req.txn_id, _rowset_ids,
                                                       _delete_bitmap);
    std::unique_ptr<RowsetWriter> rowset_writer;
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &rowset_writer));
    _rowset_writer = std::move(rowset_writer);
    _memtable_writer.init(_rowset_writer, _tablet_schema,
                          _tablet->enable_unique_key_merge_on_write());
    _calc_delete_bitmap_token = _storage_engine->calc_delete_bitmap_executor()->create_token();

    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status DeltaWriter::write(const vectorized::Block* block, const std::vector<int>& row_idxs,
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
    return _memtable_writer.write(block, row_idxs, is_append);
}

Status DeltaWriter::flush_memtable_and_wait(bool need_wait) {
    return _memtable_writer.flush_memtable_and_wait(need_wait);
}

Status DeltaWriter::wait_flush() {
    return _memtable_writer.wait_flush();
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
    return _memtable_writer.close();
}

Status DeltaWriter::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before build_rowset() being called";

    RETURN_IF_ERROR(_memtable_writer.close_wait());

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        return Status::Error<MEM_ALLOC_FAILED>("fail to build rowset");
    }
    return Status::OK();
}

Status DeltaWriter::submit_calc_delete_bitmap_task() {
    if (!_tablet->enable_unique_key_merge_on_write()) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> l(_lock);
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (_tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
        LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated later, "
                     "tablet_id: "
                  << _tablet->tablet_id() << " txn_id: " << _req.txn_id;
        return Status::OK();
    }
    auto beta_rowset = reinterpret_cast<BetaRowset*>(_cur_rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (_tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
        return Status::OK();
    }
    if (segments.size() > 1) {
        // calculate delete bitmap between segments
        RETURN_IF_ERROR(_tablet->calc_delete_bitmap_between_segments(_cur_rowset, segments,
                                                                     _delete_bitmap));
    }

    // For partial update, we need to fill in the entire row of data, during the calculation
    // of the delete bitmap. This operation is resource-intensive, and we need to minimize
    // the number of times it occurs. Therefore, we skip this operation here.
    if (_cur_rowset->tablet_schema()->is_partial_update()) {
        return Status::OK();
    }

    LOG(INFO) << "submit calc delete bitmap task to executor, tablet_id: " << _tablet->tablet_id()
              << ", txn_id: " << _req.txn_id;
    return _tablet->commit_phase_update_delete_bitmap(_cur_rowset, _rowset_ids, _delete_bitmap,
                                                      segments, _req.txn_id,
                                                      _calc_delete_bitmap_token.get(), nullptr);
}

Status DeltaWriter::wait_calc_delete_bitmap() {
    if (!_tablet->enable_unique_key_merge_on_write() ||
        _cur_rowset->tablet_schema()->is_partial_update()) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    RETURN_IF_ERROR(_calc_delete_bitmap_token->wait());
    RETURN_IF_ERROR(_calc_delete_bitmap_token->get_delete_bitmap(_delete_bitmap));
    LOG(INFO) << "Got result of calc delete bitmap task from executor, tablet_id: "
              << _tablet->tablet_id() << ", txn_id: " << _req.txn_id;
    return Status::OK();
}

Status DeltaWriter::commit_txn(const PSlaveTabletNodes& slave_tablet_nodes,
                               const bool write_single_replica) {
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_close_wait_timer);
    Status res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id,
                                                            _req.load_id, _cur_rowset, false);

    if (!res && !res.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id
                     << " for rowset: " << _cur_rowset->rowset_id();
        return res;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        _storage_engine->txn_manager()->set_txn_related_delete_bitmap(
                _req.partition_id, _req.txn_id, _tablet->tablet_id(), _tablet->schema_hash(),
                _tablet->tablet_uid(), true, _delete_bitmap, _rowset_ids);
    }

    _delta_written_success = true;

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
        success_slave_tablet_node_ids->insert({_tablet->tablet_id(), _success_slave_node_ids});
        return true;
    }
    return false;
}

void DeltaWriter::add_finished_slave_replicas(
        google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids) {
    std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
    success_slave_tablet_node_ids->insert({_tablet->tablet_id(), _success_slave_node_ids});
}

Status DeltaWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer.cancel_with_status(st));
    if (_rowset_writer && _rowset_writer->is_doing_segcompaction()) {
        _rowset_writer->wait_flying_segcompaction(); /* already cancel, ignore the return status */
    }
    if (_calc_delete_bitmap_token != nullptr) {
        _calc_delete_bitmap_token->cancel();
    }
    _is_cancelled = true;
    return Status::OK();
}

int64_t DeltaWriter::mem_consumption(MemType mem) {
    return _memtable_writer.mem_consumption(mem);
}

int64_t DeltaWriter::active_memtable_mem_consumption() {
    return _memtable_writer.active_memtable_mem_consumption();
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

void DeltaWriter::_build_current_tablet_schema(int64_t index_id,
                                               const OlapTableSchemaParam* table_schema_param,
                                               const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }

    if (indexes.size() > 0 && indexes[i]->columns.size() != 0 &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, table_schema_param->version(),
                                                    indexes[i], ori_tablet_schema);
    }
    if (_tablet_schema->schema_version() > ori_tablet_schema.schema_version()) {
        _tablet->update_max_version_schema(_tablet_schema);
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    // set partial update columns info
    _tablet_schema->set_partial_update_info(table_schema_param->is_partial_update(),
                                            table_schema_param->partial_update_input_columns());
    _tablet_schema->set_is_strict_mode(table_schema_param->is_strict_mode());
}

void DeltaWriter::_request_slave_tablet_pull_rowset(PNodeInfo node_info) {
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                    node_info.host(), node_info.async_internal_port());
    if (stub == nullptr) {
        LOG(WARNING) << "failed to send pull rowset request to slave replica. get rpc stub failed, "
                        "slave host="
                     << node_info.host() << ", port=" << node_info.async_internal_port()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _req.txn_id;
        return;
    }

    _storage_engine->txn_manager()->add_txn_tablet_delta_writer(_req.txn_id, _tablet->tablet_id(),
                                                                this);
    {
        std::lock_guard<std::shared_mutex> lock(_slave_node_lock);
        _unfinished_slave_node.insert(node_info.id());
    }

    std::vector<int64_t> indices_ids;
    auto tablet_schema = _cur_rowset->rowset_meta()->tablet_schema();
    if (!tablet_schema->skip_write_index_on_load()) {
        for (auto& column : tablet_schema->columns()) {
            const TabletIndex* index_meta = tablet_schema->get_inverted_index(column.unique_id());
            if (index_meta) {
                indices_ids.emplace_back(index_meta->index_id());
            }
        }
    }

    PTabletWriteSlaveRequest request;
    RowsetMetaPB rowset_meta_pb = _cur_rowset->rowset_meta()->get_rowset_pb();
    request.set_allocated_rowset_meta(&rowset_meta_pb);
    request.set_host(BackendOptions::get_localhost());
    request.set_http_port(config::webserver_port);
    string tablet_path = _tablet->tablet_path();
    request.set_rowset_path(tablet_path);
    request.set_token(ExecEnv::GetInstance()->token());
    request.set_brpc_port(config::brpc_port);
    request.set_node_id(node_info.id());
    for (int segment_id = 0; segment_id < _cur_rowset->rowset_meta()->num_segments();
         segment_id++) {
        std::stringstream segment_name;
        segment_name << _cur_rowset->rowset_id() << "_" << segment_id << ".dat";
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
    request.release_rowset_meta();

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
                     << ". slave host: " << node_info.host()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _req.txn_id;
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
                      << "], tablet_id=" << _tablet->tablet_id() << ", node_id=" << node_id;
    }
    _unfinished_slave_node.erase(node_id);
}

int64_t DeltaWriter::num_rows_filtered() const {
    return _rowset_writer == nullptr ? 0 : _rowset_writer->num_rows_filtered();
}

} // namespace doris
