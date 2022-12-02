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

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"

namespace doris {

Status DeltaWriter::open(WriteRequest* req, DeltaWriter** writer, const UniqueId& load_id,
                         bool is_vec) {
    *writer = new DeltaWriter(req, StorageEngine::instance(), load_id, is_vec);
    return Status::OK();
}

DeltaWriter::DeltaWriter(WriteRequest* req, StorageEngine* storage_engine, const UniqueId& load_id,
                         bool is_vec)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(new TabletSchema),
          _delta_written_success(false),
          _storage_engine(storage_engine),
          _load_id(load_id),
          _is_vec(is_vec) {}

DeltaWriter::~DeltaWriter() {
    if (_is_init && !_delta_written_success) {
        _garbage_collection();
    }

    if (!_is_init) {
        return;
    }

    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();

        if (_tablet != nullptr) {
            const FlushStatistic& stat = _flush_token->get_stats();
            _tablet->flush_bytes->increment(stat.flush_size_bytes);
            _tablet->flush_finish_count->increment(stat.flush_finish_count);
        }
    }

    if (_tablet != nullptr) {
        _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                _rowset_writer->rowset_id().to_string());
    }

    _mem_table.reset();
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
        LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id
                     << ", schema_hash=" << _req.schema_hash;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    // get rowset ids snapshot
    if (_tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::shared_mutex> lck(_tablet->get_header_lock());
        _cur_max_version = _tablet->max_version_unlocked().second;
        _rowset_ids = _tablet->all_rs_id(_cur_max_version);
    }

    // check tablet version number
    if (_tablet->version_count() > config::max_tablet_version_num - 100) {
        //trigger compaction
        StorageEngine::instance()->submit_compaction_task(_tablet,
                                                          CompactionType::CUMULATIVE_COMPACTION);
        if (_tablet->version_count() > config::max_tablet_version_num) {
            LOG(WARNING) << "failed to init delta writer. version count: "
                         << _tablet->version_count()
                         << ", exceed limit: " << config::max_tablet_version_num
                         << ". tablet: " << _tablet->full_name();
            return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_VERSION);
        }
    }

    {
        std::shared_lock base_migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!base_migration_rlock.owns_lock()) {
            return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
        }
        std::lock_guard<std::mutex> push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(_storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet,
                                                                  _req.txn_id, _req.load_id));
    }
    // build tablet schema in request level
    _build_current_tablet_schema(_req.index_id, _req.ptable_schema_param,
                                 *_tablet->tablet_schema());

    RETURN_NOT_OK(_tablet->create_rowset_writer(_req.txn_id, _req.load_id, PREPARED, OVERLAPPING,
                                                _tablet_schema, &_rowset_writer));
    _schema.reset(new Schema(_tablet_schema));
    _reset_mem_table();

    // create flush handler
    // unique key merge on write should flush serial cause calc delete bitmap should load segment serial
    bool should_serial = (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                          _tablet->enable_unique_key_merge_on_write());
    RETURN_NOT_OK(_storage_engine->memtable_flush_executor()->create_flush_token(
            &_flush_token, _rowset_writer->type(), should_serial, _req.is_high_priority));

    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::write(Tuple* tuple) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        // The writer may be cancelled at any time by other thread.
        // just return ERROR if writer is cancelled.
        return _cancel_status;
    }

    _mem_table->insert(tuple);

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        auto s = _flush_memtable_async();
        // create a new memtable for new incoming data
        _reset_mem_table();
        if (OLAP_UNLIKELY(!s.ok())) {
            return s;
        }
    }
    return Status::OK();
}

Status DeltaWriter::write(const RowBatch* row_batch, const std::vector<int>& row_idxs) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    for (const auto& row_idx : row_idxs) {
        _mem_table->insert(row_batch->get_row(row_idx)->get_tuple(0));
    }

    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        auto s = _flush_memtable_async();
        _reset_mem_table();
        if (OLAP_UNLIKELY(!s.ok())) {
            return s;
        }
    }

    return Status::OK();
}

Status DeltaWriter::write(const vectorized::Block* block, const std::vector<int>& row_idxs) {
    if (UNLIKELY(row_idxs.empty())) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    _mem_table->insert(block, row_idxs);

    if (_mem_table->need_to_agg()) {
        _mem_table->shrink_memtable_by_agg();
        if (_mem_table->is_flush()) {
            auto s = _flush_memtable_async();
            _reset_mem_table();
            if (UNLIKELY(!s.ok())) {
                return s;
            }
        }
    }

    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async() {
    return _flush_token->submit(std::move(_mem_table));
}

Status DeltaWriter::flush_memtable_and_wait(bool need_wait) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // This writer is not initialized before flushing. Do nothing
        // But we return OLAP_SUCCESS instead of Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED),
        // Because this method maybe called when trying to reduce mem consumption,
        // and at that time, the writer may not be initialized yet and that is a normal case.
        return Status::OK();
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    VLOG_NOTICE << "flush memtable to reduce mem consumption. memtable size: "
                << _mem_table->memory_usage() << ", tablet: " << _req.tablet_id
                << ", load id: " << print_id(_req.load_id);
    auto s = _flush_memtable_async();
    _reset_mem_table();
    if (UNLIKELY(!s.ok())) {
        return s;
    }

    if (need_wait) {
        // wait all memtables in flush queue to be flushed.
        RETURN_NOT_OK(_flush_token->wait());
    }
    return Status::OK();
}

Status DeltaWriter::wait_flush() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // return OLAP_SUCCESS instead of Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED) for same reason
        // as described in flush_memtable_and_wait()
        return Status::OK();
    }
    if (_is_cancelled) {
        return _cancel_status;
    }
    RETURN_NOT_OK(_flush_token->wait());
    return Status::OK();
}

void DeltaWriter::_reset_mem_table() {
    if (_tablet->enable_unique_key_merge_on_write() && _delete_bitmap == nullptr) {
        _delete_bitmap.reset(new DeleteBitmap(_tablet->tablet_id()));
    }
#ifndef BE_TEST
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num, _load_id.to_string()),
            nullptr, ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker_set());
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++, _load_id.to_string()),
            nullptr, ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker_set());
#else
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num, _load_id.to_string()));
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++, _load_id.to_string()));
#endif
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        _mem_table_tracker.push_back(mem_table_insert_tracker);
        _mem_table_tracker.push_back(mem_table_flush_tracker);
    }
    _mem_table.reset(new MemTable(_tablet, _schema.get(), _tablet_schema.get(), _req.slots,
                                  _req.tuple_desc, _rowset_writer.get(), _delete_bitmap,
                                  _rowset_ids, _cur_max_version, mem_table_insert_tracker,
                                  mem_table_flush_tracker, _is_vec));
}

Status DeltaWriter::close() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriter, so that it can create a empty rowset
        // for this tablet when being closed.
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    auto s = _flush_memtable_async();
    _mem_table.reset();
    if (OLAP_UNLIKELY(!s.ok())) {
        return s;
    } else {
        return Status::OK();
    }
}

Status DeltaWriter::close_wait(const PSlaveTabletNodes& slave_tablet_nodes,
                               const bool write_single_replica) {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return _cancel_status;
    }
    // return error if previous flush failed
    auto st = _flush_token->wait();
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "previous flush failed tablet " << _tablet->tablet_id();
        return st;
    }

    _mem_table.reset();

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        LOG(WARNING) << "fail to build rowset";
        return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
    }
    Status res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id,
                                                            _req.load_id, _cur_rowset, false);
    if (!res && res != Status::OLAPInternalError(OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST)) {
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

    const FlushStatistic& stat = _flush_token->get_stats();
    VLOG_CRITICAL << "close delta writer for tablet: " << _tablet->tablet_id()
                  << ", load id: " << print_id(_req.load_id) << ", stats: " << stat;

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
    if (!_is_init || _is_cancelled) {
        return Status::OK();
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _is_cancelled = true;
    _cancel_status = st;
    return Status::OK();
}

void DeltaWriter::save_mem_consumption_snapshot() {
    std::lock_guard<std::mutex> l(_lock);
    _mem_consumption_snapshot = mem_consumption();
    if (_mem_table == nullptr) {
        _memtable_consumption_snapshot = 0;
    } else {
        _memtable_consumption_snapshot = _mem_table->memory_usage();
    }
}

int64_t DeltaWriter::get_memtable_consumption_inflush() const {
    if (!_is_init || _flush_token->get_stats().flush_running_count == 0) return 0;
    return _mem_consumption_snapshot - _memtable_consumption_snapshot;
}

int64_t DeltaWriter::get_memtable_consumption_snapshot() const {
    return _memtable_consumption_snapshot;
}

int64_t DeltaWriter::mem_consumption() {
    if (_flush_token == nullptr) {
        // This method may be called before this writer is initialized.
        // So _flush_token may be null.
        return 0;
    }
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        for (auto mem_table_tracker : _mem_table_tracker) {
            mem_usage += mem_table_tracker->consumption();
        }
    }
    return mem_usage;
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

void DeltaWriter::_build_current_tablet_schema(int64_t index_id,
                                               const POlapTableSchemaParam& ptable_schema_param,
                                               const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);

    // find the right index id
    int i = 0;
    for (; i < ptable_schema_param.indexes_size(); i++) {
        if (ptable_schema_param.indexes(i).id() == index_id) break;
    }

    if (ptable_schema_param.indexes_size() > 0 &&
        ptable_schema_param.indexes(i).columns_desc_size() != 0 &&
        ptable_schema_param.indexes(i).columns_desc(0).unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, ptable_schema_param.version(),
                                                    ptable_schema_param.indexes(i),
                                                    ori_tablet_schema);
    }
    if (_tablet_schema->schema_version() > ori_tablet_schema.schema_version()) {
        _tablet->update_max_version_schema(_tablet_schema);
    }
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

    PTabletWriteSlaveRequest request;
    RowsetMetaPB rowset_meta_pb = _cur_rowset->rowset_meta()->get_rowset_pb();
    request.set_allocated_rowset_meta(&rowset_meta_pb);
    request.set_host(BackendOptions::get_localhost());
    request.set_http_port(config::single_replica_load_download_port);
    string tablet_path = _tablet->tablet_path();
    request.set_rowset_path(tablet_path);
    request.set_token(ExecEnv::GetInstance()->token());
    request.set_brpc_port(config::single_replica_load_brpc_port);
    request.set_node_id(node_info.id());
    for (int segment_id = 0; segment_id < _cur_rowset->rowset_meta()->num_segments();
         segment_id++) {
        std::stringstream segment_name;
        segment_name << _cur_rowset->rowset_id() << "_" << segment_id << ".dat";
        int64_t segment_size = std::filesystem::file_size(tablet_path + "/" + segment_name.str());
        request.mutable_segments_size()->insert({segment_id, segment_size});
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

} // namespace doris
