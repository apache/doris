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
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

Status DeltaWriter::open(WriteRequest* req, DeltaWriter** writer, MemTrackerLimiter* parent_tracker,
                         bool is_vec) {
    *writer = new DeltaWriter(req, StorageEngine::instance(), parent_tracker, is_vec);
    return Status::OK();
}

DeltaWriter::DeltaWriter(WriteRequest* req, StorageEngine* storage_engine,
                         MemTrackerLimiter* parent_tracker, bool is_vec)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(new TabletSchema),
          _delta_written_success(false),
          _storage_engine(storage_engine),
          _parent_tracker(parent_tracker),
          _is_vec(is_vec) {}

DeltaWriter::~DeltaWriter() {
    if (_is_init && !_delta_written_success) {
        _garbage_collection();
    }

    _mem_table.reset();

    if (!_is_init) {
        return;
    }

    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();

        if (_tablet != nullptr) {
            const FlushStatistic& stat = _flush_token->get_stats();
            _tablet->flush_bytes->increment(stat.flush_size_bytes);
            _tablet->flush_count->increment(stat.flush_count);
        }
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
        LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id
                     << ", schema_hash=" << _req.schema_hash;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }
    _mem_tracker = std::make_unique<MemTrackerLimiter>(
            -1, fmt::format("DeltaWriter:tabletId={}", _tablet->tablet_id()), _parent_tracker);
    SCOPED_ATTACH_TASK(_mem_tracker.get(), ThreadContext::TaskType::LOAD);
    // check tablet version number
    if (_tablet->version_count() > config::max_tablet_version_num) {
        //trigger quick compaction
        if (config::enable_quick_compaction) {
            StorageEngine::instance()->submit_quick_compaction_task(_tablet);
        }
        LOG(WARNING) << "failed to init delta writer. version count: " << _tablet->version_count()
                     << ", exceed limit: " << config::max_tablet_version_num
                     << ". tablet: " << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_VERSION);
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
    _build_current_tablet_schema(_req.index_id, _req.ptable_schema_param, _tablet->tablet_schema());

    RETURN_NOT_OK(_tablet->create_rowset_writer(_req.txn_id, _req.load_id, PREPARED, OVERLAPPING,
                                                _tablet_schema.get(), &_rowset_writer));
    _schema.reset(new Schema(*_tablet_schema));
    _reset_mem_table();

    // create flush handler
    RETURN_NOT_OK(_storage_engine->memtable_flush_executor()->create_flush_token(
            &_flush_token, _rowset_writer->type(), _req.is_high_priority));

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
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    int64_t prev_memtable_usage = _mem_table->memory_usage();
    _mem_table->insert(tuple);
    THREAD_MEM_TRACKER_TRANSFER_TO(_mem_table->memory_usage() - prev_memtable_usage,
                                   _mem_tracker.get());

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        if (++_segment_counter > config::max_segment_num_per_rowset) {
            return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_SEGMENTS);
        }
        RETURN_NOT_OK(_flush_memtable_async());
        // create a new memtable for new incoming data
        _reset_mem_table();
    }
    return Status::OK();
}

Status DeltaWriter::write(const RowBatch* row_batch, const std::vector<int>& row_idxs) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    // Hook automatically records that the memory is lower than the real value, so manual tracking is used.
    // Because multiple places freed memory that doesn't belong to DeltaWriter
    int64_t prev_memtable_usage = _mem_table->memory_usage();
    for (const auto& row_idx : row_idxs) {
        _mem_table->insert(row_batch->get_row(row_idx)->get_tuple(0));
    }
    THREAD_MEM_TRACKER_TRANSFER_TO(_mem_table->memory_usage() - prev_memtable_usage,
                                   _mem_tracker.get());

    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        RETURN_NOT_OK(_flush_memtable_async());
        _reset_mem_table();
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
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    SCOPED_ATTACH_TASK(_mem_tracker.get(), ThreadContext::TaskType::LOAD);
    _mem_table->insert(block, row_idxs);

    if (_mem_table->need_to_agg()) {
        _mem_table->shrink_memtable_by_agg();
        if (_mem_table->is_flush()) {
            RETURN_NOT_OK(_flush_memtable_async());
            _reset_mem_table();
        }
    }

    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async() {
    if (++_segment_counter > config::max_segment_num_per_rowset) {
        return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_SEGMENTS);
    }
    return _flush_token->submit(std::move(_mem_table), _mem_tracker.get());
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
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    SCOPED_ATTACH_TASK(_mem_tracker.get(), ThreadContext::TaskType::LOAD);
    if (mem_consumption() == _mem_table->memory_usage()) {
        // equal means there is no memtable in flush queue, just flush this memtable
        VLOG_NOTICE << "flush memtable to reduce mem consumption. memtable size: "
                    << _mem_table->memory_usage() << ", tablet: " << _req.tablet_id
                    << ", load id: " << print_id(_req.load_id);
        RETURN_NOT_OK(_flush_memtable_async());
        _reset_mem_table();
    } else {
        DCHECK(mem_consumption() > _mem_table->memory_usage());
        // this means there should be at least one memtable in flush queue.
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
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }
    RETURN_NOT_OK(_flush_token->wait());
    return Status::OK();
}

void DeltaWriter::_reset_mem_table() {
    _mem_table.reset(new MemTable(_tablet, _schema.get(), _tablet_schema.get(), _req.slots,
                                  _req.tuple_desc, _rowset_writer.get(), _is_vec));
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
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    SCOPED_ATTACH_TASK(_mem_tracker.get(), ThreadContext::TaskType::LOAD);
    RETURN_NOT_OK(_flush_memtable_async());
    _mem_table.reset();
    return Status::OK();
}

Status DeltaWriter::close_wait() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return Status::OLAPInternalError(OLAP_ERR_ALREADY_CANCELLED);
    }

    SCOPED_ATTACH_TASK(_mem_tracker.get(), ThreadContext::TaskType::LOAD);
    // return error if previous flush failed
    RETURN_NOT_OK(_flush_token->wait());

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

    _delta_written_success = true;

    const FlushStatistic& stat = _flush_token->get_stats();
    VLOG_CRITICAL << "close delta writer for tablet: " << _tablet->tablet_id()
                  << ", load id: " << print_id(_req.load_id) << ", stats: " << stat;
    return Status::OK();
}

Status DeltaWriter::cancel() {
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
    return Status::OK();
}

int64_t DeltaWriter::save_mem_consumption_snapshot() {
    _mem_consumption_snapshot = mem_consumption();
    return _mem_consumption_snapshot;
}

int64_t DeltaWriter::get_mem_consumption_snapshot() const {
    return _mem_consumption_snapshot;
}

int64_t DeltaWriter::mem_consumption() const {
    if (_mem_tracker == nullptr) {
        // This method may be called before this writer is initialized.
        // So _mem_tracker may be null.
        return 0;
    }
    return _mem_tracker->consumption();
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

void DeltaWriter::_build_current_tablet_schema(int64_t index_id,
                                               const POlapTableSchemaParam& ptable_schema_param,
                                               const TabletSchema& ori_tablet_schema) {
    *_tablet_schema = ori_tablet_schema;
    //new tablet schame if new table
    if (ptable_schema_param.indexes_size() > 0 &&
        ptable_schema_param.indexes(0).columns_desc_size() != 0 &&
        ptable_schema_param.indexes(0).columns_desc(0).unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, ptable_schema_param,
                                                    ori_tablet_schema);
    }
}

} // namespace doris
