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

#include "olap/data_dir.h"
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

OLAPStatus DeltaWriter::open(WriteRequest* req, DeltaWriter** writer) {
    *writer = new DeltaWriter(req, StorageEngine::instance());
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(WriteRequest* req, StorageEngine* storage_engine)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(nullptr),
          _delta_written_success(false),
          _storage_engine(storage_engine) {}

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
    OLAPStatus rollback_status = OLAP_SUCCESS;
    TxnManager* txn_mgr = _storage_engine->txn_manager();
    if (_tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, _tablet, _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status == OLAP_SUCCESS) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

OLAPStatus DeltaWriter::init() {
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_req.tablet_id, _req.schema_hash);
    if (_tablet == nullptr) {
        LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id
                     << ", schema_hash=" << _req.schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    _mem_tracker =
            MemTracker::create_tracker(-1, "DeltaWriter:" + std::to_string(_tablet->tablet_id()));
    // check tablet version number
    if (_tablet->version_count() > config::max_tablet_version_num) {
        LOG(WARNING) << "failed to init delta writer. version count: " << _tablet->version_count()
                     << ", exceed limit: " << config::max_tablet_version_num
                     << ". tablet: " << _tablet->full_name();
        return OLAP_ERR_TOO_MANY_VERSION;
    }

    {
        ReadLock base_migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!base_migration_rlock.owns_lock()) {
            return OLAP_ERR_RWLOCK_ERROR;
        }
        std::lock_guard<std::mutex> push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(_storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet,
                                                                  _req.txn_id, _req.load_id));
    }

    RowsetWriterContext writer_context;
    writer_context.rowset_id = _storage_engine->next_rowset_id();
    writer_context.tablet_uid = _tablet->tablet_uid();
    writer_context.tablet_id = _req.tablet_id;
    writer_context.partition_id = _req.partition_id;
    writer_context.tablet_schema_hash = _req.schema_hash;
    if (_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
        writer_context.rowset_type = BETA_ROWSET;
    } else {
        writer_context.rowset_type = ALPHA_ROWSET;
    }
    writer_context.path_desc = _tablet->tablet_path_desc();
    writer_context.tablet_schema = &(_tablet->tablet_schema());
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = _req.txn_id;
    writer_context.load_id = _req.load_id;
    writer_context.segments_overlap = OVERLAPPING;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer));

    _tablet_schema = &(_tablet->tablet_schema());
    _schema.reset(new Schema(*_tablet_schema));
    _reset_mem_table();

    // create flush handler
    RETURN_NOT_OK(_storage_engine->memtable_flush_executor()->create_flush_token(&_flush_token,
            writer_context.rowset_type, _req.is_high_priority));

    _is_init = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        // The writer may be cancelled at any time by other thread.
        // just return ERROR if writer is cancelled.
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    _mem_table->insert(tuple);

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        if (++_segment_counter > config::max_segment_num_per_rowset) {
            return OLAP_ERR_TOO_MANY_SEGMENTS;
        }
        RETURN_NOT_OK(_flush_memtable_async());
        // create a new memtable for new incoming data
        _reset_mem_table();
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(const RowBatch* row_batch, const std::vector<int>& row_idxs) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    for (const auto& row_idx : row_idxs) {
        _mem_table->insert(row_batch->get_row(row_idx)->get_tuple(0));
    }

    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        RETURN_NOT_OK(_flush_memtable_async());
        _reset_mem_table();
    }

    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::_flush_memtable_async() {
    if (++_segment_counter > config::max_segment_num_per_rowset) {
        return OLAP_ERR_TOO_MANY_SEGMENTS;
    }
    return _flush_token->submit(_mem_table);
}

OLAPStatus DeltaWriter::flush_memtable_and_wait(bool need_wait) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // This writer is not initialized before flushing. Do nothing
        // But we return OLAP_SUCCESS instead of OLAP_ERR_ALREADY_CANCELLED,
        // Because this method maybe called when trying to reduce mem consumption,
        // and at that time, the writer may not be initialized yet and that is a normal case.
        return OLAP_SUCCESS;
    }
    
    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }

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
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::wait_flush() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // return OLAP_SUCCESS instead of OLAP_ERR_ALREADY_CANCELLED for same reason
        // as described in flush_memtable_and_wait()
        return OLAP_SUCCESS;
    }
    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }
    RETURN_NOT_OK(_flush_token->wait());
    return OLAP_SUCCESS;
}

void DeltaWriter::_reset_mem_table() {
    _mem_table.reset(new MemTable(_tablet->tablet_id(), _schema.get(), _tablet_schema, _req.slots,
                                  _req.tuple_desc, _tablet->keys_type(), _rowset_writer.get()));
}

OLAPStatus DeltaWriter::close() {
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
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    RETURN_NOT_OK(_flush_memtable_async());
    _mem_table.reset();
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec, bool is_broken) {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    // return error if previous flush failed
    RETURN_NOT_OK(_flush_token->wait());
    // Cannot directly DCHECK_EQ(_mem_tracker->consumption(), 0);
    // In allocate/free of mem_pool, the consume_cache of _mem_tracker will be called,
    // and _untracked_mem must be flushed first.
    MemTracker::memory_leak_check(_mem_tracker.get());

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        LOG(WARNING) << "fail to build rowset";
        return OLAP_ERR_MALLOC_ERROR;
    }
    OLAPStatus res = _storage_engine->txn_manager()->commit_txn(
            _req.partition_id, _tablet, _req.txn_id, _req.load_id, _cur_rowset, false);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id
                     << " for rowset: " << _cur_rowset->rowset_id();
        return res;
    }

#ifndef BE_TEST
    if (!is_broken) {
        PTabletInfo* tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(_tablet->tablet_id());
        tablet_info->set_schema_hash(_tablet->schema_hash());
    }
#endif

    _delta_written_success = true;

    const FlushStatistic& stat = _flush_token->get_stats();
    VLOG_CRITICAL << "close delta writer for tablet: " << _tablet->tablet_id() 
                  << ", load id: " << print_id(_req.load_id)
                  << ", stats: " << stat;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init || _is_cancelled) {
        return OLAP_SUCCESS;
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    MemTracker::memory_leak_check(_mem_tracker.get());
    _is_cancelled = true;
    return OLAP_SUCCESS;
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

} // namespace doris
