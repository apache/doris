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

namespace doris {

OLAPStatus DeltaWriter::open(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                             DeltaWriter** writer) {
    *writer = new DeltaWriter(req, parent, StorageEngine::instance());
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                         StorageEngine* storage_engine)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _new_rowset(nullptr),
          _new_tablet(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(nullptr),
          _delta_written_success(false),
          _storage_engine(storage_engine),
          _mem_tracker(MemTracker::CreateTracker(-1, "DeltaWriter", parent)) {}

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

        const FlushStatistic& stat = _flush_token->get_stats();
        _tablet->flush_bytes->increment(stat.flush_size_bytes);
        _tablet->flush_count->increment(stat.flush_count);
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
    if (_new_tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, _new_tablet, _req.txn_id);
        if (rollback_status == OLAP_SUCCESS) {
            _storage_engine->add_unused_rowset(_new_rowset);
        }
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

    // check tablet version number
    if (_tablet->version_count() > config::max_tablet_version_num) {
        LOG(WARNING) << "failed to init delta writer. version count: " << _tablet->version_count()
                     << ", exceed limit: " << config::max_tablet_version_num
                     << ". tablet: " << _tablet->full_name();
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        ReadLock base_migration_rlock(_tablet->get_migration_lock_ptr(), TRY_LOCK);
        if (!base_migration_rlock.own_lock()) {
            return OLAP_ERR_RWLOCK_ERROR;
        }
        MutexLock push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(_storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet,
                                                                  _req.txn_id, _req.load_id));
        if (_req.need_gen_rollup) {
            AlterTabletTaskSharedPtr alter_task = _tablet->alter_task();
            if (alter_task != nullptr && alter_task->alter_state() != ALTER_FAILED) {
                TTabletId new_tablet_id = alter_task->related_tablet_id();
                TSchemaHash new_schema_hash = alter_task->related_schema_hash();
                LOG(INFO) << "load with schema change. "
                          << "old_tablet_id=" << _tablet->tablet_id() << ", "
                          << ", old_schema_hash=" << _tablet->schema_hash() << ", "
                          << ", new_tablet_id=" << new_tablet_id << ", "
                          << ", new_schema_hash=" << new_schema_hash << ", "
                          << ", transaction_id=" << _req.txn_id;
                _new_tablet = tablet_mgr->get_tablet(new_tablet_id, new_schema_hash);
                if (_new_tablet == nullptr) {
                    LOG(WARNING) << "find alter task, but could not find new tablet. "
                                 << "new_tablet_id=" << new_tablet_id
                                 << ", new_schema_hash=" << new_schema_hash;
                    return OLAP_ERR_TABLE_NOT_FOUND;
                }
                ReadLock new_migration_rlock(_new_tablet->get_migration_lock_ptr(), TRY_LOCK);
                if (!new_migration_rlock.own_lock()) {
                    return OLAP_ERR_RWLOCK_ERROR;
                }
                RETURN_NOT_OK(_storage_engine->txn_manager()->prepare_txn(
                        _req.partition_id, _new_tablet, _req.txn_id, _req.load_id));
            }
        }
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
    writer_context.rowset_path_prefix = _tablet->tablet_path();
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
    RETURN_NOT_OK(_storage_engine->memtable_flush_executor()->create_flush_token(&_flush_token, writer_context.rowset_type));

    _is_init = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    std::lock_guard<SpinLock> l(_lock); 
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
        RETURN_NOT_OK(_flush_memtable_async());
        // create a new memtable for new incoming data
        _reset_mem_table();
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::_flush_memtable_async() {
    return _flush_token->submit(_mem_table);
}

OLAPStatus DeltaWriter::flush_memtable_and_wait(bool need_wait) {
    std::lock_guard<SpinLock> l(_lock); 
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
    std::lock_guard<SpinLock> l(_lock); 
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
                                  _req.tuple_desc, _tablet->keys_type(), _rowset_writer.get(),
                                  _mem_tracker));
}

OLAPStatus DeltaWriter::close() {
    std::lock_guard<SpinLock> l(_lock); 
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

OLAPStatus DeltaWriter::close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    std::lock_guard<SpinLock> l(_lock); 
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    // return error if previous flush failed
    RETURN_NOT_OK(_flush_token->wait());
    DCHECK_EQ(_mem_tracker->consumption(), 0);

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

    if (_new_tablet != nullptr) {
        LOG(INFO) << "convert version for schema change";
        SchemaChangeHandler schema_change;
        res = schema_change.schema_version_convert(_tablet, _new_tablet, &_cur_rowset,
                                                   &_new_rowset);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert delta for new tablet in schema change."
                         << "res: " << res << ", "
                         << "new_tablet: " << _new_tablet->full_name();
            return res;
        }

        res = _storage_engine->txn_manager()->commit_txn(
                _req.partition_id, _new_tablet, _req.txn_id, _req.load_id, _new_rowset, false);

        if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            LOG(WARNING) << "Failed to save pending rowset. rowset_id:" << _new_rowset->rowset_id();
            return res;
        }
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_tablet->tablet_id());
    tablet_info->set_schema_hash(_tablet->schema_hash());
    if (_new_tablet != nullptr) {
        tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(_new_tablet->tablet_id());
        tablet_info->set_schema_hash(_new_tablet->schema_hash());
    }
#endif

    _delta_written_success = true;

    const FlushStatistic& stat = _flush_token->get_stats();
    LOG(INFO) << "close delta writer for tablet: " << _tablet->tablet_id() << ", stats: " << stat;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    std::lock_guard<SpinLock> l(_lock); 
    if (!_is_init || _is_cancelled) {
        return OLAP_SUCCESS;
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    DCHECK_EQ(_mem_tracker->consumption(), 0);
    _is_cancelled = true;
    return OLAP_SUCCESS;
}

int64_t DeltaWriter::mem_consumption() const {
    return _mem_tracker->consumption();
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

} // namespace doris
