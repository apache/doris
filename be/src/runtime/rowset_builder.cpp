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

#include "runtime/rowset_builder.h"

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
#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/data_dir.h"
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

RowsetBuilder::RowsetBuilder(BuildContext* context, const UniqueId& load_id)
        : _context(*context),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(new TabletSchema),
          _storage_engine(StorageEngine::instance()),
          _load_id(load_id),
          _success(false) {}

RowsetBuilder::~RowsetBuilder() {
    if (_is_init && !_success) {
        _garbage_collection();
    }

    if (!_is_init) {
        return;
    }

    if (_tablet != nullptr) {
        _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                _rowset_writer->rowset_id().to_string());
    }
}

void RowsetBuilder::_garbage_collection() {
    Status rollback_status = Status::OK();
    TxnManager* txn_mgr = _storage_engine->txn_manager();
    if (_tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_context.partition_id, _tablet, _context.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

Status RowsetBuilder::init() {
    _rowset_meta.reset(new RowsetMeta);
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_context.tablet_id);
    if (_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("fail to find tablet. tablet_id={}, schema_hash={}",
                                              _context.tablet_id, _context.schema_hash);
    }

    // get rowset ids snapshot
    if (_tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::shared_mutex> lck(_tablet->get_header_lock());
        _cur_max_version = _tablet->max_version_unlocked().second;
        _rowset_ids = _tablet->all_rs_id(_cur_max_version);
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
                    "failed to init delta writer. version count: {} exceeds limit: {}. tablet: {}",
                    _tablet->version_count(), config::max_tablet_version_num, _tablet->full_name());
        }
    }

    {
        std::shared_lock base_migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!base_migration_rlock.owns_lock()) {
            return Status::Error<TRY_LOCK_FAILED>("try base migration lock failed");
        }
        std::lock_guard<std::mutex> push_lock(_tablet->get_push_lock());
        RETURN_IF_ERROR(_storage_engine->txn_manager()->prepare_txn(
                _context.partition_id, _tablet, _context.txn_id, _context.load_id));
    }
    if (_tablet->enable_unique_key_merge_on_write() && _delete_bitmap == nullptr) {
        _delete_bitmap.reset(new DeleteBitmap(_tablet->tablet_id()));
    }
    // build tablet schema in request level
    RETURN_IF_ERROR(_build_current_tablet_schema(_context.index_id, _context.table_schema_param,
                                                 *_tablet->tablet_schema()));
    RowsetWriterContext context;
    context.txn_id = _context.txn_id;
    context.load_id = _context.load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.tablet_id = _tablet->table_id();
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = std::make_shared<MowContext>(_cur_max_version, _context.txn_id,
                                                       _rowset_ids, _delete_bitmap);
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &_rowset_writer));

    _is_init = true;
    return Status::OK();
}

Status RowsetBuilder::append_data(uint32_t segid, butil::IOBuf buf) {
    DCHECK(_is_init);
    io::FileWriter* file_writer = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        if (segid + 1 > _segment_file_writers.size()) {
            for (size_t i = _segment_file_writers.size(); i <= segid; i++) {
                Status st;
                io::FileWriterPtr file_writer;
                st = _rowset_writer->create_file_writer(i, file_writer);
                if (!st.ok()) {
                    _is_canceled = true;
                    return st;
                }
                LOG(INFO) << " file_writer " << file_writer << "seg id " << i;
                _segment_file_writers.push_back(std::move(file_writer));
            }
        }

        // TODO: IOBuf to Slice
        file_writer = _segment_file_writers[segid].get();
    }
    LOG(INFO) << " file_writer " << file_writer << "seg id " << segid;
    return file_writer->append(buf.to_string());
}

Status RowsetBuilder::close_segment(uint32_t segid) {
    auto st = _segment_file_writers[segid]->close();

    std::lock_guard<std::mutex> l(_segment_stat_map_lock);
#if 0
    if (_segment_stat_map.find(segid) != _segment_stat_map.end()) {
        LOG(WARNING) << "already closed. segid=" << segid;
        return Status::OK();
    }
    _segment_stat_map[segid] = stat;
#endif
    if (!st.ok()) {
        _is_canceled = true;
        return st;
    }
    if (_segment_file_writers[segid]->bytes_appended() == 0) {
        return Status::Corruption("segment {} is zero bytes", segid);
    }
    LOG(INFO) << "segid" << segid << "path " << _segment_file_writers[segid]->path() << " write "
              << _segment_file_writers[segid]->bytes_appended();
    return Status::OK();
}

Status RowsetBuilder::add_segment(uint32_t segid, SegmentStatistics& stat) {
    _rowset_writer->add_segment(segid, stat);
    return Status::OK();
}

Status RowsetBuilder::close() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this RowsetBuilder, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }

    DCHECK(_is_init)
            << "rowset builder is supposed be to initialized before close_wait() being called";

    if (_is_canceled) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("flush segment failed");
    }

    for (size_t i = 0; i < _segment_file_writers.size(); i++) {
        if (!_segment_file_writers[i]->is_closed()) {
            return Status::Corruption("segment {} is not eos", i);
        }
    }

    /*
    if (_rowset_writer->num_rows() + _memtable_stat.merged_rows != _total_received_rows) {
        LOG(WARNING) << "the rows number written doesn't match, rowset num rows written to file: "
                     << _rowset_writer->num_rows()
                     << ", merged_rows: " << _memtable_stat.merged_rows
                     << ", total received rows: " << _total_received_rows;
        return Status::InternalError("rows number written by delta writer dosen't match");
    }*/
    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    // _cur_rowset = _build_rowset();
    if (_cur_rowset == nullptr) {
        return Status::Error<MEM_ALLOC_FAILED>("fail to build rowset");
    }

    if (_tablet->enable_unique_key_merge_on_write()) {
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
        RETURN_IF_ERROR(_tablet->commit_phase_update_delete_bitmap(
                _cur_rowset, _rowset_ids, _delete_bitmap, segments, _context.txn_id,
                _rowset_writer.get()));
    }
    Status res = _storage_engine->txn_manager()->commit_txn(
            _context.partition_id, _tablet, _context.txn_id, _context.load_id, _cur_rowset, false);

    if (!res && !res.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        LOG(WARNING) << "Failed to commit txn: " << _context.txn_id
                     << " for rowset: " << _cur_rowset->rowset_id();
        return res;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        _storage_engine->txn_manager()->set_txn_related_delete_bitmap(
                _context.partition_id, _context.txn_id, _tablet->tablet_id(),
                _tablet->schema_hash(), _tablet->tablet_uid(), true, _delete_bitmap, _rowset_ids);
    }
    _is_closed = true;
    _success = true;
    return Status::OK();
}

Status RowsetBuilder::_build_current_tablet_schema(int64_t index_id,
                                                   const OlapTableSchemaParam* table_schema_param,
                                                   const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        LOG(INFO) << "index " << indexes[i]->index_id << " dest " << index_id;
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }

    if (i == indexes.size()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown index_id {}", index_id);
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
    return Status::OK();
}

RowsetSharedPtr RowsetBuilder::_build_rowset() {
    Status st;

    int64_t num_seg = 0;
    int64_t num_rows_written = 0;
    int64_t total_data_size = 0;
    int64_t total_index_size = 0;
    std::vector<KeyBoundsPB> segments_encoded_key_bounds;

    for (const auto& itr : _segment_stat_map) {
        SegmentStatisticsSharedPtr stat = _segment_stat_map[itr.first];
        num_rows_written += itr.second->row_num;
        total_data_size += itr.second->data_size;
        total_index_size += itr.second->index_size;
        segments_encoded_key_bounds.push_back(itr.second->key_bounds);
        num_seg++;
    }

    // TODO overlapping?
    _rowset_meta->set_num_segments(num_seg);
    _rowset_meta->set_num_rows(num_rows_written);
    _rowset_meta->set_total_disk_size(total_data_size);
    _rowset_meta->set_data_disk_size(total_data_size);
    _rowset_meta->set_index_disk_size(total_index_size);
    _rowset_meta->set_segments_key_bounds(segments_encoded_key_bounds);
    // TODO write zonemap to meta
    _rowset_meta->set_empty((num_rows_written) == 0);
    _rowset_meta->set_creation_time(time(nullptr));
    _rowset_meta->set_rowset_state(VISIBLE); //TODO COMMITTED?
    _rowset_meta->set_rowset_type(RowsetTypePB::BETA_ROWSET);

    RowsetSharedPtr rowset;
    st = RowsetFactory::create_rowset(_tablet_schema, _tablet->data_dir()->path(), _rowset_meta,
                                      &rowset);
    if (st != Status::OK()) {
        LOG(WARNING) << "fail to create rowset. st=" << st.to_string();
        return nullptr;
    }
    return rowset;
}

} // namespace doris
