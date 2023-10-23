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

#include "olap/rowset_builder.h"

#include <brpc/controller.h>
#include <fmt/format.h>

#include <filesystem>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "util/trace.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

RowsetBuilder::RowsetBuilder(const WriteRequest& req, RuntimeProfile* profile)
        : _req(req), _tablet_schema(std::make_shared<TabletSchema>()) {
    _init_profile(profile);
}

void RowsetBuilder::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("RowsetBuilder {}", _req.tablet_id), true, true);
    _build_rowset_timer = ADD_TIMER(_profile, "BuildRowsetTime");
    _submit_delete_bitmap_timer = ADD_TIMER(_profile, "DeleteBitmapSubmitTime");
    _wait_delete_bitmap_timer = ADD_TIMER(_profile, "DeleteBitmapWaitTime");
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
}

RowsetBuilder::~RowsetBuilder() {
    if (_is_init && !_is_committed) {
        _garbage_collection();
    }

    if (!_is_init) {
        return;
    }

    if (_calc_delete_bitmap_token != nullptr) {
        _calc_delete_bitmap_token->cancel();
    }
}

void RowsetBuilder::_garbage_collection() {
    if (config::cloud_mode) {
        return;
    }
    Status rollback_status;
    TxnManager* txn_mgr = StorageEngine::instance()->txn_manager();
    auto tablet = static_cast<Tablet*>(_tablet.get());
    if (tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, *tablet, _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will fail.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        StorageEngine::instance()->add_unused_rowset(_rowset);
    }
}

Status RowsetBuilder::init_mow_context(std::shared_ptr<MowContext>& mow_context) {
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::NotSupported("init_mow_context");
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    std::lock_guard<std::shared_mutex> lck(tablet->get_header_lock());
    int64_t cur_max_version = tablet->max_version_unlocked().second;
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
        // Disable 'partial_update' when the tablet is undergoing a 'schema changing process'
        if (_req.table_schema_param->is_partial_update()) {
            return Status::InternalError(
                    "Unable to do 'partial_update' when "
                    "the tablet is undergoing a 'schema changing process'");
        }
        _rowset_ids.clear();
    } else {
        _rowset_ids = tablet->all_rs_id(cur_max_version);
    }
    _delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
    mow_context =
            std::make_shared<MowContext>(cur_max_version, _req.txn_id, _rowset_ids, _delete_bitmap);
    return Status::OK();
}

Status RowsetBuilder::check_tablet_version_count() {
    if (!_tablet->exceed_version_limit(config::max_tablet_version_num - 100) ||
        MemInfo::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
        return Status::OK();
    }
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::OK();
    }
    auto tablet = std::static_pointer_cast<Tablet>(_tablet);
    //trigger compaction
    auto st = StorageEngine::instance()->submit_compaction_task(
            tablet, CompactionType::CUMULATIVE_COMPACTION, true);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "failed to trigger compaction, tablet_id=" << _tablet->tablet_id() << " : "
                     << st;
    }
    int version_count = tablet->version_count();
    if (version_count > config::max_tablet_version_num) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. version count: {}, exceed limit: {}, "
                "tablet: {}",
                version_count, config::max_tablet_version_num, tablet->tablet_id());
    }
    return Status::OK();
}

Status RowsetBuilder::prepare_txn() {
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::OK();
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    std::shared_lock base_migration_lock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try migration lock failed");
    }
    std::lock_guard<std::mutex> push_lock(tablet->get_push_lock());
    return StorageEngine::instance()->txn_manager()->prepare_txn(_req.partition_id, *tablet,
                                                                 _req.txn_id, _req.load_id);
}

Status RowsetBuilder::init() {
    _tablet = DORIS_TRY(ExecEnv::get_tablet(_req.tablet_id));
    std::shared_ptr<MowContext> mow_context;
    if (_tablet->enable_unique_key_merge_on_write()) {
        RETURN_IF_ERROR(init_mow_context(mow_context));
    }

    if (!config::disable_auto_compaction) {
        RETURN_IF_ERROR(check_tablet_version_count());
    }

    RETURN_IF_ERROR(prepare_txn());

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
    context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    std::unique_ptr<RowsetWriter> rowset_writer;
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &rowset_writer));
    _rowset_writer = std::move(rowset_writer);

    if (config::cloud_mode) {
        // TODO(plat1ko)
    } else {
        _calc_delete_bitmap_token =
                StorageEngine::instance()->calc_delete_bitmap_executor()->create_token();
    }

    _is_init = true;
    return Status::OK();
}

Status RowsetBuilder::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init) << "rowset builder is supposed be to initialized before "
                        "build_rowset() being called";

    SCOPED_TIMER(_build_rowset_timer);
    // use rowset meta manager to save meta
    RETURN_NOT_OK_STATUS_WITH_WARN(_rowset_writer->build(_rowset), "fail to build rowset");
    return Status::OK();
}

Status RowsetBuilder::submit_calc_delete_bitmap_task() {
    if (!_tablet->enable_unique_key_merge_on_write()) {
        return Status::OK();
    }
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::OK();
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_submit_delete_bitmap_timer);
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(tablet->tablet_id())) {
        LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated later, "
                     "tablet_id: "
                  << tablet->tablet_id() << " txn_id: " << _req.txn_id;
        return Status::OK();
    }
    auto beta_rowset = reinterpret_cast<BetaRowset*>(_rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(tablet->tablet_id())) {
        return Status::OK();
    }
    if (segments.size() > 1) {
        // calculate delete bitmap between segments
        RETURN_IF_ERROR(
                tablet->calc_delete_bitmap_between_segments(_rowset, segments, _delete_bitmap));
    }

    // For partial update, we need to fill in the entire row of data, during the calculation
    // of the delete bitmap. This operation is resource-intensive, and we need to minimize
    // the number of times it occurs. Therefore, we skip this operation here.
    if (_partial_update_info->is_partial_update) {
        return Status::OK();
    }

    LOG(INFO) << "submit calc delete bitmap task to executor, tablet_id: " << tablet->tablet_id()
              << ", txn_id: " << _req.txn_id;
    return tablet->commit_phase_update_delete_bitmap(_rowset, _rowset_ids, _delete_bitmap, segments,
                                                     _req.txn_id, _calc_delete_bitmap_token.get(),
                                                     nullptr);
}

Status RowsetBuilder::wait_calc_delete_bitmap() {
    if (!_tablet->enable_unique_key_merge_on_write() || _partial_update_info->is_partial_update) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_wait_delete_bitmap_timer);
    RETURN_IF_ERROR(_calc_delete_bitmap_token->wait());
    RETURN_IF_ERROR(_calc_delete_bitmap_token->get_delete_bitmap(_delete_bitmap));
    LOG(INFO) << "Got result of calc delete bitmap task from executor, tablet_id: "
              << _tablet->tablet_id() << ", txn_id: " << _req.txn_id;
    return Status::OK();
}

Status RowsetBuilder::commit_txn() {
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::OK();
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    if (tablet->enable_unique_key_merge_on_write() &&
        config::enable_merge_on_write_correctness_check && _rowset->num_rows() != 0 &&
        !(tablet->tablet_state() == TABLET_NOTREADY &&
          SchemaChangeHandler::tablet_in_converting(tablet->tablet_id()))) {
        auto st = tablet->check_delete_bitmap_correctness(
                _delete_bitmap, _rowset->end_version() - 1, _req.txn_id, _rowset_ids);
        if (!st.ok()) {
            LOG(WARNING) << fmt::format(
                    "[tablet_id:{}][txn_id:{}][load_id:{}][partition_id:{}] "
                    "delete bitmap correctness check failed in commit phase!",
                    _req.tablet_id, _req.txn_id, UniqueId(_req.load_id).to_string(),
                    _req.partition_id);
            return st;
        }
    }
    auto storage_engine = StorageEngine::instance();
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_commit_txn_timer);
    Status res = storage_engine->txn_manager()->commit_txn(_req.partition_id, *tablet, _req.txn_id,
                                                           _req.load_id, _rowset, false);

    if (!res && !res.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id
                     << " for rowset: " << _rowset->rowset_id();
        return res;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        storage_engine->txn_manager()->set_txn_related_delete_bitmap(
                _req.partition_id, _req.txn_id, tablet->tablet_id(), tablet->tablet_uid(), true,
                _delete_bitmap, _rowset_ids, _partial_update_info);
    }

    _is_committed = true;
    return Status::OK();
}

Status RowsetBuilder::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    if (_calc_delete_bitmap_token != nullptr) {
        _calc_delete_bitmap_token->cancel();
    }
    _is_cancelled = true;
    return Status::OK();
}

void RowsetBuilder::_build_current_tablet_schema(int64_t index_id,
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
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    _partial_update_info->init(*_tablet_schema, table_schema_param->is_partial_update(),
                               table_schema_param->partial_update_input_columns(),
                               table_schema_param->is_strict_mode());
}

} // namespace doris
