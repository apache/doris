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
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/olap_define.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "util/trace.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

BaseRowsetBuilder::BaseRowsetBuilder(const WriteRequest& req, RuntimeProfile* profile)
        : _req(req), _tablet_schema(std::make_shared<TabletSchema>()) {
    _init_profile(profile);
}

RowsetBuilder::RowsetBuilder(StorageEngine& engine, const WriteRequest& req,
                             RuntimeProfile* profile)
        : BaseRowsetBuilder(req, profile), _engine(engine) {}

void BaseRowsetBuilder::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("RowsetBuilder {}", _req.tablet_id), true, true);
    _build_rowset_timer = ADD_TIMER(_profile, "BuildRowsetTime");
    _submit_delete_bitmap_timer = ADD_TIMER(_profile, "DeleteBitmapSubmitTime");
    _wait_delete_bitmap_timer = ADD_TIMER(_profile, "DeleteBitmapWaitTime");
}

void RowsetBuilder::_init_profile(RuntimeProfile* profile) {
    BaseRowsetBuilder::_init_profile(profile);
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
}

BaseRowsetBuilder::~BaseRowsetBuilder() {
    if (!_is_init) {
        return;
    }

    if (_calc_delete_bitmap_token != nullptr) {
        _calc_delete_bitmap_token->cancel();
    }
}

RowsetBuilder::~RowsetBuilder() {
    if (_is_init && !_is_committed) {
        _garbage_collection();
    }
}

Tablet* RowsetBuilder::tablet() {
    return static_cast<Tablet*>(_tablet.get());
}

TabletSharedPtr RowsetBuilder::tablet_sptr() {
    return std::static_pointer_cast<Tablet>(_tablet);
}

void RowsetBuilder::_garbage_collection() {
    Status rollback_status;
    TxnManager* txn_mgr = _engine.txn_manager();
    if (tablet() != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, *tablet(), _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will fail.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _engine.add_unused_rowset(_rowset);
    }
}

Status BaseRowsetBuilder::init_mow_context(std::shared_ptr<MowContext>& mow_context) {
    std::lock_guard<std::shared_mutex> lck(tablet()->get_header_lock());
    _max_version_in_flush_phase = tablet()->max_version_unlocked();
    std::vector<RowsetSharedPtr> rowset_ptrs;
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet()->tablet_state() == TABLET_NOTREADY) {
        // Disable 'partial_update' when the tablet is undergoing a 'schema changing process'
        if (_req.table_schema_param->is_partial_update()) {
            return Status::InternalError(
                    "Unable to do 'partial_update' when "
                    "the tablet is undergoing a 'schema changing process'");
        }
        _rowset_ids.clear();
    } else {
        RETURN_IF_ERROR(
                tablet()->get_all_rs_id_unlocked(_max_version_in_flush_phase, &_rowset_ids));
        rowset_ptrs = tablet()->get_rowset_by_ids(&_rowset_ids);
    }
    _delete_bitmap = std::make_shared<DeleteBitmap>(tablet()->tablet_id());
    mow_context = std::make_shared<MowContext>(_max_version_in_flush_phase, _req.txn_id,
                                               _rowset_ids, rowset_ptrs, _delete_bitmap);
    return Status::OK();
}

Status RowsetBuilder::check_tablet_version_count() {
    bool injection = false;
    DBUG_EXECUTE_IF("RowsetBuilder.check_tablet_version_count.too_many_version",
                    { injection = true; });
    int32_t max_version_config = _tablet->max_version_config();
    if (injection) {
        // do not return if injection
    } else if (!_tablet->exceed_version_limit(max_version_config - 100) ||
               GlobalMemoryArbitrator::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
        return Status::OK();
    }
    //trigger compaction
    auto st = _engine.submit_compaction_task(tablet_sptr(), CompactionType::CUMULATIVE_COMPACTION,
                                             true);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "failed to trigger compaction, tablet_id=" << _tablet->tablet_id() << " : "
                     << st;
    }
    auto version_count = tablet()->version_count();
    DBUG_EXECUTE_IF("RowsetBuilder.check_tablet_version_count.too_many_version",
                    { version_count = INT_MAX; });
    if (version_count > max_version_config) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. version count: {}, exceed limit: {}, "
                "tablet: {}. Please reduce the frequency of loading data or adjust the "
                "max_tablet_version_num or time_series_max_tablet_version_num in be.conf to a "
                "larger value.",
                version_count, max_version_config, _tablet->tablet_id());
    }
    return Status::OK();
}

Status RowsetBuilder::prepare_txn() {
    return tablet()->prepare_txn(_req.partition_id, _req.txn_id, _req.load_id, false);
}

Status RowsetBuilder::init() {
    _tablet = DORIS_TRY(_engine.get_tablet(_req.tablet_id));
    std::shared_ptr<MowContext> mow_context;
    if (_tablet->enable_unique_key_merge_on_write()) {
        RETURN_IF_ERROR(init_mow_context(mow_context));
    }

    if (!config::disable_auto_compaction &&
        !_tablet->tablet_meta()->tablet_schema()->disable_auto_compaction()) {
        RETURN_IF_ERROR(check_tablet_version_count());
    }

    auto version_count = tablet()->version_count() + tablet()->stale_version_count();
    if (tablet()->avg_rs_meta_serialize_size() * version_count >
        config::tablet_meta_serialize_size_limit) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. meta serialize size : {}, exceed limit: {}, "
                "tablet: {}. Please reduce the frequency of loading data or adjust the "
                "max_tablet_version_num in be.conf to a larger value.",
                tablet()->avg_rs_meta_serialize_size() * version_count,
                config::tablet_meta_serialize_size_limit, _tablet->tablet_id());
    }

    RETURN_IF_ERROR(prepare_txn());

    DBUG_EXECUTE_IF("BaseRowsetBuilder::init.check_partial_update_column_num", {
        if (_req.table_schema_param->partial_update_input_columns().size() !=
            dp->param<int>("column_num")) {
            return Status::InternalError("partial update input column num wrong!");
        };
    })
    // build tablet schema in request level
    RETURN_IF_ERROR(_build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                                 *_tablet->tablet_schema()));
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _req.tablet_id;
    context.index_id = _req.index_id;
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    _rowset_writer = DORIS_TRY(_tablet->create_rowset_writer(context, false));
    _pending_rs_guard = _engine.pending_local_rowsets().add(context.rowset_id);

    _calc_delete_bitmap_token = _engine.calc_delete_bitmap_executor()->create_token();

    _is_init = true;
    return Status::OK();
}

Status BaseRowsetBuilder::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init) << "rowset builder is supposed be to initialized before "
                        "build_rowset() being called";

    SCOPED_TIMER(_build_rowset_timer);
    // use rowset meta manager to save meta
    RETURN_NOT_OK_STATUS_WITH_WARN(_rowset_writer->build(_rowset), "fail to build rowset");
    return Status::OK();
}

Status BaseRowsetBuilder::submit_calc_delete_bitmap_task() {
    if (!_tablet->enable_unique_key_merge_on_write() || _rowset->num_segments() == 0) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_submit_delete_bitmap_timer);
    if (_partial_update_info && _partial_update_info->is_flexible_partial_update()) {
        if (_rowset->num_segments() > 1) {
            // in flexible partial update, when there are more one segment in one load,
            // we need to do alignment process for same keys between segments, we haven't
            // implemented it yet and just report an error when encouter this situation
            return Status::NotSupported(
                    "too large input data in flexible partial update, Please "
                    "reduce the amount of data imported in a single load.");
        }
    }

    auto* beta_rowset = reinterpret_cast<BetaRowset*>(_rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
    if (segments.size() > 1) {
        // calculate delete bitmap between segments
        if (config::enable_calc_delete_bitmap_between_segments_concurrently) {
            RETURN_IF_ERROR(_calc_delete_bitmap_token->submit(
                    _tablet, _tablet_schema, _rowset->rowset_id(), segments, _delete_bitmap));
        } else {
            RETURN_IF_ERROR(_tablet->calc_delete_bitmap_between_segments(
                    _tablet_schema, _rowset->rowset_id(), segments, _delete_bitmap));
        }
    }

    // For partial update, we need to fill in the entire row of data, during the calculation
    // of the delete bitmap. This operation is resource-intensive, and we need to minimize
    // the number of times it occurs. Therefore, we skip this operation here.
    if (_partial_update_info->is_partial_update()) {
        // for partial update, the delete bitmap calculation is done while append_block()
        // we print it's summarize logs here before commit.
        LOG(INFO) << fmt::format(
                "{} calc delete bitmap summary before commit: tablet({}), txn_id({}), "
                "rowset_ids({}), cur max_version({}), bitmap num({}), bitmap_cardinality({}), num "
                "rows updated({}), num rows new added({}), num rows deleted({}), total rows({})",
                _partial_update_info->partial_update_mode_str(), tablet()->tablet_id(), _req.txn_id,
                _rowset_ids.size(), rowset_writer()->context().mow_context->max_version,
                _delete_bitmap->get_delete_bitmap_count(), _delete_bitmap->cardinality(),
                rowset_writer()->num_rows_updated(), rowset_writer()->num_rows_new_added(),
                rowset_writer()->num_rows_deleted(), rowset_writer()->num_rows());
        return Status::OK();
    }

    LOG(INFO) << "submit calc delete bitmap task to executor, tablet_id: " << tablet()->tablet_id()
              << ", txn_id: " << _req.txn_id;
    return BaseTablet::commit_phase_update_delete_bitmap(_tablet, _rowset, _rowset_ids,
                                                         _delete_bitmap, segments, _req.txn_id,
                                                         _calc_delete_bitmap_token.get(), nullptr);
}

Status BaseRowsetBuilder::wait_calc_delete_bitmap() {
    if (!_tablet->enable_unique_key_merge_on_write() || _partial_update_info->is_partial_update()) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_wait_delete_bitmap_timer);
    RETURN_IF_ERROR(_calc_delete_bitmap_token->wait());
    return Status::OK();
}

Status RowsetBuilder::commit_txn() {
    if (tablet()->enable_unique_key_merge_on_write() &&
        config::enable_merge_on_write_correctness_check && _rowset->num_rows() != 0 &&
        tablet()->tablet_state() != TABLET_NOTREADY) {
        auto st = tablet()->check_delete_bitmap_correctness(
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
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_commit_txn_timer);

    // Transfer ownership of `PendingRowsetGuard` to `TxnManager`
    Status res = _engine.txn_manager()->commit_txn(
            _req.partition_id, *tablet(), _req.txn_id, _req.load_id, _rowset,
            std::move(_pending_rs_guard), false, _partial_update_info);

    if (!res && !res.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id
                     << " for rowset: " << _rowset->rowset_id();
        return res;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        _engine.txn_manager()->set_txn_related_delete_bitmap(
                _req.partition_id, _req.txn_id, tablet()->tablet_id(), tablet()->tablet_uid(), true,
                _delete_bitmap, _rowset_ids, _partial_update_info);
    }

    _is_committed = true;
    return Status::OK();
}

Status BaseRowsetBuilder::cancel() {
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

Status BaseRowsetBuilder::_build_current_tablet_schema(
        int64_t index_id, const OlapTableSchemaParam* table_schema_param,
        const TabletSchema& ori_tablet_schema) {
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }
    if (!indexes.empty() && !indexes[i]->columns.empty() &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->shawdow_copy_without_columns(ori_tablet_schema);
        _tablet_schema->build_current_tablet_schema(
                index_id, cast_set<int32_t>(table_schema_param->version()), indexes[i],
                ori_tablet_schema);
    } else {
        _tablet_schema->copy_from(ori_tablet_schema);
    }
    if (_tablet_schema->schema_version() > ori_tablet_schema.schema_version()) {
        // After schema change, should include extracted column
        // For example: a table has two columns, k and v
        // After adding a column v2, the schema version increases, max_version_schema needs to be updated.
        // _tablet_schema includes k, v, and v2
        // if v is a variant, need to add the columns decomposed from the v to the _tablet_schema.
        if (_tablet_schema->num_variant_columns() > 0) {
            TabletSchemaSPtr max_version_schema = std::make_shared<TabletSchema>();
            max_version_schema->copy_from(*_tablet_schema);
            max_version_schema->copy_extracted_columns(ori_tablet_schema);
            _tablet->update_max_version_schema(max_version_schema);
        } else {
            _tablet->update_max_version_schema(_tablet_schema);
        }
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    _tablet_schema->set_db_id(table_schema_param->db_id());
    if (table_schema_param->is_partial_update()) {
        _tablet_schema->set_auto_increment_column(table_schema_param->auto_increment_coulumn());
    }
    // set partial update columns info
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    RETURN_IF_ERROR(_partial_update_info->init(
            tablet()->tablet_id(), _req.txn_id, *_tablet_schema,
            table_schema_param->unique_key_update_mode(),
            table_schema_param->partial_update_new_key_policy(),
            table_schema_param->partial_update_input_columns(),
            table_schema_param->is_strict_mode(), table_schema_param->timestamp_ms(),
            table_schema_param->nano_seconds(), table_schema_param->timezone(),
            table_schema_param->auto_increment_coulumn(),
            table_schema_param->sequence_map_col_uid(), _max_version_in_flush_phase));
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris
