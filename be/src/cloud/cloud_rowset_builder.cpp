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

#include "cloud/cloud_rowset_builder.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_policy.h"
#include "storage/tablet_info.h"

namespace doris {
using namespace ErrorCode;

CloudRowsetBuilder::CloudRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                                       RuntimeProfile* profile)
        : BaseRowsetBuilder(req, profile), _engine(engine) {}

CloudGroupRowsetBuilder::CloudGroupRowsetBuilder(CloudStorageEngine& engine,
                                                 const WriteRequest& group_build_req,
                                                 const WriteRequest& sub_data_req,
                                                 const WriteRequest& sub_row_binlog_req,
                                                 RuntimeProfile* profile)
        : CloudRowsetBuilder(engine, group_build_req, profile) {
    DCHECK(group_build_req.write_req_type == WriteRequestType::GROUP &&
           sub_data_req.write_req_type == WriteRequestType::DATA &&
           sub_row_binlog_req.write_req_type == WriteRequestType::ROW_BINLOG);
    _data_builder = std::make_shared<CloudRowsetBuilder>(engine, sub_data_req, profile);
    _row_binlog_builder = std::make_shared<CloudRowsetBuilder>(engine, sub_row_binlog_req, profile);
}

CloudRowsetBuilder::~CloudRowsetBuilder() {
    // Clear file cache immediately when load fails
    if (_is_init && _rowset != nullptr && _rowset->rowset_meta()->rowset_state() == PREPARED) {
        _rowset->clear_cache();
    }
}

Status CloudRowsetBuilder::init() {
    _tablet = DORIS_TRY(_engine.get_tablet(_req.tablet_id));

    std::shared_ptr<MowContext> mow_context;
    if (_tablet->enable_unique_key_merge_on_write() && is_data_builder()) {
        if (config::cloud_mow_sync_rowsets_when_load_txn_begin) {
            auto st = std::static_pointer_cast<CloudTablet>(_tablet)->sync_rowsets();
            // sync_rowsets will return INVALID_TABLET_STATE when tablet is under alter
            if (!st.ok() && !st.is<ErrorCode::INVALID_TABLET_STATE>()) {
                return st;
            }
        }
        RETURN_IF_ERROR(init_mow_context(mow_context));
    } else if (_req.write_req_type == WriteRequestType::ROW_BINLOG) {
        // Row binlog tablets use txn_delete_bitmap_cache for local make-visible.
        // The real binlog delete bitmap is derived when the base tablet calculates delete bitmap.
        _delete_bitmap = std::make_shared<DeleteBitmap>(_req.tablet_id);
    }
    RETURN_IF_ERROR(check_tablet_version_count());

    using namespace std::chrono;
    std::static_pointer_cast<CloudTablet>(_tablet)->last_load_time_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    // build tablet schema in request level
    RETURN_IF_ERROR(_build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                                 *_tablet->tablet_schema()));

    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.txn_expiration = _req.txn_expiration;
    context.load_id = _req.load_id;
    context.db_id = _req.table_schema_param->db_id();
    context.table_id = _req.table_schema_param->table_id();
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _req.tablet_id;
    context.tablet_schema_hash = _req.schema_hash;
    context.index_id = _req.index_id;
    context.tablet = _tablet;
    context.enable_segcompaction = true;
    if (_req.write_req_type == WriteRequestType::ROW_BINLOG || !_attach_rowset_ids.empty()) {
        context.enable_segcompaction = false;
    }
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    context.write_binlog_opt().enable = _req.write_req_type == WriteRequestType::ROW_BINLOG;
    context.file_cache_ttl_sec = _tablet->ttl_seconds();
    context.storage_resource = _engine.get_storage_resource(_req.storage_vault_id);
    if (!context.storage_resource) {
        return Status::InternalError("vault id not found, maybe not sync, vault id {}",
                                     _req.storage_vault_id);
    }

    _rowset_writer = DORIS_TRY(_tablet->create_rowset_writer(context, false));
    _rowset_id = context.rowset_id;

    _calc_delete_bitmap_token = _engine.calc_delete_bitmap_executor()->create_token();

    if (!_skip_writing_rowset_metadata) {
        RETURN_IF_ERROR(_engine.meta_mgr().prepare_rowset(*_rowset_writer->rowset_meta(), "",
                                                          _tablet->table_id()));
    }

    _is_init = true;
    return Status::OK();
}

Status CloudGroupRowsetBuilder::init() {
    RETURN_IF_ERROR(_row_binlog_builder->init());
    RETURN_IF_ERROR(
            _data_builder->attach_pending_rs_guard_to_txn(_row_binlog_builder->rowset_id()));
    RETURN_IF_ERROR(_data_builder->init());
    _tablet = _data_builder->tablet_sptr();

    std::unique_ptr<GroupRowsetWriter> group_writer;
    RETURN_IF_ERROR(RowsetFactory::create_empty_group_rowset_writer(&group_writer));
    group_writer->set_data_writer(_data_builder->rowset_writer());
    group_writer->set_row_binlog_writer(_row_binlog_builder->rowset_writer());

    {
        const auto& data_ctx = _data_builder->rowset_writer()->context();
        auto& binlog_ctx =
                const_cast<RowsetWriterContext&>(_row_binlog_builder->rowset_writer()->context());
        auto& cfg = binlog_ctx.write_binlog_opt().write_binlog_config();
        cfg.source.tablet_schema = data_ctx.tablet_schema;
        cfg.source.partial_update_info = data_ctx.partial_update_info;
        cfg.source.mow_context = data_ctx.mow_context;
        cfg.source.is_transient_rowset_writer = data_ctx.is_transient_rowset_writer;
        cfg.source.source_write_type = data_ctx.write_type;
        cfg.source.base_tablet = _data_builder->tablet_sptr();
    }

    _rowset_writer = std::move(group_writer);
    _is_init = true;
    return Status::OK();
}

Status CloudGroupRowsetBuilder::build_rowset() {
    RETURN_IF_ERROR(_row_binlog_builder->build_rowset());
    return _data_builder->build_rowset();
}

Status CloudGroupRowsetBuilder::submit_calc_delete_bitmap_task() {
    return _data_builder->submit_calc_delete_bitmap_task();
}

Status CloudGroupRowsetBuilder::wait_calc_delete_bitmap() {
    return _data_builder->wait_calc_delete_bitmap();
}

void CloudGroupRowsetBuilder::update_tablet_stats() {
    _data_builder->update_tablet_stats();
    _row_binlog_builder->update_tablet_stats();
}

Status CloudGroupRowsetBuilder::commit_rowset(const std::string& job_id, int64_t table_id) {
    return _engine.meta_mgr().commit_rowsets(*_data_builder->rowset_meta(),
                                             *_row_binlog_builder->rowset_meta(), job_id, table_id);
}

Status CloudGroupRowsetBuilder::set_txn_related_info() {
    RowBinlogTxnInfo attach_row_binlog;
    attach_row_binlog.rowset = _row_binlog_builder->rowset();
    attach_row_binlog.tablet = _row_binlog_builder->tablet_sptr();
    if (_data_builder->tablet()->enable_unique_key_merge_on_write()) {
        attach_row_binlog.delete_bitmap =
                std::make_shared<DeleteBitmap>(_row_binlog_builder->tablet()->tablet_id());
    }
    RETURN_IF_ERROR(_data_builder->attach_row_binlog_to_txn(attach_row_binlog));
    RETURN_IF_ERROR(_data_builder->set_txn_related_info());
    return _row_binlog_builder->set_txn_related_info();
}

void CloudGroupRowsetBuilder::set_skip_writing_rowset_metadata(bool skip) {
    _data_builder->set_skip_writing_rowset_metadata(skip);
    _row_binlog_builder->set_skip_writing_rowset_metadata(skip);
}

Status CloudRowsetBuilder::check_tablet_version_count() {
    int64_t version_count = cloud_tablet()->fetch_add_approximate_num_rowsets(0);
    DBUG_EXECUTE_IF("RowsetBuilder.check_tablet_version_count.too_many_version",
                    { version_count = INT_MAX; });
    // TODO(plat1ko): load backoff algorithm
    int32_t max_version_config = cloud_tablet()->max_version_config();
    if (version_count > max_version_config) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. version count: {}, exceed limit: {}, "
                "tablet: {}. Please reduce the frequency of loading data or adjust the "
                "max_tablet_version_num or time_series_max_tablet_version_numin be.conf to a "
                "larger value.",
                version_count, max_version_config, _tablet->tablet_id());
    }
    return Status::OK();
}

void CloudRowsetBuilder::update_tablet_stats() {
    auto* tablet = cloud_tablet();
    DCHECK(tablet);
    DCHECK(_rowset);
    tablet->fetch_add_approximate_num_rowsets(1);
    tablet->fetch_add_approximate_num_segments(_rowset->num_segments());
    tablet->fetch_add_approximate_num_rows(_rowset->num_rows());
    tablet->fetch_add_approximate_data_size(_rowset->total_disk_size());
    tablet->fetch_add_approximate_cumu_num_rowsets(1);
    tablet->fetch_add_approximate_cumu_num_deltas(_rowset->num_segments());
    tablet->write_count.fetch_add(1, std::memory_order_relaxed);
}

CloudTablet* CloudRowsetBuilder::cloud_tablet() {
    return static_cast<CloudTablet*>(_tablet.get());
}

const RowsetMetaSharedPtr& CloudRowsetBuilder::rowset_meta() {
    return _rowset_writer->rowset_meta();
}

Status CloudRowsetBuilder::commit_rowset(const std::string& job_id, int64_t table_id) {
    return _engine.meta_mgr().commit_rowset(*rowset_meta(), job_id, table_id);
}

Status CloudRowsetBuilder::set_txn_related_info() {
    if (_tablet->enable_unique_key_merge_on_write() || _tablet->is_row_binlog_tablet()) {
        // For empty rowsets when skip_writing_empty_rowset_metadata=true,
        // store only a lightweight marker instead of full rowset info.
        // This allows CalcDeleteBitmapTask to detect and skip gracefully,
        // while using minimal memory (~16 bytes per entry).
        if (_skip_writing_rowset_metadata) {
            _engine.txn_delete_bitmap_cache().mark_empty_rowset(_req.txn_id, _tablet->tablet_id(),
                                                                _req.txn_expiration);
            return Status::OK();
        }
        if (config::enable_merge_on_write_correctness_check &&
            _tablet->enable_unique_key_merge_on_write() && _rowset->num_rows() != 0) {
            auto st = _tablet->check_delete_bitmap_correctness(
                    _delete_bitmap, _rowset->end_version() - 1, _req.txn_id, *_rowset_ids);
            if (!st.ok()) {
                LOG(WARNING) << fmt::format(
                        "[tablet_id:{}][txn_id:{}][load_id:{}][partition_id:{}] "
                        "delete bitmap correctness check failed in commit phase!",
                        _req.tablet_id, _req.txn_id, UniqueId(_req.load_id).to_string(),
                        _req.partition_id);
                return st;
            }
        }
        _engine.txn_delete_bitmap_cache().set_tablet_txn_info(
                _req.txn_id, _tablet->tablet_id(), _delete_bitmap, *_rowset_ids, _rowset,
                _req.txn_expiration, _partial_update_info, _attach_row_binlog);
    } else {
        // TSO-enabled rowsets must become visible from MS rowset meta.
        if (config::enable_cloud_make_rs_visible_on_be && !_tablet_schema->enable_tso()) {
            if (_skip_writing_rowset_metadata) {
                _engine.committed_rs_mgr().mark_empty_rowset(_req.txn_id, _tablet->tablet_id(),
                                                             _req.txn_expiration);
            } else {
                _engine.meta_mgr().cache_committed_rowset(rowset_meta(), _req.txn_expiration);
            }
        }
    }
    return Status::OK();
}
} // namespace doris
