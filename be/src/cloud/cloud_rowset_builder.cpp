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

#include <bvar/bvar.h>
#include <gen_cpp/internal_service.pb.h>

#include <algorithm>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_rowset_writer.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "exec/common/variant_util.h"
#include "io/fs/file_system.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_policy.h"
#include "storage/tablet_info.h"
#include "util/defer_op.h"

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
    if (_req.write_req_type == WriteRequestType::ROW_BINLOG || !_attach_rowset_ids.empty()) {
        context.enable_segcompaction = false;
    }
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    context.write_binlog_opt().enable = _req.write_req_type == WriteRequestType::ROW_BINLOG;
    context.file_cache_expiration_time = _tablet->file_cache_ttl_expiration_time();
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
    RETURN_IF_ERROR(group_writer->init(_data_builder->rowset_writer()->context()));

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
    return _engine.meta_mgr().commit_rowset(*_data_builder->rowset_meta(), job_id, table_id,
                                            nullptr, _row_binlog_builder->rowset_meta().get());
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
    tablet->fetch_add_approximate_cumu_num_deltas(std::max<int64_t>(_rowset->num_segments(), 1));
    tablet->write_count.fetch_add(1, std::memory_order_relaxed);
}

CloudTablet* CloudRowsetBuilder::cloud_tablet() {
    return static_cast<CloudTablet*>(_tablet.get());
}

const RowsetMetaSharedPtr& CloudRowsetBuilder::rowset_meta() {
    return _rowset_writer->rowset_meta();
}

bool CloudRowsetBuilder::is_s3_storage() const {
    if (_rowset_writer == nullptr) {
        return false;
    }
    return _rowset_writer->context().fs()->type() == io::FileSystemType::S3;
}

Status CloudRowsetBuilder::commit_rowset(const std::string& job_id, int64_t table_id) {
    return _engine.meta_mgr().commit_rowset(*rowset_meta(), job_id, table_id);
}

Status CloudRowsetBuilder::get_mow_snapshot_for_sink(PCloudLoadMowSnapshot* snapshot) {
    DORIS_CHECK(_tablet->enable_unique_key_merge_on_write());
    if (_mow_snapshot_for_sink == nullptr) {
        RETURN_IF_ERROR(cloud_tablet()->sync_rowsets());
        auto context = _rowset_writer->context().mow_context;
        DORIS_CHECK(context != nullptr);
        DeleteBitmap snapshot_bitmap(_tablet->tablet_id());
        {
            std::unique_lock sync_lock(cloud_tablet()->get_sync_meta_lock());
            std::shared_lock lock(_tablet->get_header_lock());
            if (_tablet->tablet_state() != TABLET_RUNNING) {
                return Status::NotSupported("sink MOW load requires a running tablet {}",
                                            _tablet->tablet_id());
            }
            _max_version_in_flush_phase = _tablet->max_version_unlocked();
            _rowset_ids->clear();
            RETURN_IF_ERROR(_tablet->get_all_rs_id_unlocked(_max_version_in_flush_phase,
                                                            _rowset_ids.get()));
            context->max_version = _max_version_in_flush_phase;
            context->rowset_ptrs = _tablet->get_rowset_by_ids(_rowset_ids.get());
            std::vector<DeleteBitmap::RowsetIdWithSegmentIds> rowset_segments;
            for (const auto& rowset : context->rowset_ptrs) {
                std::vector<DeleteBitmap::SegmentId> ids;
                for (auto segment : rowset->segments()) {
                    ids.push_back(cast_set<DeleteBitmap::SegmentId>(segment.id()));
                }
                rowset_segments.emplace_back(rowset->rowset_id(), std::move(ids));
            }
            _tablet->tablet_meta()->delete_bitmap().subset_and_agg(
                    rowset_segments, 0, _max_version_in_flush_phase, &snapshot_bitmap);
        }
        _mow_snapshot_for_sink = std::make_unique<PCloudLoadMowSnapshot>();
        _mow_snapshot_for_sink->set_version(_max_version_in_flush_phase);
        // Keep the rowset references in MowContext until the load has finished.
        for (const auto& rowset : context->rowset_ptrs) {
            auto* meta = _mow_snapshot_for_sink->add_rowsets();
            *meta = rowset->rowset_meta()->get_rowset_pb();
            meta->clear_tablet_schema();
            rowset->tablet_schema()->to_schema_pb(meta->mutable_tablet_schema());
        }
        *_mow_snapshot_for_sink->mutable_delete_bitmap() = snapshot_bitmap.to_pb();
    }
    *snapshot = *_mow_snapshot_for_sink;
    DBUG_EXECUTE_IF("CloudRowsetBuilder.sink_mow.snapshot_ready", {
        // Expose readiness while blocked, independent of asynchronous log flushing.
        static bvar::Adder<int64_t> waiters("cloud_memtable_mow_snapshot_waiters");
        waiters << 1;
        Defer release([] { waiters << -1; });
        DBUG_BLOCK;
    });
    return Status::OK();
}

Status CloudRowsetBuilder::validate_sink_mow_result(const PCloudLoadMowResult& result,
                                                    int64_t snapshot_version) {
    if (!result.has_snapshot_version() || result.snapshot_version() != snapshot_version ||
        !result.has_delete_bitmap()) {
        return Status::InvalidArgument("missing or mismatched sink MOW snapshot result");
    }
    const auto& bitmap = result.delete_bitmap();
    const auto count = bitmap.rowset_ids_size();
    if (bitmap.segment_ids_size() != count || bitmap.versions_size() != count ||
        bitmap.segment_delete_bitmaps_size() != count) {
        return Status::InvalidArgument("misaligned sink MOW delete bitmap");
    }
    for (int pos = 0; pos < count; ++pos) {
        if (bitmap.versions(pos) != DeleteBitmap::TEMP_VERSION_COMMON) {
            return Status::InvalidArgument("sink MOW bitmap must use the temporary version");
        }
        const auto& bytes = bitmap.segment_delete_bitmaps(pos);
        const auto size =
                roaring::api::roaring_bitmap_portable_deserialize_size(bytes.data(), bytes.size());
        if (size == 0 || size != bytes.size()) {
            return Status::Corruption("invalid serialized sink MOW bitmap");
        }
    }
    return Status::OK();
}

Status CloudRowsetBuilder::merge_sink_mow_bitmap(const PCloudLoadMowResult& result) {
    DORIS_CHECK(_mow_snapshot_for_sink != nullptr);
    RETURN_IF_ERROR(validate_sink_mow_result(result, _mow_snapshot_for_sink->version()));
    _delete_bitmap->merge(DeleteBitmap::from_pb(result.delete_bitmap(), _tablet->tablet_id()));
    return Status::OK();
}

Status CloudRowsetBuilder::validate_partial_rowset_meta(const RowsetMetaPB& base_meta,
                                                        const RowsetMetaPB& partial_meta,
                                                        int32_t segment_start_id,
                                                        int32_t segment_capacity) {
    const auto count = partial_meta.num_segments();
    if (partial_meta.rowset_id_v2() != base_meta.rowset_id_v2() ||
        partial_meta.tablet_id() != base_meta.tablet_id() ||
        partial_meta.txn_id() != base_meta.txn_id() ||
        partial_meta.resource_id() != base_meta.resource_id() ||
        partial_meta.index_id() != base_meta.index_id() ||
        partial_meta.partition_id() != base_meta.partition_id() ||
        partial_meta.tablet_schema_hash() != base_meta.tablet_schema_hash() ||
        partial_meta.table_id() != base_meta.table_id() ||
        partial_meta.db_id() != base_meta.db_id() ||
        UniqueId(partial_meta.load_id()) != UniqueId(base_meta.load_id())) {
        return Status::InvalidArgument("sink upload rowset identity mismatch for tablet {}",
                                       base_meta.tablet_id());
    }
    if (!partial_meta.has_tablet_schema() || count < 0 || count > segment_capacity ||
        partial_meta.segment_ids_size() != count || partial_meta.num_segment_rows_size() != count ||
        partial_meta.segments_key_bounds_size() != count ||
        partial_meta.segments_file_size_size() != count ||
        partial_meta.segments_key_bounds_aggregated() ||
        (partial_meta.inverted_index_file_info_size() != 0 &&
         partial_meta.inverted_index_file_info_size() != count)) {
        return Status::InvalidArgument("misaligned sink upload metadata for tablet {}",
                                       base_meta.tablet_id());
    }
    const bool has_index =
            std::ranges::any_of(partial_meta.tablet_schema().index(), [](const auto& index) {
                return index.index_type() == IndexType::INVERTED ||
                       index.index_type() == IndexType::ANN;
            });
    if (has_index && partial_meta.inverted_index_file_info_size() != count) {
        return Status::InvalidArgument("missing sink upload index metadata for tablet {}",
                                       base_meta.tablet_id());
    }
    int64_t partial_rows = 0;
    for (int pos = 0; pos < count; ++pos) {
        const int64_t id = partial_meta.segment_ids(pos);
        if (id < segment_start_id ||
            id >= static_cast<int64_t>(segment_start_id) + segment_capacity ||
            (pos > 0 && id <= partial_meta.segment_ids(pos - 1)) ||
            partial_meta.num_segment_rows(pos) < 0 || partial_meta.segments_file_size(pos) <= 0) {
            return Status::InvalidArgument("invalid sink upload segment {} for tablet {}", id,
                                           base_meta.tablet_id());
        }
        partial_rows += partial_meta.num_segment_rows(pos);
    }
    if (partial_rows != partial_meta.num_rows() || partial_meta.data_disk_size() < 0 ||
        partial_meta.index_disk_size() < 0 ||
        partial_meta.total_disk_size() !=
                partial_meta.data_disk_size() + partial_meta.index_disk_size()) {
        return Status::InvalidArgument("invalid sink upload statistics for tablet {}",
                                       base_meta.tablet_id());
    }
    return Status::OK();
}

Status CloudRowsetBuilder::assemble_rowset_meta_from_partials(
        const RowsetMetaPB& base_meta, const std::map<int32_t, RowsetMetaPB>& partial_rowset_metas,
        int32_t max_segments_per_rowset, RowsetMetaPB* result) {
    *result = base_meta;
    int64_t rows = 0;
    int64_t data_size = 0;
    int64_t index_size = 0;
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(base_meta.tablet_schema());
    const bool has_variant = schema->num_variant_columns() > 0;
    std::vector<TabletSchemaSPtr> schemas;
    for (const auto& [segment_start_id, partial_meta] : partial_rowset_metas) {
        const auto count = partial_meta.num_segments();
        if (count > 0 && result->segment_ids_size() > 0 &&
            partial_meta.segment_ids(0) <= result->segment_ids(result->segment_ids_size() - 1)) {
            return Status::InvalidArgument("overlapping sink upload segment ranges");
        }
        result->mutable_segment_ids()->MergeFrom(partial_meta.segment_ids());
        result->mutable_num_segment_rows()->MergeFrom(partial_meta.num_segment_rows());
        result->mutable_segments_file_size()->MergeFrom(partial_meta.segments_file_size());
        result->mutable_segments_key_bounds()->MergeFrom(partial_meta.segments_key_bounds());
        result->mutable_inverted_index_file_info()->MergeFrom(
                partial_meta.inverted_index_file_info());
        for (const auto& [path, location] : partial_meta.packed_slice_locations()) {
            if (!result->mutable_packed_slice_locations()->emplace(path, location).second) {
                return Status::InvalidArgument("duplicate packed slice {}", path);
            }
        }
        rows += partial_meta.num_rows();
        data_size += partial_meta.data_disk_size();
        index_size += partial_meta.index_disk_size();
        result->set_segments_key_bounds_truncated(result->segments_key_bounds_truncated() ||
                                                  partial_meta.segments_key_bounds_truncated());
        if (has_variant) {
            auto partial_schema = std::make_shared<TabletSchema>();
            partial_schema->init_from_pb(partial_meta.tablet_schema());
            schemas.push_back(std::move(partial_schema));
        }
    }
    if (result->segment_ids_size() > max_segments_per_rowset) {
        return Status::InvalidArgument("too many sink upload segments for tablet {}",
                                       base_meta.tablet_id());
    }
    if (has_variant && !schemas.empty()) {
        TabletSchemaSPtr merged_schema;
        schemas.push_back(schema);
        RETURN_IF_ERROR(variant_util::get_least_common_schema(schemas, nullptr, merged_schema));
        result->clear_tablet_schema();
        merged_schema->to_schema_pb(result->mutable_tablet_schema());
    }
    result->set_num_segments(result->segment_ids_size());
    result->set_num_rows(rows);
    result->set_data_disk_size(data_size);
    result->set_index_disk_size(index_size);
    result->set_total_disk_size(data_size + index_size);
    result->set_empty(rows == 0);
    result->set_segments_overlap_pb(
            schema->cluster_key_uids().empty() &&
                            !is_segment_overlapping(result->segments_key_bounds())
                    ? NONOVERLAPPING
                    : OVERLAPPING);
    result->set_enable_segments_file_size(true);
    result->set_enable_inverted_index_file_info(true);
    result->set_creation_time(UnixSeconds());
    result->set_newest_write_timestamp(UnixSeconds());
    result->set_rowset_state(COMMITTED);
    return Status::OK();
}

Status CloudRowsetBuilder::build_rowset_from_assembled_meta(const RowsetMetaPB& meta) {
    return static_cast<CloudRowsetWriter*>(_rowset_writer.get())
            ->build_from_assembled_meta(meta, _rowset);
}

Status CloudRowsetBuilder::commit_txn() {
    DCHECK(is_data_builder());
    if (!_skip_writing_rowset_metadata) {
        RETURN_IF_ERROR(commit_rowset("", _tablet->table_id()));
    }
    RETURN_IF_ERROR(set_txn_related_info());
    update_tablet_stats();
    _is_committed = true;
    return Status::OK();
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
        if (config::enable_cloud_make_rs_visible_on_be && !_tablet_schema->is_tso_enabled()) {
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
