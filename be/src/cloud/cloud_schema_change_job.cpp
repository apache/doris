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

#include "cloud/cloud_schema_change_job.h"

#include <gen_cpp/cloud.pb.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/status.h"
#include "olap/delete_handler.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "service/backend_options.h"
#include "util/debug_points.h"

namespace doris {
using namespace ErrorCode;

static constexpr int ALTER_TABLE_BATCH_SIZE = 4096;
static constexpr int SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID = -2;

std::unique_ptr<SchemaChange> get_sc_procedure(const BlockChanger& changer, bool sc_sorting,
                                               int64_t mem_limit) {
    if (sc_sorting) {
        return std::make_unique<VBaseSchemaChangeWithSorting>(changer, mem_limit);
    }
    // else sc_directly
    return std::make_unique<VSchemaChangeDirectly>(changer);
}

CloudSchemaChangeJob::CloudSchemaChangeJob(CloudStorageEngine& cloud_storage_engine,
                                           std::string job_id, int64_t expiration)
        : _cloud_storage_engine(cloud_storage_engine),
          _job_id(std::move(job_id)),
          _expiration(expiration) {
    _initiator = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                 std::numeric_limits<int64_t>::max();
}

CloudSchemaChangeJob::~CloudSchemaChangeJob() = default;

Status CloudSchemaChangeJob::process_alter_tablet(const TAlterTabletReqV2& request) {
    DBUG_EXECUTE_IF("CloudSchemaChangeJob::process_alter_tablet.block", DBUG_BLOCK);
    // new tablet has to exist
    _new_tablet = DORIS_TRY(_cloud_storage_engine.tablet_mgr().get_tablet(request.new_tablet_id));
    if (_new_tablet->tablet_state() == TABLET_RUNNING) {
        LOG(INFO) << "schema change job has already finished. base_tablet_id="
                  << request.base_tablet_id << ", new_tablet_id=" << request.new_tablet_id
                  << ", alter_version=" << request.alter_version << ", job_id=" << _job_id;
        return Status::OK();
    }

    _base_tablet = DORIS_TRY(_cloud_storage_engine.tablet_mgr().get_tablet(request.base_tablet_id));

    static constexpr long TRY_LOCK_TIMEOUT = 30;
    std::unique_lock schema_change_lock(_base_tablet->get_schema_change_lock(), std::defer_lock);
    bool owns_lock = schema_change_lock.try_lock_for(std::chrono::seconds(TRY_LOCK_TIMEOUT));

    _new_tablet->set_alter_failed(false);
    Defer defer([this] {
        // if tablet state is not TABLET_RUNNING when return, indicates that alter has failed.
        if (_new_tablet->tablet_state() != TABLET_RUNNING) {
            _new_tablet->set_alter_failed(true);
        }
    });

    if (!owns_lock) {
        LOG(WARNING) << "Failed to obtain schema change lock, there might be inverted index being "
                        "built on base_tablet="
                     << request.base_tablet_id;
        return Status::Error<TRY_LOCK_FAILED>(
                "Failed to obtain schema change lock, there might be inverted index being "
                "built on base_tablet=",
                request.base_tablet_id);
    }
    // MUST sync rowsets before capturing rowset readers and building DeleteHandler
    SyncOptions options;
    options.query_version = request.alter_version;
    RETURN_IF_ERROR(_base_tablet->sync_rowsets(options));
    // ATTN: Only convert rowsets of version larger than 1, MUST let the new tablet cache have rowset [0-1]
    _output_cumulative_point = _base_tablet->cumulative_layer_point();
    std::vector<RowSetSplits> rs_splits;
    int64_t base_max_version = _base_tablet->max_version_unlocked();
    cloud::TabletJobInfoPB job;
    auto* idx = job.mutable_idx();
    idx->set_tablet_id(_base_tablet->tablet_id());
    idx->set_table_id(_base_tablet->table_id());
    idx->set_index_id(_base_tablet->index_id());
    idx->set_partition_id(_base_tablet->partition_id());
    auto* sc_job = job.mutable_schema_change();
    sc_job->set_id(_job_id);
    sc_job->set_initiator(BackendOptions::get_localhost() + ':' +
                          std::to_string(config::heartbeat_service_port));
    sc_job->set_alter_version(base_max_version);
    auto* new_tablet_idx = sc_job->mutable_new_tablet_idx();
    new_tablet_idx->set_tablet_id(_new_tablet->tablet_id());
    new_tablet_idx->set_table_id(_new_tablet->table_id());
    new_tablet_idx->set_index_id(_new_tablet->index_id());
    new_tablet_idx->set_partition_id(_new_tablet->partition_id());
    cloud::StartTabletJobResponse start_resp;
    auto st = _cloud_storage_engine.meta_mgr().prepare_tablet_job(job, &start_resp);
    if (!st.ok()) {
        if (start_resp.status().code() == cloud::JOB_ALREADY_SUCCESS) {
            st = _new_tablet->sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", _new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }
    DBUG_EXECUTE_IF("CloudSchemaChangeJob::process_alter_tablet.alter_fail", {
        auto res =
                Status::InternalError("inject alter tablet failed. base_tablet={}, new_tablet={}",
                                      request.base_tablet_id, request.new_tablet_id);
        LOG(WARNING) << "inject error. res=" << res;
        return res;
    });
    if (request.alter_version > 1) {
        // [0-1] is a placeholder rowset, no need to convert
        RETURN_IF_ERROR(_base_tablet->capture_rs_readers({2, start_resp.alter_version()},
                                                         &rs_splits, false));
    }
    Defer defer2 {[&]() {
        _new_tablet->set_alter_version(-1);
        _base_tablet->set_alter_version(-1);
    }};
    _new_tablet->set_alter_version(start_resp.alter_version());
    _base_tablet->set_alter_version(start_resp.alter_version());
    LOG(INFO) << "Begin to alter tablet. base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << start_resp.alter_version() << ", job_id=" << _job_id;
    sc_job->set_alter_version(start_resp.alter_version());

    // FIXME(cyx): Should trigger compaction on base_tablet if there are too many rowsets to convert.

    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    _base_tablet_schema = std::make_shared<TabletSchema>();
    _base_tablet_schema->update_tablet_columns(*_base_tablet->tablet_schema(), request.columns);
    _new_tablet_schema = _new_tablet->tablet_schema();

    std::vector<ColumnId> return_columns;
    return_columns.resize(_base_tablet_schema->num_columns());
    std::iota(return_columns.begin(), return_columns.end(), 0);

    // delete handlers to filter out deleted rows
    DeleteHandler delete_handler;
    std::vector<RowsetMetaSharedPtr> delete_predicates;
    for (auto& split : rs_splits) {
        auto& rs_meta = split.rs_reader->rowset()->rowset_meta();
        if (rs_meta->has_delete_predicate()) {
            _base_tablet_schema->merge_dropped_columns(*rs_meta->tablet_schema());
            delete_predicates.push_back(rs_meta);
        }
    }
    RETURN_IF_ERROR(delete_handler.init(_base_tablet_schema, delete_predicates,
                                        start_resp.alter_version()));

    // reader_context is stack variables, it's lifetime MUST keep the same with rs_readers
    RowsetReaderContext reader_context;
    reader_context.reader_type = ReaderType::READER_ALTER_TABLE;
    reader_context.tablet_schema = _base_tablet_schema;
    reader_context.need_ordered_result = true;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
    reader_context.is_unique = _base_tablet->keys_type() == UNIQUE_KEYS;
    reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
    reader_context.delete_bitmap = &_base_tablet->tablet_meta()->delete_bitmap();
    reader_context.version = Version(0, start_resp.alter_version());
    std::vector<uint32_t> cluster_key_idxes;
    if (!_base_tablet_schema->cluster_key_uids().empty()) {
        for (const auto& uid : _base_tablet_schema->cluster_key_uids()) {
            cluster_key_idxes.emplace_back(_base_tablet_schema->field_index(uid));
        }
        reader_context.read_orderby_key_columns = &cluster_key_idxes;
        reader_context.is_unique = false;
        reader_context.sequence_id_idx = -1;
    }

    for (auto& split : rs_splits) {
        RETURN_IF_ERROR(split.rs_reader->init(&reader_context));
    }

    SchemaChangeParams sc_params;

    RETURN_IF_ERROR(DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl));
    sc_params.ref_rowset_readers.reserve(rs_splits.size());
    for (RowSetSplits& split : rs_splits) {
        sc_params.ref_rowset_readers.emplace_back(std::move(split.rs_reader));
    }
    sc_params.delete_handler = &delete_handler;
    sc_params.be_exec_version = request.be_exec_version;
    DCHECK(request.__isset.alter_tablet_type);
    switch (request.alter_tablet_type) {
    case TAlterTabletType::SCHEMA_CHANGE:
        sc_params.alter_tablet_type = AlterTabletType::SCHEMA_CHANGE;
        break;
    case TAlterTabletType::ROLLUP:
        sc_params.alter_tablet_type = AlterTabletType::ROLLUP;
        break;
    case TAlterTabletType::MIGRATION:
        sc_params.alter_tablet_type = AlterTabletType::MIGRATION;
        break;
    }
    sc_params.vault_id = request.storage_vault_id;
    if (!request.__isset.materialized_view_params) {
        return _convert_historical_rowsets(sc_params, job);
    }
    for (auto item : request.materialized_view_params) {
        AlterMaterializedViewParam mv_param;
        mv_param.column_name = item.column_name;
        /*
         * origin_column_name is always be set now,
         * but origin_column_name may be not set in some materialized view function. eg:count(1)
        */
        if (item.__isset.origin_column_name) {
            mv_param.origin_column_name = item.origin_column_name;
        }

        if (item.__isset.mv_expr) {
            mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
        }
        sc_params.materialized_params_map.insert(
                std::make_pair(to_lower(item.column_name), mv_param));
    }
    sc_params.enable_unique_key_merge_on_write = _new_tablet->enable_unique_key_merge_on_write();
    return _convert_historical_rowsets(sc_params, job);
}

Status CloudSchemaChangeJob::_convert_historical_rowsets(const SchemaChangeParams& sc_params,
                                                         cloud::TabletJobInfoPB& job) {
    LOG(INFO) << "Begin to convert historical rowsets for new_tablet from base_tablet. base_tablet="
              << _base_tablet->tablet_id() << ", new_tablet=" << _new_tablet->tablet_id()
              << ", job_id=" << _job_id;

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    BlockChanger changer(_new_tablet->tablet_schema(), *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // 1. Parse the Alter request and convert it into an internal representation
    RETURN_IF_ERROR(SchemaChangeJob::parse_request(sc_params, _base_tablet_schema.get(),
                                                   _new_tablet_schema.get(), &changer, &sc_sorting,
                                                   &sc_directly));
    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        LOG(INFO) << "Don't support to add materialized view by linked schema change";
        return Status::InternalError(
                "Don't support to add materialized view by linked schema change");
    }

    LOG(INFO) << "schema change type, sc_sorting: " << sc_sorting
              << ", sc_directly: " << sc_directly << ", base_tablet=" << _base_tablet->tablet_id()
              << ", new_tablet=" << _new_tablet->tablet_id();

    // 2. Generate historical data converter
    auto sc_procedure = get_sc_procedure(
            changer, sc_sorting,
            _cloud_storage_engine.memory_limitation_bytes_per_thread_for_schema_change());

    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_convert_historical_rowsets.block", DBUG_BLOCK);

    // 3. Convert historical data
    bool already_exist_any_version = false;
    for (const auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "Begin to convert a history rowset. version=" << rs_reader->version();

        RowsetWriterContext context;
        context.txn_id = rs_reader->rowset()->txn_id();
        context.txn_expiration = _expiration;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = _new_tablet->tablet_schema();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.storage_resource = _cloud_storage_engine.get_storage_resource(sc_params.vault_id);
        if (!context.storage_resource) {
            return Status::InternalError("vault id not found, maybe not sync, vault id {}",
                                         sc_params.vault_id);
        }

        context.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
        // TODO if support VerticalSegmentWriter, also need to handle cluster key primary key index
        bool vertical = false;
        if (sc_sorting && !_new_tablet->tablet_schema()->cluster_key_uids().empty()) {
            // see VBaseSchemaChangeWithSorting::_external_sorting
            vertical = true;
        }
        auto rowset_writer = DORIS_TRY(_new_tablet->create_rowset_writer(context, vertical));

        RowsetMetaSharedPtr existed_rs_meta;
        auto st = _cloud_storage_engine.meta_mgr().prepare_rowset(*rowset_writer->rowset_meta(),
                                                                  _job_id, &existed_rs_meta);
        if (!st.ok()) {
            if (st.is<ALREADY_EXIST>()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << _new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RETURN_IF_ERROR(
                        RowsetFactory::create_rowset(nullptr, "", existed_rs_meta, &rowset));
                _output_rowsets.push_back(std::move(rowset));
                already_exist_any_version = true;
                continue;
            } else {
                return st;
            }
        }

        st = sc_procedure->process(rs_reader, rowset_writer.get(), _new_tablet, _base_tablet,
                                   _base_tablet_schema, _new_tablet_schema);
        if (!st.ok()) {
            return Status::InternalError(
                    "failed to process schema change on rowset, version=[{}-{}], status={}",
                    rs_reader->version().first, rs_reader->version().second, st.to_string());
        }

        RowsetSharedPtr new_rowset;
        st = rowset_writer->build(new_rowset);
        if (!st.ok()) {
            return Status::InternalError("failed to build rowset, version=[{}-{}] status={}",
                                         rs_reader->version().first, rs_reader->version().second,
                                         st.to_string());
        }

        st = _cloud_storage_engine.meta_mgr().commit_rowset(*rowset_writer->rowset_meta(), _job_id,
                                                            &existed_rs_meta);
        if (!st.ok()) {
            if (st.is<ALREADY_EXIST>()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << _new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RETURN_IF_ERROR(
                        RowsetFactory::create_rowset(nullptr, "", existed_rs_meta, &rowset));
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }
        _output_rowsets.push_back(std::move(new_rowset));

        VLOG_TRACE << "Successfully convert a history version " << rs_reader->version();
    }
    auto* sc_job = job.mutable_schema_change();
    if (!sc_params.ref_rowset_readers.empty()) {
        int64_t num_output_rows = 0;
        int64_t size_output_rowsets = 0;
        int64_t num_output_segments = 0;
        int64_t index_size_output_rowsets = 0;
        int64_t segment_size_output_rowsets = 0;
        for (auto& rs : _output_rowsets) {
            sc_job->add_txn_ids(rs->txn_id());
            sc_job->add_output_versions(rs->end_version());
            num_output_rows += rs->num_rows();
            size_output_rowsets += rs->total_disk_size();
            num_output_segments += rs->num_segments();
            index_size_output_rowsets += rs->index_disk_size();
            segment_size_output_rowsets += rs->data_disk_size();
        }
        sc_job->set_num_output_rows(num_output_rows);
        sc_job->set_size_output_rowsets(size_output_rowsets);
        sc_job->set_num_output_segments(num_output_segments);
        sc_job->set_num_output_rowsets(_output_rowsets.size());
        sc_job->set_index_size_output_rowsets(index_size_output_rowsets);
        sc_job->set_segment_size_output_rowsets(segment_size_output_rowsets);
    }
    _output_cumulative_point = std::min(_output_cumulative_point, sc_job->alter_version() + 1);
    sc_job->set_output_cumulative_point(_output_cumulative_point);

    DBUG_EXECUTE_IF("CloudSchemaChangeJob.process_alter_tablet.sleep", DBUG_BLOCK);
    // process delete bitmap if the table is MOW
    bool has_stop_token {false};
    bool should_clear_stop_token {true};
    Defer defer {[&]() {
        if (has_stop_token) {
            static_cast<void>(_cloud_storage_engine.unregister_compaction_stop_token(
                    _new_tablet, should_clear_stop_token));
        }
    }};
    if (_new_tablet->enable_unique_key_merge_on_write()) {
        has_stop_token = true;
        // If there are historical versions of rowsets, we need to recalculate their delete
        // bitmaps, otherwise we will miss the delete bitmaps of incremental rowsets
        int64_t start_calc_delete_bitmap_version =
                // [0-1] is a placeholder rowset, start from 2.
                already_exist_any_version ? 2 : sc_job->alter_version() + 1;
        RETURN_IF_ERROR(_process_delete_bitmap(sc_job->alter_version(),
                                               start_calc_delete_bitmap_version, _initiator,
                                               sc_params.vault_id));
        sc_job->set_delete_bitmap_lock_initiator(_initiator);
    }

    cloud::FinishTabletJobResponse finish_resp;
    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_convert_historical_rowsets.test_conflict", {
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        int random_value = std::rand() % 100;
        if (random_value < 20) {
            return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR>("test txn conflict");
        }
    });
    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_convert_historical_rowsets.fail.before.commit_job", {
        LOG_INFO("inject retryable error before commit sc job, tablet={}",
                 _new_tablet->tablet_id());
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR>("injected retryable error");
    });
    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_convert_historical_rowsets.before.commit_job",
                    DBUG_BLOCK);
    auto st = _cloud_storage_engine.meta_mgr().commit_tablet_job(job, &finish_resp);
    if (!st.ok()) {
        if (finish_resp.status().code() == cloud::JOB_ALREADY_SUCCESS) {
            st = _new_tablet->sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", _new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    } else {
        should_clear_stop_token = false;
    }
    const auto& stats = finish_resp.stats();
    {
        // to prevent the converted historical rowsets be replaced by rowsets written on new tablet
        // during double write phase by `CloudMetaMgr::sync_tablet_rowsets` in another thread
        std::unique_lock lock {_new_tablet->get_sync_meta_lock()};
        std::unique_lock wlock(_new_tablet->get_header_lock());
        _new_tablet->add_rowsets(std::move(_output_rowsets), true, wlock);
        _new_tablet->set_cumulative_layer_point(_output_cumulative_point);
        _new_tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                             stats.num_rows(), stats.data_size());
        RETURN_IF_ERROR(_new_tablet->set_tablet_state(TABLET_RUNNING));
    }
    return Status::OK();
}

Status CloudSchemaChangeJob::_process_delete_bitmap(int64_t alter_version,
                                                    int64_t start_calc_delete_bitmap_version,
                                                    int64_t initiator,
                                                    const std::string& vault_id) {
    LOG_INFO("process mow table")
            .tag("new_tablet_id", _new_tablet->tablet_id())
            .tag("out_rowset_size", _output_rowsets.size())
            .tag("start_calc_delete_bitmap_version", start_calc_delete_bitmap_version)
            .tag("alter_version", alter_version);
    RETURN_IF_ERROR(_cloud_storage_engine.register_compaction_stop_token(_new_tablet, initiator));
    TabletMetaSharedPtr tmp_meta = std::make_shared<TabletMeta>(*(_new_tablet->tablet_meta()));
    tmp_meta->delete_bitmap().delete_bitmap.clear();
    std::shared_ptr<CloudTablet> tmp_tablet =
            std::make_shared<CloudTablet>(_cloud_storage_engine, tmp_meta);
    {
        std::unique_lock wlock(tmp_tablet->get_header_lock());
        tmp_tablet->add_rowsets(_output_rowsets, true, wlock);
    }

    // step 1, process incremental rowset without delete bitmap update lock
    std::vector<RowsetSharedPtr> incremental_rowsets;
    RETURN_IF_ERROR(_cloud_storage_engine.meta_mgr().sync_tablet_rowsets(tmp_tablet.get()));
    int64_t max_version = tmp_tablet->max_version().second;
    LOG(INFO) << "alter table for mow table, calculate delete bitmap of "
              << "incremental rowsets without lock, version: " << start_calc_delete_bitmap_version
              << "-" << max_version << " new_table_id: " << _new_tablet->tablet_id();
    if (max_version >= start_calc_delete_bitmap_version) {
        RETURN_IF_ERROR(tmp_tablet->capture_consistent_rowsets_unlocked(
                {start_calc_delete_bitmap_version, max_version}, &incremental_rowsets));
        DBUG_EXECUTE_IF("CloudSchemaChangeJob::_process_delete_bitmap.after.capture_without_lock",
                        DBUG_BLOCK);
        {
            std::unique_lock wlock(tmp_tablet->get_header_lock());
            tmp_tablet->add_rowsets(_output_rowsets, true, wlock);
        }
        for (auto rowset : incremental_rowsets) {
            RETURN_IF_ERROR(CloudTablet::update_delete_bitmap_without_lock(tmp_tablet, rowset));
        }
    }

    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_process_delete_bitmap.before_new_inc.block",
                    DBUG_BLOCK);

    // step 2, process incremental rowset with delete bitmap update lock
    RETURN_IF_ERROR(_cloud_storage_engine.meta_mgr().get_delete_bitmap_update_lock(
            *_new_tablet, SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID, initiator));
    RETURN_IF_ERROR(_cloud_storage_engine.meta_mgr().sync_tablet_rowsets(tmp_tablet.get()));
    int64_t new_max_version = tmp_tablet->max_version().second;
    LOG(INFO) << "alter table for mow table, calculate delete bitmap of "
              << "incremental rowsets with lock, version: " << max_version + 1 << "-"
              << new_max_version << " new_tablet_id: " << _new_tablet->tablet_id();
    std::vector<RowsetSharedPtr> new_incremental_rowsets;
    if (new_max_version > max_version) {
        RETURN_IF_ERROR(tmp_tablet->capture_consistent_rowsets_unlocked(
                {max_version + 1, new_max_version}, &new_incremental_rowsets));
        {
            std::unique_lock wlock(tmp_tablet->get_header_lock());
            tmp_tablet->add_rowsets(_output_rowsets, true, wlock);
        }
        for (auto rowset : new_incremental_rowsets) {
            RETURN_IF_ERROR(CloudTablet::update_delete_bitmap_without_lock(tmp_tablet, rowset));
        }
    }

    DBUG_EXECUTE_IF("CloudSchemaChangeJob::_process_delete_bitmap.inject_sleep", {
        auto p = dp->param("percent", 0.01);
        auto sleep_time = dp->param("sleep", 100);
        std::mt19937 gen {std::random_device {}()};
        std::bernoulli_distribution inject_fault {p};
        if (inject_fault(gen)) {
            LOG_INFO("injection sleep for {} seconds, tablet_id={}, sc job_id={}", sleep_time,
                     _new_tablet->tablet_id(), _job_id);
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
    });

    auto& delete_bitmap = tmp_tablet->tablet_meta()->delete_bitmap();
    auto storage_resource = _cloud_storage_engine.get_storage_resource(vault_id);
    // step4, store delete bitmap
    RETURN_IF_ERROR(_cloud_storage_engine.meta_mgr().update_delete_bitmap(
            *_new_tablet, SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID, initiator, &delete_bitmap,
            &delete_bitmap, "", storage_resource, config::delete_bitmap_store_write_version));

    _new_tablet->tablet_meta()->delete_bitmap() = delete_bitmap;
    return Status::OK();
}

void CloudSchemaChangeJob::clean_up_on_failure() {
    if (_new_tablet == nullptr) {
        return;
    }
    if (_new_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _new_tablet->enable_unique_key_merge_on_write()) {
        _cloud_storage_engine.meta_mgr().remove_delete_bitmap_update_lock(
                _new_tablet->table_id(), SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID, _initiator,
                _new_tablet->tablet_id());
    }
    for (const auto& output_rs : _output_rowsets) {
        if (output_rs.use_count() > 2) {
            LOG(WARNING) << "Rowset " << output_rs->rowset_id().to_string() << " has "
                         << output_rs.use_count()
                         << " references. File Cache won't be recycled when query is using it.";
            return;
        }
        output_rs->clear_cache();
    }
}

} // namespace doris
