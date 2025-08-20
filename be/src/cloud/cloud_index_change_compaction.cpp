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

#include "cloud/cloud_index_change_compaction.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"

namespace doris {

CloudIndexChangeCompaction::~CloudIndexChangeCompaction() = default;

CloudIndexChangeCompaction::CloudIndexChangeCompaction(CloudStorageEngine& engine,
                                                       CloudTabletSPtr tablet, bool is_drop,
                                                       std::vector<TOlapTableIndex>& alter_indexes)
        : CloudCompactionMixin(engine, tablet,
                               "CloudIndexChangeCompaction:" + std::to_string(tablet->tablet_id())),
          _is_drop(is_drop),
          _alter_indexes(alter_indexes) {}

Status CloudIndexChangeCompaction::prepare_compact() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudIndexChangeCompaction::prepare_compact", Status::OK());

    std::set<int64_t> alter_index_ids;
    for (auto index : _alter_indexes) {
        alter_index_ids.insert(index.index_id);
    }

    if (alter_index_ids.size() == 0) {
        LOG(WARNING) << "[index_change] no index is specified.";
        return Status::InternalError("no specified index.");
    } else {
        VLOG_DEBUG << "[index_change] alter_index_ids size=" << alter_index_ids.size();
    }

    if (_tablet->tablet_state() != TABLET_RUNNING) {
        LOG(WARNING) << "[index_change] tablet state is not running. tablet_id="
                     << _tablet->tablet_id();
        return Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
    }

    RETURN_IF_ERROR(cloud_tablet()->sync_rowsets());

    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = cloud_tablet()->base_compaction_cnt();
        _cumulative_compaction_cnt = cloud_tablet()->cumulative_compaction_cnt();
    }

    bool is_base_rowset = false;
    auto input_rowset = DORIS_TRY(cloud_tablet()->pick_a_rowset_for_index_change(
            alter_index_ids, _is_drop, is_base_rowset));
    if (input_rowset == nullptr) {
        return Status::OK();
    }

    if (is_base_rowset) {
        _compact_type = cloud::TabletCompactionJobPB::BASE;
    } else {
        _compact_type = cloud::TabletCompactionJobPB::CUMULATIVE;
    }

    _input_rowsets.push_back(input_rowset);

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_data_size += rs->data_disk_size();
        _input_rowsets_index_size += rs->index_disk_size();
        _input_rowsets_total_size += rs->total_disk_size();
    }
    LOG_INFO("[index_change]prepare CloudIndexChangeCompaction, tablet_id={}, range=[{}-{}]",
             _tablet->tablet_id(), _input_rowsets.front()->start_version(),
             _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("tablet_max_version", cloud_tablet()->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0));

    return Status::OK();
}

TabletSchemaSPtr CloudIndexChangeCompaction::get_output_schema() {
    TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
    output_rs_tablet_schema->copy_from(*_cur_tablet_schema);
    _output_schema = _is_drop ? _build_output_rs_index_schema_for_drop(output_rs_tablet_schema)
                              : _build_output_rs_index_schema_for_add(output_rs_tablet_schema);
    return _output_schema;
}

Status CloudIndexChangeCompaction::request_global_lock(bool& should_skip_err) {
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);

    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());

    if (is_base_compaction()) {
        compaction_job->set_type(cloud::TabletCompactionJobPB::BASE);
    } else {
        compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
        // Set input version range to let meta-service check version range conflict
        compaction_job->set_check_input_versions_range(config::enable_parallel_cumu_compaction);
    }

    cloud::StartTabletJobResponse resp;
    Status st = _engine.meta_mgr().prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
            should_skip_err = true;
        } else if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
#ifndef BE_TEST
            cloud_tablet()->clear_cache();
#endif
        } else if (resp.status().code() == cloud::JOB_TABLET_BUSY) {
            LOG_WARNING("[index_change]failed to prepare index change compaction")
                    .tag("job_id", _uuid)
                    .tag("msg", resp.status().msg());
            return Status::Error<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>(
                    "index change compaction no suitable versions: job tablet busy");
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            // NOTE: usually index change job and schema change job won't run run simultaneously.
            // just log here in case;
            (static_cast<CloudTablet*>(_tablet.get()))->set_alter_version(resp.alter_version());
            std::stringstream ss;
            ss << "[index_change]failed to prepare index change compaction. Check compaction input "
                  "versions "
                  "failed in schema change. "
                  "input_version_start="
               << compaction_job->input_versions(0)
               << " input_version_end=" << compaction_job->input_versions(1)
               << " schema_change_alter_version=" << resp.alter_version();
            std::string msg = ss.str();
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        return st;
    }

    return Status::OK();
}

Status CloudIndexChangeCompaction::execute_compact() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudIndexChangeCompaction::execute_compact", Status::OK());
    SCOPED_ATTACH_TASK(_mem_tracker);

    using namespace std::chrono;
    auto start = steady_clock::now();
    Status st;
    st = CloudCompactionMixin::execute_compact();
    if (!st.ok()) {
        LOG(WARNING) << "[index_change]fail to do " << compaction_name() << ". res=" << st
                     << ", tablet=" << _tablet->tablet_id()
                     << ", output_version=" << _output_version;
        return st;
    }
    LOG_INFO(
            "[index_change]finish CloudIndexChangeCompaction, tablet_id={}, cost={}ms, "
            "range=[{}-{}]",
            _tablet->tablet_id(), duration_cast<milliseconds>(steady_clock::now() - start).count(),
            _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_rowset_data_size", _output_rowset->data_disk_size())
            .tag("output_rowset_index_size", _output_rowset->index_disk_size())
            .tag("output_rowset_total_size", _output_rowset->total_disk_size())
            .tag("tablet_max_version", _tablet->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0))
            .tag("local_read_time_us", _stats.cloud_local_read_time)
            .tag("remote_read_time_us", _stats.cloud_remote_read_time)
            .tag("local_read_bytes", _local_read_bytes_total)
            .tag("remote_read_bytes", _remote_read_bytes_total);

    _state = CompactionState::SUCCESS;

    st = Status::OK();
    return st;
}

Status CloudIndexChangeCompaction::modify_rowsets() {
    // commit compaction job
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_input_cumulative_point(cloud_tablet()->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(cloud_tablet()->cumulative_layer_point());
    compaction_job->set_num_input_rows(_input_row_num);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(_input_rowsets_total_size);
    compaction_job->set_size_output_rowsets(_output_rowset->total_disk_size());
    compaction_job->set_num_input_segments(_input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(_input_rowsets.size());
    compaction_job->set_num_output_rowsets(1);
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    compaction_job->add_output_versions(_output_rowset->end_version());
    compaction_job->add_txn_id(_output_rowset->txn_id());
    compaction_job->add_output_rowset_ids(_output_rowset->rowset_id().to_string());
    compaction_job->set_index_size_input_rowsets(_input_rowsets_index_size);
    compaction_job->set_segment_size_input_rowsets(_input_rowsets_data_size);
    compaction_job->set_index_size_output_rowsets(_output_rowset->index_disk_size());
    compaction_job->set_segment_size_output_rowsets(_output_rowset->data_disk_size());
    compaction_job->set_type(_compact_type);

    DeleteBitmapPtr output_rowset_delete_bitmap = nullptr;
    int64_t initiator = this->initiator();
    int64_t get_delete_bitmap_lock_start_time = 0;
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        RETURN_IF_ERROR(cloud_tablet()->calc_delete_bitmap_for_compaction(
                _input_rowsets, _output_rowset, *_rowid_conversion, compaction_type(),
                _stats.merged_rows, _stats.filtered_rows, initiator, output_rowset_delete_bitmap,
                _allow_delete_in_cumu_compaction, get_delete_bitmap_lock_start_time));
        LOG_INFO(
                "[index_change]update delete bitmap in CloudIndexChangeCompaction, tablet_id={}, "
                "range=[{}-{}]",
                _tablet->tablet_id(), _input_rowsets.front()->start_version(),
                _input_rowsets.back()->end_version())
                .tag("job_id", _uuid)
                .tag("initiator", initiator)
                .tag("input_rowsets", _input_rowsets.size())
                .tag("input_rows", _input_row_num)
                .tag("input_segments", _input_segments)
                .tag("number_output_delete_bitmap",
                     output_rowset_delete_bitmap->delete_bitmap.size());
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }

    cloud::FinishTabletJobResponse resp;
    auto st = _engine.meta_mgr().commit_tablet_job(job, &resp);
    //TODO: add metric for record hold delete bitmap's lock.

    if (!st.ok()) {
        if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
#ifndef BE_TEST
            cloud_tablet()->clear_cache();
#endif
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            std::stringstream ss;
            ss << "[index_change]failed to prepare index change compaction. Check compaction input "
                  "versions "
                  "failed in schema change. "
                  "input_version_start="
               << compaction_job->input_versions(0)
               << " input_version_end=" << compaction_job->input_versions(1)
               << " schema_change_alter_version=" << resp.alter_version();
            std::string msg = ss.str();
            LOG(WARNING) << msg;
            cloud_tablet()->set_alter_version(resp.alter_version());
            return Status::InternalError(msg);
        }
        return st;
    }

    {
        if (is_base_compaction()) {
            _update_tablet_for_base_compaction(resp, output_rowset_delete_bitmap);
        } else {
            _update_tablet_for_cumu_compaction(resp, output_rowset_delete_bitmap);
        }
    }
    return Status::OK();
}

void CloudIndexChangeCompaction::_update_tablet_for_base_compaction(
        cloud::FinishTabletJobResponse resp, DeleteBitmapPtr output_rowset_delete_bitmap) {
    auto& stats = resp.stats();
    LOG(INFO) << "[index_change] tablet stats=" << stats.ShortDebugString();

    {
        std::unique_lock wrlock(_tablet->get_header_lock());
        // clang-format off
        cloud_tablet()->set_last_base_compaction_success_time(std::max(cloud_tablet()->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        cloud_tablet()->set_last_cumu_compaction_success_time(std::max(cloud_tablet()->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        cloud_tablet()->set_last_full_compaction_success_time(std::max(cloud_tablet()->last_full_compaction_success_time(), stats.last_full_compaction_time_ms()));
        // clang-format on
        if (cloud_tablet()->base_compaction_cnt() >= stats.base_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return;
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        if (_input_rowsets.size() == 1) {
            DCHECK_EQ(_output_rowset->version(), _input_rowsets[0]->version());
            // MUST NOT move input rowset to stale path.
            cloud_tablet()->add_rowsets({_output_rowset}, true, wrlock);
        } else {
            cloud_tablet()->delete_rowsets(_input_rowsets, wrlock);
            cloud_tablet()->add_rowsets({_output_rowset}, false, wrlock);
        }
        // ATTN: MUST NOT update `cumu_compaction_cnt` or `cumu_point` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by cumu compaction.
        cloud_tablet()->set_base_compaction_cnt(stats.base_compaction_cnt());
        if (output_rowset_delete_bitmap) {
            _tablet->tablet_meta()->delete_bitmap().merge(*output_rowset_delete_bitmap);
        }
        if (stats.cumulative_compaction_cnt() >= cloud_tablet()->cumulative_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
}

void CloudIndexChangeCompaction::_update_tablet_for_cumu_compaction(
        cloud::FinishTabletJobResponse resp, DeleteBitmapPtr output_rowset_delete_bitmap) {
    auto& stats = resp.stats();
    LOG(INFO) << "[index_change]tablet stats=" << stats.ShortDebugString();
    {
        std::unique_lock wrlock(_tablet->get_header_lock());
        // clang-format off
        cloud_tablet()->set_last_base_compaction_success_time(std::max(cloud_tablet()->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        cloud_tablet()->set_last_cumu_compaction_success_time(std::max(cloud_tablet()->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        cloud_tablet()->set_last_full_compaction_success_time(std::max(cloud_tablet()->last_full_compaction_success_time(), stats.last_full_compaction_time_ms()));
        // clang-format on
        if (cloud_tablet()->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`, or parallel cumu compactions which are
            // committed later increase tablet.cumulative_compaction_cnt (see CloudCompactionTest.parallel_cumu_compaction)
            return;
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        if (stats.cumulative_point() > cloud_tablet()->cumulative_layer_point() &&
            stats.cumulative_compaction_cnt() != cloud_tablet()->cumulative_compaction_cnt() + 1) {
            // This could happen when there are multiple parallel cumu compaction committed, tablet cache lags several
            // cumu compactions behind meta-service (stats.cumulative_compaction_cnt > tablet.cumulative_compaction_cnt + 1).
            // If `cumu_point` of the tablet cache also falls behind, MUST ONLY synchronize tablet cache from meta-service,
            // otherwise may cause the tablet to be unable to synchronize the rowset meta changes generated by other cumu compaction.
            return;
        }
        if (_input_rowsets.size() == 1) {
            DCHECK_EQ(_output_rowset->version(), _input_rowsets[0]->version());
            // MUST NOT move input rowset to stale path.
            cloud_tablet()->add_rowsets({_output_rowset}, true, wrlock);
        } else {
            cloud_tablet()->delete_rowsets(_input_rowsets, wrlock);
            cloud_tablet()->add_rowsets({_output_rowset}, false, wrlock);
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        cloud_tablet()->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        cloud_tablet()->set_cumulative_layer_point(stats.cumulative_point());
        if (output_rowset_delete_bitmap) {
            _tablet->tablet_meta()->delete_bitmap().merge(*output_rowset_delete_bitmap);
        }
        if (stats.base_compaction_cnt() >= cloud_tablet()->base_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
}

void CloudIndexChangeCompaction::do_lease() {
    cloud::TabletJobInfoPB job;
    if (_state == CompactionState::SUCCESS) {
        return;
    }
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    using namespace std::chrono;
    int64_t lease_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                         config::lease_compaction_interval_seconds * 4;
    compaction_job->set_lease(lease_time);
    auto st = _engine.meta_mgr().lease_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("[index_change]failed to lease compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

Status CloudIndexChangeCompaction::garbage_collection() {
    RETURN_IF_ERROR(CloudCompactionMixin::garbage_collection());
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(_compact_type);

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        compaction_job->set_delete_bitmap_lock_initiator(this->initiator());
    }
    auto st = _engine.meta_mgr().abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("[index_change]failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    return st;
}

TabletSchemaSPtr CloudIndexChangeCompaction::_build_output_rs_index_schema_for_drop(
        const TabletSchemaSPtr& output_rs_tablet_schema) {
    for (const auto& t_index : _alter_indexes) {
        DCHECK_EQ(t_index.columns.size(), 1);
        auto column_name = t_index.columns[0];
        auto column_idx = output_rs_tablet_schema->field_index(column_name);
        if (column_idx < 0) {
            if (!t_index.column_unique_ids.empty()) {
                auto column_unique_id = t_index.column_unique_ids[0];
                column_idx = output_rs_tablet_schema->field_index(column_unique_id);
            }
            if (column_idx < 0) {
                LOG(WARNING) << "[index_change]referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_idx
                             << "]";
                continue;
            }
        }

        auto column = output_rs_tablet_schema->column(column_idx);
        if (t_index.index_type == TIndexType::type::INVERTED) {
            std::vector<const TabletIndex*> exist_index_list =
                    output_rs_tablet_schema->inverted_indexs(column);
            if (exist_index_list.size() == 0) {
                LOG(WARNING) << "[index_change]column: " << column_name
                             << " has no inverted index, index_id: " << t_index.index_id
                             << " for drop.";
                continue;
            }
            for (const auto& index_meta : exist_index_list) {
                output_rs_tablet_schema->remove_index(index_meta->index_id());
            }
        } else if (t_index.index_type == TIndexType::type::NGRAM_BF) {
            auto index_meta = output_rs_tablet_schema->get_ngram_bf_index(column.unique_id());
            if (index_meta == nullptr) {
                LOG(WARNING) << "[index_change]column: " << column_name
                             << " has no ngram index, index_id: " << t_index.index_id
                             << " for drop.";
                continue;
            }
            output_rs_tablet_schema->remove_index(index_meta->index_id());
        }
    }

    return output_rs_tablet_schema;
}

TabletSchemaSPtr CloudIndexChangeCompaction::_build_output_rs_index_schema_for_add(
        const TabletSchemaSPtr& input_rs_tablet_schema) {
    TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
    output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);

    for (auto t_index : _alter_indexes) {
        TabletIndex index;
        index.init_from_thrift(t_index, *output_rs_tablet_schema);
        auto column_uid = index.col_unique_ids()[0];
        if (column_uid < 0) {
            LOG(WARNING) << "[index_change]referenced column was missing. "
                         << "[column=" << t_index.columns[0] << " referenced_column=" << column_uid
                         << "]";
            continue;
        }
        const TabletColumn& column = output_rs_tablet_schema->column_by_uid(column_uid);
        if (t_index.index_type == TIndexType::type::INVERTED) {
            std::vector<const TabletIndex*> exist_index_list =
                    output_rs_tablet_schema->inverted_indexs(column);
            for (const auto& exist_index : exist_index_list) {
                if (exist_index->index_id() != index.index_id()) {
                    LOG(WARNING) << fmt::format(
                            "column: {} has a exist inverted index, but the index id not "
                            "equal "
                            "request's index id, exist index id: {}, request's index id: "
                            "{}, "
                            "remove exist index in new output_rs_tablet_schema",
                            column_uid, exist_index->index_id(), index.index_id());
                    output_rs_tablet_schema->remove_index(exist_index->index_id());
                }
            }
        } else if (t_index.index_type == TIndexType::type::NGRAM_BF) {
            const TabletIndex* exist_index =
                    output_rs_tablet_schema->get_ngram_bf_index(column.unique_id());
            if (exist_index && exist_index->index_id() != index.index_id()) {
                LOG(WARNING) << fmt::format(
                        "[index_change]column: {} has a exist ngram index, but the index id not "
                        "equal "
                        "request's index id, exist index id: {}, request's index id: {}, "
                        "remove exist index in new output_rs_tablet_schema",
                        column_uid, exist_index->index_id(), index.index_id());
                output_rs_tablet_schema->remove_index(exist_index->index_id());
            }
        } else {
            LOG(WARNING) << "unexpected index type:" << t_index.index_type;
            continue;
        }
        output_rs_tablet_schema->append_index(std::move(index));
    }
    return output_rs_tablet_schema;
}

} // namespace doris