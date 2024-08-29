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

#include "cloud/cloud_cumulative_compaction.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "service/backend_options.h"
#include "util/trace.h"
#include "util/uuid_generator.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<uint64_t> cumu_output_size("cumu_compaction", "output_size");

CloudCumulativeCompaction::CloudCumulativeCompaction(CloudStorageEngine& engine,
                                                     CloudTabletSPtr tablet)
        : CloudCompactionMixin(engine, tablet,
                               "BaseCompaction:" + std::to_string(tablet->tablet_id())) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudCumulativeCompaction::~CloudCumulativeCompaction() = default;

Status CloudCumulativeCompaction::prepare_compact() {
    if (_tablet->tablet_state() != TABLET_RUNNING &&
        (!config::enable_new_tablet_do_compaction ||
         static_cast<CloudTablet*>(_tablet.get())->alter_version() == -1)) {
        return Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
    }

    std::vector<std::shared_ptr<CloudCumulativeCompaction>> cumu_compactions;
    _engine.get_cumu_compaction(_tablet->tablet_id(), cumu_compactions);
    if (!cumu_compactions.empty()) {
        for (auto& cumu : cumu_compactions) {
            _max_conflict_version =
                    std::max(_max_conflict_version, cumu->_input_rowsets.back()->end_version());
        }
    }

    int tried = 0;
PREPARE_TRY_AGAIN:

    bool need_sync_tablet = true;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        // If number of rowsets is equal to approximate_num_rowsets, it is very likely that this tablet has been
        // synchronized with meta-service.
        if (_tablet->tablet_meta()->all_rs_metas().size() >=
                    cloud_tablet()->fetch_add_approximate_num_rowsets(0) &&
            cloud_tablet()->last_sync_time_s > 0) {
            need_sync_tablet = false;
        }
    }
    if (need_sync_tablet) {
        RETURN_IF_ERROR(cloud_tablet()->sync_rowsets());
    }

    // pick rowsets to compact
    auto st = pick_rowsets_to_compact();
    if (!st.ok()) {
        if (tried == 0 && _last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doesn't matter.
            update_cumulative_point();
        }
        return st;
    }

    // prepare compaction job
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);

    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    // Set input version range to let meta-service check version range conflict
    compaction_job->set_check_input_versions_range(config::enable_parallel_cumu_compaction);
    cloud::StartTabletJobResponse resp;
    st = _engine.meta_mgr().prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
        } else if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
            cloud_tablet()->clear_cache();
        } else if (resp.status().code() == cloud::JOB_TABLET_BUSY) {
            if (config::enable_parallel_cumu_compaction && resp.version_in_compaction_size() > 0 &&
                ++tried <= 2) {
                _max_conflict_version = *std::max_element(resp.version_in_compaction().begin(),
                                                          resp.version_in_compaction().end());
                LOG_INFO("retry pick input rowsets")
                        .tag("job_id", _uuid)
                        .tag("max_conflict_version", _max_conflict_version)
                        .tag("tried", tried)
                        .tag("msg", resp.status().msg());
                goto PREPARE_TRY_AGAIN;
            } else {
                LOG_WARNING("failed to prepare cumu compaction")
                        .tag("job_id", _uuid)
                        .tag("msg", resp.status().msg());
                return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
            }
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            (static_cast<CloudTablet*>(_tablet.get()))->set_alter_version(resp.alter_version());
            std::stringstream ss;
            ss << "failed to prepare cumu compaction. Check compaction input versions "
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

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_size += rs->data_disk_size();
    }
    LOG_INFO("start CloudCumulativeCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size)
            .tag("tablet_max_version", cloud_tablet()->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0));
    return st;
}

Status CloudCumulativeCompaction::execute_compact() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudCumulativeCompaction::execute_compact_impl",
                                      Status::OK(), this);

    SCOPED_ATTACH_TASK(_mem_tracker);

    using namespace std::chrono;
    auto start = steady_clock::now();
    auto res = CloudCompactionMixin::execute_compact();
    if (!res.ok()) {
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << res
                     << ", tablet=" << _tablet->tablet_id()
                     << ", output_version=" << _output_version;
        return res;
    }
    LOG_INFO("finish CloudCumulativeCompaction, tablet_id={}, cost={}ms", _tablet->tablet_id(),
             duration_cast<milliseconds>(steady_clock::now() - start).count())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_data_size", _output_rowset->data_disk_size())
            .tag("tablet_max_version", _tablet->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0));

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(_input_rowsets_size);
    cumu_output_size << _output_rowset->data_disk_size();

    return Status::OK();
}

Status CloudCumulativeCompaction::modify_rowsets() {
    // calculate new cumulative point
    int64_t input_cumulative_point = cloud_tablet()->cumulative_layer_point();
    auto compaction_policy = cloud_tablet()->tablet_meta()->compaction_policy();
    int64_t new_cumulative_point =
            _engine.cumu_compaction_policy(compaction_policy)
                    ->new_cumulative_point(cloud_tablet(), _output_rowset, _last_delete_version,
                                           input_cumulative_point);
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(new_cumulative_point);
    compaction_job->set_num_input_rows(_input_row_num);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(_input_rowsets_size);
    compaction_job->set_size_output_rowsets(_output_rowset->data_disk_size());
    compaction_job->set_num_input_segments(_input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(_input_rowsets.size());
    compaction_job->set_num_output_rowsets(1);
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    compaction_job->add_output_versions(_output_rowset->end_version());
    compaction_job->add_txn_id(_output_rowset->txn_id());
    compaction_job->add_output_rowset_ids(_output_rowset->rowset_id().to_string());

    DeleteBitmapPtr output_rowset_delete_bitmap = nullptr;
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        int64_t initiator = HashUtil::hash64(_uuid.data(), _uuid.size(), 0) &
                            std::numeric_limits<int64_t>::max();
        RETURN_IF_ERROR(cloud_tablet()->calc_delete_bitmap_for_compaction(
                _input_rowsets, _output_rowset, _rowid_conversion, compaction_type(),
                _stats.merged_rows, initiator, output_rowset_delete_bitmap,
                _allow_delete_in_cumu_compaction));
        LOG_INFO("update delete bitmap in CloudCumulativeCompaction, tablet_id={}, range=[{}-{}]",
                 _tablet->tablet_id(), _input_rowsets.front()->start_version(),
                 _input_rowsets.back()->end_version())
                .tag("job_id", _uuid)
                .tag("initiator", initiator)
                .tag("input_rowsets", _input_rowsets.size())
                .tag("input_rows", _input_row_num)
                .tag("input_segments", _input_segments)
                .tag("update_bitmap_size", output_rowset_delete_bitmap->delete_bitmap.size());
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }

    cloud::FinishTabletJobResponse resp;
    auto st = _engine.meta_mgr().commit_tablet_job(job, &resp);
    if (resp.has_alter_version()) {
        (static_cast<CloudTablet*>(_tablet.get()))->set_alter_version(resp.alter_version());
    }
    if (!st.ok()) {
        if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            cloud_tablet()->clear_cache();
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            std::stringstream ss;
            ss << "failed to prepare cumu compaction. Check compaction input versions "
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

    auto& stats = resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
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
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        if (stats.cumulative_point() > cloud_tablet()->cumulative_layer_point() &&
            stats.cumulative_compaction_cnt() != cloud_tablet()->cumulative_compaction_cnt() + 1) {
            // This could happen when there are multiple parallel cumu compaction committed, tablet cache lags several
            // cumu compactions behind meta-service (stats.cumulative_compaction_cnt > tablet.cumulative_compaction_cnt + 1).
            // If `cumu_point` of the tablet cache also falls behind, MUST ONLY synchronize tablet cache from meta-service,
            // otherwise may cause the tablet to be unable to synchronize the rowset meta changes generated by other cumu compaction.
            return Status::OK();
        }
        if (_input_rowsets.size() == 1) {
            DCHECK_EQ(_output_rowset->version(), _input_rowsets[0]->version());
            // MUST NOT move input rowset to stale path
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
    return Status::OK();
}

void CloudCumulativeCompaction::garbage_collection() {
    CloudCompactionMixin::garbage_collection();
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        int64_t initiator = HashUtil::hash64(_uuid.data(), _uuid.size(), 0) &
                            std::numeric_limits<int64_t>::max();
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }
    auto st = _engine.meta_mgr().abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

Status CloudCumulativeCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = cloud_tablet()->base_compaction_cnt();
        _cumulative_compaction_cnt = cloud_tablet()->cumulative_compaction_cnt();
        int64_t candidate_version = std::max(
                std::max(cloud_tablet()->cumulative_layer_point(), _max_conflict_version + 1),
                cloud_tablet()->alter_version() + 1);
        // Get all rowsets whose version >= `candidate_version` as candidate rowsets
        cloud_tablet()->traverse_rowsets(
                [&candidate_rowsets, candidate_version](const RowsetSharedPtr& rs) {
                    if (rs->start_version() >= candidate_version) {
                        candidate_rowsets.push_back(rs);
                    }
                });
    }
    if (candidate_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    if (auto st = check_version_continuity(candidate_rowsets); !st.ok()) {
        DCHECK(false) << st;
        return st;
    }

    int64_t max_score = config::cumulative_compaction_max_deltas;
    auto process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();
    bool memory_usage_high = process_memory_usage > MemInfo::soft_mem_limit() * 0.8;
    if (cloud_tablet()->last_compaction_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() ||
        memory_usage_high) {
        max_score = std::max(config::cumulative_compaction_max_deltas /
                                     config::cumulative_compaction_max_deltas_factor,
                             config::cumulative_compaction_min_deltas + 1);
    }

    size_t compaction_score = 0;
    auto compaction_policy = cloud_tablet()->tablet_meta()->compaction_policy();
    _engine.cumu_compaction_policy(compaction_policy)
            ->pick_input_rowsets(cloud_tablet(), candidate_rowsets, max_score,
                                 config::cumulative_compaction_min_deltas, &_input_rowsets,
                                 &_last_delete_version, &compaction_score);

    if (_input_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    } else if (_input_rowsets.size() == 1 &&
               !_input_rowsets.front()->rowset_meta()->is_segments_overlapping()) {
        VLOG_DEBUG << "there is only one rowset and not overlapping. tablet_id="
                   << _tablet->tablet_id() << ", version=" << _input_rowsets.front()->version();
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    }
    return Status::OK();
}

void CloudCumulativeCompaction::update_cumulative_point() {
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::EMPTY_CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    int64_t now = time(nullptr);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds);
    // No need to set expiration time, since there is no output rowset
    cloud::StartTabletJobResponse start_resp;
    auto st = _engine.meta_mgr().prepare_tablet_job(job, &start_resp);
    if (!st.ok()) {
        if (start_resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
        } else if (start_resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
            cloud_tablet()->clear_cache();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    int64_t input_cumulative_point = cloud_tablet()->cumulative_layer_point();
    int64_t output_cumulative_point = _last_delete_version.first + 1;
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(output_cumulative_point);
    cloud::FinishTabletJobResponse finish_resp;
    st = _engine.meta_mgr().commit_tablet_job(job, &finish_resp);
    if (!st.ok()) {
        if (finish_resp.status().code() == cloud::TABLET_NOT_FOUND) {
            cloud_tablet()->clear_cache();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    LOG_INFO("do empty cumulative compaction to update cumulative point")
            .tag("job_id", _uuid)
            .tag("tablet_id", _tablet->tablet_id())
            .tag("input_cumulative_point", input_cumulative_point)
            .tag("output_cumulative_point", output_cumulative_point);
    auto& stats = finish_resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        // clang-format off
        cloud_tablet()->set_last_base_compaction_success_time(std::max(cloud_tablet()->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        cloud_tablet()->set_last_cumu_compaction_success_time(std::max(cloud_tablet()->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        // clang-format on
        if (cloud_tablet()->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return;
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        cloud_tablet()->set_cumulative_compaction_cnt(cloud_tablet()->cumulative_compaction_cnt() +
                                                      1);
        cloud_tablet()->set_cumulative_layer_point(stats.cumulative_point());
        if (stats.base_compaction_cnt() >= cloud_tablet()->base_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
}

void CloudCumulativeCompaction::do_lease() {
    TEST_INJECTION_POINT_RETURN_WITH_VOID("CloudCumulativeCompaction::do_lease");
    if (_state == CompactionState::SUCCESS) {
        return;
    }
    cloud::TabletJobInfoPB job;
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
        LOG_WARNING("failed to lease compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

} // namespace doris
