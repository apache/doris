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

#include "cloud/cloud_full_compaction.h"

#include <boost/container_hash/hash.hpp>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/compaction.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet_meta.h"
#include "service/backend_options.h"
#include "util/debug_points.h"
#include "util/thread.h"
#include "util/uuid_generator.h"
#include "vec/columns/column.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<uint64_t> full_output_size("full_compaction", "output_size");

CloudFullCompaction::CloudFullCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet)
        : CloudCompactionMixin(engine, tablet,
                               "BaseCompaction:" + std::to_string(tablet->tablet_id())) {}

CloudFullCompaction::~CloudFullCompaction() = default;

Status CloudFullCompaction::prepare_compact() {
    Status st;
    Defer defer_set_st([&] {
        if (!st.ok()) {
            cloud_tablet()->set_last_full_compaction_status(st.to_string());
            cloud_tablet()->set_last_full_compaction_failure_time(UnixMillis());
        }
    });
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        st = Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
        return st;
    }

    // always sync latest rowset for full compaction
    st = cloud_tablet()->sync_rowsets();
    RETURN_IF_ERROR(st);

    st = pick_rowsets_to_compact();
    RETURN_IF_ERROR(st);

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_data_size += rs->data_disk_size();
        _input_rowsets_index_size += rs->index_disk_size();
        _input_rowsets_total_size += rs->total_disk_size();
    }
    LOG_INFO("start CloudFullCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size);
    return Status::OK();
}
Status CloudFullCompaction::request_global_lock() {
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::FULL);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);
    // Set input version range to let meta-service judge version range conflict
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    cloud::StartTabletJobResponse resp;
    auto st = _engine.meta_mgr().prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
        } else if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
            cloud_tablet()->clear_cache();
        }
    }
    return st;
}

Status CloudFullCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = cloud_tablet()->base_compaction_cnt();
        _cumulative_compaction_cnt = cloud_tablet()->cumulative_compaction_cnt();
        _input_rowsets = cloud_tablet()->pick_candidate_rowsets_to_full_compaction();
    }
    if (auto st = check_version_continuity(_input_rowsets); !st.ok()) {
        DCHECK(false) << st;
        return st;
    }
    if (_input_rowsets.size() <= 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>(
                "insufficent compaction input rowset, #rowsets={}", _input_rowsets.size());
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do full compaction.
        return Status::Error<BE_NO_SUITABLE_VERSION>("no suitable versions for compaction");
    }

    return Status::OK();
}

Status CloudFullCompaction::execute_compact() {
    DBUG_EXECUTE_IF("CloudFullCompaction::execute_compact.block", {
        auto target_tablet_id = dp->param<int64_t>("tablet_id", -1);
        LOG_INFO(
                "[verbose] CloudFullCompaction::execute_compact.block, target_tablet_id={}, "
                "tablet_id={}",
                target_tablet_id, cloud_tablet()->tablet_id());
        if (target_tablet_id == cloud_tablet()->tablet_id()) {
            DBUG_BLOCK;
        }
    });
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudFullCompaction::execute_compact_impl", Status::OK(),
                                      this);
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif

    SCOPED_ATTACH_TASK(_mem_tracker);

    using namespace std::chrono;
    auto start = steady_clock::now();
    auto res = CloudCompactionMixin::execute_compact();
    cloud_tablet()->set_last_full_compaction_status(res.to_string());
    if (!res.ok()) {
        cloud_tablet()->set_last_full_compaction_failure_time(UnixMillis());
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << res
                     << ", tablet=" << _tablet->tablet_id()
                     << ", output_version=" << _output_version;
        return res;
    }
    LOG_INFO("finish CloudFullCompaction, tablet_id={}, cost={}ms", _tablet->tablet_id(),
             duration_cast<milliseconds>(steady_clock::now() - start).count())
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
            .tag("local_read_time_us", _stats.cloud_local_read_time)
            .tag("remote_read_time_us", _stats.cloud_remote_read_time)
            .tag("local_read_bytes", _local_read_bytes_total)
            .tag("remote_read_bytes", _remote_read_bytes_total);

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->full_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->full_compaction_bytes_total->increment(_input_rowsets_total_size);
    full_output_size << _output_rowset->total_disk_size();

    cloud_tablet()->set_last_full_compaction_success_time(UnixMillis());
    cloud_tablet()->set_last_full_compaction_status(Status::OK().to_string());
    return Status::OK();
}

Status CloudFullCompaction::modify_rowsets() {
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::FULL);
    compaction_job->set_input_cumulative_point(cloud_tablet()->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(_output_rowset->end_version() + 1);
    compaction_job->set_num_input_rows(_input_row_num);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(_input_rowsets_total_size);
    compaction_job->set_size_output_rowsets(_output_rowset->total_disk_size());
    DBUG_EXECUTE_IF("CloudFullCompaction::modify_rowsets.wrong_compaction_data_size", {
        compaction_job->set_size_input_rowsets(1);
        compaction_job->set_size_output_rowsets(10000001);
    })
    compaction_job->set_num_input_segments(_input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(num_input_rowsets());
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

    DBUG_EXECUTE_IF("CloudFullCompaction::modify_rowsets.block", DBUG_BLOCK);

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        RETURN_IF_ERROR(_cloud_full_compaction_update_delete_bitmap(this->initiator()));
        compaction_job->set_delete_bitmap_lock_initiator(this->initiator());
    }

    cloud::FinishTabletJobResponse resp;
    auto st = _engine.meta_mgr().commit_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            cloud_tablet()->clear_cache();
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
        if (cloud_tablet()->base_compaction_cnt() >= stats.base_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        cloud_tablet()->delete_rowsets(_input_rowsets, wrlock);
        cloud_tablet()->add_rowsets({_output_rowset}, false, wrlock);
        cloud_tablet()->set_base_compaction_cnt(stats.base_compaction_cnt());
        cloud_tablet()->set_full_compaction_cnt(stats.full_compaction_cnt());
        cloud_tablet()->set_cumulative_layer_point(stats.cumulative_point());
        if (stats.cumulative_compaction_cnt() >= cloud_tablet()->cumulative_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
    _tablet->prefill_dbm_agg_cache_after_compaction(_output_rowset);
    return Status::OK();
}

Status CloudFullCompaction::garbage_collection() {
    //file_cache_garbage_collection();
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
    compaction_job->set_type(cloud::TabletCompactionJobPB::FULL);
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        compaction_job->set_delete_bitmap_lock_initiator(this->initiator());
    }
    auto st = _engine.meta_mgr().abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    return st;
}

void CloudFullCompaction::do_lease() {
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

Status CloudFullCompaction::_cloud_full_compaction_update_delete_bitmap(int64_t initiator) {
    DCHECK(_output_rowset->start_version() <= 2) << _output_rowset->start_version();
    std::vector<RowsetSharedPtr> tmp_rowsets {};
    DeleteBitmapPtr delete_bitmap =
            std::make_shared<DeleteBitmap>(_tablet->tablet_meta()->tablet_id());
    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(cloud_tablet()));
    int64_t max_version = cloud_tablet()->max_version().second;
    DCHECK(max_version >= _output_rowset->version().second);
    if (max_version > _output_rowset->version().second) {
        RETURN_IF_ERROR(cloud_tablet()->capture_consistent_rowsets_unlocked(
                {_output_rowset->version().second + 1, max_version}, &tmp_rowsets));
    }
    for (const auto& it : tmp_rowsets) {
        int64_t cur_version = it->rowset_meta()->start_version();
        RETURN_IF_ERROR(_cloud_full_compaction_calc_delete_bitmap(it, cur_version, delete_bitmap));
    }

    RETURN_IF_ERROR(
            _engine.meta_mgr().get_delete_bitmap_update_lock(*cloud_tablet(), -1, initiator));
    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(cloud_tablet()));
    std::lock_guard header_lock(_tablet->get_header_lock());
    for (const auto& it : cloud_tablet()->rowset_map()) {
        int64_t cur_version = it.first.first;
        const RowsetSharedPtr& published_rowset = it.second;
        if (cur_version > max_version) {
            RETURN_IF_ERROR(_cloud_full_compaction_calc_delete_bitmap(published_rowset, cur_version,
                                                                      delete_bitmap));
        }
    }
    std::optional<StorageResource> storage_resource;
    auto storage_resource_result = _output_rowset->rowset_meta()->remote_storage_resource();
    if (storage_resource_result) {
        storage_resource = *storage_resource_result.value();
    }
    RETURN_IF_ERROR(_engine.meta_mgr().update_delete_bitmap(
            *cloud_tablet(), -1, initiator, delete_bitmap.get(), delete_bitmap.get(),
            _output_rowset->rowset_id().to_string(), storage_resource,
            config::delete_bitmap_store_write_version));
    LOG_INFO("update delete bitmap in CloudFullCompaction, tablet_id={}, range=[{}-{}]",
             _tablet->tablet_id(), _input_rowsets.front()->start_version(),
             _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("initiator", initiator)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("update_bitmap_size", delete_bitmap->delete_bitmap.size());
    _tablet->tablet_meta()->delete_bitmap().merge(*delete_bitmap);
    return Status::OK();
}

Status CloudFullCompaction::_cloud_full_compaction_calc_delete_bitmap(
        const RowsetSharedPtr& published_rowset, int64_t cur_version,
        DeleteBitmapPtr delete_bitmap) {
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(
            std::static_pointer_cast<BetaRowset>(published_rowset)->load_segments(&segments));
    std::vector<RowsetSharedPtr> specified_rowsets {_output_rowset};
    DeleteBitmapPtr tmp_delete_bitmap = std::make_shared<DeleteBitmap>(_tablet->tablet_id());

    OlapStopWatch watch;
    auto token = _engine.calc_delete_bitmap_executor()->create_token();
    RETURN_IF_ERROR(BaseTablet::calc_delete_bitmap(
            _tablet, published_rowset, segments, specified_rowsets, tmp_delete_bitmap, cur_version,
            token.get(), _output_rs_writer.get()));
    RETURN_IF_ERROR(token->wait());
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    for (const auto& [k, v] : tmp_delete_bitmap->delete_bitmap) {
        if (std::get<0>(k) == _output_rowset->rowset_id() &&
            std::get<1>(k) != DeleteBitmap::INVALID_SEGMENT_ID) {
            delete_bitmap->merge({std::get<0>(k), std::get<1>(k), cur_version}, v);
        }
    }
    VLOG_DEBUG << "[Full compaction] construct delete bitmap tablet: " << _tablet->tablet_id()
               << ", published rowset version: [" << published_rowset->version().first << "-"
               << published_rowset->version().second << "]"
               << ", full compaction rowset version: [" << _output_rowset->version().first << "-"
               << _output_rowset->version().second << "]"
               << ", cost: " << watch.get_elapse_time_us() << "(us), total rows: " << total_rows;
    return Status::OK();
}

} // namespace doris
