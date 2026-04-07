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

#include "cloud/cloud_calc_delete_bitmap_async_publish_task.h"

#include <fmt/format.h>

#include <memory>
#include <ranges>
#include <type_traits>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "common/status.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/txn/txn_manager.h"
#include "storage/utils.h"
#include "util/defer_op.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

int64_t count_total_tablets(const TCalcDeleteBitmapAsyncPublishRequest& request) {
    int64_t total_tablet_num = 0;
    for (const auto& partition : request.partitions) {
        total_tablet_num += partition.tablet_ids.size();
    }
    return total_tablet_num;
}

bool is_already_succeeded_tablet(const TCalcDeleteBitmapAsyncPublishRequest& request,
                                 TTabletId tablet_id) {
    return request.__isset.already_succeeded_tablet_ids &&
           request.already_succeeded_tablet_ids.contains(tablet_id);
}

int64_t count_pending_tablets(const TCalcDeleteBitmapAsyncPublishRequest& request) {
    int64_t pending_tablet_num = 0;
    for (const auto& partition : request.partitions) {
        for (auto tablet_id : partition.tablet_ids) {
            if (!is_already_succeeded_tablet(request, tablet_id)) {
                ++pending_tablet_num;
            }
        }
    }
    return pending_tablet_num;
}

int64_t count_total_partitions(const TCalcDeleteBitmapAsyncPublishRequest& request) {
    return request.partitions.size();
}

int64_t get_be_local_retry_count(const TCalcDeleteBitmapAsyncPublishRequest& request) {
    return request.__isset.be_local_retry_count ? request.be_local_retry_count : 0;
}

} // namespace

CloudCalcDeleteBitmapAsyncPublishTask::CloudCalcDeleteBitmapAsyncPublishTask(
        CloudStorageEngine& engine, const TCalcDeleteBitmapAsyncPublishRequest& request,
        std::vector<TTabletId>* error_tablet_ids, std::vector<TTabletId>* succ_tablet_ids)
        : _engine(engine),
          _request(request),
          _error_tablet_ids(error_tablet_ids),
          _succ_tablet_ids(succ_tablet_ids) {
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "CloudCalcDeleteBitmapAsyncPublishTask");
}

void CloudCalcDeleteBitmapAsyncPublishTask::add_error_tablet_id(int64_t tablet_id,
                                                                const Status& err) {
    std::lock_guard<std::mutex> lck(_mutex);
    _error_tablet_ids->push_back(tablet_id);
    if (_res.ok() || _res.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>() ||
        _res.is<ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS>()) {
        _res = err;
    }
}

void CloudCalcDeleteBitmapAsyncPublishTask::add_succ_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_mutex);
    _succ_tablet_ids->push_back(tablet_id);
}

Status CloudCalcDeleteBitmapAsyncPublishTask::execute() {
    int64_t transaction_id = _request.transaction_id;
    const int64_t transaction_start_time_us = MonotonicMicros();
    const int64_t total_partition_num = count_total_partitions(_request);
    const int64_t total_tablet_num = count_total_tablets(_request);
    const int64_t pending_tablet_num = count_pending_tablets(_request);
    const int64_t already_succeeded_tablet_num = total_tablet_num - pending_tablet_num;
    const int64_t be_local_retry_count = get_be_local_retry_count(_request);
    OlapStopWatch watch;
    LOG(INFO) << "begin to calculate delete bitmap for async publish on transaction"
              << ", transaction_id=" << transaction_id
              << ", total_partition_num=" << total_partition_num
              << ", total_tablet_num=" << total_tablet_num
              << ", already_succeeded_tablet_num=" << already_succeeded_tablet_num
              << ", pending_tablet_num=" << pending_tablet_num
              << ", be_local_retry_count=" << be_local_retry_count << ", thread_pool="
              << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_info();
    std::unique_ptr<ThreadPoolToken> token =
            _engine.calc_tablet_delete_bitmap_task_thread_pool().new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
    auto first_tablet_start_logged = std::make_shared<std::atomic_bool>(false);
    for (const auto& partition : _request.partitions) {
        int64_t version = partition.version;
        int64_t db_id = partition.__isset.db_id ? partition.db_id : -1;
        int64_t table_id = partition.__isset.table_id ? partition.table_id : -1;
        for (size_t i = 0; i < partition.tablet_ids.size(); i++) {
            auto tablet_id = partition.tablet_ids[i];
            if (is_already_succeeded_tablet(_request, tablet_id)) {
                continue;
            }
            int64_t index_id = (partition.__isset.index_ids && i < partition.index_ids.size())
                                       ? partition.index_ids[i]
                                       : -1;
            auto tablet_task = std::make_shared<CloudTabletCalcDeleteBitmapAsyncPublishTask>(
                    _engine, tablet_id, transaction_id, version, db_id, table_id, index_id,
                    partition.partition_id, transaction_start_time_us, total_partition_num,
                    total_tablet_num, first_tablet_start_logged);
            auto submit_st = token->submit_func([tablet_id, tablet_task, this]() {
                auto st = tablet_task->handle();
                if (st.ok()) {
                    add_succ_tablet_id(tablet_id);
                } else {
                    LOG(WARNING) << "handle calc delete bitmap async publish fail, st="
                                 << st.to_string();
                    add_error_tablet_id(tablet_id, st);
                }
            });
            if (!submit_st.ok()) {
                _res = submit_st;
                break;
            }
            LOG(INFO) << "successfully submit async publish tablet task"
                      << ", transaction_id=" << transaction_id << ", tablet_id=" << tablet_id
                      << ", tablet_task_queue_size="
                      << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_queue_size();
        }
    }
    LOG(INFO) << "submitted tablet tasks for async publish transaction"
              << ", transaction_id=" << transaction_id
              << ", total_partition_num=" << total_partition_num
              << ", total_tablet_num=" << total_tablet_num
              << ", already_succeeded_tablet_num=" << already_succeeded_tablet_num
              << ", pending_tablet_num=" << pending_tablet_num
              << ", be_local_retry_count=" << be_local_retry_count
              << ", submit_tablet_task_time_us=" << MonotonicMicros() - transaction_start_time_us
              << ", thread_pool="
              << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_info();
    // wait for all finished
    token->wait();

    LOG(INFO) << "finish to calculate delete bitmap for async publish on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
              << ", be_local_retry_count=" << be_local_retry_count
              << ", error_tablet_size=" << _error_tablet_ids->size()
              << ", res=" << _res.to_string();
    return _res;
}

CloudTabletCalcDeleteBitmapAsyncPublishTask::CloudTabletCalcDeleteBitmapAsyncPublishTask(
        CloudStorageEngine& engine, int64_t tablet_id, int64_t transaction_id, int64_t version,
        int64_t db_id, int64_t table_id, int64_t index_id, int64_t partition_id,
        int64_t transaction_start_time_us, int64_t transaction_total_partition_num,
        int64_t transaction_total_tablet_num,
        std::shared_ptr<std::atomic_bool> first_tablet_start_logged)
        : _engine(engine),
          _tablet_id(tablet_id),
          _transaction_id(transaction_id),
          _version(version),
          _db_id(db_id),
          _table_id(table_id),
          _index_id(index_id),
          _partition_id(partition_id),
          _transaction_start_time_us(transaction_start_time_us),
          _transaction_total_partition_num(transaction_total_partition_num),
          _transaction_total_tablet_num(transaction_total_tablet_num),
          _first_tablet_start_logged(std::move(first_tablet_start_logged)) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER,
            fmt::format("CloudTabletCalcDeleteBitmapAsyncPublishTask#_transaction_id={}",
                        _transaction_id));
}

Status CloudTabletCalcDeleteBitmapAsyncPublishTask::handle() const {
    const int64_t tablet_start_time_us = MonotonicMicros();
    Status status = Status::OK();
    ExecutionStats exec_stats;
    exec_stats.queue_wait_us = tablet_start_time_us - _transaction_start_time_us;
    int64_t table_id_for_log = _table_id;
    Defer defer_log_finish {[&]() {
        LOG_INFO("finish calculate delete bitmap for async publish on tablet")
                .tag("table_id", table_id_for_log)
                .tag("transaction_id", _transaction_id)
                .tag("tablet_id", _tablet_id)
                .tag("queue_wait_us", exec_stats.queue_wait_us)
                .tag("total_time_us", MonotonicMicros() - tablet_start_time_us)
                .tag("get_tablet_time_us", exec_stats.get_tablet_time_us)
                .tag("acquire_delete_bitmap_and_rowset_layout_lock_time_us",
                     exec_stats.acquire_delete_bitmap_and_rowset_layout_lock_time_us)
                .tag("get_delete_bitmap_tablet_lock_time_us",
                     exec_stats.get_delete_bitmap_tablet_lock_time_us)
                .tag("sync_rowset_time_us", exec_stats.sync_rowset_time_us)
                .tag("handle_rowset_time_us", exec_stats.handle_rowset_time_us)
                .tag("update_delete_bitmap_time_us", exec_stats.update_delete_bitmap_time_us)
                .tag("save_delete_bitmap_to_ms_time_us",
                     exec_stats.save_delete_bitmap_to_ms_time_us)
                .tag("async_publish_time_us", exec_stats.async_publish_time_us)
                .tag("convert_tmp_rowset_time_us", exec_stats.convert_tmp_rowset_time_us)
                .tag("local_apply_time_us", exec_stats.local_apply_time_us)
                .tag("remove_tablet_txn_info_time_us", exec_stats.remove_tablet_txn_info_time_us)
                .tag("remove_delete_bitmap_tablet_lock_time_us",
                     exec_stats.remove_delete_bitmap_tablet_lock_time_us)
                .tag("is_empty_rowset", exec_stats.is_empty_rowset)
                .tag("res", status.to_string());
    }};
    bool expected = false;
    if (_first_tablet_start_logged && _first_tablet_start_logged->compare_exchange_strong(
                                              expected, true, std::memory_order_relaxed)) {
        LOG(INFO) << "first tablet task starts for async publish transaction"
                  << ", transaction_id=" << _transaction_id << ", tablet_id=" << _tablet_id
                  << ", queue_wait_us=" << exec_stats.queue_wait_us
                  << ", total_partition_num=" << _transaction_total_partition_num
                  << ", total_tablet_num=" << _transaction_total_tablet_num << ", thread_pool="
                  << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_info();
    }
    VLOG_DEBUG << "start calculate delete bitmap for async publish on tablet " << _tablet_id
               << ", txn_id=" << _transaction_id << ", queue_wait_us=" << exec_stats.queue_wait_us;
    SCOPED_ATTACH_TASK(_mem_tracker);
    int64_t t1 = MonotonicMicros();
    auto base_tablet_res = _engine.get_tablet(_tablet_id);
    exec_stats.get_tablet_time_us = MonotonicMicros() - t1;
    if (!base_tablet_res.has_value()) {
        status = base_tablet_res.error();
        return status;
    }
    auto base_tablet = std::move(base_tablet_res.value());
    std::shared_ptr<CloudTablet> tablet = std::dynamic_pointer_cast<CloudTablet>(base_tablet);
    if (tablet == nullptr) {
        status = Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                "can't get tablet when calculate delete bitmap for async publish. tablet_id={}",
                _tablet_id);
        return status;
    }
    table_id_for_log = tablet->table_id();

    {
        std::shared_lock rlock(tablet->get_header_lock());
        if (tablet->max_version_unlocked() >= _version) {
            LOG(INFO) << "tablet already has version " << _version
                      << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
            status = Status::OK();
            return status;
        }
    }

    int64_t t_lock = MonotonicMicros();
    std::unique_lock delete_bitmap_and_rowset_layout_lock(
            tablet->get_delete_bitmap_and_rowset_layout_lock());
    exec_stats.acquire_delete_bitmap_and_rowset_layout_lock_time_us = MonotonicMicros() - t_lock;

    {
        std::shared_lock rlock(tablet->get_header_lock());
        if (tablet->max_version_unlocked() >= _version) {
            LOG(INFO) << "tablet already has version " << _version
                      << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
            status = Status::OK();
            return status;
        }
    }

    // Acquire MS tablet-level lock right after the cloud async publish lock,
    // before delete bitmap calculation, to ensure mutual exclusion with compaction
    cloud::CloudMetaMgr::DeleteBitmapTabletLockInfo ms_lock_info;
    int64_t t_ms_lock = MonotonicMicros();
    auto lock_st = _engine.meta_mgr().get_delete_bitmap_tablet_lock(*tablet, _transaction_id, -1,
                                                                    &ms_lock_info);
    exec_stats.get_delete_bitmap_tablet_lock_time_us = MonotonicMicros() - t_ms_lock;
    if (!lock_st.ok()) {
        status = lock_st;
        return status;
    }
    // Release MS tablet lock on any exit path
    Defer defer_release_tablet_lock {[&]() {
        int64_t t_release_lock = MonotonicMicros();
        _engine.meta_mgr().remove_delete_bitmap_tablet_lock(*tablet, _transaction_id, -1);
        exec_stats.remove_delete_bitmap_tablet_lock_time_us = MonotonicMicros() - t_release_lock;
    }};

    int64_t max_version = tablet->max_version_unlocked();
    int64_t t2 = MonotonicMicros();

    auto should_sync_rowsets = [&]() {
        if (_version != max_version + 1) {
            return true;
        }
        if (ms_lock_info.base_compaction_cnt == -1) {
            return true;
        }

        std::shared_lock rlock(tablet->get_header_lock());
        return ms_lock_info.base_compaction_cnt > tablet->base_compaction_cnt() ||
               ms_lock_info.cumulative_compaction_cnt > tablet->cumulative_compaction_cnt() ||
               ms_lock_info.cumulative_point > tablet->cumulative_layer_point() ||
               (ms_lock_info.tablet_state.has_value() &&
                ms_lock_info.tablet_state.value() !=
                        static_cast<std::underlying_type_t<TabletState>>(tablet->tablet_state()));
    };
    if (should_sync_rowsets()) {
        auto sync_st = tablet->sync_rowsets();
        if (!sync_st.ok()) {
            LOG(WARNING) << "failed to sync rowsets. tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", status=" << sync_st;
            status = sync_st;
            return status;
        }
        if (tablet->tablet_state() != TABLET_RUNNING) [[unlikely]] {
            LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated later, "
                         "tablet_id: "
                      << _tablet_id << " txn_id: " << _transaction_id
                      << ", request_version=" << _version;
            status = Status::OK();
            return status;
        }
    }
    exec_stats.sync_rowset_time_us = MonotonicMicros() - t2;
    max_version = tablet->max_version_unlocked();

    // If already applied, skip calc delete bitmap
    if (max_version >= _version) {
        LOG(INFO) << "tablet already has version " << _version
                  << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
        status = Status::OK();
        return status;
    }

    if (_version != max_version + 1) {
        bool need_log = (config::publish_version_gap_logging_threshold < 0 ||
                         max_version + config::publish_version_gap_logging_threshold >= _version);
        if (need_log) {
            LOG(WARNING) << "version not continuous, current max version=" << max_version
                         << ", request_version=" << _version << " tablet_id=" << _tablet_id;
        }
        status = Status::Error<ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS>(
                "version not continuous for cloud async publish, tablet_id={}, "
                "tablet_max_version={}, publish_version={}",
                _tablet_id, max_version, _version);
        return status;
    }

    int64_t t_handle_rowset = MonotonicMicros();
    if (_engine.txn_delete_bitmap_cache().is_empty_rowset(_transaction_id, _tablet_id)) {
        exec_stats.is_empty_rowset = true;
        LOG(INFO) << "tablet=" << _tablet_id << ", txn=" << _transaction_id
                  << " is empty rowset, skip delete bitmap calculation for async publish";
        status = Status::OK();
    } else {
        status = _handle_rowset(tablet, _version, ms_lock_info.base_compaction_cnt,
                                ms_lock_info.cumulative_compaction_cnt,
                                ms_lock_info.cumulative_point, &exec_stats);
        if (!status.ok()) {
            LOG(INFO) << "failed to calculate delete bitmap for async publish on tablet"
                      << ", table_id=" << tablet->table_id()
                      << ", transaction_id=" << _transaction_id
                      << ", tablet_id=" << tablet->tablet_id() << ", version=" << _version
                      << ", status=" << status;
            return status;
        }
    }
    exec_stats.handle_rowset_time_us = MonotonicMicros() - t_handle_rowset;

    // Async publish: convert tmp rowset + local apply
    if (status.ok()) {
        int64_t t_async_publish = MonotonicMicros();
        status = _handle_async_publish(tablet, _version, &exec_stats);
        exec_stats.async_publish_time_us = MonotonicMicros() - t_async_publish;
        if (!status.ok()) {
            LOG(WARNING) << "async publish failed, tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", status=" << status;
            return status;
        }
    }
    return status;
}

Status CloudTabletCalcDeleteBitmapAsyncPublishTask::_handle_rowset(
        std::shared_ptr<CloudTablet> tablet, int64_t version, int64_t ms_base_compaction_cnt,
        int64_t ms_cumulative_compaction_cnt, int64_t ms_cumulative_point,
        ExecutionStats* exec_stats) const {
    std::string txn_str = "txn_id=" + std::to_string(_transaction_id);
    RowsetSharedPtr rowset;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    std::shared_ptr<PublishStatus> publish_status;
    int64_t txn_expiration;
    TxnPublishInfo previous_publish_info;
    Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
            _transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids, &txn_expiration,
            &partial_update_info, &publish_status, &previous_publish_info);
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << _tablet_id << ", " << txn_str
                     << ", status=" << status;
        return status;
    }

    rowset->set_version(Version(version, version));
    TabletTxnInfo txn_info;
    txn_info.rowset = rowset;
    txn_info.delete_bitmap = delete_bitmap;
    txn_info.rowset_ids = rowset_ids;
    txn_info.partial_update_info = partial_update_info;
    txn_info.publish_status = publish_status;
    txn_info.publish_info = {.publish_version = version,
                             .base_compaction_cnt = ms_base_compaction_cnt,
                             .cumulative_compaction_cnt = ms_cumulative_compaction_cnt,
                             .cumulative_point = ms_cumulative_point};
    if (txn_info.publish_status && (*(txn_info.publish_status) == PublishStatus::SUCCEED) &&
        version == previous_publish_info.publish_version &&
        ms_base_compaction_cnt == previous_publish_info.base_compaction_cnt &&
        ms_cumulative_compaction_cnt == previous_publish_info.cumulative_compaction_cnt &&
        ms_cumulative_point == previous_publish_info.cumulative_point) {
        // we still need to update delete bitmap KVs to MS when we skip to calcalate delete bitmaps,
        // because the pending delete bitmap KVs in MS we wrote before may have been removed and replaced by other txns
        int64_t lock_id = txn_info.is_txn_load ? txn_info.lock_id : -1;
        int64_t next_visible_version =
                txn_info.is_txn_load ? txn_info.next_visible_version : version;
        int64_t t_save_delete_bitmap_to_ms = MonotonicMicros();
        auto st = tablet->save_delete_bitmap_to_ms(version, _transaction_id, delete_bitmap, lock_id,
                                                   next_visible_version, rowset);
        if (exec_stats != nullptr) {
            exec_stats->save_delete_bitmap_to_ms_time_us =
                    MonotonicMicros() - t_save_delete_bitmap_to_ms;
        }
        RETURN_IF_ERROR(st);

        LOG(INFO) << "tablet=" << _tablet_id << ", " << txn_str
                  << ", publish_status=SUCCEED, not need to re-calculate delete_bitmaps.";
    } else {
        if (rowset->num_segments() > 1 &&
            !delete_bitmap->has_calculated_for_multi_segments(rowset->rowset_id())) {
            // delete bitmap cache missed, should re-calculate delete bitmaps between segments
            std::vector<segment_v2::SegmentSharedPtr> segments;
            RETURN_IF_ERROR(std::static_pointer_cast<BetaRowset>(rowset)->load_segments(&segments));
            RETURN_IF_ERROR(tablet->calc_delete_bitmap_between_segments(
                    rowset->tablet_schema(), rowset->rowset_id(), segments, delete_bitmap));
        }

        int64_t t_update_delete_bitmap = MonotonicMicros();
        status = CloudTablet::update_delete_bitmap(tablet, &txn_info, _transaction_id,
                                                   txn_expiration);
        if (exec_stats != nullptr) {
            exec_stats->update_delete_bitmap_time_us = MonotonicMicros() - t_update_delete_bitmap;
        }
    }
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to calculate delete bitmap. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet_id << ", " << txn_str << ", status=" << status;
        return status;
    }
    return Status::OK();
}

Status CloudTabletCalcDeleteBitmapAsyncPublishTask::_apply_rowset_to_tablet(
        std::shared_ptr<CloudTablet> tablet, int64_t version, RowsetSharedPtr& rowset,
        const std::shared_ptr<DeleteBitmap>& delete_bitmap, int64_t visible_ts_ms,
        std::unique_lock<std::shared_mutex>& meta_lock) const {
    bool is_empty_rowset = (rowset == nullptr);

    if (is_empty_rowset) {
        // Empty rowset: create hole filler rowset locally
        Versions existing_versions;
        for (const auto& [_, rs] : tablet->tablet_meta()->all_rs_metas()) {
            existing_versions.emplace_back(rs->version());
        }
        if (existing_versions.empty()) {
            return Status::InternalError<false>("no existing rowsets for empty rowset");
        }
        auto max_version = std::ranges::max(existing_versions, {}, &Version::first);
        auto prev_rowset = tablet->get_rowset_by_version(max_version);
        RETURN_IF_ERROR(_engine.meta_mgr().create_empty_rowset_for_hole(
                tablet.get(), version, prev_rowset->rowset_meta(), &rowset));
    } else {
        // Non-empty rowset: merge delete bitmap
        if (delete_bitmap) {
            for (const auto& [delete_bitmap_key, bitmap_value] : delete_bitmap->delete_bitmap) {
                // Skip sentinel mark
                if (std::get<1>(delete_bitmap_key) != DeleteBitmap::INVALID_SEGMENT_ID) {
                    tablet->tablet_meta()->delete_bitmap().merge(
                            {std::get<0>(delete_bitmap_key), std::get<1>(delete_bitmap_key),
                             version},
                            bitmap_value);
                }
            }
        }
    }

    // Set version fields and add rowset to tablet
    rowset->rowset_meta()->set_cloud_fields_after_visible(version, visible_ts_ms);
    tablet->add_rowsets({rowset}, false /* version_overlap */, meta_lock);
    return Status::OK();
}

Status CloudTabletCalcDeleteBitmapAsyncPublishTask::_handle_async_publish(
        std::shared_ptr<CloudTablet> tablet, int64_t version, ExecutionStats* exec_stats) const {
    // MS tablet-level lock is already held (acquired in handle() before delete bitmap calculation)

    // Step 1: Get rowset and delete bitmap from local cache
    auto res = _engine.txn_delete_bitmap_cache().get_rowset_and_delete_bitmap(_transaction_id,
                                                                              _tablet_id);
    if (!res.has_value()) {
        LOG(WARNING) << "async publish cache entry not found, tablet_id=" << _tablet_id
                     << ", txn_id=" << _transaction_id << ", version=" << version;
        return Status::InternalError<false>("rowset not found in txn_delete_bitmap_cache");
    }
    auto [rowset, delete_bitmap] = res.value();
    bool is_empty_rowset = (rowset == nullptr);

    // Step 2: Local apply - merge delete bitmap and add rowset to tablet (best-effort)
    {
        std::unique_lock meta_lock(tablet->get_header_lock());
        if (tablet->max_version_unlocked() >= version) {
            LOG(INFO) << "tablet=" << _tablet_id << " already applied version=" << version;
            return Status::OK();
        }

        int64_t visible_ts_ms;
        if (is_empty_rowset) {
            // Empty rowset: use local time
            visible_ts_ms = ::time(nullptr) * 1000;
        } else {
            // Non-empty rowset: call MS convert_tmp_rowset (only this error propagates)
            RowsetMetaSharedPtr ms_rowset_meta;
            int64_t t_convert_tmp_rowset = MonotonicMicros();
            auto st = _engine.meta_mgr().convert_tmp_rowset(_transaction_id, _tablet_id, version,
                                                            _db_id, _table_id, _index_id,
                                                            _partition_id, &ms_rowset_meta);
            if (exec_stats != nullptr) {
                exec_stats->convert_tmp_rowset_time_us = MonotonicMicros() - t_convert_tmp_rowset;
            }
            RETURN_IF_ERROR(st);
            visible_ts_ms = ms_rowset_meta->visible_ts_ms();
        }

        // Local apply is best-effort
        int64_t t_local_apply = MonotonicMicros();
        auto st = _apply_rowset_to_tablet(tablet, version, rowset, delete_bitmap, visible_ts_ms,
                                          meta_lock);
        if (exec_stats != nullptr) {
            exec_stats->local_apply_time_us = MonotonicMicros() - t_local_apply;
        }
        if (!st.ok()) {
            LOG(WARNING) << "async publish local apply failed, tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", version=" << version
                         << ", st=" << st.to_string();
        }
    }

    // Step 3: Clean up the cache entry
    int64_t t_remove_unused_tablet_txn_info = MonotonicMicros();
    _engine.txn_delete_bitmap_cache().remove_unused_tablet_txn_info(_transaction_id, _tablet_id);
    if (exec_stats != nullptr) {
        exec_stats->remove_tablet_txn_info_time_us =
                MonotonicMicros() - t_remove_unused_tablet_txn_info;
    }

    LOG(INFO) << "async publish apply succeeded, tablet_id=" << _tablet_id
              << ", txn_id=" << _transaction_id << ", version=" << version
              << ", is_empty_rowset=" << is_empty_rowset;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
