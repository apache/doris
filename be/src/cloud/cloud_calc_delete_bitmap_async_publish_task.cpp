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

int64_t count_total_partitions(const TCalcDeleteBitmapAsyncPublishRequest& request) {
    return request.partitions.size();
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
    OlapStopWatch watch;
    LOG(INFO) << "begin to calculate delete bitmap for async publish on transaction"
              << ", transaction_id=" << transaction_id
              << ", total_partition_num=" << total_partition_num
              << ", total_tablet_num=" << total_tablet_num << ", thread_pool="
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
            int64_t index_id = (partition.__isset.index_ids && i < partition.index_ids.size())
                                       ? partition.index_ids[i]
                                       : -1;
            auto tablet_task = std::make_shared<CloudTabletCalcDeleteBitmapAsyncPublishTask>(
                    _engine, tablet_id, transaction_id, version, db_id, table_id, index_id,
                    partition.partition_id, transaction_start_time_us, total_partition_num,
                    total_tablet_num,
                    first_tablet_start_logged);
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
              << ", submit_tablet_task_time_us="
              << MonotonicMicros() - transaction_start_time_us << ", thread_pool="
              << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_info();
    // wait for all finished
    token->wait();

    LOG(INFO) << "finish to calculate delete bitmap for async publish on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
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
    const int64_t queue_wait_us = tablet_start_time_us - _transaction_start_time_us;
    bool expected = false;
    if (_first_tablet_start_logged &&
        _first_tablet_start_logged->compare_exchange_strong(expected, true,
                                                            std::memory_order_relaxed)) {
        LOG(INFO) << "first tablet task starts for async publish transaction"
                  << ", transaction_id=" << _transaction_id << ", tablet_id=" << _tablet_id
                  << ", queue_wait_us=" << queue_wait_us
                  << ", total_partition_num=" << _transaction_total_partition_num
                  << ", total_tablet_num=" << _transaction_total_tablet_num << ", thread_pool="
                  << _engine.calc_tablet_delete_bitmap_task_thread_pool().get_info();
    }
    VLOG_DEBUG << "start calculate delete bitmap for async publish on tablet " << _tablet_id
               << ", txn_id=" << _transaction_id << ", queue_wait_us=" << queue_wait_us;
    SCOPED_ATTACH_TASK(_mem_tracker);
    int64_t t1 = MonotonicMicros();
    auto base_tablet = DORIS_TRY(_engine.get_tablet(_tablet_id));
    auto get_tablet_time_us = MonotonicMicros() - t1;
    std::shared_ptr<CloudTablet> tablet = std::dynamic_pointer_cast<CloudTablet>(base_tablet);
    if (tablet == nullptr) {
        return Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                "can't get tablet when calculate delete bitmap for async publish. tablet_id={}",
                _tablet_id);
    }

    {
        std::shared_lock rlock(tablet->get_header_lock());
        if (tablet->max_version_unlocked() >= _version) {
            LOG(INFO) << "tablet already has version " << _version
                      << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
            return Status::OK();
        }
    }

    std::unique_lock delete_bitmap_and_rowset_layout_lock(
            tablet->get_delete_bitmap_and_rowset_layout_lock());

    {
        std::shared_lock rlock(tablet->get_header_lock());
        if (tablet->max_version_unlocked() >= _version) {
            LOG(INFO) << "tablet already has version " << _version
                      << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
            return Status::OK();
        }
    }

    // Acquire MS tablet-level lock right after the cloud async publish lock,
    // before delete bitmap calculation, to ensure mutual exclusion with compaction
    cloud::CloudMetaMgr::DeleteBitmapTabletLockInfo ms_lock_info;
    auto lock_st = _engine.meta_mgr().get_delete_bitmap_tablet_lock(*tablet, _transaction_id, -1,
                                                                    &ms_lock_info);
    if (!lock_st.ok()) {
        return lock_st;
    }
    // Release MS tablet lock on any exit path
    Defer defer_release_tablet_lock {[&]() {
        _engine.meta_mgr().remove_delete_bitmap_tablet_lock(*tablet, _transaction_id, -1);
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
            return sync_st;
        }
        if (tablet->tablet_state() != TABLET_RUNNING) [[unlikely]] {
            LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated later, "
                         "tablet_id: "
                      << _tablet_id << " txn_id: " << _transaction_id
                      << ", request_version=" << _version;
            return Status::OK();
        }
    }
    auto sync_rowset_time_us = MonotonicMicros() - t2;
    max_version = tablet->max_version_unlocked();

    // If already applied, skip calc delete bitmap
    if (max_version >= _version) {
        LOG(INFO) << "tablet already has version " << _version
                  << ", skip calc delete bitmap for async publish, tablet_id=" << _tablet_id;
        return Status::OK();
    }

    if (_version != max_version + 1) {
        bool need_log = (config::publish_version_gap_logging_threshold < 0 ||
                         max_version + config::publish_version_gap_logging_threshold >= _version);
        if (need_log) {
            LOG(WARNING) << "version not continuous, current max version=" << max_version
                         << ", request_version=" << _version << " tablet_id=" << _tablet_id;
        }
        return Status::Error<ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS>(
                "version not continuous for cloud async publish, tablet_id={}, "
                "tablet_max_version={}, publish_version={}",
                _tablet_id, max_version, _version);
    }

    int64_t t3 = MonotonicMicros();
    Status status;
    if (_engine.txn_delete_bitmap_cache().is_empty_rowset(_transaction_id, _tablet_id)) {
        LOG(INFO) << "tablet=" << _tablet_id << ", txn=" << _transaction_id
                  << " is empty rowset, skip delete bitmap calculation for async publish";
        status = Status::OK();
    } else {
        status = _handle_rowset(tablet, _version, ms_lock_info.base_compaction_cnt,
                                ms_lock_info.cumulative_compaction_cnt,
                                ms_lock_info.cumulative_point);
        if (!status.ok()) {
            LOG(INFO) << "failed to calculate delete bitmap for async publish on tablet"
                      << ", table_id=" << tablet->table_id()
                      << ", transaction_id=" << _transaction_id
                      << ", tablet_id=" << tablet->tablet_id() << ", version=" << _version
                      << ", status=" << status;
            return status;
        }
    }

    // Async publish: convert tmp rowset + local apply
    if (status.ok()) {
        status = _handle_async_publish(tablet, _version);
        if (!status.ok()) {
            LOG(WARNING) << "async publish failed, tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", status=" << status;
            return status;
        }
    }

    auto total_update_delete_bitmap_time_us = MonotonicMicros() - t3;
    LOG(INFO) << "finish calculate delete bitmap for async publish on tablet"
              << ", table_id=" << tablet->table_id() << ", transaction_id=" << _transaction_id
              << ", tablet_id=" << tablet->tablet_id()
              << ", get_tablet_time_us=" << get_tablet_time_us
              << ", sync_rowset_time_us=" << sync_rowset_time_us
              << ", total_update_delete_bitmap_time_us=" << total_update_delete_bitmap_time_us
              << ", res=" << status;
    return status;
}

Status CloudTabletCalcDeleteBitmapAsyncPublishTask::_handle_rowset(
        std::shared_ptr<CloudTablet> tablet, int64_t version, int64_t ms_base_compaction_cnt,
        int64_t ms_cumulative_compaction_cnt, int64_t ms_cumulative_point) const {
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
        RETURN_IF_ERROR(tablet->save_delete_bitmap_to_ms(version, _transaction_id, delete_bitmap,
                                                         lock_id, next_visible_version, rowset));

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

        status = CloudTablet::update_delete_bitmap(tablet, &txn_info, _transaction_id,
                                                   txn_expiration);
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
        std::shared_ptr<CloudTablet> tablet, int64_t version) const {
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
            RETURN_IF_ERROR(_engine.meta_mgr().convert_tmp_rowset(
                    _transaction_id, _tablet_id, version, _db_id, _table_id, _index_id,
                    _partition_id, &ms_rowset_meta));
            visible_ts_ms = ms_rowset_meta->visible_ts_ms();
        }

        // Local apply is best-effort
        auto st = _apply_rowset_to_tablet(tablet, version, rowset, delete_bitmap, visible_ts_ms,
                                          meta_lock);
        if (!st.ok()) {
            LOG(WARNING) << "async publish local apply failed, tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", version=" << version
                         << ", st=" << st.to_string();
        }
    }

    // Step 3: Clean up the cache entry
    _engine.txn_delete_bitmap_cache().remove_unused_tablet_txn_info(_transaction_id, _tablet_id);

    LOG(INFO) << "async publish apply succeeded, tablet_id=" << _tablet_id
              << ", txn_id=" << _transaction_id << ", version=" << version
              << ", is_empty_rowset=" << is_empty_rowset;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
