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

#include "cloud/cloud_engine_calc_delete_bitmap_task.h"

#include <fmt/format.h>

#include <memory>
#include <random>
#include <thread>
#include <type_traits>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {
#include "common/compile_check_begin.h"

CloudEngineCalcDeleteBitmapTask::CloudEngineCalcDeleteBitmapTask(
        CloudStorageEngine& engine, const TCalcDeleteBitmapRequest& cal_delete_bitmap_req,
        std::vector<TTabletId>* error_tablet_ids, std::vector<TTabletId>* succ_tablet_ids)
        : _engine(engine),
          _cal_delete_bitmap_req(cal_delete_bitmap_req),
          _error_tablet_ids(error_tablet_ids),
          _succ_tablet_ids(succ_tablet_ids) {
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "CloudEngineCalcDeleteBitmapTask");
}

void CloudEngineCalcDeleteBitmapTask::add_error_tablet_id(int64_t tablet_id, const Status& err) {
    std::lock_guard<std::mutex> lck(_mutex);
    _error_tablet_ids->push_back(tablet_id);
    if (_res.ok() || _res.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>()) {
        _res = err;
    }
}

void CloudEngineCalcDeleteBitmapTask::add_succ_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_mutex);
    _succ_tablet_ids->push_back(tablet_id);
}

Status CloudEngineCalcDeleteBitmapTask::execute() {
    int64_t transaction_id = _cal_delete_bitmap_req.transaction_id;
    OlapStopWatch watch;
    VLOG_NOTICE << "begin to calculate delete bitmap. transaction_id=" << transaction_id;
    std::unique_ptr<ThreadPoolToken> token =
            _engine.calc_tablet_delete_bitmap_task_thread_pool().new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
    DBUG_EXECUTE_IF("CloudEngineCalcDeleteBitmapTask.execute.enable_wait", {
        auto sleep_time = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                "CloudEngineCalcDeleteBitmapTask.execute.enable_wait", "sleep_time", 3);
        sleep(sleep_time);
    });
    for (const auto& partition : _cal_delete_bitmap_req.partitions) {
        int64_t version = partition.version;
        bool has_compaction_stats = partition.__isset.base_compaction_cnts &&
                                    partition.__isset.cumulative_compaction_cnts &&
                                    partition.__isset.cumulative_points;
        bool has_tablet_states = partition.__isset.tablet_states;
        for (size_t i = 0; i < partition.tablet_ids.size(); i++) {
            auto tablet_id = partition.tablet_ids[i];
            auto tablet_calc_delete_bitmap_ptr = std::make_shared<CloudTabletCalcDeleteBitmapTask>(
                    _engine, tablet_id, transaction_id, version, partition.sub_txn_ids);
            if (has_compaction_stats) {
                tablet_calc_delete_bitmap_ptr->set_compaction_stats(
                        partition.base_compaction_cnts[i], partition.cumulative_compaction_cnts[i],
                        partition.cumulative_points[i]);
            }
            if (has_tablet_states) {
                tablet_calc_delete_bitmap_ptr->set_tablet_state(partition.tablet_states[i]);
            }
            auto submit_st = token->submit_func([tablet_id, tablet_calc_delete_bitmap_ptr, this]() {
                auto st = tablet_calc_delete_bitmap_ptr->handle();
                if (st.ok()) {
                    add_succ_tablet_id(tablet_id);
                } else {
                    LOG(WARNING) << "handle calc delete bitmap fail, st=" << st.to_string();
                    add_error_tablet_id(tablet_id, st);
                }
            });
            VLOG_DEBUG << "submit TabletCalcDeleteBitmapTask for tablet=" << tablet_id;
            if (!submit_st.ok()) {
                _res = submit_st;
                break;
            }
        }
    }
    // wait for all finished
    token->wait();

    LOG(INFO) << "finish to calculate delete bitmap on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
              << ", error_tablet_size=" << _error_tablet_ids->size()
              << ", res=" << _res.to_string();
    return _res;
}

CloudTabletCalcDeleteBitmapTask::CloudTabletCalcDeleteBitmapTask(
        CloudStorageEngine& engine, int64_t tablet_id, int64_t transaction_id, int64_t version,
        const std::vector<int64_t>& sub_txn_ids)
        : _engine(engine),
          _tablet_id(tablet_id),
          _transaction_id(transaction_id),
          _version(version),
          _sub_txn_ids(sub_txn_ids) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER,
            fmt::format("CloudTabletCalcDeleteBitmapTask#_transaction_id={}", _transaction_id));
}

void CloudTabletCalcDeleteBitmapTask::set_compaction_stats(int64_t ms_base_compaction_cnt,
                                                           int64_t ms_cumulative_compaction_cnt,
                                                           int64_t ms_cumulative_point) {
    _ms_base_compaction_cnt = ms_base_compaction_cnt;
    _ms_cumulative_compaction_cnt = ms_cumulative_compaction_cnt;
    _ms_cumulative_point = ms_cumulative_point;
}
void CloudTabletCalcDeleteBitmapTask::set_tablet_state(int64_t tablet_state) {
    _ms_tablet_state = tablet_state;
}

Status CloudTabletCalcDeleteBitmapTask::handle() const {
    VLOG_DEBUG << "start calculate delete bitmap on tablet " << _tablet_id
               << ", txn_id=" << _transaction_id;
    SCOPED_ATTACH_TASK(_mem_tracker);
    int64_t t1 = MonotonicMicros();
    auto base_tablet = DORIS_TRY(_engine.get_tablet(_tablet_id));
    auto get_tablet_time_us = MonotonicMicros() - t1;
    std::shared_ptr<CloudTablet> tablet = std::dynamic_pointer_cast<CloudTablet>(base_tablet);
    if (tablet == nullptr) {
        return Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                "can't get tablet when calculate delete bitmap. tablet_id={}", _tablet_id);
    }
    // After https://github.com/apache/doris/pull/50417, there may be multiple calc delete bitmap tasks
    // with different signatures on the same (txn_id, tablet_id) load in same BE. We use _rowset_update_lock
    // to avoid them being executed concurrently to avoid correctness problem.
    std::unique_lock wrlock(tablet->get_rowset_update_lock());

    int64_t max_version = tablet->max_version_unlocked();
    int64_t t2 = MonotonicMicros();

    auto should_sync_rowsets = [&]() {
        if (_version != max_version + 1) {
            return true;
        }
        if (_ms_base_compaction_cnt == -1) {
            return true;
        }

        // some compaction jobs finished on other BEs during this load job
        // we should sync rowsets and their delete bitmaps produced by compaction jobs
        std::shared_lock rlock(tablet->get_header_lock());
        return _ms_base_compaction_cnt > tablet->base_compaction_cnt() ||
               _ms_cumulative_compaction_cnt > tablet->cumulative_compaction_cnt() ||
               _ms_cumulative_point > tablet->cumulative_layer_point() ||
               (_ms_tablet_state.has_value() &&
                _ms_tablet_state.value() != // an SC job finished on other BEs during this load job
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
    if (_version != max_version + 1) {
        bool need_log = (config::publish_version_gap_logging_threshold < 0 ||
                         max_version + config::publish_version_gap_logging_threshold >= _version);
        if (need_log) {
            LOG(WARNING) << "version not continuous, current max version=" << max_version
                         << ", request_version=" << _version << " tablet_id=" << _tablet_id;
        }
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>("version not continuous");
    }

    int64_t t3 = MonotonicMicros();
    DBUG_EXECUTE_IF("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep", {
        auto p = dp->param("percent", 0.01);
        // 100s > Config.calculate_delete_bitmap_task_timeout_seconds = 60s
        auto sleep_time = dp->param("sleep", 100);
        std::mt19937 gen {std::random_device {}()};
        std::bernoulli_distribution inject_fault {p};
        if (inject_fault(gen)) {
            LOG_INFO("injection sleep for {} seconds, txn={}, tablet_id={}", sleep_time,
                     _transaction_id, _tablet_id);
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
    });
    Status status;
    if (_sub_txn_ids.empty()) {
        status = _handle_rowset(tablet, _version);
    } else {
        std::stringstream ss;
        for (const auto& sub_txn_id : _sub_txn_ids) {
            ss << sub_txn_id << ", ";
        }
        LOG(INFO) << "start calc delete bitmap for txn_id=" << _transaction_id << ", sub_txn_ids=["
                  << ss.str() << "], table_id=" << tablet->table_id()
                  << ", partition_id=" << tablet->partition_id() << ", tablet_id=" << _tablet_id
                  << ", start_version=" << _version;
        std::vector<RowsetSharedPtr> invisible_rowsets;
        DeleteBitmapPtr tablet_delete_bitmap =
                std::make_shared<DeleteBitmap>(tablet->tablet_meta()->delete_bitmap());
        for (int i = 0; i < _sub_txn_ids.size(); ++i) {
            int64_t sub_txn_id = _sub_txn_ids[i];
            int64_t version = _version + i;
            LOG(INFO) << "start calc delete bitmap for txn_id=" << _transaction_id
                      << ", sub_txn_id=" << sub_txn_id << ", table_id=" << tablet->table_id()
                      << ", partition_id=" << tablet->partition_id() << ", tablet_id=" << _tablet_id
                      << ", start_version=" << _version << ", cur_version=" << version;
            status = _handle_rowset(tablet, version, sub_txn_id, &invisible_rowsets,
                                    tablet_delete_bitmap);
            if (!status.ok()) {
                LOG(INFO) << "failed to calculate delete bitmap on tablet"
                          << ", table_id=" << tablet->table_id()
                          << ", transaction_id=" << _transaction_id << ", sub_txn_id=" << sub_txn_id
                          << ", tablet_id=" << tablet->tablet_id() << ", start version=" << _version
                          << ", cur_version=" << version << ", status=" << status;
                return status;
            }
            DCHECK(invisible_rowsets.size() == i + 1);
        }
    }
    DBUG_EXECUTE_IF("CloudCalcDbmTask.handle.return.block",
                    auto target_tablet_id = dp->param<int64_t>("tablet_id", 0);
                    if (target_tablet_id == tablet->tablet_id()) {DBUG_BLOCK});
    DBUG_EXECUTE_IF("CloudCalcDbmTask.handle.return.inject_err", {
        auto target_tablet_id = dp->param<int64_t>("tablet_id", 0);
        if (target_tablet_id == tablet->tablet_id()) {
            LOG_INFO("inject error when CloudTabletCalcDeleteBitmapTask::handle");
            return Status::InternalError("injected error");
        }
    });
    auto total_update_delete_bitmap_time_us = MonotonicMicros() - t3;
    LOG(INFO) << "finish calculate delete bitmap on tablet"
              << ", table_id=" << tablet->table_id() << ", transaction_id=" << _transaction_id
              << ", tablet_id=" << tablet->tablet_id()
              << ", get_tablet_time_us=" << get_tablet_time_us
              << ", sync_rowset_time_us=" << sync_rowset_time_us
              << ", total_update_delete_bitmap_time_us=" << total_update_delete_bitmap_time_us
              << ", res=" << status;
    return status;
}

Status CloudTabletCalcDeleteBitmapTask::_handle_rowset(
        std::shared_ptr<CloudTablet> tablet, int64_t version, int64_t sub_txn_id,
        std::vector<RowsetSharedPtr>* invisible_rowsets,
        DeleteBitmapPtr tablet_delete_bitmap) const {
    int64_t transaction_id = sub_txn_id == -1 ? _transaction_id : sub_txn_id;
    std::string txn_str = "txn_id=" + std::to_string(_transaction_id) +
                          (sub_txn_id == -1 ? "" : ", sub_txn_id=" + std::to_string(sub_txn_id));
    RowsetSharedPtr rowset;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    std::shared_ptr<PublishStatus> publish_status;
    int64_t txn_expiration;
    TxnPublishInfo previous_publish_info;
    Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
            transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids, &txn_expiration,
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
                             .base_compaction_cnt = _ms_base_compaction_cnt,
                             .cumulative_compaction_cnt = _ms_cumulative_compaction_cnt,
                             .cumulative_point = _ms_cumulative_point};
    if (txn_info.publish_status && (*(txn_info.publish_status) == PublishStatus::SUCCEED) &&
        version == previous_publish_info.publish_version &&
        _ms_base_compaction_cnt == previous_publish_info.base_compaction_cnt &&
        _ms_cumulative_compaction_cnt == previous_publish_info.cumulative_compaction_cnt &&
        _ms_cumulative_point == previous_publish_info.cumulative_point) {
        // if version or compaction stats can't match, it means that this is a retry and there are
        // compaction or other loads finished successfully on the same tablet. So the previous publish
        // is stale and we should re-calculate the delete bitmap

        // we still need to update delete bitmap KVs to MS when we skip to calcalate delete bitmaps,
        // because the pending delete bitmap KVs in MS we wrote before may have been removed and replaced by other txns
        int64_t lock_id = txn_info.is_txn_load ? txn_info.lock_id : -1;
        int64_t next_visible_version =
                txn_info.is_txn_load ? txn_info.next_visible_version : version;
        RETURN_IF_ERROR(tablet->save_delete_bitmap_to_ms(version, transaction_id, delete_bitmap,
                                                         lock_id, next_visible_version, rowset));

        LOG(INFO) << "tablet=" << _tablet_id << ", " << txn_str
                  << ", publish_status=SUCCEED, not need to re-calculate delete_bitmaps.";
    } else {
        if (rowset->num_segments() > 1 &&
            !delete_bitmap->has_calculated_for_multi_segments(rowset->rowset_id())) {
            // delete bitmap cache missed, should re-calculate delete bitmaps between segments
            std::vector<segment_v2::SegmentSharedPtr> segments;
            RETURN_IF_ERROR(std::static_pointer_cast<BetaRowset>(rowset)->load_segments(&segments));
            DBUG_EXECUTE_IF("_handle_rowset.inject.before.calc_between_segments", {
                LOG_INFO("inject error when CloudTabletCalcDeleteBitmapTask::_handle_rowset");
                return Status::MemoryLimitExceeded("injected MemoryLimitExceeded error");
            });
            RETURN_IF_ERROR(tablet->calc_delete_bitmap_between_segments(
                    rowset->tablet_schema(), rowset->rowset_id(), segments, delete_bitmap));
        }

        if (invisible_rowsets == nullptr) {
            status = CloudTablet::update_delete_bitmap(tablet, &txn_info, transaction_id,
                                                       txn_expiration);
        } else {
            txn_info.is_txn_load = true;
            txn_info.invisible_rowsets = *invisible_rowsets;
            txn_info.lock_id = _transaction_id;
            txn_info.next_visible_version = _version;
            status = CloudTablet::update_delete_bitmap(tablet, &txn_info, transaction_id,
                                                       txn_expiration, tablet_delete_bitmap);
        }
    }
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to calculate delete bitmap. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet_id << ", " << txn_str << ", status=" << status;
        return status;
    }

    if (invisible_rowsets != nullptr) {
        invisible_rowsets->push_back(rowset);
        // see CloudTablet::save_delete_bitmap
        auto dm = txn_info.delete_bitmap->delete_bitmap;
        for (auto it = dm.begin(); it != dm.end(); ++it) {
            if (std::get<1>(it->first) != DeleteBitmap::INVALID_SEGMENT_ID) {
                tablet_delete_bitmap->merge(
                        {std::get<0>(it->first), std::get<1>(it->first), version}, it->second);
            }
        }
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
