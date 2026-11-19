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

#include <bvar/reducer.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include <memory>
#include <random>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <type_traits>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/txn/txn_manager.h"
#include "storage/utils.h"

namespace doris {

static bvar::Adder<int64_t> g_cloud_mow_2pc_baseline_recompute_count(
        "cloud_mow_2pc_baseline_recompute_count");
// Counts cache misses and cached baselines that do not match the pinned rowset.
static bvar::Adder<int64_t> g_cloud_mow_2pc_cache_recovery_count(
        "cloud_mow_2pc_cache_recovery_count");

CloudEngineCalcDeleteBitmapTask::CloudEngineCalcDeleteBitmapTask(
        CloudStorageEngine& engine, const TCalcDeleteBitmapRequest& cal_delete_bitmap_req,
        std::vector<TTabletId>* error_tablet_ids, std::vector<TTabletId>* succ_tablet_ids)
        : _engine(engine),
          _cal_delete_bitmap_req(cal_delete_bitmap_req),
          _lock_initiator(cal_delete_bitmap_req.__isset.lock_initiator
                                  ? cal_delete_bitmap_req.lock_initiator
                                  : -1),
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
                    _engine, tablet_id, transaction_id, version, partition.sub_txn_ids,
                    _lock_initiator, this);
            if (has_compaction_stats) {
                tablet_calc_delete_bitmap_ptr->set_compaction_stats(
                        partition.base_compaction_cnts[i], partition.cumulative_compaction_cnts[i],
                        partition.cumulative_points[i]);
            }
            if (has_tablet_states) {
                tablet_calc_delete_bitmap_ptr->set_tablet_state(partition.tablet_states[i]);
            }
            const auto submit_time_us = MonotonicMicros();
            auto submit_st = token->submit_func(
                    [tablet_id, tablet_calc_delete_bitmap_ptr, this, submit_time_us]() {
                        const auto queue_time_us = MonotonicMicros() - submit_time_us;
                        auto st = tablet_calc_delete_bitmap_ptr->handle(queue_time_us);
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

Status CloudEngineCalcDeleteBitmapTask::get_pending_rowset_id(int64_t tablet_id,
                                                              std::string_view* rowset_id) {
    const int64_t txn_id = _cal_delete_bitmap_req.transaction_id;
    std::call_once(_load_precommit_once, [&] {
        cloud::TxnInfoPB txn;
        _load_precommit_status = _engine.meta_mgr().get_txn(txn_id, -1, &txn);
        if (!_load_precommit_status.ok()) {
            return;
        }
        if (!txn.is_mow_load() || txn.status() != cloud::TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
            _load_precommit_status = Status::InternalError<false>(
                    "Missing precommitted MoW transaction info for txn_id={}. Abort and reload.",
                    txn_id);
            return;
        }
        for (const auto& info : txn.tablet_commit_infos()) {
            if (info.has_pending_rowset_id()) {
                _pending_rowset_ids.emplace(info.tablet_id(), info.pending_rowset_id());
            }
        }
    });
    RETURN_IF_ERROR(_load_precommit_status);
    const auto& pending_rowset_ids = _pending_rowset_ids;
    auto expected = pending_rowset_ids.find(tablet_id);
    if (expected == pending_rowset_ids.end()) {
        return Status::InternalError<false>(
                "Missing MoW precommit rowset mapping, txn_id={}, tablet_id={}", txn_id, tablet_id);
    }
    *rowset_id = expected->second;
    return Status::OK();
}

Status CloudEngineCalcDeleteBitmapTask::recover_pending_rowset(int64_t tablet_id,
                                                               RowsetSharedPtr* rowset) {
    const int64_t txn_id = _cal_delete_bitmap_req.transaction_id;
    std::string_view expected;
    RETURN_IF_ERROR(get_pending_rowset_id(tablet_id, &expected));
    RETURN_IF_ERROR(_engine.meta_mgr().get_pending_rowset(txn_id, tablet_id, rowset));
    const auto actual_id = *rowset ? (*rowset)->rowset_id().to_string() : std::string();
    // A missing rowset is safe only when precommit recorded an omitted rowset.
    if (actual_id != expected) {
        return Status::InternalError<false>(
                "Pending rowset differs from MoW precommit, txn_id={}, tablet_id={}", txn_id,
                tablet_id);
    }
    return Status::OK();
}

CloudTabletCalcDeleteBitmapTask::CloudTabletCalcDeleteBitmapTask(
        CloudStorageEngine& engine, int64_t tablet_id, int64_t transaction_id, int64_t version,
        const std::vector<int64_t>& sub_txn_ids, int64_t lock_initiator,
        CloudEngineCalcDeleteBitmapTask* engine_task)
        : _engine(engine),
          _tablet_id(tablet_id),
          _transaction_id(transaction_id),
          _version(version),
          _sub_txn_ids(sub_txn_ids),
          _lock_initiator(lock_initiator),
          _engine_task(engine_task) {
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

Status CloudTabletCalcDeleteBitmapTask::handle(int64_t queue_time_us) const {
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
    if (tablet->is_changelog_tablet()) {
        VLOG_DEBUG << "skip calculating delete bitmap for changelog tablet, tablet_id="
                   << _tablet_id << ", txn_id=" << _transaction_id
                   << ", it will be handled with its base tablet";
        return Status::OK();
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
    bool sync_required = should_sync_rowsets();
    if (sync_required) {
        auto sync_st = tablet->sync_rowsets();
        if (!sync_st.ok()) {
            LOG(WARNING) << "failed to sync rowsets. tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", status=" << sync_st;
            return sync_st;
        }
    }
    if ((sync_required || _lock_initiator != -1) && tablet->tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated later, "
                     "tablet_id: "
                  << _tablet_id << " txn_id: " << _transaction_id
                  << ", request_version=" << _version;
        if (_lock_initiator != -1) {
            if (tablet->tablet_state() != TABLET_NOTREADY) {
                return Status::InternalError<false>("Tablet {} cannot publish in state {}",
                                                    _tablet_id,
                                                    static_cast<int>(tablet->tablet_state()));
            }
            RowsetSharedPtr rowset;
            RETURN_IF_ERROR(_engine_task->recover_pending_rowset(_tablet_id, &rowset));
            if (rowset != nullptr) {
                // Schema change will compute the bitmap. Record the fenced publish version.
                auto bitmap = std::make_shared<DeleteBitmap>(_tablet_id);
                return tablet->save_delete_bitmap_to_ms(_version, _transaction_id, bitmap, -1,
                                                        _version, rowset, _lock_initiator);
            }
        }
        return Status::OK();
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
    DBUG_EXECUTE_IF("CloudEngineCalcDeleteBitmapTask.handle.block_for_restart", {
        if (dp->param<int64_t>("tablet_id", -1) == _tablet_id) {
            // The restart regression pairs this marker with the persisted MS lock owner.
            LOG(INFO) << "delete bitmap restart barrier token="
                      << dp->param<std::string>("token", "") << " lock_id=" << _transaction_id
                      << " tablet_id=" << _tablet_id;
            DBUG_BLOCK;
        }
    });
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
        // Check empty rowset for non-sub_txn case
        if (_lock_initiator == -1 &&
            _engine.txn_delete_bitmap_cache().is_empty_rowset(_transaction_id, _tablet_id)) {
            LOG(INFO) << "tablet=" << _tablet_id << ", txn=" << _transaction_id
                      << " is empty rowset, skip delete bitmap calculation";
            return Status::OK();
        }
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
        size_t empty_rowset_count = 0;
        for (size_t i = 0; i < _sub_txn_ids.size(); ++i) {
            int64_t sub_txn_id = _sub_txn_ids[i];
            int64_t version = _version + i;
            // Check empty rowset for each sub_txn using sub_txn_id
            if (_engine.txn_delete_bitmap_cache().is_empty_rowset(sub_txn_id, _tablet_id)) {
                LOG(INFO) << "tablet=" << _tablet_id << ", sub_txn=" << sub_txn_id
                          << ", version=" << version
                          << " is empty rowset, skip delete bitmap calculation";
                empty_rowset_count++;
                continue;
            }
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
            DCHECK(invisible_rowsets.size() == i + 1 - empty_rowset_count);
        }
    }
    DBUG_EXECUTE_IF("CloudTabletCalcDeleteBitmapTask.handle.block_after_calc", {
        auto target_tablet_id = dp->param<int64_t>("tablet_id", 0);
        auto target_transaction_id = dp->param<int64_t>("transaction_id", 0);
        if (status.ok() && target_tablet_id == _tablet_id &&
            (target_transaction_id == 0 || target_transaction_id == _transaction_id)) {
            // Delay the response after calculation without preventing another transaction from
            // calculating the delete bitmap on the same tablet.
            wrlock.unlock();
            LOG(INFO) << "start debug block after calculating delete bitmap"
                      << ", tablet_id=" << _tablet_id << ", txn_id=" << _transaction_id;
            while (DebugPoints::instance()->is_enable(DP_NAME)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            LOG(INFO) << "finish debug block after calculating delete bitmap"
                      << ", tablet_id=" << _tablet_id << ", txn_id=" << _transaction_id;
        }
    });
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
              << ", tablet_id=" << tablet->tablet_id() << ", queue_time_us=" << queue_time_us
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
    int64_t txn_expiration = 0;
    TxnPublishInfo previous_publish_info;
    ChangelogTxnInfo attach_changelog;
    Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
            transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids, &txn_expiration,
            &partial_update_info, &publish_status, &previous_publish_info, &attach_changelog);
    const bool fenced = _lock_initiator != -1;
    bool baseline_recompute = false;
    if (status.ok() && fenced) {
        // The transaction lookup is shared across tablets. A cached rowset for the same
        // transaction and tablet cannot supply a baseline for a different pinned rowset.
        std::string_view pending_rowset_id;
        RETURN_IF_ERROR(_engine_task->get_pending_rowset_id(_tablet_id, &pending_rowset_id));
        if (rowset->rowset_id().to_string() != pending_rowset_id) {
            status = Status::Error<ErrorCode::NOT_FOUND, false>(
                    "writer baseline does not match pinned rowset");
        } else if (rowset->tablet_schema()->has_sequence_col()) {
            // Without a sequence column, pending rows beat visible rows, so self-marks stay valid.
            // Compaction can invalidate a sequence self-mark. The multi-segment and
            // segment-compaction sentinels share INVALID_SEGMENT_ID, which the probe skips.
            // Intra-load duplicates on sequence tables may also trigger a safe recompute.
            std::shared_lock lock(delete_bitmap->lock);
            const auto& bitmap = delete_bitmap->delete_bitmap;
            const auto pending_id = rowset->rowset_id();
            for (auto mark = bitmap.lower_bound({pending_id, 0U, 0U});
                 mark != bitmap.end() && std::get<0>(mark->first) == pending_id; ++mark) {
                if (std::get<1>(mark->first) != DeleteBitmap::INVALID_SEGMENT_ID) {
                    baseline_recompute = true;
                    status = Status::Error<ErrorCode::NOT_FOUND, false>(
                            "writer baseline has pending-rowset delete marks");
                    break;
                }
            }
        }
    }
    if (!status.ok()) {
        if (!fenced || !status.is<ErrorCode::NOT_FOUND>()) {
            LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << _tablet_id << ", "
                         << txn_str << ", status=" << status;
            return status;
        }

        if (baseline_recompute) {
            g_cloud_mow_2pc_baseline_recompute_count << 1;
        } else {
            g_cloud_mow_2pc_cache_recovery_count << 1;
        }
        rowset.reset();
        rowset_ids.clear();
        txn_expiration = 0;
        partial_update_info.reset();
        publish_status.reset();
        attach_changelog = {};
        RETURN_IF_ERROR(_engine_task->recover_pending_rowset(_tablet_id, &rowset));
        if (rowset == nullptr) {
            return Status::OK(); // MetaService recorded an omitted rowset at precommit.
        }
        // An empty baseline forces the existing publish calculation to visit every visible rowset.
        delete_bitmap = std::make_shared<DeleteBitmap>(_tablet_id);
    } else if (fenced) {
        // A retry can assign a different version while the losing attempt is still running.
        // Keep both the bitmap and the pending rowset's mutable metadata private to this attempt.
        delete_bitmap = std::make_shared<DeleteBitmap>(*delete_bitmap);
        auto meta = std::make_shared<RowsetMeta>();
        DORIS_CHECK(meta->init(rowset->rowset_meta().get()));
        RowsetSharedPtr private_rowset;
        RETURN_IF_ERROR(
                RowsetFactory::create_rowset(rowset->tablet_schema(), "", meta, &private_rowset));
        rowset = std::move(private_rowset);
    }

    rowset->set_version(Version(version, version));
    if (rowset->tablet_schema()->is_tso_enabled() && rowset->commit_tso().start_tso() == -1) {
        DORIS_CHECK_EQ(rowset->commit_tso().end_tso(), -1);
        // FE allocates the real commit TSO after delete-bitmap calculation. Partial-update
        // alignment reads this pending rowset with its assigned version, so use the physical
        // COMMIT_TSO placeholder here. MS overwrites it when committing the transaction; TSO
        // tablets fetch the published metadata instead of using the BE fast-publish cache.
        rowset->rowset_meta()->set_commit_tso(0);
    }
    if (_lock_initiator != -1 && rowset->num_segments() == 0) {
        return tablet->save_delete_bitmap_to_ms(version, transaction_id, delete_bitmap, -1, version,
                                                rowset, _lock_initiator);
    }
    TabletTxnInfo txn_info;
    txn_info.rowset = rowset;
    txn_info.lock_initiator = _lock_initiator;
    txn_info.delete_bitmap = delete_bitmap;
    txn_info.attach_changelog = attach_changelog;
    txn_info.rowset_ids = rowset_ids;
    txn_info.partial_update_info = partial_update_info;
    txn_info.publish_status = publish_status;
    txn_info.publish_info = {.publish_version = version,
                             .base_compaction_cnt = _ms_base_compaction_cnt,
                             .cumulative_compaction_cnt = _ms_cumulative_compaction_cnt,
                             .cumulative_point = _ms_cumulative_point};
    if (_lock_initiator == -1 && txn_info.publish_status &&
        *txn_info.publish_status == PublishStatus::SUCCEED &&
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
                                                         lock_id, next_visible_version, rowset,
                                                         _lock_initiator));

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

} // namespace doris
