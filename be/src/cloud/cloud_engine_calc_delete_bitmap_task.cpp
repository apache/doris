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

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

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

    for (const auto& partition : _cal_delete_bitmap_req.partitions) {
        int64_t version = partition.version;
        for (auto tablet_id : partition.tablet_ids) {
            auto base_tablet = DORIS_TRY(_engine.get_tablet(tablet_id));
            std::shared_ptr<CloudTablet> tablet =
                    std::dynamic_pointer_cast<CloudTablet>(base_tablet);
            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when calculate delete bitmap. tablet_id="
                             << tablet_id;
                _error_tablet_ids->push_back(tablet_id);
                _res = Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                        "can't get tablet when calculate delete bitmap. tablet_id={}", tablet_id);
                break;
            }

            Status st = tablet->sync_rowsets();
            if (!st.ok() && !st.is<ErrorCode::INVALID_TABLET_STATE>()) {
                return st;
            }
            if (st.is<ErrorCode::INVALID_TABLET_STATE>()) [[unlikely]] {
                add_succ_tablet_id(tablet->tablet_id());
                LOG(INFO)
                        << "tablet is under alter process, delete bitmap will be calculated later, "
                           "tablet_id: "
                        << tablet->tablet_id() << " txn_id: " << transaction_id
                        << ", request_version=" << version;
                continue;
            }
            int64_t max_version = tablet->max_version_unlocked();
            if (version != max_version + 1) {
                _error_tablet_ids->push_back(tablet_id);
                _res = Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>(
                        "version not continuous");
                LOG(WARNING) << "version not continuous, current max version=" << max_version
                             << ", request_version=" << version
                             << " tablet_id=" << tablet->tablet_id();
                break;
            }

            auto tablet_calc_delete_bitmap_ptr = std::make_shared<CloudTabletCalcDeleteBitmapTask>(
                    _engine, this, tablet, transaction_id, version);
            auto submit_st = token->submit_func([=]() { tablet_calc_delete_bitmap_ptr->handle(); });
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
        CloudStorageEngine& engine, CloudEngineCalcDeleteBitmapTask* engine_task,
        std::shared_ptr<CloudTablet> tablet, int64_t transaction_id, int64_t version)
        : _engine(engine),
          _engine_calc_delete_bitmap_task(engine_task),
          _tablet(tablet),
          _transaction_id(transaction_id),
          _version(version) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER,
            fmt::format("CloudTabletCalcDeleteBitmapTask#_transaction_id={}", _transaction_id));
}

void CloudTabletCalcDeleteBitmapTask::handle() const {
    SCOPED_ATTACH_TASK(_mem_tracker);
    RowsetSharedPtr rowset;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    int64_t txn_expiration;
    Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
            _transaction_id, _tablet->tablet_id(), &rowset, &delete_bitmap, &rowset_ids,
            &txn_expiration, &partial_update_info);
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << _tablet->tablet_id()
                     << ", txn_id=" << _transaction_id << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet->tablet_id(), status);
        return;
    }

    rowset->set_version(Version(_version, _version));
    TabletTxnInfo txn_info;
    txn_info.rowset = rowset;
    txn_info.delete_bitmap = delete_bitmap;
    txn_info.rowset_ids = rowset_ids;
    txn_info.partial_update_info = partial_update_info;
    status = CloudTablet::update_delete_bitmap(_tablet, &txn_info, _transaction_id, txn_expiration);
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to calculate delete bitmap. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _transaction_id
                     << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet->tablet_id(), status);
        return;
    }

    _engine_calc_delete_bitmap_task->add_succ_tablet_id(_tablet->tablet_id());
    LOG(INFO) << "calculate delete bitmap successfully on tablet"
              << ", table_id=" << _tablet->table_id() << ", transaction_id=" << _transaction_id
              << ", num_rows=" << rowset->num_rows() << ", res=" << status;
}

} // namespace doris
