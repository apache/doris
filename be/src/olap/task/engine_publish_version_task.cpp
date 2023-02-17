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

#include "olap/task/engine_publish_version_task.h"

#include <util/defer_op.h>

#include <map>

#include "olap/data_dir.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/tablet_manager.h"

namespace doris {

using namespace ErrorCode;

using std::map;

EnginePublishVersionTask::EnginePublishVersionTask(TPublishVersionRequest& publish_version_req,
                                                   std::vector<TTabletId>* error_tablet_ids,
                                                   std::vector<TTabletId>* succ_tablet_ids)
        : _publish_version_req(publish_version_req),
          _error_tablet_ids(error_tablet_ids),
          _succ_tablet_ids(succ_tablet_ids) {}

void EnginePublishVersionTask::add_error_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_tablet_ids_mutex);
    _error_tablet_ids->push_back(tablet_id);
}

void EnginePublishVersionTask::add_succ_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_tablet_ids_mutex);
    _succ_tablet_ids->push_back(tablet_id);
}

void EnginePublishVersionTask::wait() {
    std::unique_lock<std::mutex> lock(_tablet_finish_sleep_mutex);
    _tablet_finish_sleep_cond.wait_for(lock, std::chrono::milliseconds(10));
}

void EnginePublishVersionTask::notify() {
    std::unique_lock<std::mutex> lock(_tablet_finish_sleep_mutex);
    _tablet_finish_sleep_cond.notify_one();
}

Status EnginePublishVersionTask::finish() {
    Status res = Status::OK();
    int64_t transaction_id = _publish_version_req.transaction_id;
    OlapStopWatch watch;
    VLOG_NOTICE << "begin to process publish version. transaction_id=" << transaction_id;

    // each partition
    std::atomic<int64_t> total_task_num(0);
    for (auto& par_ver_info : _publish_version_req.partition_version_infos) {
        int64_t partition_id = par_ver_info.partition_id;
        // get all partition related tablets and check whether the tablet have the related version
        std::set<TabletInfo> partition_related_tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_partition_related_tablets(
                partition_id, &partition_related_tablet_infos);
        if (_publish_version_req.strict_mode && partition_related_tablet_infos.empty()) {
            LOG(INFO) << "could not find related tablet for partition " << partition_id
                      << ", skip publish version";
            continue;
        }

        map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                transaction_id, partition_id, &tablet_related_rs);

        Version version(par_ver_info.version, par_ver_info.version);

        // each tablet
        for (auto& tablet_rs : tablet_related_rs) {
            TabletInfo tablet_info = tablet_rs.first;
            RowsetSharedPtr rowset = tablet_rs.second;
            VLOG_CRITICAL << "begin to publish version on tablet. "
                          << "tablet_id=" << tablet_info.tablet_id
                          << ", schema_hash=" << tablet_info.schema_hash
                          << ", version=" << version.first << ", transaction_id=" << transaction_id;
            // if rowset is null, it means this be received write task, but failed during write
            // and receive fe's publish version task
            // this be must return as an error tablet
            if (rowset == nullptr) {
                LOG(WARNING) << "could not find related rowset for tablet " << tablet_info.tablet_id
                             << " txn id " << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = Status::Error<PUSH_ROWSET_NOT_FOUND>();
                continue;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_info.tablet_id, tablet_info.tablet_uid);
            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when publish version. tablet_id="
                             << tablet_info.tablet_id << " schema_hash=" << tablet_info.schema_hash;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = Status::Error<PUSH_TABLE_NOT_EXIST>();
                continue;
            }
            // in uniq key model with merge-on-write, we should see all
            // previous version when update delete bitmap, so add a check
            // here and wait pre version publish or lock timeout
            if (tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                tablet->enable_unique_key_merge_on_write()) {
                Version max_version;
                TabletState tablet_state;
                {
                    std::shared_lock rdlock(tablet->get_header_lock());
                    max_version = tablet->max_version();
                    tablet_state = tablet->tablet_state();
                }
                if (tablet_state == TabletState::TABLET_RUNNING &&
                    version.first != max_version.second + 1) {
                    VLOG_NOTICE << "uniq key with merge-on-write version not continuous, current "
                                   "max "
                                   "version="
                                << max_version.second << ", publish_version=" << version.first
                                << " tablet_id=" << tablet->tablet_id();
                    // If a tablet migrates out and back, the previously failed
                    // publish task may retry on the new tablet, so check
                    // whether the version exists. if not exist, then set
                    // publish failed
                    if (!tablet->check_version_exist(version)) {
                        add_error_tablet_id(tablet_info.tablet_id);
                        res = Status::Error<PUBLISH_VERSION_NOT_CONTINUOUS>();
                    }
                    continue;
                }
            }
            total_task_num.fetch_add(1);
            auto tablet_publish_txn_ptr = std::make_shared<TabletPublishTxnTask>(
                    this, tablet, rowset, partition_id, transaction_id, version, tablet_info,
                    &total_task_num);
            auto submit_st =
                    StorageEngine::instance()->tablet_publish_txn_thread_pool()->submit_func(
                            [=]() { tablet_publish_txn_ptr->handle(); });
            CHECK(submit_st.ok());
        }
    }
    // wait for all publish txn finished
    while (total_task_num.load() != 0) {
        wait();
    }

    // check if the related tablet remained all have the version
    for (auto& par_ver_info : _publish_version_req.partition_version_infos) {
        int64_t partition_id = par_ver_info.partition_id;
        // get all partition related tablets and check whether the tablet have the related version
        std::set<TabletInfo> partition_related_tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_partition_related_tablets(
                partition_id, &partition_related_tablet_infos);

        Version version(par_ver_info.version, par_ver_info.version);
        for (auto& tablet_info : partition_related_tablet_infos) {
            // has to use strict mode to check if check all tablets
            if (!_publish_version_req.strict_mode) {
                break;
            }
            TabletSharedPtr tablet =
                    StorageEngine::instance()->tablet_manager()->get_tablet(tablet_info.tablet_id);
            if (tablet == nullptr) {
                add_error_tablet_id(tablet_info.tablet_id);
            } else {
                // check if the version exist, if not exist, then set publish failed
                if (!tablet->check_version_exist(version)) {
                    add_error_tablet_id(tablet_info.tablet_id);
                }
            }
        }
    }

    LOG(INFO) << "finish to publish version on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
              << ", error_tablet_size=" << _error_tablet_ids->size() << ", res=" << res.to_string();
    return res;
}

TabletPublishTxnTask::TabletPublishTxnTask(EnginePublishVersionTask* engine_task,
                                           TabletSharedPtr tablet, RowsetSharedPtr rowset,
                                           int64_t partition_id, int64_t transaction_id,
                                           Version version, const TabletInfo& tablet_info,
                                           std::atomic<int64_t>* total_task_num)
        : _engine_publish_version_task(engine_task),
          _tablet(tablet),
          _rowset(rowset),
          _partition_id(partition_id),
          _transaction_id(transaction_id),
          _version(version),
          _tablet_info(tablet_info),
          _total_task_num(total_task_num) {}

void TabletPublishTxnTask::handle() {
    Defer defer {[&] {
        if (_total_task_num->fetch_sub(1) == 1) {
            _engine_publish_version_task->notify();
        }
    }};
    auto publish_status = StorageEngine::instance()->txn_manager()->publish_txn(
            _partition_id, _tablet, _transaction_id, _version);
    if (publish_status != Status::OK()) {
        LOG(WARNING) << "failed to publish version. rowset_id=" << _rowset->rowset_id()
                     << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id=" << _transaction_id;
        _engine_publish_version_task->add_error_tablet_id(_tablet_info.tablet_id);
        return;
    }

    // add visible rowset to tablet
    publish_status = _tablet->add_inc_rowset(_rowset);
    if (publish_status != Status::OK() && !publish_status.is<PUSH_VERSION_ALREADY_EXIST>()) {
        LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id=" << _rowset->rowset_id()
                     << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id=" << _transaction_id
                     << ", res=" << publish_status;
        _engine_publish_version_task->add_error_tablet_id(_tablet_info.tablet_id);
        return;
    }
    _engine_publish_version_task->add_succ_tablet_id(_tablet_info.tablet_id);
    LOG(INFO) << "publish version successfully on tablet"
              << ", table_id=" << _tablet->table_id() << ", tablet=" << _tablet->full_name()
              << ", transaction_id=" << _transaction_id << ", version=" << _version.first
              << ", num_rows=" << _rowset->num_rows() << ", res=" << publish_status;
}

} // namespace doris
