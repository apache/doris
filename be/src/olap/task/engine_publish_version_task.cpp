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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <util/defer_op.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/logging.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "util/bvar_helper.h"
#include "util/threadpool.h"

namespace doris {

using namespace ErrorCode;

using std::map;

static bvar::LatencyRecorder g_tablet_publish_latency("doris_pk", "tablet_publish");
static bvar::LatencyRecorder g_tablet_publish_schedule_latency("doris_pk",
                                                               "tablet_publish_schedule");
static bvar::LatencyRecorder g_tablet_publish_lock_wait_latency("doris_pk",
                                                                "tablet_publish_lock_wait");
static bvar::LatencyRecorder g_tablet_publish_save_meta_latency("doris_pk",
                                                                "tablet_publish_save_meta");
static bvar::LatencyRecorder g_tablet_publish_delete_bitmap_latency("doris_pk",
                                                                    "tablet_publish_delete_bitmap");
static bvar::LatencyRecorder g_tablet_publish_partial_update_latency(
        "doris_pk", "tablet_publish_partial_update");
static bvar::LatencyRecorder g_tablet_publish_add_inc_latency("doris_pk",
                                                              "tablet_publish_add_inc_rowset");

void TabletPublishStatistics::record_in_bvar() {
    g_tablet_publish_schedule_latency << schedule_time_us;
    g_tablet_publish_lock_wait_latency << lock_wait_time_us;
    g_tablet_publish_save_meta_latency << save_meta_time_us;
    g_tablet_publish_delete_bitmap_latency << calc_delete_bitmap_time_us;
    g_tablet_publish_partial_update_latency << partial_update_write_segment_us;
    g_tablet_publish_add_inc_latency << add_inc_rowset_us;
}

EnginePublishVersionTask::EnginePublishVersionTask(
        const TPublishVersionRequest& publish_version_req, std::set<TTabletId>* error_tablet_ids,
        std::map<TTabletId, TVersion>* succ_tablets,
        std::vector<std::tuple<int64_t, int64_t, int64_t>>* discontinuous_version_tablets,
        std::map<TTableId, int64_t>* table_id_to_num_delta_rows)
        : _publish_version_req(publish_version_req),
          _error_tablet_ids(error_tablet_ids),
          _succ_tablets(succ_tablets),
          _discontinuous_version_tablets(discontinuous_version_tablets),
          _table_id_to_num_delta_rows(table_id_to_num_delta_rows) {}

void EnginePublishVersionTask::add_error_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_tablet_ids_mutex);
    _error_tablet_ids->insert(tablet_id);
}

Status EnginePublishVersionTask::finish() {
    Status res = Status::OK();
    int64_t transaction_id = _publish_version_req.transaction_id;
    OlapStopWatch watch;
    VLOG_NOTICE << "begin to process publish version. transaction_id=" << transaction_id;
    std::unique_ptr<ThreadPoolToken> token =
            StorageEngine::instance()->tablet_publish_txn_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
    std::unordered_map<int64_t, int64_t> tablet_id_to_num_delta_rows;
    // each partition
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
                          << "tablet_id=" << tablet_info.tablet_id << ", version=" << version.first
                          << ", transaction_id=" << transaction_id;
            // if rowset is null, it means this be received write task, but failed during write
            // and receive fe's publish version task
            // this be must return as an error tablet
            if (rowset == nullptr) {
                add_error_tablet_id(tablet_info.tablet_id);
                res = Status::Error<PUSH_ROWSET_NOT_FOUND>(
                        "could not find related rowset for tablet {}, txn id {}",
                        tablet_info.tablet_id, transaction_id);
                continue;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_info.tablet_id, tablet_info.tablet_uid);
            if (tablet == nullptr) {
                add_error_tablet_id(tablet_info.tablet_id);
                res = Status::Error<PUSH_TABLE_NOT_EXIST>(
                        "can't get tablet when publish version. tablet_id={}",
                        tablet_info.tablet_id);
                continue;
            }
            // in uniq key model with merge-on-write, we should see all
            // previous version when update delete bitmap, so add a check
            // here and wait pre version publish or lock timeout
            if (tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                tablet->enable_unique_key_merge_on_write()) {
                bool first_time_update = false;
                if (StorageEngine::instance()->txn_manager()->get_txn_by_tablet_version(
                            tablet_info.tablet_id, version.second) < 0) {
                    first_time_update = true;
                    StorageEngine::instance()->txn_manager()->update_tablet_version_txn(
                            tablet_info.tablet_id, version.second, transaction_id);
                }
                Version max_version;
                TabletState tablet_state;
                {
                    std::shared_lock rdlock(tablet->get_header_lock());
                    max_version = tablet->max_version_unlocked();
                    tablet_state = tablet->tablet_state();
                }
                if (tablet_state == TabletState::TABLET_RUNNING &&
                    version.first != max_version.second + 1) {
                    // If a tablet migrates out and back, the previously failed
                    // publish task may retry on the new tablet, so check
                    // whether the version exists. if not exist, then set
                    // publish failed
                    if (!tablet->check_version_exist(version)) {
                        add_error_tablet_id(tablet_info.tablet_id);
                        _discontinuous_version_tablets->emplace_back(
                                partition_id, tablet_info.tablet_id, version.first);
                        res = Status::Error<PUBLISH_VERSION_NOT_CONTINUOUS>(
                                "check_version_exist failed");
                        int64_t missed_version = max_version.second + 1;
                        int64_t missed_txn_id =
                                StorageEngine::instance()->txn_manager()->get_txn_by_tablet_version(
                                        tablet->tablet_id(), missed_version);
                        auto msg = fmt::format(
                                "uniq key with merge-on-write version not continuous, "
                                "missed version={}, it's transaction_id={}, current publish "
                                "version={}, tablet_id={}, transaction_id={}",
                                missed_version, missed_txn_id, version.second, tablet->tablet_id(),
                                _publish_version_req.transaction_id);
                        if (first_time_update) {
                            LOG(INFO) << msg;
                        } else {
                            LOG_EVERY_SECOND(INFO) << msg;
                        }
                    }
                    continue;
                }
            }

            auto rowset_meta_ptr = rowset->rowset_meta();
            tablet_id_to_num_delta_rows.insert(
                    {rowset_meta_ptr->tablet_id(), rowset_meta_ptr->num_rows()});

            auto tablet_publish_txn_ptr = std::make_shared<TabletPublishTxnTask>(
                    this, tablet, rowset, partition_id, transaction_id, version, tablet_info);
            auto submit_st = token->submit_func([=]() { tablet_publish_txn_ptr->handle(); });
            CHECK(submit_st.ok()) << submit_st;
        }
    }
    token->wait();

    _succ_tablets->clear();
    // check if the related tablet remained all have the version
    for (auto& par_ver_info : _publish_version_req.partition_version_infos) {
        int64_t partition_id = par_ver_info.partition_id;
        // get all partition related tablets and check whether the tablet have the related version
        std::set<TabletInfo> partition_related_tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_partition_related_tablets(
                partition_id, &partition_related_tablet_infos);
        Version version(par_ver_info.version, par_ver_info.version);
        for (auto& tablet_info : partition_related_tablet_infos) {
            TabletSharedPtr tablet =
                    StorageEngine::instance()->tablet_manager()->get_tablet(tablet_info.tablet_id);
            auto tablet_id = tablet_info.tablet_id;
            if (tablet == nullptr) {
                add_error_tablet_id(tablet_id);
                _succ_tablets->erase(tablet_id);
                LOG(WARNING) << "publish version failed on transaction, not found tablet. "
                             << "transaction_id=" << transaction_id << ", tablet_id=" << tablet_id
                             << ", version=" << par_ver_info.version;
            } else {
                // check if the version exist, if not exist, then set publish failed
                if (_error_tablet_ids->find(tablet_id) == _error_tablet_ids->end()) {
                    if (tablet->check_version_exist(version)) {
                        // it's better to report the max continous succ version,
                        // but it maybe time cost now.
                        // current just report 0
                        (*_succ_tablets)[tablet_id] = 0;
                    } else {
                        add_error_tablet_id(tablet_id);
                        if (res.ok()) {
                            res = Status::Error<VERSION_NOT_EXIST>(
                                    "tablet {} not exists version {}", tablet_id,
                                    par_ver_info.version);
                        }
                        LOG(WARNING) << "publish version failed on transaction, tablet version not "
                                        "exists. "
                                     << "transaction_id=" << transaction_id
                                     << ", tablet_id=" << tablet_id
                                     << ", version=" << par_ver_info.version;
                    }
                }
            }
        }
    }
    _calculate_tbl_num_delta_rows(tablet_id_to_num_delta_rows);

    if (!res.is<PUBLISH_VERSION_NOT_CONTINUOUS>()) {
        LOG(INFO) << "finish to publish version on transaction."
                  << "transaction_id=" << transaction_id
                  << ", cost(us): " << watch.get_elapse_time_us()
                  << ", error_tablet_size=" << _error_tablet_ids->size()
                  << ", res=" << res.to_string();
    }
    return res;
}

void EnginePublishVersionTask::_calculate_tbl_num_delta_rows(
        const std::unordered_map<int64_t, int64_t>& tablet_id_to_num_delta_rows) {
    for (const auto& kv : tablet_id_to_num_delta_rows) {
        auto table_id =
                StorageEngine::instance()->tablet_manager()->get_tablet(kv.first)->get_table_id();
        (*_table_id_to_num_delta_rows)[table_id] += kv.second;
    }
}

TabletPublishTxnTask::TabletPublishTxnTask(EnginePublishVersionTask* engine_task,
                                           TabletSharedPtr tablet, RowsetSharedPtr rowset,
                                           int64_t partition_id, int64_t transaction_id,
                                           Version version, const TabletInfo& tablet_info)
        : _engine_publish_version_task(engine_task),
          _tablet(tablet),
          _rowset(rowset),
          _partition_id(partition_id),
          _transaction_id(transaction_id),
          _version(version),
          _tablet_info(tablet_info) {
    _stats.submit_time_us = MonotonicMicros();
}

void TabletPublishTxnTask::handle() {
    std::unique_lock<std::mutex> rowset_update_lock(_tablet->get_rowset_update_lock(),
                                                    std::defer_lock);
    if (_tablet->enable_unique_key_merge_on_write()) {
        rowset_update_lock.lock();
    }
    _stats.schedule_time_us = MonotonicMicros() - _stats.submit_time_us;
    auto publish_status = StorageEngine::instance()->txn_manager()->publish_txn(
            _partition_id, _tablet, _transaction_id, _version, &_stats);
    if (!publish_status.ok()) {
        LOG(WARNING) << "failed to publish version. rowset_id=" << _rowset->rowset_id()
                     << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id=" << _transaction_id
                     << ", res=" << publish_status;
        _engine_publish_version_task->add_error_tablet_id(_tablet_info.tablet_id);
        return;
    }

    // add visible rowset to tablet
    int64_t t1 = MonotonicMicros();
    publish_status = _tablet->add_inc_rowset(_rowset);
    _stats.add_inc_rowset_us = MonotonicMicros() - t1;
    if (!publish_status.ok() && !publish_status.is<PUSH_VERSION_ALREADY_EXIST>()) {
        LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id=" << _rowset->rowset_id()
                     << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id=" << _transaction_id
                     << ", res=" << publish_status;
        _engine_publish_version_task->add_error_tablet_id(_tablet_info.tablet_id);
        return;
    }
    int64_t cost_us = MonotonicMicros() - _stats.submit_time_us;
    g_tablet_publish_latency << cost_us;
    _stats.record_in_bvar();
    LOG(INFO) << "publish version successfully on tablet"
              << ", table_id=" << _tablet->table_id() << ", tablet=" << _tablet->tablet_id()
              << ", transaction_id=" << _transaction_id << ", version=" << _version.first
              << ", num_rows=" << _rowset->num_rows() << ", res=" << publish_status
              << ", cost: " << cost_us << "(us) "
              << (cost_us > 500 * 1000 ? _stats.to_string() : "");
}

void AsyncTabletPublishTask::handle() {
    std::lock_guard<std::mutex> wrlock(_tablet->get_rowset_update_lock());
    _stats.schedule_time_us = MonotonicMicros() - _stats.submit_time_us;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            _transaction_id, _partition_id, &tablet_related_rs);
    auto iter = tablet_related_rs.find(TabletInfo(_tablet->tablet_id(), _tablet->tablet_uid()));
    if (iter == tablet_related_rs.end()) {
        return;
    }
    RowsetSharedPtr rowset = iter->second;
    Version version(_version, _version);
    auto publish_status = StorageEngine::instance()->txn_manager()->publish_txn(
            _partition_id, _tablet, _transaction_id, version, &_stats);
    if (!publish_status.ok()) {
        LOG(WARNING) << "failed to publish version. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _transaction_id
                     << ", res=" << publish_status;
        return;
    }

    // add visible rowset to tablet
    int64_t t1 = MonotonicMicros();
    publish_status = _tablet->add_inc_rowset(rowset);
    _stats.add_inc_rowset_us = MonotonicMicros() - t1;
    if (!publish_status.ok() && !publish_status.is<PUSH_VERSION_ALREADY_EXIST>()) {
        LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _transaction_id
                     << ", res=" << publish_status;
        return;
    }
    int64_t cost_us = MonotonicMicros() - _stats.submit_time_us;
    // print stats if publish cost > 500ms
    g_tablet_publish_latency << cost_us;
    _stats.record_in_bvar();
    LOG(INFO) << "async publish version successfully on tablet, table_id=" << _tablet->table_id()
              << ", tablet=" << _tablet->tablet_id() << ", transaction_id=" << _transaction_id
              << ", version=" << _version << ", num_rows=" << rowset->num_rows()
              << ", res=" << publish_status << ", cost: " << cost_us << "(us) "
              << (cost_us > 500 * 1000 ? _stats.to_string() : "");
}

} // namespace doris
