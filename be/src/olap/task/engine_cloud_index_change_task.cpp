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

#include "olap/task/engine_cloud_index_change_task.h"

#include "cloud/cloud_index_change_compaction.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cpp/sync_point.h"
#include "olap/tablet_manager.h"

namespace doris {

EngineCloudIndexChangeTask::EngineCloudIndexChangeTask(CloudStorageEngine& engine,
                                                       const TAlterInvertedIndexReq& request)
        : _engine(engine),
          _index_list(request.indexes_desc),
          _columns(request.columns),
          _tablet_id(request.tablet_id),
          _schema_version(request.schema_version) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::SCHEMA_CHANGE,
            fmt::format("EngineCloudIndexChangeTask#tabletId={}", std::to_string(_tablet_id)),
            engine.memory_limitation_bytes_per_thread_for_schema_change());
}

EngineCloudIndexChangeTask::~EngineCloudIndexChangeTask() = default;

Result<std::shared_ptr<CloudTablet>> EngineCloudIndexChangeTask::_get_tablet() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("EngineCloudIndexChangeTask::_get_tablet",
                                      Result<std::shared_ptr<CloudTablet>>(nullptr));
    return _engine.tablet_mgr().get_tablet(_tablet_id);
}

Status EngineCloudIndexChangeTask::execute() {
    int64_t begin_time = MonotonicSeconds();
    std::string tablet_id_str = " tableid:" + std::to_string(_tablet_id);
    // get tablet
    CloudTabletSPtr tablet = DORIS_TRY(_get_tablet());
    if (tablet == nullptr) {
        LOG(WARNING) << "[index_change]tablet: " << _tablet_id << " not exist";
        return Status::InternalError("tablet not exist, tablet_id={}.", _tablet_id);
    }
    RETURN_IF_ERROR(tablet->sync_rowsets());
    RETURN_IF_ERROR(tablet->check_rowset_schema_for_build_index(_columns, _schema_version));

    while (true) {
        int64_t time_cost = MonotonicSeconds() - begin_time;
        if (time_cost > config::cloud_index_change_task_timeout_second) {
            return Status::InternalError("index change compaction timeout, tablet_id={}.",
                                         _tablet_id);
        }

        // get tablet
        CloudTabletSPtr tablet = DORIS_TRY(_get_tablet());
        if (tablet == nullptr) {
            LOG(WARNING) << "[index_change]tablet: " << _tablet_id << " not exist";
            return Status::InternalError("tablet not exist, tablet_id={}.", _tablet_id);
        }

        // pre check to determine whether this round of iteration is base compaction or cumu compaction.
        bool is_current_iter_base_compact = false;
        RETURN_IF_ERROR(tablet->sync_rowsets());
        auto pre_input_rowset = DORIS_TRY(tablet->pick_a_rowset_for_index_change(
                _schema_version, is_current_iter_base_compact));
        if (pre_input_rowset == nullptr) {
            LOG(INFO) << "[index_change]there are no rowsets need to do index change, task finish."
                      << tablet_id_str << ";"
                      << "sc version:" << _schema_version;
            return Status::OK();
        }

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(_engine, tablet, _schema_version,
                                                             _index_list, _columns);

        Defer defer {[&]() {
            _engine.unregister_index_change_compaction(_tablet_id, is_current_iter_base_compact);
            VLOG_DEBUG << "[index_change] unregister compaction , " << tablet_id_str;
        }};

        std::string err_msg;
        bool is_register_succ = _engine.register_index_change_compaction(
                index_change_compact, _tablet_id, is_current_iter_base_compact, err_msg);
        if (!is_register_succ) {
            LOG_EVERY_T(INFO, 60) << "[index_change]register index change compaction failed,"
                                  << tablet_id_str << ", reason:" << err_msg;
            sleep(30);
            continue;
        }

        VLOG_DEBUG << "[index_change] begin prepare index change compact, " << tablet_id_str;
        Status prepare_ret = index_change_compact->prepare_compact();
        if (!prepare_ret.ok()) {
            LOG(WARNING) << "[index_change] prepare index compact failed, " << tablet_id_str
                         << ", err reason:" << prepare_ret.to_string_no_stack();
            return prepare_ret;
        }

        if (index_change_compact->is_finish_index_change()) {
            LOG(INFO) << "[index_change] index change task finish." << tablet_id_str;
            return Status::OK();
        }

        // if pre check type is not same with prepare result, it can retry at once;
        // because this case should rarely happen.
        bool could_continue_execution =
                (is_current_iter_base_compact && index_change_compact->is_base_compaction()) ||
                (!is_current_iter_base_compact && !index_change_compact->is_base_compaction());
        if (!could_continue_execution) {
            LOG_EVERY_T(INFO, 10) << "[index_change] pre rowset type not match real rowset type."
                                  << tablet_id_str;
            continue;
        }

        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        if (!ret.ok()) {
            // if request lock failed because of stale rowset, we can sync rowsets and retry.
            if (should_skip_err) {
                continue;
            } else {
                LOG(WARNING) << "[index_change] request global lock failed." << tablet_id_str;
                return ret;
            }
        }

        VLOG_DEBUG << "[index_change] begin execute index change compact." << tablet_id_str;
        Status exec_ret = index_change_compact->execute_compact();
        if (!exec_ret.ok()) {
            LOG(WARNING) << "[index_change] exec index change compaction failed." << tablet_id_str;
            return exec_ret;
        }
        VLOG_DEBUG << "[index_change] exec compaction succ." << tablet_id_str;
    }

    return Status::OK();
}
} // namespace doris