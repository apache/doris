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

#include <map>

#include "olap/data_dir.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/tablet_manager.h"

namespace doris {

using std::map;

EnginePublishVersionTask::EnginePublishVersionTask(TPublishVersionRequest& publish_version_req,
                                                   std::vector<TTabletId>* error_tablet_ids)
        : _publish_version_req(publish_version_req), _error_tablet_ids(error_tablet_ids) {}

Status EnginePublishVersionTask::finish() {
    Status res = Status::OK();
    int64_t transaction_id = _publish_version_req.transaction_id;
    VLOG_NOTICE << "begin to process publish version. transaction_id=" << transaction_id;

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
            Status publish_status = Status::OK();
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
                res = Status::OLAPInternalError(OLAP_ERR_PUSH_ROWSET_NOT_FOUND);
                continue;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_info.tablet_id, tablet_info.tablet_uid);
            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when publish version. tablet_id="
                             << tablet_info.tablet_id << " schema_hash=" << tablet_info.schema_hash;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = Status::OLAPInternalError(OLAP_ERR_PUSH_TABLE_NOT_EXIST);
                continue;
            }

            publish_status = StorageEngine::instance()->txn_manager()->publish_txn(
                    partition_id, tablet, transaction_id, version);
            if (publish_status != Status::OK()) {
                LOG(WARNING) << "failed to publish version. rowset_id=" << rowset->rowset_id()
                             << ", tablet_id=" << tablet_info.tablet_id
                             << ", txn_id=" << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }

            // add visible rowset to tablet
            publish_status = tablet->add_inc_rowset(rowset);
            if (publish_status != Status::OK() &&
                publish_status != Status::OLAPInternalError(OLAP_ERR_PUSH_VERSION_ALREADY_EXIST)) {
                LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id="
                             << rowset->rowset_id() << ", tablet_id=" << tablet_info.tablet_id
                             << ", txn_id=" << transaction_id << ", res=" << publish_status;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }
            partition_related_tablet_infos.erase(tablet_info);
            VLOG_NOTICE << "publish version successfully on tablet. tablet=" << tablet->full_name()
                        << ", transaction_id=" << transaction_id << ", version=" << version.first
                        << ", res=" << publish_status;
        }

        // check if the related tablet remained all have the version
        for (auto& tablet_info : partition_related_tablet_infos) {
            // has to use strict mode to check if check all tablets
            if (!_publish_version_req.strict_mode) {
                break;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                    tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet == nullptr) {
                _error_tablet_ids->push_back(tablet_info.tablet_id);
            } else {
                // check if the version exist, if not exist, then set publish failed
                if (!tablet->check_version_exist(version)) {
                    _error_tablet_ids->push_back(tablet_info.tablet_id);
                }
            }
        }
    }

    LOG(INFO) << "finish to publish version on transaction."
              << "transaction_id=" << transaction_id
              << ", error_tablet_size=" << _error_tablet_ids->size();
    return res;
}

} // namespace doris
