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
#include "olap/data_dir.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/tablet_manager.h"
#include <map>

namespace doris {

using std::map;

EnginePublishVersionTask::EnginePublishVersionTask(TPublishVersionRequest& publish_version_req, 
                                                   vector<TTabletId>* error_tablet_ids)
    : _publish_version_req(publish_version_req), 
      _error_tablet_ids(error_tablet_ids) {}

OLAPStatus EnginePublishVersionTask::finish() {
    LOG(INFO) << "begin to process publish version. transaction_id="
              << _publish_version_req.transaction_id;

    int64_t transaction_id = _publish_version_req.transaction_id;
    OLAPStatus res = OLAP_SUCCESS;

    // each partition
    for (auto& partitionVersionInfo
         : _publish_version_req.partition_version_infos) {

        int64_t partition_id = partitionVersionInfo.partition_id;
        map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(transaction_id, partition_id, &tablet_related_rs);

        Version version(partitionVersionInfo.version, partitionVersionInfo.version);
        VersionHash version_hash = partitionVersionInfo.version_hash;

        // each tablet
        for (auto& tablet_rs : tablet_related_rs) {
            OLAPStatus publish_status = OLAP_SUCCESS;
            TabletInfo tablet_info = tablet_rs.first;
            RowsetSharedPtr rowset = tablet_rs.second;
            LOG(INFO) << "begin to publish version on tablet. "
                    << "tablet_id=" << tablet_info.tablet_id
                    << ", schema_hash=" << tablet_info.schema_hash
                    << ", version=" << version.first
                    << ", version_hash=" << version_hash
                    << ", transaction_id=" << transaction_id;
            // if rowset is null, it means this be received write task, but failed during write
            // and receive fe's publish version task
            // this be must return as an error tablet
            if (rowset == nullptr) {
                LOG(WARNING) << "could not find related rowset for tablet " << tablet_info.tablet_id
                             << " txn id " << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_ROWSET_NOT_FOUND;
                continue;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_info.tablet_id, 
                tablet_info.schema_hash, tablet_info.tablet_uid);

            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when publish version. tablet_id=" << tablet_info.tablet_id
                             << " schema_hash=" << tablet_info.schema_hash;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_TABLE_NOT_EXIST;
                continue;
            }

            publish_status = StorageEngine::instance()->txn_manager()->publish_txn(tablet->data_dir()->get_meta(), 
                partition_id, 
                transaction_id, tablet_info.tablet_id, tablet_info.schema_hash, tablet_info.tablet_uid, 
                version, version_hash);
            
            if (publish_status != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to publish for rowset_id:" << rowset->rowset_id()
                             << "tablet id: " << tablet_info.tablet_id
                             << "txn id:" << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }
            // add visible rowset to tablet
            publish_status = tablet->add_inc_rowset(rowset);
            if (publish_status != OLAP_SUCCESS && publish_status != OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
                LOG(WARNING) << "add visible rowset to tablet failed rowset_id:" << rowset->rowset_id()
                             << "tablet id: " << tablet_info.tablet_id
                             << "txn id:" << transaction_id
                             << "res:" << publish_status;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }
            if (publish_status == OLAP_SUCCESS || publish_status == OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
                LOG(INFO) << "publish version successfully on tablet. tablet=" << tablet->full_name()
                          << ", transaction_id=" << transaction_id << ", version=" << version.first
                          << ", res=" << publish_status;
                // delete rowset from meta env, because add inc rowset alreay saved the rowset meta to tablet meta
                RowsetMetaManager::remove(tablet->data_dir()->get_meta(), tablet->tablet_uid(), rowset->rowset_id());
                // delete txn info
            } else {
                LOG(WARNING) << "fail to publish version on tablet. tablet=" << tablet->full_name().c_str()
                             << "transaction_id=" << transaction_id
                             << "version=" <<  version.first
                             << " res=" << publish_status;
                _error_tablet_ids->push_back(tablet->tablet_id());
                res = publish_status;
            }
        }
    }

    LOG(INFO) << "finish to publish version on transaction."
              << "transaction_id=" << transaction_id
              << ", error_tablet_size=" << _error_tablet_ids->size();
    return res;
}

} // doris
