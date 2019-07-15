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

#ifndef DORIS_BE_SRC_OLAP_TXN_MANAGER_H
#define DORIS_BE_SRC_OLAP_TXN_MANAGER_H

#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <condition_variable>
#include <set>
#include <string>
#include <vector>
#include <thread>

#include <rapidjson/document.h>
#include <pthread.h>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/atomic.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

struct TabletTxnInfo {
    PUniqueId load_id;
    RowsetSharedPtr rowset;
    int64_t creation_time;

    TabletTxnInfo(
        PUniqueId load_id,
        RowsetSharedPtr rowset) :
        load_id(load_id),
        rowset(rowset),
        creation_time(time(nullptr)) {}

    TabletTxnInfo() {}
};

// txn manager is used to manage mapping between tablet and txns
class TxnManager {
public:
    TxnManager();

    ~TxnManager() {
        _txn_tablet_map.clear();
        _txn_locks.clear();
    }
    // add a txn to manager
    // partition id is useful in publish version stage because version is associated with partition
    OLAPStatus prepare_txn(TPartitionId partition_id, TTransactionId transaction_id,
                           TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, 
                           const PUniqueId& load_id);
    
    OLAPStatus commit_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                          TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, 
                          const PUniqueId& load_id, RowsetSharedPtr rowset_ptr, 
                          bool is_recovery);
    
    // remove a txn from txn manager
    // not persist rowset meta because 
    OLAPStatus publish_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                           TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, 
                           Version& version, VersionHash& version_hash);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    OLAPStatus rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                            TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);

    // remove the txn from txn manager
    // delete the related rowset if it is not null
    // delete rowset related data if it is not null
    OLAPStatus delete_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                          TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);

    void get_tablet_related_txns(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, int64_t* partition_id,
                                std::set<int64_t>* transaction_ids);

    void get_txn_related_tablets(const TTransactionId transaction_id,
                                 TPartitionId partition_ids,
                                 std::map<TabletInfo, RowsetSharedPtr>* tablet_infos);
    
    void get_all_related_tablets(std::set<TabletInfo>* tablet_infos);

    // just check if the txn exists
    bool has_txn(TPartitionId partition_id, TTransactionId transaction_id,
                 TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);

    bool get_expire_txns(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, std::vector<int64_t>* transaction_ids);

    void force_rollback_tablet_related_txns(OlapMeta* meta, TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);
    
private:
    RWMutex* _get_txn_lock(TTransactionId txn_id) {
        return _txn_locks[txn_id % _txn_lock_num].get();
    }

private:
    RWMutex _txn_map_lock;
    using TxnKey = std::pair<int64_t, int64_t>; // partition_id, transaction_id;
    std::map<TxnKey, std::map<TabletInfo, TabletTxnInfo>> _txn_tablet_map;

    const int32_t _txn_lock_num = 100;
    std::map<int32_t, std::shared_ptr<RWMutex>> _txn_locks;

    DISALLOW_COPY_AND_ASSIGN(TxnManager);
};  // TxnManager

}
#endif // DORIS_BE_SRC_OLAP_TXN_MANAGER_H
