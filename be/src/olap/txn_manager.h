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

namespace doris {

// txn manager is used to manage mapping between tablet and txns
class TxnManager {
public:
    TxnManager() {}
    ~TxnManager() {
        _transaction_tablet_map.clear();
    }
    // add a txn to manager
    // partition id is useful in publish version stage because version is associated with partition
    OLAPStatus add_txn(TPartitionId partition_id, TTransactionId transaction_id,
                               TTabletId tablet_id, SchemaHash schema_hash,
                               const PUniqueId& load_id);

    OLAPStatus delete_txn(TPartitionId partition_id, TTransactionId transaction_id,
                            TTabletId tablet_id, SchemaHash schema_hash);

    void get_tablet_related_txns(TabletSharedPtr tablet, int64_t* partition_id,
                                    std::set<int64_t>* transaction_ids);

    void get_txn_related_tablets(const TTransactionId transaction_id,
                                TPartitionId partition_ids,
                                std::vector<TabletInfo>* tablet_infos);

    bool has_txn(TPartitionId partition_id, TTransactionId transaction_id,
                         TTabletId tablet_id, SchemaHash schema_hash);

private:
    RWMutex _txn_map_lock;
    using TxnKey = std::pair<int64_t, int64_t>; // partition_id, transaction_id;
    std::map<TxnKey, std::map<TabletInfo, std::vector<PUniqueId>>> _transaction_tablet_map;
};  // TxnManager

}
#endif // DORIS_BE_SRC_OLAP_TXN_MANAGER_H