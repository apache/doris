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

#include <pthread.h>
#include <rapidjson/document.h>

#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "util/time.h"

namespace doris {

struct TabletTxnInfo {
    PUniqueId load_id;
    RowsetSharedPtr rowset;
    int64_t creation_time;

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset)
            : load_id(load_id), rowset(rowset), creation_time(UnixSeconds()) {}

    TabletTxnInfo() {}
};

// txn manager is used to manage mapping between tablet and txns
class TxnManager {
public:
    TxnManager(int32_t txn_map_shard_size, int32_t txn_shard_size);

    ~TxnManager() {
        delete[] _txn_tablet_maps;
        delete[] _txn_partition_maps;
        delete[] _txn_map_locks;
        delete[] _txn_mutex;
    }

    OLAPStatus prepare_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                           TTransactionId transaction_id, const PUniqueId& load_id);

    OLAPStatus commit_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                          TTransactionId transaction_id, const PUniqueId& load_id,
                          const RowsetSharedPtr& rowset_ptr, bool is_recovery);

    OLAPStatus publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                           TTransactionId transaction_id, const Version& version);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    OLAPStatus rollback_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                            TTransactionId transaction_id);

    OLAPStatus delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                          TTransactionId transaction_id);

    // add a txn to manager
    // partition id is useful in publish version stage because version is associated with partition
    OLAPStatus prepare_txn(TPartitionId partition_id, TTransactionId transaction_id,
                           TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
                           const PUniqueId& load_id);

    OLAPStatus commit_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                          TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
                          const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr,
                          bool is_recovery);

    // remove a txn from txn manager
    // not persist rowset meta because
    OLAPStatus publish_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                           TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
                           const Version& version);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    OLAPStatus rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                            TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);

    // remove the txn from txn manager
    // delete the related rowset if it is not null
    // delete rowset related data if it is not null
    OLAPStatus delete_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                          TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid);

    void get_tablet_related_txns(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
                                 int64_t* partition_id, std::set<int64_t>* transaction_ids);

    void get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_ids,
                                 std::map<TabletInfo, RowsetSharedPtr>* tablet_infos);

    void get_all_related_tablets(std::set<TabletInfo>* tablet_infos);

    // Just check if the txn exists.
    bool has_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                 SchemaHash schema_hash, TabletUid tablet_uid);

    // Get all expired txns and save them in expire_txn_map.
    // This is currently called before reporting all tablet info, to avoid iterating txn map for every tablets.
    void build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map);

    void force_rollback_tablet_related_txns(OlapMeta* meta, TTabletId tablet_id,
                                            SchemaHash schema_hash, TabletUid tablet_uid);

    void get_partition_ids(const TTransactionId transaction_id,
                           std::vector<TPartitionId>* partition_ids);

private:
    using TxnKey = std::pair<int64_t, int64_t>; // partition_id, transaction_id;

    // Implement TxnKey hash function to support TxnKey as a key for `unordered_map`.
    struct TxnKeyHash {
        template <typename T, typename U>
        size_t operator()(const std::pair<T, U>& e) const {
            return std::hash<T>()(e.first) ^ std::hash<U>()(e.second);
        }
    };

    // Implement TxnKey equal function to support TxnKey as a key for `unordered_map`.
    struct TxnKeyEqual {
        template <class T, typename U>
        bool operator()(const std::pair<T, U>& l, const std::pair<T, U>& r) const {
            return l.first == r.first && l.second == r.second;
        }
    };

    typedef std::unordered_map<TxnKey, std::map<TabletInfo, TabletTxnInfo>, TxnKeyHash, TxnKeyEqual>
            txn_tablet_map_t;
    typedef std::unordered_map<int64_t, std::unordered_set<int64_t>> txn_partition_map_t;

    inline std::shared_mutex& _get_txn_map_lock(TTransactionId transactionId);

    inline txn_tablet_map_t& _get_txn_tablet_map(TTransactionId transactionId);

    inline txn_partition_map_t& _get_txn_partition_map(TTransactionId transactionId);

    inline Mutex& _get_txn_lock(TTransactionId transactionId);

    // Insert or remove (transaction_id, partition_id) from _txn_partition_map
    // get _txn_map_lock before calling.
    void _insert_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id);
    void _clear_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id);

private:
    const int32_t _txn_map_shard_size;

    const int32_t _txn_shard_size;

    // _txn_map_locks[i] protect _txn_tablet_maps[i], i=0,1,2...,and i < _txn_map_shard_size
    txn_tablet_map_t* _txn_tablet_maps;
    // transaction_id -> corresponding partition ids
    // This is mainly for the clear txn task received from FE, which may only has transaction id,
    // so we need this map to find out which partitions are corresponding to a transaction id.
    // The _txn_partition_maps[i] should be constructed/deconstructed/modified alongside with '_txn_tablet_maps[i]'
    txn_partition_map_t* _txn_partition_maps;

    std::shared_mutex* _txn_map_locks;

    Mutex* _txn_mutex;
    DISALLOW_COPY_AND_ASSIGN(TxnManager);
}; // TxnManager

inline std::shared_mutex& TxnManager::_get_txn_map_lock(TTransactionId transactionId) {
    return _txn_map_locks[transactionId & (_txn_map_shard_size - 1)];
}

inline TxnManager::txn_tablet_map_t& TxnManager::_get_txn_tablet_map(TTransactionId transactionId) {
    return _txn_tablet_maps[transactionId & (_txn_map_shard_size - 1)];
}

inline TxnManager::txn_partition_map_t& TxnManager::_get_txn_partition_map(
        TTransactionId transactionId) {
    return _txn_partition_maps[transactionId & (_txn_map_shard_size - 1)];
}

inline Mutex& TxnManager::_get_txn_lock(TTransactionId transactionId) {
    return _txn_mutex[transactionId & (_txn_shard_size - 1)];
}

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_TXN_MANAGER_H
