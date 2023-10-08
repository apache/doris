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

#pragma once

#include <butil/macros.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <boost/container/detail/std_fwd.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
class DeltaWriter;
class OlapMeta;
struct TabletPublishStatistics;

struct TabletTxnInfo {
    PUniqueId load_id;
    RowsetSharedPtr rowset;
    bool unique_key_merge_on_write {false};
    DeleteBitmapPtr delete_bitmap;
    // records rowsets calc in commit txn
    RowsetIdUnorderedSet rowset_ids;
    int64_t creation_time;
    bool ingest {false};
    std::shared_ptr<PartialUpdateInfo> partial_update_info;

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset)
            : load_id(load_id), rowset(rowset), creation_time(UnixSeconds()) {}

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset, bool ingest_arg)
            : load_id(load_id), rowset(rowset), creation_time(UnixSeconds()), ingest(ingest_arg) {}

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset, bool merge_on_write,
                  DeleteBitmapPtr delete_bitmap, const RowsetIdUnorderedSet& ids)
            : load_id(load_id),
              rowset(rowset),
              unique_key_merge_on_write(merge_on_write),
              delete_bitmap(delete_bitmap),
              rowset_ids(ids),
              creation_time(UnixSeconds()) {}

    TabletTxnInfo() {}
};

struct CommitTabletTxnInfo {
    CommitTabletTxnInfo(TPartitionId partition_id, TTransactionId transaction_id,
                        DeleteBitmapPtr delete_bitmap, RowsetIdUnorderedSet rowset_ids)
            : transaction_id(transaction_id),
              partition_id(partition_id),
              delete_bitmap(delete_bitmap),
              rowset_ids(rowset_ids) {}
    TTransactionId transaction_id;
    TPartitionId partition_id;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
};

using CommitTabletTxnInfoVec = std::vector<CommitTabletTxnInfo>;

// txn manager is used to manage mapping between tablet and txns
class TxnManager {
public:
    TxnManager(int32_t txn_map_shard_size, int32_t txn_shard_size);

    ~TxnManager() {
        delete[] _txn_tablet_maps;
        delete[] _txn_partition_maps;
        delete[] _txn_map_locks;
        delete[] _txn_mutex;
        delete[] _txn_tablet_delta_writer_map;
        delete[] _txn_tablet_delta_writer_map_locks;
        delete _tablet_version_cache;
    }

    // add a txn to manager
    // partition id is useful in publish version stage because version is associated with partition
    Status prepare_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                       TTransactionId transaction_id, const PUniqueId& load_id,
                       bool is_ingest = false);
    // most used for ut
    Status prepare_txn(TPartitionId partition_id, TTransactionId transaction_id,
                       TTabletId tablet_id, TabletUid tablet_uid, const PUniqueId& load_id,
                       bool is_ingest = false);

    Status commit_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                      TTransactionId transaction_id, const PUniqueId& load_id,
                      const RowsetSharedPtr& rowset_ptr, bool is_recovery,
                      std::shared_ptr<PartialUpdateInfo> partial_update_info);

    Status publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                       TTransactionId transaction_id, const Version& version,
                       TabletPublishStatistics* stats);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    Status rollback_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                        TTransactionId transaction_id);

    Status delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                      TTransactionId transaction_id);

    Status commit_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                      TTabletId tablet_id, TabletUid tablet_uid, const PUniqueId& load_id,
                      const RowsetSharedPtr& rowset_ptr, bool is_recovery,
                      std::shared_ptr<PartialUpdateInfo> partial_update_info);

    // remove a txn from txn manager
    // not persist rowset meta because
    Status publish_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                       TTabletId tablet_id, TabletUid tablet_uid, const Version& version,
                       TabletPublishStatistics* stats);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    Status rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                        TTabletId tablet_id, TabletUid tablet_uid);

    // remove the txn from txn manager
    // delete the related rowset if it is not null
    // delete rowset related data if it is not null
    Status delete_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                      TTabletId tablet_id, TabletUid tablet_uid);

    void get_tablet_related_txns(TTabletId tablet_id, TabletUid tablet_uid, int64_t* partition_id,
                                 std::set<int64_t>* transaction_ids);

    void get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_ids,
                                 std::map<TabletInfo, RowsetSharedPtr>* tablet_infos);

    void get_all_related_tablets(std::set<TabletInfo>* tablet_infos);

    // Just check if the txn exists.
    bool has_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                 TabletUid tablet_uid);

    // Get all expired txns and save them in expire_txn_map.
    // This is currently called before reporting all tablet info, to avoid iterating txn map for every tablets.
    void build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map);

    void force_rollback_tablet_related_txns(OlapMeta* meta, TTabletId tablet_id,
                                            TabletUid tablet_uid);

    void get_partition_ids(const TTransactionId transaction_id,
                           std::vector<TPartitionId>* partition_ids);

    void add_txn_tablet_delta_writer(int64_t transaction_id, int64_t tablet_id,
                                     DeltaWriter* delta_writer);
    void clear_txn_tablet_delta_writer(int64_t transaction_id);
    void finish_slave_tablet_pull_rowset(int64_t transaction_id, int64_t tablet_id, int64_t node_id,
                                         bool is_succeed);

    void set_txn_related_delete_bitmap(TPartitionId partition_id, TTransactionId transaction_id,
                                       TTabletId tablet_id, TabletUid tablet_uid,
                                       bool unique_key_merge_on_write,
                                       DeleteBitmapPtr delete_bitmap,
                                       const RowsetIdUnorderedSet& rowset_ids);
    void get_all_commit_tablet_txn_info_by_tablet(
            const TabletSharedPtr& tablet, CommitTabletTxnInfoVec* commit_tablet_txn_info_vec);

    int64_t get_txn_by_tablet_version(int64_t tablet_id, int64_t version);
    void update_tablet_version_txn(int64_t tablet_id, int64_t version, int64_t txn_id);

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

    using txn_tablet_map_t = std::unordered_map<TxnKey, std::map<TabletInfo, TabletTxnInfo>,
                                                TxnKeyHash, TxnKeyEqual>;
    using txn_partition_map_t = std::unordered_map<int64_t, std::unordered_set<int64_t>>;
    using txn_tablet_delta_writer_map_t =
            std::unordered_map<int64_t, std::map<int64_t, DeltaWriter*>>;

    std::shared_mutex& _get_txn_map_lock(TTransactionId transactionId);

    txn_tablet_map_t& _get_txn_tablet_map(TTransactionId transactionId);

    txn_partition_map_t& _get_txn_partition_map(TTransactionId transactionId);

    inline std::shared_mutex& _get_txn_lock(TTransactionId transactionId);

    std::shared_mutex& _get_txn_tablet_delta_writer_map_lock(TTransactionId transactionId);

    txn_tablet_delta_writer_map_t& _get_txn_tablet_delta_writer_map(TTransactionId transactionId);

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

    std::shared_mutex* _txn_mutex;

    txn_tablet_delta_writer_map_t* _txn_tablet_delta_writer_map;
    ShardedLRUCache* _tablet_version_cache;
    std::shared_mutex* _txn_tablet_delta_writer_map_locks;
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

inline std::shared_mutex& TxnManager::_get_txn_lock(TTransactionId transactionId) {
    return _txn_mutex[transactionId & (_txn_shard_size - 1)];
}

inline std::shared_mutex& TxnManager::_get_txn_tablet_delta_writer_map_lock(
        TTransactionId transactionId) {
    return _txn_tablet_delta_writer_map_locks[transactionId & (_txn_map_shard_size - 1)];
}

inline TxnManager::txn_tablet_delta_writer_map_t& TxnManager::_get_txn_tablet_delta_writer_map(
        TTransactionId transactionId) {
    return _txn_tablet_delta_writer_map[transactionId & (_txn_map_shard_size - 1)];
}

} // namespace doris
