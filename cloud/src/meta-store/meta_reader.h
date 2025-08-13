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

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"

namespace doris::cloud {

// A versioned meta reader that encapsulates the logic to read versioned metadata.
//
// This class is lightweight and does not hold any state. So constructing multiple instances
// is cheap and does not require any cleanup.
//
// But the caller should ensure that the referenced instance_id and txn_kv are valid
// throughout the lifetime of the MetaReader instance.
class MetaReader {
public:
    MetaReader(std::string_view instance_id) : MetaReader(instance_id, nullptr) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv)
            : MetaReader(instance_id, txn_kv, Versionstamp::max(), false) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, Versionstamp snapshot_version)
            : MetaReader(instance_id, txn_kv, snapshot_version, false) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, bool snapshot)
            : MetaReader(instance_id, txn_kv, Versionstamp::max(), snapshot) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, Versionstamp snapshot_version,
               bool snapshot)
            : instance_id_(instance_id),
              snapshot_version_(snapshot_version),
              snapshot_(snapshot),
              txn_kv_(txn_kv) {}
    MetaReader(const MetaReader&) = delete;
    MetaReader& operator=(const MetaReader&) = delete;

    // Get the version of the table_version_key with the given table_id.
    TxnErrorCode get_table_version(int64_t table_id, Versionstamp* table_version, bool snapshot);
    TxnErrorCode get_table_version(Transaction* txn, int64_t table_id, Versionstamp* table_version,
                                   bool snapshot);
    TxnErrorCode get_table_version(int64_t table_id, Versionstamp* table_version) {
        return get_table_version(table_id, table_version, snapshot_);
    }
    TxnErrorCode get_table_version(Transaction* txn, int64_t table_id,
                                   Versionstamp* table_version) {
        return get_table_version(txn, table_id, table_version, snapshot_);
    }

    // Get the versions of the table_version_key with the given table_ids.
    //
    // The table versions are returned in the `table_versions` map, where the key is the table_id
    // and the value is the versionstamp. If a tablet_id does not exists, then it will not be
    // included in the map.
    TxnErrorCode get_table_versions(const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions,
                                    bool snapshot);
    TxnErrorCode get_table_versions(Transaction* txn, const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions,
                                    bool snapshot);
    TxnErrorCode get_table_versions(const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions) {
        return get_table_versions(table_ids, table_versions, snapshot_);
    }
    TxnErrorCode get_table_versions(Transaction* txn, const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions) {
        return get_table_versions(txn, table_ids, table_versions, snapshot_);
    }

    // Get the partition version for the given partition
    //
    // If the `version` is not nullptr, it will be filled with the deserialized VersionPB.
    TxnErrorCode get_partition_version(int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_partition_version(Transaction* txn, int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_partition_version(int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp) {
        return get_partition_version(partition_id, version, versionstamp, snapshot_);
    }
    TxnErrorCode get_partition_version(Transaction* txn, int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp) {
        return get_partition_version(txn, partition_id, version, versionstamp, snapshot_);
    }

    // Get the partition versions and versionstamps for the given partition_ids.
    //
    // Only the set parameters will be filled:
    // - If `versions` is not nullptr, it will be filled with the deserialized VersionPB for each partition_id.
    // - If `versionstamps` is not nullptr, it will be filled with the Versionstamp for each partition_id.
    //
    // If a partition_id does not exists, then it will not be included in the respective map.
    TxnErrorCode get_partition_versions(const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, VersionPB>* versions,
                                        std::unordered_map<int64_t, Versionstamp>* versionstamps,
                                        bool snapshot);
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, VersionPB>* versions,
                                        std::unordered_map<int64_t, Versionstamp>* versionstamps,
                                        bool snapshot);
    TxnErrorCode get_partition_versions(const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, VersionPB>* versions,
                                        std::unordered_map<int64_t, Versionstamp>* versionstamps) {
        return get_partition_versions(partition_ids, versions, versionstamps, snapshot_);
    }
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, VersionPB>* versions,
                                        std::unordered_map<int64_t, Versionstamp>* versionstamps) {
        return get_partition_versions(txn, partition_ids, versions, versionstamps, snapshot_);
    }

    // Get the partition versions from the given partition_ids.
    // If the partition id is not found, it will be set to 1 in the versions map.
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, int64_t>* versions,
                                        int64_t* last_pending_txn_id, bool snapshot);
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, int64_t>* versions,
                                        int64_t* last_pending_txn_id) {
        return get_partition_versions(txn, partition_ids, versions, last_pending_txn_id, snapshot_);
    }

    // Get the tablet load stats for the given tablet
    //
    // If the `tablet_stats` is not nullptr, it will be filled with the deserialized TabletStatsPB.
    TxnErrorCode get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                       Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                       TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                       bool snapshot);
    TxnErrorCode get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                       Versionstamp* versionstamp) {
        return get_tablet_load_stats(tablet_id, tablet_stats, versionstamp, snapshot_);
    }
    TxnErrorCode get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                       TabletStatsPB* tablet_stats, Versionstamp* versionstamp) {
        return get_tablet_load_stats(txn, tablet_id, tablet_stats, versionstamp, snapshot_);
    }

    // Get the tablet compact stats for the given tablet
    //
    // If the `tablet_stats` is not nullptr, it will be filled with the deserialized TabletStatsPB.
    TxnErrorCode get_tablet_compact_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                          Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_tablet_compact_stats(Transaction* txn, int64_t tablet_id,
                                          TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                          bool snapshot);
    TxnErrorCode get_tablet_compact_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                          Versionstamp* versionstamp) {
        return get_tablet_compact_stats(tablet_id, tablet_stats, versionstamp, snapshot_);
    }
    TxnErrorCode get_tablet_compact_stats(Transaction* txn, int64_t tablet_id,
                                          TabletStatsPB* tablet_stats, Versionstamp* versionstamp) {
        return get_tablet_compact_stats(txn, tablet_id, tablet_stats, versionstamp, snapshot_);
    }

    // Get the merged (load, compact) tablet stats for the given tablet.
    //
    // The `tablet_stats` will be filled with the merged TabletStatsPB.
    // The `versionstamp` will be set to the latest versionstamp of the load or compact stats.
    TxnErrorCode get_tablet_merged_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                         Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_tablet_merged_stats(Transaction* txn, int64_t tablet_id,
                                         TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                         bool snapshot);
    TxnErrorCode get_tablet_merged_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                         Versionstamp* versionstamp) {
        return get_tablet_merged_stats(tablet_id, tablet_stats, versionstamp, snapshot_);
    }
    TxnErrorCode get_tablet_merged_stats(Transaction* txn, int64_t tablet_id,
                                         TabletStatsPB* tablet_stats, Versionstamp* versionstamp) {
        return get_tablet_merged_stats(txn, tablet_id, tablet_stats, versionstamp, snapshot_);
    }

    // Get the tablet index for the given tablet_id.
    TxnErrorCode get_tablet_index(int64_t tablet_id, TabletIndexPB* tablet_index, bool snapshot);
    TxnErrorCode get_tablet_index(Transaction* txn, int64_t tablet_id, TabletIndexPB* tablet_index,
                                  bool snapshot);
    TxnErrorCode get_tablet_index(int64_t tablet_id, TabletIndexPB* tablet_index) {
        return get_tablet_index(tablet_id, tablet_index, snapshot_);
    }
    TxnErrorCode get_tablet_index(Transaction* txn, int64_t tablet_id,
                                  TabletIndexPB* tablet_index) {
        return get_tablet_index(txn, tablet_id, tablet_index, snapshot_);
    }

    // Get the tablet indexes for the given tablet_ids.
    //
    // If a tablet_id does not exists, then it will not be included in the respective map.
    TxnErrorCode get_tablet_indexes(const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes,
                                    bool snapshot);
    TxnErrorCode get_tablet_indexes(Transaction* txn, const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes,
                                    bool snapshot);
    TxnErrorCode get_tablet_indexes(const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes) {
        return get_tablet_indexes(tablet_ids, tablet_indexes, snapshot_);
    }
    TxnErrorCode get_tablet_indexes(Transaction* txn, const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes) {
        return get_tablet_indexes(txn, tablet_ids, tablet_indexes, snapshot_);
    }

    // Get the rowset meta for the given tablet_id and version range.
    //
    // The `rowset_metas` will be filled with the RowsetMetaCloudPB for each version in the range,
    // in ascending order.
    TxnErrorCode get_rowset_metas(int64_t tablet_id, int64_t start_version, int64_t end_version,
                                  std::vector<RowsetMetaCloudPB>* rowset_metas, bool snapshot);
    TxnErrorCode get_rowset_metas(Transaction* txn, int64_t tablet_id, int64_t start_version,
                                  int64_t end_version, std::vector<RowsetMetaCloudPB>* rowset_metas,
                                  bool snapshot);
    TxnErrorCode get_rowset_metas(int64_t tablet_id, int64_t start_version, int64_t end_version,
                                  std::vector<RowsetMetaCloudPB>* rowset_metas) {
        return get_rowset_metas(tablet_id, start_version, end_version, rowset_metas, snapshot_);
    }
    TxnErrorCode get_rowset_metas(Transaction* txn, int64_t tablet_id, int64_t start_version,
                                  int64_t end_version,
                                  std::vector<RowsetMetaCloudPB>* rowset_metas) {
        return get_rowset_metas(txn, tablet_id, start_version, end_version, rowset_metas,
                                snapshot_);
    }

    // Get the load rowset meta for the given tablet_id and version.
    TxnErrorCode get_load_rowset_meta(int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta, bool snapshot);
    TxnErrorCode get_load_rowset_meta(Transaction* txn, int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta, bool snapshot);
    TxnErrorCode get_load_rowset_meta(int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta) {
        return get_load_rowset_meta(tablet_id, version, rowset_meta, snapshot_);
    }
    TxnErrorCode get_load_rowset_meta(Transaction* txn, int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta) {
        return get_load_rowset_meta(txn, tablet_id, version, rowset_meta, snapshot_);
    }

    // Get the tablet meta keys.
    TxnErrorCode get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                 Versionstamp* versionstamp, bool snapshot);
    TxnErrorCode get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                 TabletMetaCloudPB* tablet_meta, Versionstamp* versionstamp,
                                 bool snapshot);
    TxnErrorCode get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                 Versionstamp* versionstamp) {
        return get_tablet_meta(tablet_id, tablet_meta, versionstamp, snapshot_);
    }
    TxnErrorCode get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                 TabletMetaCloudPB* tablet_meta, Versionstamp* versionstamp) {
        return get_tablet_meta(txn, tablet_id, tablet_meta, versionstamp, snapshot_);
    }

    // Gets the first pending transaction ID from the partition version.
    //
    // The first pending txn id is stored in `first_txn_id`. Sets -1 if no pending transactions exist.
    TxnErrorCode get_partition_pending_txn_id(int64_t partition_id, int64_t* first_txn_id,
                                              bool snapshot);
    TxnErrorCode get_partition_pending_txn_id(Transaction* txn, int64_t partition_id,
                                              int64_t* first_txn_id, bool snapshot);
    TxnErrorCode get_partition_pending_txn_id(int64_t partition_id, int64_t* first_txn_id) {
        return get_partition_pending_txn_id(partition_id, first_txn_id, snapshot_);
    }
    TxnErrorCode get_partition_pending_txn_id(Transaction* txn, int64_t partition_id,
                                              int64_t* first_txn_id) {
        return get_partition_pending_txn_id(txn, partition_id, first_txn_id, snapshot_);
    }

    // Get the index of the given index id.
    TxnErrorCode get_index_index(int64_t index_id, IndexIndexPB* index, bool snapshot);
    TxnErrorCode get_index_index(Transaction* txn, int64_t index_id, IndexIndexPB* index,
                                 bool snapshot);
    TxnErrorCode get_index_index(int64_t index_id, IndexIndexPB* index) {
        return get_index_index(index_id, index, snapshot_);
    }
    TxnErrorCode get_index_index(Transaction* txn, int64_t index_id, IndexIndexPB* index) {
        return get_index_index(txn, index_id, index, snapshot_);
    }

    // Get the partition index for the given partition_id.
    TxnErrorCode get_partition_index(int64_t partition_id, PartitionIndexPB* partition_index,
                                     bool snapshot);
    TxnErrorCode get_partition_index(Transaction* txn, int64_t partition_id,
                                     PartitionIndexPB* partition_index, bool snapshot);
    TxnErrorCode get_partition_index(int64_t partition_id, PartitionIndexPB* partition_index) {
        return get_partition_index(partition_id, partition_index, snapshot_);
    }
    TxnErrorCode get_partition_index(Transaction* txn, int64_t partition_id,
                                     PartitionIndexPB* partition_index) {
        return get_partition_index(txn, partition_id, partition_index, snapshot_);
    }

private:
    const std::string_view instance_id_;
    const Versionstamp snapshot_version_;
    const bool snapshot_;
    TxnKv* txn_kv_;
};

} // namespace doris::cloud
