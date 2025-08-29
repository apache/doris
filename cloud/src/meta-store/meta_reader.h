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
            : MetaReader(instance_id, txn_kv, Versionstamp::max()) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, Versionstamp snapshot_version)
            : instance_id_(instance_id),
              snapshot_version_(snapshot_version),
              txn_kv_(txn_kv),
              min_read_versionstamp_(Versionstamp::max()) {}
    MetaReader(const MetaReader&) = delete;
    MetaReader& operator=(const MetaReader&) = delete;

    uint64_t min_read_version() const { return min_read_versionstamp_.version(); }
    Versionstamp min_read_versionstamp() const { return min_read_versionstamp_; }

    // Get the version of the table_version_key with the given table_id.
    TxnErrorCode get_table_version(int64_t table_id, Versionstamp* table_version,
                                   bool snapshot = false);
    TxnErrorCode get_table_version(Transaction* txn, int64_t table_id, Versionstamp* table_version,
                                   bool snapshot = false);

    // Get the versions of the table_version_key with the given table_ids.
    //
    // The table versions are returned in the `table_versions` map, where the key is the table_id
    // and the value is the versionstamp. If a tablet_id does not exists, then it will not be
    // included in the map.
    TxnErrorCode get_table_versions(const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions,
                                    bool snapshot = false);
    TxnErrorCode get_table_versions(Transaction* txn, const std::vector<int64_t>& table_ids,
                                    std::unordered_map<int64_t, Versionstamp>* table_versions,
                                    bool snapshot = false);

    // Get the partition version for the given partition
    //
    // If the `version` is not nullptr, it will be filled with the deserialized VersionPB.
    TxnErrorCode get_partition_version(int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp, bool snapshot = false);
    TxnErrorCode get_partition_version(Transaction* txn, int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp, bool snapshot = false);

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
                                        bool snapshot = false);
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, VersionPB>* versions,
                                        std::unordered_map<int64_t, Versionstamp>* versionstamps,
                                        bool snapshot = false);

    // Get the partition versions from the given partition_ids.
    // If the partition id is not found, it will be set to 1 in the versions map.
    TxnErrorCode get_partition_versions(Transaction* txn, const std::vector<int64_t>& partition_ids,
                                        std::unordered_map<int64_t, int64_t>* versions,
                                        int64_t* last_pending_txn_id, bool snapshot = false);

    // Get the tablet load stats for the given tablet
    //
    // If the `tablet_stats` is not nullptr, it will be filled with the deserialized TabletStatsPB.
    TxnErrorCode get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                       Versionstamp* versionstamp, bool snapshot = false);
    TxnErrorCode get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                       TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                       bool snapshot = false);

    // Get the tablet compact stats for the given tablet
    //
    // If the `tablet_stats` is not nullptr, it will be filled with the deserialized TabletStatsPB.
    TxnErrorCode get_tablet_compact_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                          Versionstamp* versionstamp, bool snapshot = false);
    TxnErrorCode get_tablet_compact_stats(Transaction* txn, int64_t tablet_id,
                                          TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                          bool snapshot = false);

    // Get the tablet compact stats for the given tablet_ids.
    // If a tablet_id does not exist, then it will not be included in the respective map.
    TxnErrorCode get_tablet_compact_stats(const std::vector<int64_t>& tablet_ids,
                                          std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
                                          std::unordered_map<int64_t, Versionstamp>* versionstamps,
                                          bool snapshot = false);
    TxnErrorCode get_tablet_compact_stats(Transaction* txn, const std::vector<int64_t>& tablet_ids,
                                          std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
                                          std::unordered_map<int64_t, Versionstamp>* versionstamps,
                                          bool snapshot = false);

    // Get the merged (load, compact) tablet stats for the given tablet.
    //
    // The `tablet_stats` will be filled with the merged TabletStatsPB.
    // The `versionstamp` will be set to the latest versionstamp of the load or compact stats.
    TxnErrorCode get_tablet_merged_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                         Versionstamp* versionstamp, bool snapshot = false);
    TxnErrorCode get_tablet_merged_stats(Transaction* txn, int64_t tablet_id,
                                         TabletStatsPB* tablet_stats, Versionstamp* versionstamp,
                                         bool snapshot = false);

    // Get the tablet index for the given tablet_id.
    TxnErrorCode get_tablet_index(int64_t tablet_id, TabletIndexPB* tablet_index,
                                  bool snapshot = false);
    TxnErrorCode get_tablet_index(Transaction* txn, int64_t tablet_id, TabletIndexPB* tablet_index,
                                  bool snapshot = false);

    // Get the tablet indexes for the given tablet_ids.
    //
    // If a tablet_id does not exists, then it will not be included in the respective map.
    TxnErrorCode get_tablet_indexes(const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes,
                                    bool snapshot = false);
    TxnErrorCode get_tablet_indexes(Transaction* txn, const std::vector<int64_t>& tablet_ids,
                                    std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes,
                                    bool snapshot = false);

    // Get the rowset meta for the given tablet_id and version range.
    //
    // The `rowset_metas` will be filled with the RowsetMetaCloudPB for each version in the range,
    // in ascending order.
    TxnErrorCode get_rowset_metas(int64_t tablet_id, int64_t start_version, int64_t end_version,
                                  std::vector<RowsetMetaCloudPB>* rowset_metas,
                                  bool snapshot = false);
    TxnErrorCode get_rowset_metas(Transaction* txn, int64_t tablet_id, int64_t start_version,
                                  int64_t end_version, std::vector<RowsetMetaCloudPB>* rowset_metas,
                                  bool snapshot = false);

    // Get the load rowset meta for the given tablet_id and version.
    TxnErrorCode get_load_rowset_meta(int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta, bool snapshot = false);
    TxnErrorCode get_load_rowset_meta(Transaction* txn, int64_t tablet_id, int64_t version,
                                      RowsetMetaCloudPB* rowset_meta, bool snapshot = false);

    // Get the tablet meta keys.
    TxnErrorCode get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                 Versionstamp* versionstamp, bool snapshot = false);
    TxnErrorCode get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                 TabletMetaCloudPB* tablet_meta, Versionstamp* versionstamp,
                                 bool snapshot = false);

    // Get the tablet schema keys.
    TxnErrorCode get_tablet_schema(int64_t index_id, int64_t schema_version,
                                   TabletSchemaCloudPB* tablet_schema, bool snapshot = false);
    TxnErrorCode get_tablet_schema(Transaction* txn, int64_t index_id, int64_t schema_version,
                                   TabletSchemaCloudPB* tablet_schema, bool snapshot = false);

    // Gets the first pending transaction ID from the partition version.
    //
    // The first pending txn id is stored in `first_txn_id`. Sets -1 if no pending transactions exist.
    TxnErrorCode get_partition_pending_txn_id(int64_t partition_id, int64_t* first_txn_id,
                                              bool snapshot = false);
    TxnErrorCode get_partition_pending_txn_id(Transaction* txn, int64_t partition_id,
                                              int64_t* first_txn_id, bool snapshot = false);

    // Get the index of the given index id.
    TxnErrorCode get_index_index(int64_t index_id, IndexIndexPB* index, bool snapshot = false);
    TxnErrorCode get_index_index(Transaction* txn, int64_t index_id, IndexIndexPB* index,
                                 bool snapshot = false);

    // Get the partition index for the given partition_id.
    TxnErrorCode get_partition_index(int64_t partition_id, PartitionIndexPB* partition_index,
                                     bool snapshot = false);
    TxnErrorCode get_partition_index(Transaction* txn, int64_t partition_id,
                                     PartitionIndexPB* partition_index, bool snapshot = false);

    // Check if the index exists in the given transaction.
    // Returns TXN_OK if the index exists, or TXN_KEY_NOT_FOUND if it does not.
    TxnErrorCode is_index_exists(int64_t index_id, bool snapshot = false);
    TxnErrorCode is_index_exists(Transaction* txn, int64_t index_id, bool snapshot = false);

    // Check if the partition exists in the given transaction.
    // Returns TXN_OK if the partition exists, or TXN_KEY_NOT_FOUND if it does not.
    TxnErrorCode is_partition_exists(int64_t partition_id, bool snapshot = false);
    TxnErrorCode is_partition_exists(Transaction* txn, int64_t partition_id, bool snapshot = false);

private:
    const std::string_view instance_id_;
    const Versionstamp snapshot_version_;

    TxnKv* txn_kv_;
    Versionstamp min_read_versionstamp_;
};

} // namespace doris::cloud
