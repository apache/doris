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

#include "clone_chain_reader.h"

#include "common/logging.h"
#include "common/util.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

bool CloneChainReader::get_source_snapshot_info(const std::string& instance_id,
                                                std::string* source_instance_id,
                                                Versionstamp* source_snapshot_version) {
    return resource_mgr_->get_source_snapshot_info(instance_id, source_instance_id,
                                                   source_snapshot_version);
}

TxnErrorCode CloneChainReader::get_table_version(int64_t table_id, Versionstamp* table_version,
                                                 bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_table_version(txn.get(), table_id, table_version, snapshot);
}

TxnErrorCode CloneChainReader::get_table_version(Transaction* txn, int64_t table_id,
                                                 Versionstamp* table_version, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_table_version(txn, table_id, table_version, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_table_versions(
        const std::vector<int64_t>& table_ids,
        std::unordered_map<int64_t, Versionstamp>* table_versions, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_table_versions(txn.get(), table_ids, table_versions, snapshot);
}

TxnErrorCode CloneChainReader::get_table_versions(
        Transaction* txn, const std::vector<int64_t>& table_ids,
        std::unordered_map<int64_t, Versionstamp>* table_versions, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::vector<int64_t> remaining_ids = table_ids;

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        std::unordered_map<int64_t, Versionstamp> current_versions;
        TxnErrorCode err =
                reader.get_table_versions(txn, remaining_ids, &current_versions, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        min_read_versionstamp_ = std::min(reader.min_read_versionstamp(), min_read_versionstamp_);

        // Add found results to output
        for (const auto& [table_id, version] : current_versions) {
            (*table_versions)[table_id] = version;
        }

        // Remove found ids from remaining_ids
        std::vector<int64_t> new_remaining_ids;
        for (int64_t table_id : remaining_ids) {
            if (!current_versions.contains(table_id)) {
                new_remaining_ids.push_back(table_id);
            }
        }
        remaining_ids = std::move(new_remaining_ids);

        // If all found or no more to search, break
        if (remaining_ids.empty()) {
            break;
        }

        // Try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, remaining ids are not found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_partition_version(int64_t partition_id, VersionPB* version,
                                                     Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_partition_version(txn.get(), partition_id, version, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_partition_version(Transaction* txn, int64_t partition_id,
                                                     VersionPB* version, Versionstamp* versionstamp,
                                                     bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err =
                reader.get_partition_version(txn, partition_id, version, versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_partition_versions(
        const std::vector<int64_t>& partition_ids, std::unordered_map<int64_t, VersionPB>* versions,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_partition_versions(txn.get(), partition_ids, versions, versionstamps, snapshot);
}

TxnErrorCode CloneChainReader::get_partition_versions(
        Transaction* txn, const std::vector<int64_t>& partition_ids,
        std::unordered_map<int64_t, VersionPB>* versions,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::vector<int64_t> remaining_ids = partition_ids;

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        std::unordered_map<int64_t, VersionPB> current_versions;
        std::unordered_map<int64_t, Versionstamp> current_versionstamps;
        TxnErrorCode err = reader.get_partition_versions(txn, remaining_ids, &current_versions,
                                                         &current_versionstamps, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        min_read_versionstamp_ = std::min(reader.min_read_versionstamp(), min_read_versionstamp_);

        // Add found results to output
        if (versions) {
            for (const auto& [partition_id, version] : current_versions) {
                (*versions)[partition_id] = std::move(version);
            }
        }
        if (versionstamps) {
            for (const auto& [partition_id, versionstamp] : current_versionstamps) {
                (*versionstamps)[partition_id] = versionstamp;
            }
        }

        // Remove found ids from remaining_ids
        std::vector<int64_t> new_remaining_ids;
        for (int64_t partition_id : remaining_ids) {
            if (!current_versions.contains(partition_id)) {
                new_remaining_ids.push_back(partition_id);
            }
        }
        remaining_ids = std::move(new_remaining_ids);

        // If all found or no more to search, break
        if (remaining_ids.empty()) {
            break;
        }

        // Try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, remaining ids are not found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_partition_versions(
        Transaction* txn, const std::vector<int64_t>& partition_ids,
        std::unordered_map<int64_t, int64_t>* versions, int64_t* last_pending_txn_id,
        bool snapshot) {
    std::unordered_map<int64_t, VersionPB> version_pb_map;
    TxnErrorCode err =
            get_partition_versions(txn, partition_ids, &version_pb_map, nullptr, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (int64_t partition_id : partition_ids) {
        auto it = version_pb_map.find(partition_id);
        if (it == version_pb_map.end()) {
            versions->emplace(partition_id, 1);
        } else {
            const VersionPB& version_pb = it->second;
            int64_t version = version_pb.version();
            versions->emplace(partition_id, version);
            if (last_pending_txn_id && version_pb.pending_txn_ids_size() > 0) {
                *last_pending_txn_id = version_pb.pending_txn_ids(0);
            }
        }
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                                     Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_load_stats(txn.get(), tablet_id, tablet_stats, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                                     TabletStatsPB* tablet_stats,
                                                     Versionstamp* versionstamp, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err =
                reader.get_tablet_load_stats(txn, tablet_id, tablet_stats, versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_tablet_load_stats(
        const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_load_stats(txn.get(), tablet_ids, tablet_stats, versionstamps, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_load_stats(
        Transaction* txn, const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::vector<int64_t> remaining_ids = tablet_ids;

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        std::unordered_map<int64_t, TabletStatsPB> current_stats;
        std::unordered_map<int64_t, Versionstamp> current_versionstamps;
        TxnErrorCode err = reader.get_tablet_load_stats(txn, remaining_ids, &current_stats,
                                                        &current_versionstamps, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        min_read_versionstamp_ = std::min(reader.min_read_versionstamp(), min_read_versionstamp_);

        // Add found results to output
        if (tablet_stats) {
            for (const auto& [tablet_id, stats] : current_stats) {
                (*tablet_stats)[tablet_id] = stats;
            }
        }
        if (versionstamps) {
            for (const auto& [tablet_id, versionstamp] : current_versionstamps) {
                (*versionstamps)[tablet_id] = versionstamp;
            }
        }

        // Remove found ids from remaining_ids
        std::vector<int64_t> new_remaining_ids;
        for (int64_t tablet_id : remaining_ids) {
            if (!current_stats.contains(tablet_id)) {
                new_remaining_ids.push_back(tablet_id);
            }
        }
        remaining_ids = std::move(new_remaining_ids);

        // If all found or no more to search, break
        if (remaining_ids.empty()) {
            break;
        }

        // Try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, remaining ids are not found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_tablet_compact_stats(int64_t tablet_id,
                                                        TabletStatsPB* tablet_stats,
                                                        Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_compact_stats(txn.get(), tablet_id, tablet_stats, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_compact_stats(Transaction* txn, int64_t tablet_id,
                                                        TabletStatsPB* tablet_stats,
                                                        Versionstamp* versionstamp, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_tablet_compact_stats(txn, tablet_id, tablet_stats,
                                                           versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_tablet_compact_stats(
        const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_compact_stats(txn.get(), tablet_ids, tablet_stats, versionstamps, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_compact_stats(
        Transaction* txn, const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::vector<int64_t> remaining_ids = tablet_ids;

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        std::unordered_map<int64_t, TabletStatsPB> current_stats;
        std::unordered_map<int64_t, Versionstamp> current_versionstamps;
        TxnErrorCode err = reader.get_tablet_compact_stats(txn, remaining_ids, &current_stats,
                                                           &current_versionstamps, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        min_read_versionstamp_ = std::min(reader.min_read_versionstamp(), min_read_versionstamp_);

        // Add found results to output
        if (tablet_stats) {
            for (const auto& [tablet_id, stats] : current_stats) {
                (*tablet_stats)[tablet_id] = stats;
            }
        }
        if (versionstamps) {
            for (const auto& [tablet_id, versionstamp] : current_versionstamps) {
                (*versionstamps)[tablet_id] = versionstamp;
            }
        }

        // Remove found ids from remaining_ids
        std::vector<int64_t> new_remaining_ids;
        for (int64_t tablet_id : remaining_ids) {
            if (!current_stats.contains(tablet_id)) {
                new_remaining_ids.push_back(tablet_id);
            }
        }
        remaining_ids = std::move(new_remaining_ids);

        // If all found or no more to search, break
        if (remaining_ids.empty()) {
            break;
        }

        // Try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, remaining ids are not found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_tablet_merged_stats(int64_t tablet_id,
                                                       TabletStatsPB* tablet_stats,
                                                       Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_merged_stats(txn.get(), tablet_id, tablet_stats, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_merged_stats(Transaction* txn, int64_t tablet_id,
                                                       TabletStatsPB* tablet_stats,
                                                       Versionstamp* versionstamp, bool snapshot) {
    TabletStatsPB load_stats, compact_stats;
    Versionstamp load_versionstamp, compact_versionstamp;

    TxnErrorCode err =
            get_tablet_load_stats(txn, tablet_id, &load_stats, &load_versionstamp, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    err = get_tablet_compact_stats(txn, tablet_id, &compact_stats, &compact_versionstamp, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    MetaReader::merge_tablet_stats(load_stats, compact_stats, tablet_stats);
    if (versionstamp) {
        *versionstamp = std::min(load_versionstamp, compact_versionstamp);
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_tablet_index(int64_t tablet_id, TabletIndexPB* tablet_index,
                                                bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_index(txn.get(), tablet_id, tablet_index, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_index(Transaction* txn, int64_t tablet_id,
                                                TabletIndexPB* tablet_index, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_tablet_index(txn, tablet_id, tablet_index, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_tablet_indexes(
        const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_indexes(txn.get(), tablet_ids, tablet_indexes, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_indexes(
        Transaction* txn, const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::vector<int64_t> remaining_ids = tablet_ids;

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        std::unordered_map<int64_t, TabletIndexPB> current_indexes;
        TxnErrorCode err =
                reader.get_tablet_indexes(txn, remaining_ids, &current_indexes, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }

        // Add found results to output
        for (const auto& [tablet_id, index] : current_indexes) {
            (*tablet_indexes)[tablet_id] = index;
        }

        // Remove found ids from remaining_ids
        std::vector<int64_t> new_remaining_ids;
        for (int64_t tablet_id : remaining_ids) {
            if (!current_indexes.contains(tablet_id)) {
                new_remaining_ids.push_back(tablet_id);
            }
        }
        remaining_ids = std::move(new_remaining_ids);

        // If all found or no more to search, break
        if (remaining_ids.empty()) {
            break;
        }

        // Try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, remaining ids are not found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                               Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_meta(txn.get(), tablet_id, tablet_meta, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                               TabletMetaCloudPB* tablet_meta,
                                               Versionstamp* versionstamp, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err =
                reader.get_tablet_meta(txn, tablet_id, tablet_meta, versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_tablet_schema(int64_t index_id, int64_t schema_version,
                                                 TabletSchemaCloudPB* tablet_schema,
                                                 bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_schema(txn.get(), index_id, schema_version, tablet_schema, snapshot);
}

TxnErrorCode CloneChainReader::get_tablet_schema(Transaction* txn, int64_t index_id,
                                                 int64_t schema_version,
                                                 TabletSchemaCloudPB* tablet_schema,
                                                 bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err =
                reader.get_tablet_schema(txn, index_id, schema_version, tablet_schema, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_rowset_meta(int64_t tablet_id, int64_t end_version,
                                               RowsetMetaCloudPB* rowset_meta, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_rowset_meta(txn.get(), tablet_id, end_version, rowset_meta, snapshot);
}

TxnErrorCode CloneChainReader::get_rowset_meta(Transaction* txn, int64_t tablet_id,
                                               int64_t end_version, RowsetMetaCloudPB* rowset_meta,
                                               bool snapshot) {
    RowsetMetaCloudPB load_rowset_meta, compact_rowset_meta;
    Versionstamp load_versionstamp, compact_versionstamp;

    TxnErrorCode err = get_load_rowset_meta(txn, tablet_id, end_version, &load_rowset_meta,
                                            &load_versionstamp, snapshot);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return err;
    }
    bool load_rowset_exists = (err == TxnErrorCode::TXN_OK);

    err = get_compact_rowset_meta(txn, tablet_id, end_version, &compact_rowset_meta,
                                  &compact_versionstamp, snapshot);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return err;
    }
    bool compact_rowset_exists = (err == TxnErrorCode::TXN_OK);

    if (!load_rowset_exists && !compact_rowset_exists) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }

    if (load_rowset_exists && !compact_rowset_exists) {
        *rowset_meta = std::move(load_rowset_meta);
    } else if (!load_rowset_exists && compact_rowset_exists) {
        *rowset_meta = std::move(compact_rowset_meta);
    } else if (load_versionstamp < compact_versionstamp) {
        *rowset_meta = std::move(compact_rowset_meta);
    } else {
        *rowset_meta = std::move(load_rowset_meta);
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_rowset_metas(int64_t tablet_id, int64_t start_version,
                                                int64_t end_version,
                                                std::vector<RowsetMetaCloudPB>* rowset_metas,
                                                bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_rowset_metas(txn.get(), tablet_id, start_version, end_version, rowset_metas,
                            snapshot);
}

TxnErrorCode CloneChainReader::get_rowset_metas(Transaction* txn, int64_t tablet_id,
                                                int64_t start_version, int64_t end_version,
                                                std::vector<RowsetMetaCloudPB>* rowset_metas,
                                                bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    std::map<int64_t, RowsetMetaCloudPB> version_to_rowset;

    std::vector<std::pair<int64_t, int64_t>> searching_ranges;
    searching_ranges.emplace_back(start_version, end_version);

    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        for (auto& [start, end] : searching_ranges) {
            std::vector<RowsetMetaCloudPB> current_rowsets;
            TxnErrorCode err =
                    reader.get_rowset_metas(txn, tablet_id, start, end, &current_rowsets, snapshot);
            if (err != TxnErrorCode::TXN_OK) {
                return err;
            }
            min_read_versionstamp_ =
                    std::min(reader.min_read_versionstamp(), min_read_versionstamp_);

            // Add found rowsets to version_to_rowset map (only if not already found)
            for (auto&& rowset : current_rowsets) {
                int64_t version = rowset.end_version();
                if (version_to_rowset.contains(version)) {
                    LOG_WARNING("Duplicate rowset for version {} found in clone chain, ignoring",
                                version);
                    return TxnErrorCode::TXN_INVALID_DATA;
                }
                if (!rowset.has_reference_instance_id()) {
                    rowset.set_reference_instance_id(current_instance_id);
                }
                version_to_rowset[version] = std::move(rowset);
            }
        }

        // There should be no overlapping ranges in version_to_rowset
        searching_ranges.clear();

        int64_t next_version = start_version;
        for (auto&& [end_version, rowset] : version_to_rowset) {
            if (end_version < next_version) {
                LOG_WARNING("Overlapping rowset for version {} found in clone chain", end_version);
                return TxnErrorCode::TXN_INVALID_DATA;
            }
            if (next_version != rowset.start_version()) {
                // missing version range, need to search in previous clone chain
                searching_ranges.emplace_back(next_version, rowset.start_version() - 1);
            }
            next_version = end_version + 1;
        }
        if (next_version <= end_version) {
            // missing version range at the end
            searching_ranges.emplace_back(next_version, end_version);
        }

        if (searching_ranges.empty()) {
            // All versions found
            break;
        }

        // Try to find missing versions in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain, break with what we found
            break;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);

    rowset_metas->clear();
    rowset_metas->reserve(version_to_rowset.size());
    for (auto&& [version, rowset] : version_to_rowset) {
        rowset_metas->push_back(std::move(rowset));
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode CloneChainReader::get_load_rowset_meta(int64_t tablet_id, int64_t version,
                                                    RowsetMetaCloudPB* rowset_meta,
                                                    Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_load_rowset_meta(txn.get(), tablet_id, version, rowset_meta, versionstamp, snapshot);
}

TxnErrorCode CloneChainReader::get_load_rowset_meta(Transaction* txn, int64_t tablet_id,
                                                    int64_t version, RowsetMetaCloudPB* rowset_meta,
                                                    Versionstamp* versionstamp, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_load_rowset_meta(txn, tablet_id, version, rowset_meta,
                                                       versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_compact_rowset_meta(int64_t tablet_id, int64_t version,
                                                       RowsetMetaCloudPB* rowset_meta,
                                                       Versionstamp* versionstamp, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_compact_rowset_meta(txn.get(), tablet_id, version, rowset_meta, versionstamp,
                                   snapshot);
}

TxnErrorCode CloneChainReader::get_compact_rowset_meta(Transaction* txn, int64_t tablet_id,
                                                       int64_t version,
                                                       RowsetMetaCloudPB* rowset_meta,
                                                       Versionstamp* versionstamp, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_compact_rowset_meta(txn, tablet_id, version, rowset_meta,
                                                          versionstamp, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_partition_pending_txn_id(int64_t partition_id,
                                                            int64_t* first_txn_id,
                                                            int64_t* partition_version,
                                                            bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_partition_pending_txn_id(txn.get(), partition_id, first_txn_id, partition_version,
                                        snapshot);
}

TxnErrorCode CloneChainReader::get_partition_pending_txn_id(Transaction* txn, int64_t partition_id,
                                                            int64_t* first_txn_id,
                                                            int64_t* partition_version,
                                                            bool snapshot) {
    // Initialize to -1 to indicate no pending transactions
    *first_txn_id = -1;

    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);

        VersionPB version_pb;
        Versionstamp versionstamp;
        TxnErrorCode err =
                get_partition_version(txn, partition_id, &version_pb, &versionstamp, snapshot);
        if (err == TxnErrorCode::TXN_OK) {
            if (version_pb.pending_txn_ids_size() > 0) {
                *first_txn_id = version_pb.pending_txn_ids(0);
            }
            *partition_version = version_pb.version();
            min_read_versionstamp_ =
                    std::min(reader.min_read_versionstamp(), min_read_versionstamp_);
            return TxnErrorCode::TXN_OK;
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_OK;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_index_index(int64_t index_id, IndexIndexPB* index,
                                               bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_index_index(txn.get(), index_id, index, snapshot);
}

TxnErrorCode CloneChainReader::get_index_index(Transaction* txn, int64_t index_id,
                                               IndexIndexPB* index, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_index_index(txn, index_id, index, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_partition_index(int64_t partition_id,
                                                   PartitionIndexPB* partition_index,
                                                   bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_partition_index(txn.get(), partition_id, partition_index, snapshot);
}

TxnErrorCode CloneChainReader::get_partition_index(Transaction* txn, int64_t partition_id,
                                                   PartitionIndexPB* partition_index,
                                                   bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.get_partition_index(txn, partition_id, partition_index, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::is_index_exists(int64_t index_id, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return is_index_exists(txn.get(), index_id, snapshot);
}

TxnErrorCode CloneChainReader::is_index_exists(Transaction* txn, int64_t index_id, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.is_index_exists(txn, index_id, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::is_partition_exists(int64_t partition_id, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return is_partition_exists(txn.get(), partition_id, snapshot);
}

TxnErrorCode CloneChainReader::is_partition_exists(Transaction* txn, int64_t partition_id,
                                                   bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.is_partition_exists(txn, partition_id, snapshot);
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            if (err == TxnErrorCode::TXN_OK) {
                min_read_versionstamp_ = reader.min_read_versionstamp();
            }
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::has_no_indexes(int64_t db_id, int64_t table_id, bool* no_indexes,
                                              bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return has_no_indexes(txn.get(), db_id, table_id, no_indexes, snapshot);
}

TxnErrorCode CloneChainReader::has_no_indexes(Transaction* txn, int64_t db_id, int64_t table_id,
                                              bool* no_indexes, bool snapshot) {
    std::string current_instance_id(instance_id_);
    Versionstamp current_snapshot_version = snapshot_version_;

    bool local_no_indexes = true;
    do {
        MetaReader reader(current_instance_id, current_snapshot_version);
        TxnErrorCode err = reader.has_no_indexes(txn, db_id, table_id, &local_no_indexes, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        } else if (!local_no_indexes) {
            *no_indexes = false;
            min_read_versionstamp_ =
                    std::min(reader.min_read_versionstamp(), min_read_versionstamp_);
            return TxnErrorCode::TXN_OK;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            *no_indexes = true;
            return TxnErrorCode::TXN_OK;
        }
        current_instance_id = std::move(prev_instance_id);
        current_snapshot_version = prev_snapshot_version;
    } while (true);
}

TxnErrorCode CloneChainReader::get_delete_bitmap_v2(int64_t tablet_id, const std::string& rowset_id,
                                                    DeleteBitmapStoragePB* delete_bitmap,
                                                    bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_delete_bitmap_v2(txn.get(), tablet_id, rowset_id, delete_bitmap, snapshot);
}

TxnErrorCode CloneChainReader::get_delete_bitmap_v2(Transaction* txn, int64_t tablet_id,
                                                    const std::string& rowset_id,
                                                    DeleteBitmapStoragePB* delete_bitmap,
                                                    bool snapshot) {
    std::string current_instance_id(instance_id_);
    do {
        std::string key =
                versioned::meta_delete_bitmap_key({current_instance_id, tablet_id, rowset_id});
        ValueBuf val_buf;
        TxnErrorCode err = cloud::blob_get(txn, key, &val_buf);
        if (err == TxnErrorCode::TXN_OK) {
            if (!val_buf.to_pb(delete_bitmap)) {
                return TxnErrorCode::TXN_INVALID_DATA;
            }
            return TxnErrorCode::TXN_OK;
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return err;
        }

        // not found, try to find in previous clone chain
        std::string prev_instance_id;
        Versionstamp prev_snapshot_version;
        if (!get_source_snapshot_info(current_instance_id, &prev_instance_id,
                                      &prev_snapshot_version)) {
            // no previous clone chain
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
        current_instance_id = std::move(prev_instance_id);
    } while (true);
}

} // namespace doris::cloud
