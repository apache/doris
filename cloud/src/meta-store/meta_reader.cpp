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

#include "meta-store/meta_reader.h"

#include <gen_cpp/olap_file.pb.h>

#include <limits>
#include <memory>

#include "common/logging.h"
#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/document_message_get_range.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

namespace doris::cloud {

TxnErrorCode MetaReader::get_table_version(int64_t table_id, Versionstamp* table_version,
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

TxnErrorCode MetaReader::get_table_version(Transaction* txn, int64_t table_id,
                                           Versionstamp* table_version, bool snapshot) {
    std::string table_version_key = versioned::table_version_key({instance_id_, table_id});
    std::string table_version_value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, table_version_key, snapshot_version_, &key_version,
                                     &table_version_value, snapshot);
    if (err == TxnErrorCode::TXN_OK) {
        if (table_version) {
            *table_version = key_version;
        }
        min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);
    }
    return err;
}

TxnErrorCode MetaReader::get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
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

TxnErrorCode MetaReader::get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                         TabletMetaCloudPB* tablet_meta, Versionstamp* versionstamp,
                                         bool snapshot) {
    std::string tablet_meta_key = versioned::meta_tablet_key({instance_id_, tablet_id});
    Versionstamp key_version;
    TxnErrorCode err = versioned::document_get(txn, tablet_meta_key, snapshot_version_, tablet_meta,
                                               &key_version, snapshot);
    if (err == TxnErrorCode::TXN_OK) {
        if (versionstamp) {
            *versionstamp = key_version;
        }
        min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);
    }
    return err;
}

TxnErrorCode MetaReader::get_tablet_schema(int64_t index_id, int64_t schema_version,
                                           TabletSchemaCloudPB* tablet_schema, bool snapshot) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    return get_tablet_schema(txn.get(), index_id, schema_version, tablet_schema, snapshot);
}

TxnErrorCode MetaReader::get_tablet_schema(Transaction* txn, int64_t index_id,
                                           int64_t schema_version,
                                           TabletSchemaCloudPB* tablet_schema, bool snapshot) {
    std::string tablet_schema_key =
            versioned::meta_schema_key({instance_id_, index_id, schema_version});
    return document_get(txn, tablet_schema_key, tablet_schema, snapshot);
}

TxnErrorCode MetaReader::get_partition_version(int64_t partition_id, VersionPB* version,
                                               Versionstamp* partition_version, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    return get_partition_version(txn.get(), partition_id, version, partition_version, snapshot);
}

TxnErrorCode MetaReader::get_partition_version(Transaction* txn, int64_t partition_id,
                                               VersionPB* version, Versionstamp* partition_version,
                                               bool snapshot) {
    std::string partition_version_key =
            versioned::partition_version_key({instance_id_, partition_id});
    std::string partition_version_value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, partition_version_key, snapshot_version_, &key_version,
                                     &partition_version_value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (partition_version) {
        *partition_version = key_version;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);

    if (version && !version->ParseFromString(partition_version_value)) {
        LOG_ERROR("Failed to parse VersionPB")
                .tag("instance_id", instance_id_)
                .tag("partition_id", partition_id)
                .tag("key", hex(partition_version_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
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

TxnErrorCode MetaReader::get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                               TabletStatsPB* tablet_stats,
                                               Versionstamp* versionstamp, bool snapshot) {
    std::string tablet_load_stats_key = versioned::tablet_load_stats_key({instance_id_, tablet_id});
    std::string tablet_load_stats_value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, tablet_load_stats_key, snapshot_version_, &key_version,
                                     &tablet_load_stats_value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (versionstamp) {
        *versionstamp = key_version;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);

    if (tablet_stats && !tablet_stats->ParseFromString(tablet_load_stats_value)) {
        LOG_ERROR("Failed to parse TabletStatsPB")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("key", hex(tablet_load_stats_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_compact_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
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

TxnErrorCode MetaReader::get_tablet_compact_stats(Transaction* txn, int64_t tablet_id,
                                                  TabletStatsPB* tablet_stats,
                                                  Versionstamp* versionstamp, bool snapshot) {
    std::string tablet_compact_stats_key =
            versioned::tablet_compact_stats_key({instance_id_, tablet_id});
    std::string tablet_compact_stats_value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, tablet_compact_stats_key, snapshot_version_, &key_version,
                                     &tablet_compact_stats_value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (versionstamp) {
        *versionstamp = key_version;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);

    if (tablet_stats && !tablet_stats->ParseFromString(tablet_compact_stats_value)) {
        LOG_ERROR("Failed to parse TabletStatsPB")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("key", hex(tablet_compact_stats_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_compact_stats(
        const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_compact_stats(txn.get(), tablet_ids, tablet_stats, versionstamps, snapshot);
}

TxnErrorCode MetaReader::get_tablet_compact_stats(
        Transaction* txn, const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletStatsPB>* tablet_stats,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    if (tablet_ids.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    std::vector<std::string> tablet_compact_stats_keys;
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        int64_t tablet_id = tablet_ids[i];
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id_, tablet_id});
        tablet_compact_stats_keys.push_back(std::move(tablet_compact_stats_key));
    }

    std::vector<std::optional<std::pair<std::string, Versionstamp>>> versioned_values;
    TxnErrorCode err = versioned_batch_get(txn, tablet_compact_stats_keys, snapshot_version_,
                                           &versioned_values, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (size_t i = 0; i < versioned_values.size(); ++i) {
        const auto& kv = versioned_values[i];
        if (!kv.has_value()) {
            continue; // Key not found, skip
        }

        const std::string& value = kv->first;
        Versionstamp versionstamp = kv->second;
        int64_t tablet_id = tablet_ids[i];

        if (versionstamps) {
            versionstamps->emplace(tablet_id, versionstamp);
        }

        if (tablet_stats) {
            TabletStatsPB tablet_stat;
            if (!tablet_stat.ParseFromString(value)) {
                LOG_ERROR("Failed to parse TabletStatsPB")
                        .tag("instance_id", instance_id_)
                        .tag("tablet_id", tablet_id)
                        .tag("key", hex(tablet_compact_stats_keys[i]))
                        .tag("value", hex(value));
                return TxnErrorCode::TXN_INVALID_DATA;
            }
            tablet_stats->emplace(tablet_id, std::move(tablet_stat));
        }
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_merged_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
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

TxnErrorCode MetaReader::get_tablet_merged_stats(Transaction* txn, int64_t tablet_id,
                                                 TabletStatsPB* tablet_stats,
                                                 Versionstamp* versionstamp, bool snapshot) {
    TabletStatsPB load_stats, compact_stats;
    Versionstamp load_version, compact_version;
    TxnErrorCode err = get_tablet_load_stats(txn, tablet_id, &load_stats, &load_version, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    err = get_tablet_compact_stats(txn, tablet_id, &compact_stats, &compact_version, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    if (tablet_stats) {
        tablet_stats->set_base_compaction_cnt(compact_stats.base_compaction_cnt());
        tablet_stats->set_cumulative_compaction_cnt(compact_stats.cumulative_compaction_cnt());
        tablet_stats->set_cumulative_point(compact_stats.cumulative_point());
        tablet_stats->set_last_base_compaction_time_ms(
                compact_stats.last_base_compaction_time_ms());
        tablet_stats->set_last_cumu_compaction_time_ms(
                compact_stats.last_cumu_compaction_time_ms());
        tablet_stats->set_full_compaction_cnt(compact_stats.full_compaction_cnt());
        tablet_stats->set_last_full_compaction_time_ms(
                compact_stats.last_full_compaction_time_ms());

        tablet_stats->set_num_rows(load_stats.num_rows() + compact_stats.num_rows());
        tablet_stats->set_num_rowsets(load_stats.num_rowsets() + compact_stats.num_rowsets());
        tablet_stats->set_num_segments(load_stats.num_segments() + compact_stats.num_segments());
        tablet_stats->set_data_size(load_stats.data_size() + compact_stats.data_size());
        tablet_stats->set_index_size(load_stats.index_size() + compact_stats.index_size());
        tablet_stats->set_segment_size(load_stats.segment_size() + compact_stats.segment_size());
        if (load_stats.has_idx()) {
            tablet_stats->mutable_idx()->CopyFrom(load_stats.idx());
        } else if (compact_stats.has_idx()) {
            tablet_stats->mutable_idx()->CopyFrom(compact_stats.idx());
        }
    }
    Versionstamp read_version = std::min(load_version, compact_version);
    if (versionstamp) {
        *versionstamp = read_version;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, read_version);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_index(int64_t tablet_id, TabletIndexPB* tablet_index,
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

TxnErrorCode MetaReader::get_tablet_index(Transaction* txn, int64_t tablet_id,
                                          TabletIndexPB* tablet_index, bool snapshot) {
    std::string tablet_index_key = versioned::tablet_index_key({instance_id_, tablet_id});
    std::string value;
    TxnErrorCode err = txn->get(tablet_index_key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (tablet_index && !tablet_index->ParseFromString(value)) {
        LOG_ERROR("Failed to parse TabletIndexPB")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("key", hex(tablet_index_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_table_versions(
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

TxnErrorCode MetaReader::get_table_versions(
        Transaction* txn, const std::vector<int64_t>& table_ids,
        std::unordered_map<int64_t, Versionstamp>* table_versions, bool snapshot) {
    if (table_ids.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    std::vector<std::string> version_keys;
    for (size_t i = 0; i < table_ids.size(); ++i) {
        int64_t table_id = table_ids[i];
        std::string table_version_key = versioned::table_version_key({instance_id_, table_id});
        version_keys.push_back(std::move(table_version_key));
    }

    std::vector<std::optional<std::pair<std::string, Versionstamp>>> versioned_values;
    TxnErrorCode err =
            versioned_batch_get(txn, version_keys, snapshot_version_, &versioned_values, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (size_t i = 0; i < versioned_values.size(); ++i) {
        const auto& kv = versioned_values[i];
        if (!kv.has_value()) {
            continue; // Key not found, skip
        }

        Versionstamp version = kv->second;
        int64_t table_id = table_ids[i];
        table_versions->emplace(table_id, version);
        min_read_versionstamp_ = std::min(min_read_versionstamp_, version);
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_partition_versions(
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

TxnErrorCode MetaReader::get_partition_versions(
        Transaction* txn, const std::vector<int64_t>& partition_ids,
        std::unordered_map<int64_t, VersionPB>* versions,
        std::unordered_map<int64_t, Versionstamp>* versionstamps, bool snapshot) {
    if (partition_ids.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    std::vector<std::string> version_keys;
    for (size_t i = 0; i < partition_ids.size(); ++i) {
        int64_t partition_id = partition_ids[i];
        std::string partition_version_key =
                versioned::partition_version_key({instance_id_, partition_id});
        version_keys.push_back(std::move(partition_version_key));
    }

    std::vector<std::optional<std::pair<std::string, Versionstamp>>> versioned_values;
    TxnErrorCode err =
            versioned_batch_get(txn, version_keys, snapshot_version_, &versioned_values, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (size_t i = 0; i < versioned_values.size(); ++i) {
        const auto& kv = versioned_values[i];
        if (!kv.has_value()) {
            continue; // Key not found, skip
        }

        const std::string& value = kv->first;
        Versionstamp versionstamp = kv->second;
        int64_t partition_id = partition_ids[i];

        if (versionstamps) {
            versionstamps->emplace(partition_id, versionstamp);
        }
        min_read_versionstamp_ = std::min(min_read_versionstamp_, versionstamp);

        if (versions) {
            VersionPB version;
            if (!version.ParseFromString(value)) {
                LOG_ERROR("Failed to parse VersionPB")
                        .tag("instance_id", instance_id_)
                        .tag("partition_id", partition_id)
                        .tag("key", hex(version_keys[i]))
                        .tag("value", hex(value));
                return TxnErrorCode::TXN_INVALID_DATA;
            }
            versions->emplace(partition_id, std::move(version));
        }
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_partition_versions(Transaction* txn,
                                                const std::vector<int64_t>& partition_ids,
                                                std::unordered_map<int64_t, int64_t>* versions,
                                                int64_t* last_pending_txn_id, bool snapshot) {
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

TxnErrorCode MetaReader::get_rowset_metas(int64_t tablet_id, int64_t start_version,
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

TxnErrorCode MetaReader::get_rowset_metas(Transaction* txn, int64_t tablet_id,
                                          int64_t start_version, int64_t end_version,
                                          std::vector<RowsetMetaCloudPB>* rowset_metas,
                                          bool snapshot) {
    std::map<int64_t, RowsetMetaCloudPB> rowset_graph;

    {
        std::string start_key =
                versioned::meta_rowset_load_key({instance_id_, tablet_id, start_version});
        std::string end_key =
                versioned::meta_rowset_load_key({instance_id_, tablet_id, end_version});

        // [start, end]
        versioned::ReadDocumentMessagesOptions options;
        options.snapshot = snapshot;
        options.snapshot_version = snapshot_version_;
        options.exclude_begin_key = false;
        options.exclude_end_key = false;

        auto iter =
                versioned::document_get_range<RowsetMetaCloudPB>(txn, start_key, end_key, options);
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, version, rowset_meta] = *kvp;
            rowset_graph.emplace(rowset_meta.end_version(), std::move(rowset_meta));
            min_read_versionstamp_ = std::min(min_read_versionstamp_, version);
            DCHECK(version < snapshot_version_)
                    << "version: " << version.to_string()
                    << ", snapshot_version: " << snapshot_version_.to_string();
        }
        if (!iter->is_valid()) {
            LOG_ERROR("failed to get loaded rowset metas")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id)
                    .tag("start_version", start_version)
                    .tag("end_version", end_version)
                    .tag("error_code", iter->error_code());
            return iter->error_code();
        }
    }

    {
        std::string start_key =
                versioned::meta_rowset_compact_key({instance_id_, tablet_id, start_version});
        std::string end_key =
                versioned::meta_rowset_compact_key({instance_id_, tablet_id, end_version});

        // [start, end]
        versioned::ReadDocumentMessagesOptions options;
        options.snapshot = snapshot;
        options.snapshot_version = snapshot_version_;
        options.exclude_begin_key = false;
        options.exclude_end_key = false;

        int64_t last_start_version = std::numeric_limits<int64_t>::max();
        auto iter =
                versioned::document_get_range<RowsetMetaCloudPB>(txn, start_key, end_key, options);
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, version, rowset_meta] = *kvp;
            DCHECK(version < snapshot_version_)
                    << "version: " << version.to_string()
                    << ", snapshot_version: " << snapshot_version_.to_string();

            int64_t start_version = rowset_meta.start_version();
            int64_t end_version = rowset_meta.end_version();
            if (last_start_version <= start_version) {
                // This compact rowset has been covered by a large compact rowset
                continue;
            }

            min_read_versionstamp_ = std::min(min_read_versionstamp_, version);
            last_start_version = start_version;
            // erase the rowsets that are covered by this compact rowset
            rowset_graph.erase(rowset_graph.lower_bound(start_version),
                               rowset_graph.upper_bound(end_version));
            rowset_graph.emplace(end_version, std::move(rowset_meta));
        }
        if (!iter->is_valid()) {
            LOG_ERROR("failed to get compacted rowset metas")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id)
                    .tag("start_version", start_version)
                    .tag("end_version", end_version)
                    .tag("error_code", iter->error_code());
            return iter->error_code();
        }
    }

    rowset_metas->clear();
    rowset_metas->reserve(rowset_graph.size());
    for (auto&& [version, rowset_meta] : rowset_graph) {
        rowset_metas->emplace_back(std::move(rowset_meta));
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_load_rowset_meta(int64_t tablet_id, int64_t version,
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
    return get_load_rowset_meta(txn.get(), tablet_id, version, rowset_meta, snapshot);
}

TxnErrorCode MetaReader::get_load_rowset_meta(Transaction* txn, int64_t tablet_id, int64_t version,
                                              RowsetMetaCloudPB* rowset_meta, bool snapshot) {
    std::string load_rowset_key =
            versioned::meta_rowset_load_key({instance_id_, tablet_id, version});
    Versionstamp versionstamp;
    TxnErrorCode err = versioned::document_get(txn, load_rowset_key, snapshot_version_, rowset_meta,
                                               &versionstamp, snapshot);
    if (err == TxnErrorCode::TXN_OK) {
        min_read_versionstamp_ = std::min(min_read_versionstamp_, versionstamp);
    }
    return err;
}

TxnErrorCode MetaReader::get_tablet_indexes(
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

TxnErrorCode MetaReader::get_tablet_indexes(
        Transaction* txn, const std::vector<int64_t>& tablet_ids,
        std::unordered_map<int64_t, TabletIndexPB>* tablet_indexes, bool snapshot) {
    if (tablet_ids.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    std::vector<std::string> index_keys;
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        int64_t tablet_id = tablet_ids[i];
        std::string tablet_index_key = versioned::tablet_index_key({instance_id_, tablet_id});
        index_keys.push_back(std::move(tablet_index_key));
    }

    std::vector<std::optional<std::string>> values;
    Transaction::BatchGetOptions options;
    options.snapshot = snapshot;
    TxnErrorCode err = txn->batch_get(&values, index_keys, options);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (size_t i = 0; i < values.size(); ++i) {
        const auto& kv = values[i];
        if (!kv.has_value()) {
            continue; // Key not found, skip
        }

        const std::string& value = kv.value();
        int64_t tablet_id = tablet_ids[i];

        TabletIndexPB tablet_index;
        if (!tablet_index.ParseFromString(value)) {
            LOG_ERROR("Failed to parse TabletIndexPB")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id)
                    .tag("key", hex(index_keys[i]))
                    .tag("value", hex(value));
            return TxnErrorCode::TXN_INVALID_DATA;
        }
        tablet_indexes->emplace(tablet_id, std::move(tablet_index));
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_partition_pending_txn_id(int64_t partition_id, int64_t* first_txn_id,
                                                      int64_t* partition_version, bool snapshot) {
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

TxnErrorCode MetaReader::get_partition_pending_txn_id(Transaction* txn, int64_t partition_id,
                                                      int64_t* first_txn_id,
                                                      int64_t* partition_version, bool snapshot) {
    // Initialize to -1 to indicate no pending transactions
    *first_txn_id = -1;

    VersionPB version_pb;
    Versionstamp versionstamp;
    TxnErrorCode err =
            get_partition_version(txn, partition_id, &version_pb, &versionstamp, snapshot);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // No version found, no pending transactions
        return TxnErrorCode::TXN_OK;
    } else if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    min_read_versionstamp_ = std::min(min_read_versionstamp_, versionstamp);
    if (version_pb.pending_txn_ids_size() > 0) {
        *first_txn_id = version_pb.pending_txn_ids(0);
    }
    *partition_version = version_pb.version();

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_index_index(int64_t index_id, IndexIndexPB* index, bool snapshot) {
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

TxnErrorCode MetaReader::get_index_index(Transaction* txn, int64_t index_id, IndexIndexPB* index,
                                         bool snapshot) {
    std::string index_index_key = versioned::index_index_key({instance_id_, index_id});
    std::string value;
    TxnErrorCode err = txn->get(index_index_key, &value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (index && !index->ParseFromString(value)) {
        LOG_ERROR("Failed to parse IndexIndexPB")
                .tag("instance_id", instance_id_)
                .tag("index_id", index_id)
                .tag("key", hex(index_index_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_partition_index(int64_t partition_id,
                                             PartitionIndexPB* partition_index, bool snapshot) {
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

TxnErrorCode MetaReader::get_partition_index(Transaction* txn, int64_t partition_id,
                                             PartitionIndexPB* partition_index, bool snapshot) {
    std::string partition_index_key = versioned::partition_index_key({instance_id_, partition_id});
    std::string value;
    TxnErrorCode err = txn->get(partition_index_key, &value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (partition_index && !partition_index->ParseFromString(value)) {
        LOG_ERROR("Failed to parse PartitionIndexPB")
                .tag("instance_id", instance_id_)
                .tag("partition_id", partition_id)
                .tag("key", hex(partition_index_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::is_index_exists(int64_t index_id, bool snapshot) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return is_index_exists(txn.get(), index_id, snapshot);
}

TxnErrorCode MetaReader::is_index_exists(Transaction* txn, int64_t index_id, bool snapshot) {
    std::string key = versioned::meta_index_key({instance_id_, index_id});
    std::string value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, key, snapshot_version_, &key_version, &value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::is_partition_exists(int64_t partition_id, bool snapshot) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return is_partition_exists(txn.get(), partition_id, snapshot);
}

TxnErrorCode MetaReader::is_partition_exists(Transaction* txn, int64_t partition_id,
                                             bool snapshot) {
    std::string key = versioned::meta_partition_key({instance_id_, partition_id});
    std::string value;
    Versionstamp key_version;
    TxnErrorCode err = versioned_get(txn, key, snapshot_version_, &key_version, &value, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    min_read_versionstamp_ = std::min(min_read_versionstamp_, key_version);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_snapshots(
        Transaction* txn, std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots) {
    std::string snapshot_key = versioned::snapshot_full_key({instance_id_});
    std::string snapshot_start_key = encode_versioned_key(snapshot_key, Versionstamp::min());
    std::string snapshot_end_key = encode_versioned_key(snapshot_key, Versionstamp::max());

    FullRangeGetOptions range_options;
    range_options.prefetch = true;
    auto it = txn->full_range_get(snapshot_start_key, snapshot_end_key, range_options);
    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, snapshot_value] = *kvp;

        Versionstamp version;
        std::string_view key_view(key);
        if (decode_tailing_versionstamp_end(&key_view) ||
            decode_tailing_versionstamp(&key_view, &version)) {
            LOG_WARNING("failed to decode versionstamp from snapshot full key")
                    .tag("instance_id", instance_id_)
                    .tag("key", hex(key));
            return TxnErrorCode::TXN_INVALID_DATA;
        }

        SnapshotPB snapshot;
        if (!snapshot.ParseFromArray(snapshot_value.data(), snapshot_value.size())) {
            LOG_ERROR("Failed to parse SnapshotPB")
                    .tag("instance_id", instance_id_)
                    .tag("key", hex(key));
            return TxnErrorCode::TXN_INVALID_DATA;
        }

        snapshots->emplace_back(std::move(snapshot), version);
    }

    if (!it->is_valid()) {
        LOG_ERROR("failed to get snapshots")
                .tag("instance_id", instance_id_)
                .tag("error_code", it->error_code());
        return it->error_code();
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_snapshots(
        std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_snapshots(txn.get(), snapshots);
}

TxnErrorCode MetaReader::has_snapshot_references(Versionstamp snapshot_version,
                                                 bool* has_references, bool snapshot) {
    DCHECK(txn_kv_) << "TxnKv must be set before calling";
    if (!txn_kv_) {
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return has_snapshot_references(txn.get(), snapshot_version, has_references, snapshot);
}

TxnErrorCode MetaReader::has_snapshot_references(Transaction* txn, Versionstamp snapshot_version,
                                                 bool* has_references, bool snapshot) {
    std::string snapshot_ref_key_start =
            versioned::snapshot_reference_key_prefix(instance_id_, snapshot_version);
    std::string snapshot_ref_key_end = snapshot_ref_key_start + '\xFF';

    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(snapshot_ref_key_start, snapshot_ref_key_end, &it, snapshot, 1);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    *has_references = it->has_next();
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_load_rowset_metas(
        int64_t tablet_id, std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>>* rowset_metas,
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
    return get_load_rowset_metas(txn.get(), tablet_id, rowset_metas, snapshot);
}

TxnErrorCode MetaReader::get_load_rowset_metas(
        Transaction* txn, int64_t tablet_id,
        std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>>* rowset_metas, bool snapshot) {
    std::string start_key = versioned::meta_rowset_load_key({instance_id_, tablet_id, 0});
    std::string end_key = versioned::meta_rowset_load_key(
            {instance_id_, tablet_id, std::numeric_limits<int64_t>::max()});

    versioned::ReadDocumentMessagesOptions options;
    options.snapshot = snapshot;
    options.snapshot_version = snapshot_version_;
    options.exclude_begin_key = false;
    options.exclude_end_key = false;

    auto iter = versioned::document_get_range<RowsetMetaCloudPB>(txn, start_key, end_key, options);
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, version, rowset_meta] = *kvp;
        rowset_metas->emplace_back(std::move(rowset_meta), version);
        min_read_versionstamp_ = std::min(min_read_versionstamp_, version);
        DCHECK(version < snapshot_version_)
                << "version: " << version.to_string()
                << ", snapshot_version: " << snapshot_version_.to_string();
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get load rowset metas")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("error_code", iter->error_code());
        return iter->error_code();
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_compact_rowset_metas(
        int64_t tablet_id, std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>>* rowset_metas,
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
    return get_compact_rowset_metas(txn.get(), tablet_id, rowset_metas, snapshot);
}

TxnErrorCode MetaReader::get_compact_rowset_metas(
        Transaction* txn, int64_t tablet_id,
        std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>>* rowset_metas, bool snapshot) {
    std::string start_key = versioned::meta_rowset_compact_key({instance_id_, tablet_id, 0});
    std::string end_key = versioned::meta_rowset_compact_key(
            {instance_id_, tablet_id, std::numeric_limits<int64_t>::max()});

    versioned::ReadDocumentMessagesOptions options;
    options.snapshot = snapshot;
    options.snapshot_version = snapshot_version_;
    options.exclude_begin_key = false;
    options.exclude_end_key = false;

    auto iter = versioned::document_get_range<RowsetMetaCloudPB>(txn, start_key, end_key, options);
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, version, rowset_meta] = *kvp;
        rowset_metas->emplace_back(std::move(rowset_meta), version);
        min_read_versionstamp_ = std::min(min_read_versionstamp_, version);
        DCHECK(version < snapshot_version_)
                << "version: " << version.to_string()
                << ", snapshot_version: " << snapshot_version_.to_string();
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get compact rowset metas")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("error_code", iter->error_code());
        return iter->error_code();
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::has_no_indexes(int64_t db_id, int64_t table_id, bool* no_indexes,
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

TxnErrorCode MetaReader::has_no_indexes(Transaction* txn, int64_t db_id, int64_t table_id,
                                        bool* no_indexes, bool snapshot) {
    std::string start_key = versioned::index_inverted_key({instance_id_, db_id, table_id, 0});
    std::string end_key = versioned::index_inverted_key(
            {instance_id_, db_id, table_id, std::numeric_limits<int64_t>::max()});

    FullRangeGetOptions options;
    options.prefetch = false;
    options.exact_limit = 1; // We only need to know if there is at least one index
    options.snapshot = snapshot;
    auto it = txn->full_range_get(start_key, end_key, options);
    if (it->has_next()) {
        *no_indexes = false;
        return TxnErrorCode::TXN_OK;
    } else if (it->is_valid()) {
        *no_indexes = true;
        return TxnErrorCode::TXN_OK;
    } else {
        return it->error_code();
    }
}

} // namespace doris::cloud
