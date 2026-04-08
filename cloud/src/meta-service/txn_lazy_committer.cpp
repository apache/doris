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

#include "txn_lazy_committer.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <cerrno>
#include <chrono>
#include <random>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/stats.h"
#include "common/sync_executor.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "resource-manager/resource_manager.h"

using namespace std::chrono;

namespace doris::cloud {

void get_txn_db_id(TxnKv* txn_kv, const std::string& instance_id, int64_t txn_id,
                   MetaServiceCode& code, std::string& msg, int64_t* db_id, KVStats* stats);

void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta,
        KVStats* stats);

void update_tablet_stats(const StatsTabletKeyInfo& info, const TabletStats& stats,
                         std::unique_ptr<Transaction>& txn, MetaServiceCode& code,
                         std::string& msg);

static uint64_t generate_unique_seed() {
    if (config::txn_lazy_commit_shuffle_seed != 0) {
        return config::txn_lazy_commit_shuffle_seed;
    } else {
        auto now = high_resolution_clock::now();
        return duration_cast<nanoseconds>(now.time_since_epoch()).count();
    }
}

// Get the partition version from the txn kv store.
// If is_versioned_read is true, use versioned get to read the partition version with versionstamp.
static std::pair<MetaServiceCode, std::string> get_partition_version(
        Transaction* txn, std::string_view instance_id, int64_t txn_id, int64_t db_id,
        int64_t table_id, int64_t partition_id, VersionPB* partition_version,
        Versionstamp* versionstamp, bool is_versioned_read) {
    std::string partition_key, partition_version_value;
    if (is_versioned_read) {
        partition_key = versioned::partition_version_key({instance_id, partition_id});
        TxnErrorCode err =
                versioned_get(txn, partition_key, versionstamp, &partition_version_value);
        if (err != TxnErrorCode::TXN_OK) {
            MetaServiceCode code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                           ? MetaServiceCode::TXN_ID_NOT_FOUND
                                           : cast_as<ErrCategory::READ>(err);
            return {code, fmt::format("failed to get partition version, txn_id={}, key={} err={}",
                                      txn_id, hex(partition_key), err)};
        }

    } else {
        partition_key = partition_version_key({instance_id, db_id, table_id, partition_id});
        TxnErrorCode err = txn->get(partition_key, &partition_version_value);
        if (err != TxnErrorCode::TXN_OK) {
            MetaServiceCode code = cast_as<ErrCategory::READ>(err);
            return {code,
                    fmt::format("failed to get partition version, txn_id={}, key={} err={}, "
                                "db_id={}, table_id={}, partition_id={}",
                                txn_id, hex(partition_key), err, db_id, table_id, partition_id)};
        }
    }
    if (!partition_version->ParseFromString(partition_version_value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                fmt::format("failed to parse partition version pb, txn_id={}, key={}", txn_id,
                            hex(partition_key))};
    }

    return {MetaServiceCode::OK, ""};
}

static std::pair<MetaServiceCode, std::string> get_partition_version(
        TxnKv* txn_kv, std::string_view instance_id, int64_t txn_id, int64_t db_id,
        int64_t table_id, int64_t partition_id, VersionPB* partition_version,
        Versionstamp* versionstamp, bool is_versioned_read) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err),
                fmt::format("failed to create txn, txn_id={}, err={}", txn_id, err)};
    }

    return get_partition_version(txn.get(), instance_id, txn_id, db_id, table_id, partition_id,
                                 partition_version, versionstamp, is_versioned_read);
}

static std::pair<MetaServiceCode, std::string> get_txn_info(TxnKv* txn_kv,
                                                            const std::string& instance_id,
                                                            int64_t db_id, int64_t txn_id,
                                                            TxnInfoPB* txn_info) {
    std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_value;

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err),
                fmt::format("failed to create txn, txn_id={}, err={}", txn_id, err)};
    }

    err = txn->get(info_key, &info_value);
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                       ? MetaServiceCode::TXN_ID_NOT_FOUND
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to get txn info, key={}, err={}", hex(info_key), err)};
    }

    if (!txn_info->ParseFromString(info_value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                fmt::format("failed to parse txn info pb, key={}", hex(info_key))};
    }

    return {MetaServiceCode::OK, ""};
}

static std::pair<MetaServiceCode, std::string> get_tablet_index(
        CloneChainReader& meta_reader, Transaction* txn, std::string_view instance_id,
        int64_t txn_id, int64_t tablet_id, TabletIndexPB* tablet_idx, bool is_versioned_read) {
    std::stringstream ss;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;

    if (!is_versioned_read) {
        std::string tablet_idx_key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string tablet_idx_val;
        TxnErrorCode err = txn->get(tablet_idx_key, &tablet_idx_val, true);
        if (TxnErrorCode::TXN_OK != err) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get tablet idx, txn_id=" << txn_id << " key=" << hex(tablet_idx_key)
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return {code, msg};
        }

        if (!tablet_idx->ParseFromString(tablet_idx_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse tablet idx pb txn_id=" << txn_id
               << " key=" << hex(tablet_idx_key);
            msg = ss.str();
            return {code, msg};
        }
    } else {
        TxnErrorCode err = meta_reader.get_tablet_index(txn, tablet_id, tablet_idx);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get tablet index, txn_id=" << txn_id << " tablet_id=" << tablet_id
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return {code, msg};
        }
    }
    return {code, msg};
}

static std::pair<MetaServiceCode, std::string> get_tablet_index(
        CloneChainReader& meta_reader, TxnKv* txn_kv, std::string_view instance_id, int64_t txn_id,
        int64_t tablet_id, TabletIndexPB* tablet_idx, bool is_versioned_read) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err),
                fmt::format("failed to create txn, txn_id={}, err={}", txn_id, err)};
    }
    return get_tablet_index(meta_reader, txn.get(), instance_id, txn_id, tablet_id, tablet_idx,
                            is_versioned_read);
}

void convert_tmp_rowsets(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta,
        std::map<int64_t, TabletIndexPB>& tablet_ids, bool is_versioned_write,
        bool is_versioned_read, Versionstamp versionstamp, ResourceManager* resource_mgr,
        bool defer_deleting_pending_delete_bitmaps) {
    std::stringstream ss;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    CloneChainReader meta_reader(instance_id, txn_kv.get(), resource_mgr);

    // partition_id -> VersionPB
    std::unordered_map<int64_t, VersionPB> partition_versions;
    // tablet_id -> stats
    std::unordered_map<int64_t, TabletStats> tablet_stats;

    OperationLogPB op_log;
    if (is_versioned_write) {
        std::string log_key = encode_versioned_key(versioned::log_key({instance_id}), versionstamp);
        ValueBuf log_value;
        err = blob_get(txn.get(), log_key, &log_value);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get operation log, txn_id=" << txn_id << " key=" << hex(log_key)
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        if (!log_value.to_pb(&op_log)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse operation log pb, txn_id=" << txn_id << " key=" << hex(log_key);
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
    }

    int64_t rowsets_visible_ts_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowsets_meta) {
        std::string tmp_rowst_data;
        err = txn->get(tmp_rowset_key, &tmp_rowst_data);
        if (TxnErrorCode::TXN_KEY_NOT_FOUND == err) {
            // the tmp rowset has been converted
            VLOG_DEBUG << "tmp rowset has been converted, key=" << hex(tmp_rowset_key);
            continue;
        }

        if (TxnErrorCode::TXN_OK != err) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get tmp_rowset_key, txn_id=" << txn_id
               << " key=" << hex(tmp_rowset_key) << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        if (!tablet_ids.contains(tmp_rowset_pb.tablet_id())) {
            TabletIndexPB tablet_idx_pb;
            std::tie(code, msg) =
                    get_tablet_index(meta_reader, txn.get(), instance_id, txn_id,
                                     tmp_rowset_pb.tablet_id(), &tablet_idx_pb, is_versioned_read);
            if (code != MetaServiceCode::OK) {
                return;
            }
            tablet_ids.emplace(tmp_rowset_pb.tablet_id(), tablet_idx_pb);
        }
        const TabletIndexPB& tablet_idx_pb = tablet_ids[tmp_rowset_pb.tablet_id()];

        if (!partition_versions.contains(tmp_rowset_pb.partition_id())) {
            VersionPB version_pb;
            if (!is_versioned_read) {
                std::string ver_val;
                std::string ver_key =
                        partition_version_key({instance_id, db_id, tablet_idx_pb.table_id(),
                                               tmp_rowset_pb.partition_id()});
                err = txn->get(ver_key, &ver_val);
                if (TxnErrorCode::TXN_OK != err) {
                    code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                   ? MetaServiceCode::TXN_ID_NOT_FOUND
                                   : cast_as<ErrCategory::READ>(err);
                    ss << "failed to get partiton version, txn_id=" << txn_id
                       << " key=" << hex(ver_key) << " err=" << err;
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
                if (!version_pb.ParseFromString(ver_val)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse version pb txn_id=" << txn_id << " key=" << hex(ver_key);
                    msg = ss.str();
                    return;
                }
                LOG(INFO) << "txn_id=" << txn_id << " key=" << hex(ver_key)
                          << " version_pb:" << version_pb.ShortDebugString();
            } else {
                err = meta_reader.get_partition_version(txn.get(), tmp_rowset_pb.partition_id(),
                                                        &version_pb, nullptr);
                if (err != TxnErrorCode::TXN_OK) {
                    code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                   ? MetaServiceCode::TXN_ID_NOT_FOUND
                                   : cast_as<ErrCategory::READ>(err);
                    ss << "failed to get partition version, txn_id=" << txn_id
                       << " partition_id=" << tmp_rowset_pb.partition_id() << " err=" << err;
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
                LOG(INFO) << "txn_id=" << txn_id << " partition_id=" << tmp_rowset_pb.partition_id()
                          << " version_pb:" << version_pb.ShortDebugString();
            }

            if (version_pb.pending_txn_ids_size() == 0 || version_pb.pending_txn_ids(0) != txn_id) {
                LOG(INFO) << "txn_id=" << txn_id << " partition_id=" << tmp_rowset_pb.partition_id()
                          << " tmp_rowset_key=" << hex(tmp_rowset_key)
                          << " version has already been converted."
                          << " version_pb:" << version_pb.ShortDebugString();
                TEST_SYNC_POINT_CALLBACK("convert_tmp_rowsets::already_been_converted",
                                         &version_pb);
                return;
            }

            partition_versions.emplace(tmp_rowset_pb.partition_id(), version_pb);
            DCHECK_EQ(partition_versions.size(), 1) << partition_versions.size();
        }

        const VersionPB& version_pb = partition_versions[tmp_rowset_pb.partition_id()];
        DCHECK(version_pb.pending_txn_ids_size() > 0);
        DCHECK_EQ(version_pb.pending_txn_ids(0), txn_id);

        int64_t version = version_pb.has_version() ? (version_pb.version() + 1) : 2;

        std::string rowset_key = meta_rowset_key({instance_id, tmp_rowset_pb.tablet_id(), version});
        std::string rowset_val;
        if (!is_versioned_read) {
            err = txn->get(rowset_key, &rowset_val);
            if (TxnErrorCode::TXN_OK == err) {
                // tmp rowset key has been converted
                continue;
            }
            if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "failed to get rowset_key, txn_id=" << txn_id << " key=" << hex(rowset_key)
                   << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            DCHECK(err == TxnErrorCode::TXN_KEY_NOT_FOUND);
        } else {
            int64_t tablet_id = tmp_rowset_pb.tablet_id();
            RowsetMetaCloudPB rowset_val_pb;
            err = meta_reader.get_load_rowset_meta(txn.get(), tablet_id, version, &rowset_val_pb);
            if (TxnErrorCode::TXN_OK == err) {
                // tmp rowset key has been converted
                continue;
            }
            if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "failed to get load_rowset_key, txn_id=" << txn_id
                   << " tablet_id=" << tablet_id << " version=" << version << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            DCHECK(err == TxnErrorCode::TXN_KEY_NOT_FOUND);
        }

        tmp_rowset_pb.set_start_version(version);
        tmp_rowset_pb.set_end_version(version);
        tmp_rowset_pb.set_visible_ts_ms(rowsets_visible_ts_ms);

        rowset_val.clear();
        if (!tmp_rowset_pb.SerializeToString(&rowset_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize rowset_meta, txn_id=" << txn_id
               << " key=" << hex(rowset_key);
            msg = ss.str();
            return;
        }

        txn->put(rowset_key, rowset_val);
        LOG(INFO) << "put rowset_key=" << hex(rowset_key) << " txn_id=" << txn_id
                  << " rowset_size=" << rowset_key.size() + rowset_val.size();

        // Accumulate affected rows
        auto& stats = tablet_stats[tmp_rowset_pb.tablet_id()];
        stats.data_size += tmp_rowset_pb.total_disk_size();
        stats.num_rows += tmp_rowset_pb.num_rows();
        ++stats.num_rowsets;
        stats.num_segs += tmp_rowset_pb.num_segments();
        stats.index_size += tmp_rowset_pb.index_disk_size();
        stats.segment_size += tmp_rowset_pb.data_disk_size();

        if (is_versioned_write) {
            // If this is a versioned write, we need to put the rowset with versionstamp
            RowsetMetaCloudPB copied_rowset_meta(tmp_rowset_pb);
            std::string rowset_load_key = versioned::meta_rowset_load_key(
                    {instance_id, tmp_rowset_pb.tablet_id(), version});
            if (!versioned::document_put(txn.get(), rowset_load_key, versionstamp,
                                         std::move(copied_rowset_meta))) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize rowset_meta, txn_id=" << txn_id
                   << " key=" << hex(rowset_load_key);
                msg = ss.str();
                return;
            }
            LOG(INFO) << "put versioned rowset_key=" << hex(rowset_load_key)
                      << " txn_id=" << txn_id;
        }
    }

    // Batch get existing versioned tablet stats if needed
    std::unordered_map<int64_t, TabletStatsPB> existing_versioned_stats;
    if (is_versioned_write && !tablet_stats.empty()) {
        internal_get_load_tablet_stats_batch(code, msg, meta_reader, txn.get(), instance_id,
                                             tablet_ids, &existing_versioned_stats);
        if (code != MetaServiceCode::OK) {
            LOG(WARNING) << "batch get versioned tablet stats failed, code=" << code
                         << " msg=" << msg << " txn_id=" << txn_id;
            return;
        }
        LOG(INFO) << "batch get " << existing_versioned_stats.size()
                  << " versioned tablet stats, txn_id=" << txn_id;

        int64_t min_timestamp = meta_reader.min_read_versionstamp().version();
        int64_t old_min_timestamp = op_log.min_timestamp();
        if (min_timestamp < op_log.min_timestamp()) {
            op_log.set_min_timestamp(min_timestamp);

            std::string log_key_prefix = versioned::log_key({instance_id});
            versioned::blob_put(txn.get(), log_key_prefix, versionstamp, op_log);
            LOG(INFO) << "update operation log min_timestamp from " << old_min_timestamp << " to "
                      << min_timestamp << " txn_id=" << txn_id
                      << " log_versionstamp=" << versionstamp.to_string();
        }
    }

    for (auto& [tablet_id, stats] : tablet_stats) {
        DCHECK(tablet_ids.count(tablet_id));
        auto& tablet_idx = tablet_ids[tablet_id];
        StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
        update_tablet_stats(info, stats, txn, code, msg);
        if (code != MetaServiceCode::OK) return;

        if (is_versioned_write) {
            TabletStatsPB stats_pb = existing_versioned_stats[tablet_id];
            merge_tablet_stats(stats_pb, stats);
            std::string stats_key = versioned::tablet_load_stats_key({instance_id, tablet_id});

            // put with specified versionstamp
            if (!versioned::document_put(txn.get(), stats_key, versionstamp, std::move(stats_pb))) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize versioned tablet stats";
                LOG(WARNING) << msg << " tablet_id=" << tablet_id << " txn_id=" << txn_id;
                return;
            }

            LOG(INFO) << "put versioned tablet stats key=" << hex(stats_key)
                      << " tablet_id=" << tablet_id << " txn_id=" << txn_id;
        }
    }

    if (defer_deleting_pending_delete_bitmaps) {
        for (auto& [tablet_id, _] : tablet_stats) {
            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
            txn->remove(pending_key);
            LOG(INFO) << "remove delete bitmap pending key, pending_key=" << hex(pending_key)
                      << " txn_id=" << txn_id;
        }
    }

    TEST_SYNC_POINT_RETURN_WITH_VOID("convert_tmp_rowsets::before_commit", &code);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }
}

void make_committed_txn_visible(const std::string& instance_id, int64_t db_id, int64_t txn_id,
                                std::shared_ptr<TxnKv> txn_kv, MetaServiceCode& code,
                                std::string& msg, bool defer_deleting_pending_delete_bitmaps) {
    // 1. visible txn info
    // 2. remove running key and put recycle txn key

    std::stringstream ss;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    std::string info_val;
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    VLOG_DEBUG << "txn_info:" << txn_info.ShortDebugString();
    DCHECK((txn_info.status() == TxnStatusPB::TXN_STATUS_COMMITTED) ||
           (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE));

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_COMMITTED) {
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

        if (!txn_info.SerializeToString(&info_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        txn->put(info_key, info_val);
        LOG(INFO) << "put info_key=" << hex(info_key) << " txn_id=" << txn_id;

        const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
        LOG(INFO) << "remove running_key=" << hex(running_key) << " txn_id=" << txn_id;
        txn->remove(running_key);

        // Remove delete bitmap locks if deferring deletion
        if (defer_deleting_pending_delete_bitmaps) {
            for (int64_t table_id : txn_info.table_ids()) {
                std::string lock_key =
                        meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
                // Read the lock first to check if it still belongs to the current txn
                std::string lock_val;
                TxnErrorCode err = txn->get(lock_key, &lock_val);
                if (err == TxnErrorCode::TXN_OK) {
                    DeleteBitmapUpdateLockPB lock_info;
                    if (lock_info.ParseFromString(lock_val)) {
                        // Only remove the lock if it still belongs to the current txn
                        if (lock_info.lock_id() == txn_id) {
                            txn->remove(lock_key);
                            LOG(INFO) << "remove delete bitmap lock, lock_key=" << hex(lock_key)
                                      << " table_id=" << table_id << " txn_id=" << txn_id;
                        } else {
                            LOG(WARNING) << "delete bitmap lock is held by another txn, "
                                         << "lock_key=" << hex(lock_key) << " table_id=" << table_id
                                         << " expected_txn_id=" << txn_id
                                         << " actual_lock_id=" << lock_info.lock_id();
                        }
                    }
                } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    LOG(WARNING) << "failed to get delete bitmap lock, lock_key=" << hex(lock_key)
                                 << " table_id=" << table_id << " err=" << err;
                }
            }
        }
        // The recycle txn pb will be written when recycle the commit txn log,
        // if the txn versioned write is enabled.
        if (!txn_info.versioned_write()) {
            std::string recycle_val;
            std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
            RecycleTxnPB recycle_pb;
            auto now_time = system_clock::now();
            uint64_t visible_time =
                    duration_cast<milliseconds>(now_time.time_since_epoch()).count();
            recycle_pb.set_creation_time(visible_time);
            recycle_pb.set_label(txn_info.label());

            if (!recycle_pb.SerializeToString(&recycle_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize recycle_pb, txn_id=" << txn_id;
                msg = ss.str();
                return;
            }

            txn->put(recycle_key, recycle_val);
            LOG(INFO) << "put recycle_key=" << hex(recycle_key) << " txn_id=" << txn_id;
        }
        TEST_SYNC_POINT_RETURN_WITH_VOID("TxnLazyCommitTask::make_committed_txn_visible::commit",
                                         &code);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }
    }
}

TxnLazyCommitTask::TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                                     std::shared_ptr<TxnKv> txn_kv,
                                     TxnLazyCommitter* txn_lazy_committer)
        : instance_id_(instance_id),
          txn_id_(txn_id),
          txn_kv_(txn_kv),
          txn_lazy_committer_(txn_lazy_committer) {
    DCHECK(txn_id > 0);
}

void TxnLazyCommitTask::commit() {
    StopWatch sw;
    DORIS_CLOUD_DEFER {
        {
            std::unique_lock lock(mutex_);
            this->finished_ = true;
        }
        this->cond_.notify_all();
        g_bvar_txn_lazy_committer_committing_duration << sw.elapsed_us();
    };

    int64_t db_id;
    get_txn_db_id(txn_kv_.get(), instance_id_, txn_id_, code_, msg_, &db_id, nullptr);
    if (code_ != MetaServiceCode::OK) {
        LOG(WARNING) << "get_txn_db_id failed, txn_id=" << txn_id_ << " code=" << code_
                     << " msg=" << msg_;
        return;
    }

    TxnInfoPB txn_info;
    std::tie(code_, msg_) = get_txn_info(txn_kv_.get(), instance_id_, db_id, txn_id_, &txn_info);
    if (code_ != MetaServiceCode::OK) {
        LOG(WARNING) << "get_txn_info failed, txn_id=" << txn_id_ << " code=" << code_
                     << " msg=" << msg_;
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        // The txn has been committed and made visible, no need to commit again.
        LOG(INFO) << "txn_id=" << txn_id_ << " is already visible, skip commit";
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        // The txn has been aborted, no need to commit again.
        code_ = MetaServiceCode::TXN_ALREADY_ABORTED;
        LOG(INFO) << "txn_id=" << txn_id_ << " is already aborted, skip commit";
        return;
    }

    bool defer_deleting_pending_delete_bitmaps = txn_info.defer_deleting_pending_delete_bitmaps();
    bool is_versioned_write = txn_info.versioned_write();
    bool is_versioned_read = txn_info.versioned_read();
    CloneChainReader meta_reader(instance_id_, txn_kv_.get(),
                                 txn_lazy_committer_->resource_manager().get());

    std::stringstream ss;
    int retry_times = 0;
    do {
        LOG(INFO) << "lazy task commit txn_id=" << txn_id_ << " retry_times=" << retry_times;
        do {
            code_ = MetaServiceCode::OK;
            msg_.clear();

            std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> all_tmp_rowset_metas;
            scan_tmp_rowset(instance_id_, txn_id_, txn_kv_, code_, msg_, &all_tmp_rowset_metas,
                            nullptr);
            if (code_ != MetaServiceCode::OK) {
                LOG(WARNING) << "scan_tmp_rowset failed, txn_id=" << txn_id_ << " code=" << code_;
                break;
            }
            VLOG_DEBUG << "txn_id=" << txn_id_
                       << " tmp_rowset_metas.size()=" << all_tmp_rowset_metas.size();
            if (all_tmp_rowset_metas.size() == 0) {
                LOG(INFO) << "empty tmp_rowset_metas, txn_id=" << txn_id_;
            }

            // <partition_id, tmp_rowsets>
            std::map<int64_t, std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>>
                    partition_to_tmp_rowset_metas;
            for (auto& [tmp_rowset_key, tmp_rowset_pb] : all_tmp_rowset_metas) {
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].emplace_back();
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].back().first =
                        tmp_rowset_key;
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].back().second =
                        tmp_rowset_pb;
            }

            // Shuffle partition ids to reduce the conflict probability
            std::vector<int64_t> partition_ids;
            for (const auto& [partition_id, _] : partition_to_tmp_rowset_metas) {
                partition_ids.push_back(partition_id);
            }
            if (config::txn_lazy_commit_shuffle_partitions) {
                std::mt19937 rng(generate_unique_seed());
                std::shuffle(partition_ids.begin(), partition_ids.end(), rng);
            }

            if (config::enable_cloud_parallel_txn_lazy_commit) {
                SyncExecutor<std::pair<MetaServiceCode, std::string>> executor(
                        txn_lazy_committer_->parallel_commit_pool(),
                        fmt::format("txn_{}_parallel_commit", txn_id_));
                for (int64_t partition_id : partition_ids) {
                    executor.add([&, partition_id, this]() {
                        return commit_partition(db_id, partition_id,
                                                partition_to_tmp_rowset_metas.at(partition_id),
                                                is_versioned_read, is_versioned_write,
                                                defer_deleting_pending_delete_bitmaps);
                    });
                }
                bool finished = false;
                auto task_results = executor.when_all(&finished);
                for (auto&& [code, msg] : task_results) {
                    if (code != MetaServiceCode::OK) {
                        code_ = code;
                        msg_ = std::move(msg);
                        break;
                    }
                }
                if (code_ == MetaServiceCode::OK && !finished) {
                    LOG(WARNING) << "some partition commit tasks not finished, txn_id=" << txn_id_;
                    code_ = MetaServiceCode::KV_TXN_CONFLICT;
                    break;
                }
            } else {
                for (int64_t partition_id : partition_ids) {
                    std::tie(code_, msg_) = commit_partition(
                            db_id, partition_id, partition_to_tmp_rowset_metas[partition_id],
                            is_versioned_read, is_versioned_write,
                            defer_deleting_pending_delete_bitmaps);
                    if (code_ != MetaServiceCode::OK) break;
                }
            }

            if (code_ != MetaServiceCode::OK) {
                LOG(WARNING) << "txn_id=" << txn_id_ << " code=" << code_ << " msg=" << msg_;
                break;
            }
            make_committed_txn_visible(instance_id_, db_id, txn_id_, txn_kv_, code_, msg_,
                                       defer_deleting_pending_delete_bitmaps);
        } while (false);
    } while (code_ == MetaServiceCode::KV_TXN_CONFLICT &&
             retry_times++ < config::txn_store_retry_times);
}

std::pair<MetaServiceCode, std::string> TxnLazyCommitTask::commit_partition(
        int64_t db_id, int64_t partition_id,
        const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowset_metas,
        bool is_versioned_read, bool is_versioned_write,
        bool defer_deleting_pending_delete_bitmaps) {
    std::stringstream ss;
    CloneChainReader meta_reader(instance_id_, txn_kv_.get(),
                                 txn_lazy_committer_->resource_manager().get());
    MetaServiceCode code;
    std::string msg;

    StopWatch sw;
    DORIS_CLOUD_DEFER {
        g_bvar_txn_lazy_committer_commit_partition_duration << sw.elapsed_us();
    };

    // tablet_id -> TabletIndexPB
    std::map<int64_t, TabletIndexPB> tablet_ids;
    int64_t table_id = -1;
    {
        DCHECK(tmp_rowset_metas.size() > 0);
        int64_t first_tablet_id = tmp_rowset_metas.begin()->second.tablet_id();
        TabletIndexPB first_tablet_index;
        std::tie(code, msg) =
                get_tablet_index(meta_reader, txn_kv_.get(), instance_id_, txn_id_, first_tablet_id,
                                 &first_tablet_index, is_versioned_read);
        if (code != MetaServiceCode::OK) {
            return {code, msg};
        }
        table_id = first_tablet_index.table_id();
        tablet_ids.emplace(first_tablet_id, first_tablet_index);
    }

    // The partition version key is constructed during the txn commit process, so the versionstamp
    // can be retrieved from the key itself.
    Versionstamp versionstamp;
    VersionPB original_partition_version;
    std::tie(code, msg) = get_partition_version(txn_kv_.get(), instance_id_, txn_id_, db_id,
                                                table_id, partition_id, &original_partition_version,
                                                &versionstamp, is_versioned_read);
    if (code != MetaServiceCode::OK) {
        return {code, msg};
    } else if (original_partition_version.pending_txn_ids_size() == 0 ||
               original_partition_version.pending_txn_ids(0) != txn_id_) {
        // The partition version does not contain the target pending txn, it might have been committed
        LOG(INFO) << "txn_id=" << txn_id_ << " partition_id=" << partition_id
                  << " version has already been converted."
                  << " version_pb:" << original_partition_version.ShortDebugString();
        TEST_SYNC_POINT_CALLBACK("TxnLazyCommitTask::commit::already_been_converted",
                                 &original_partition_version);
        return {MetaServiceCode::OK, ""};
    }

    size_t max_rowset_meta_size = 0;
    for (const auto& [_, tmp_rowset_pb] : tmp_rowset_metas) {
        max_rowset_meta_size = std::max(max_rowset_meta_size, tmp_rowset_pb.ByteSizeLong());
    }

    // fdb txn limit 10MB, we use 4MB as the max size for each batch.
    size_t max_rowsets_per_batch = config::txn_lazy_max_rowsets_per_batch;
    if (max_rowset_meta_size > 0) {
        max_rowsets_per_batch = std::min((4UL << 20) / max_rowset_meta_size,
                                         size_t(config::txn_lazy_max_rowsets_per_batch));
        TEST_SYNC_POINT_CALLBACK("TxnLazyCommitTask::commit::max_rowsets_per_batch",
                                 &max_rowsets_per_batch, &max_rowset_meta_size);
    }

    for (size_t i = 0; i < tmp_rowset_metas.size(); i += max_rowsets_per_batch) {
        size_t end = (i + max_rowsets_per_batch) > tmp_rowset_metas.size()
                             ? tmp_rowset_metas.size()
                             : i + max_rowsets_per_batch;
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>
                sub_partition_tmp_rowset_metas(tmp_rowset_metas.begin() + i,
                                               tmp_rowset_metas.begin() + end);
        convert_tmp_rowsets(instance_id_, txn_id_, txn_kv_, code, msg, db_id,
                            sub_partition_tmp_rowset_metas, tablet_ids, is_versioned_write,
                            is_versioned_read, versionstamp,
                            txn_lazy_committer_->resource_manager().get(),
                            defer_deleting_pending_delete_bitmaps);
        if (code != MetaServiceCode::OK) {
            return {code, msg};
        }
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id_ << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return {code, msg};
    }

    DCHECK(table_id > 0);
    DCHECK(partition_id > 0);

    // Notice: get the versionstamp again, since it must not be changed here.
    // But if you want to support multi pending txns in the partition version, this must be changed.
    VersionPB version_pb;
    std::tie(code, msg) =
            get_partition_version(txn.get(), instance_id_, txn_id_, db_id, table_id, partition_id,
                                  &version_pb, &versionstamp, is_versioned_read);
    if (code != MetaServiceCode::OK) {
        return {code, msg};
    }

    if (version_pb.pending_txn_ids_size() > 0 && version_pb.pending_txn_ids(0) == txn_id_) {
        DCHECK(version_pb.pending_txn_ids_size() == 1);
        version_pb.clear_pending_txn_ids();

        if (version_pb.has_version()) {
            version_pb.set_version(version_pb.version() + 1);
        } else {
            // first commit txn version is 2
            version_pb.set_version(2);
        }
        std::string ver_key = partition_version_key({instance_id_, db_id, table_id, partition_id});
        std::string ver_val;
        if (!version_pb.SerializeToString(&ver_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize version_pb when saving, txn_id=" << txn_id_;
            msg = ss.str();
            return {code, msg};
        }
        txn->put(ver_key, ver_val);
        LOG(INFO) << "put ver_key=" << hex(ver_key) << " txn_id=" << txn_id_
                  << " version_pb=" << version_pb.ShortDebugString();
        if (is_versioned_write) {
            // Update the partition version with the specified versionstamp.
            std::string versioned_key =
                    versioned::partition_version_key({instance_id_, partition_id});
            versioned_put(txn.get(), versioned_key, versionstamp, ver_val);
            LOG(INFO) << "put versioned ver_key=" << hex(versioned_key) << " txn_id=" << txn_id_
                      << " version_pb=" << version_pb.ShortDebugString();
        }

        for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowset_metas) {
            txn->remove(tmp_rowset_key);
            LOG(INFO) << "remove tmp_rowset_key=" << hex(tmp_rowset_key) << " txn_id=" << txn_id_;
        }

        TEST_SYNC_POINT_CALLBACK("TxnLazyCommitter::commit");
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id_ << " err=" << err;
            msg = ss.str();
            return {code, msg};
        }
    }
    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> TxnLazyCommitTask::wait() {
    constexpr auto WAIT_FOR_MICROSECONDS = 5 * 1000000;

    StopWatch sw;
    uint64_t round = 0;

    {
        std::unique_lock lock(mutex_);
        while (!finished_) {
            if (cond_.wait_for(lock, WAIT_FOR_MICROSECONDS) == ETIMEDOUT) {
                LOG(INFO) << "txn_id=" << txn_id_ << " wait_for 5s timeout round=" << ++round;
            }
        }
    }

    sw.pause();
    g_bvar_txn_lazy_committer_waiting_duration << sw.elapsed_us();
    if (sw.elapsed_us() > 1000000) {
        LOG(INFO) << "txn_lazy_commit task wait more than 1000ms, cost=" << sw.elapsed_us() / 1000
                  << " ms txn_id=" << txn_id_;
    }
    return std::make_pair(this->code_, this->msg_);
}

TxnLazyCommitter::TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv)
        : TxnLazyCommitter(txn_kv, std::make_shared<ResourceManager>(txn_kv)) {}

TxnLazyCommitter::TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv,
                                   std::shared_ptr<ResourceManager> resource_mgr)
        : txn_kv_(txn_kv), resource_mgr_(std::move(resource_mgr)) {
    worker_pool_ = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_num_threads,
                                                      "txn_lazy_commiter");
    worker_pool_->start();

    int32_t parallel_commit_threads = std::max(0, config::parallel_txn_lazy_commit_num_threads);
    if (parallel_commit_threads == 0) {
        parallel_commit_threads = std::thread::hardware_concurrency();
    }
    parallel_commit_pool_ =
            std::make_shared<SimpleThreadPool>(parallel_commit_threads, "parallel_commit");
    parallel_commit_pool_->start();
}

/**
 * @brief Submit a lazy commit txn task
 * 
 * @param instance_id
 * @param txn_id
 * @return std::shared_ptr<TxnLazyCommitTask>
 */

std::shared_ptr<TxnLazyCommitTask> TxnLazyCommitter::submit(const std::string& instance_id,
                                                            int64_t txn_id) {
    std::shared_ptr<TxnLazyCommitTask> task;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto iter = running_tasks_.find(txn_id);
        if (iter != running_tasks_.end()) {
            return iter->second;
        }

        task = std::make_shared<TxnLazyCommitTask>(instance_id, txn_id, txn_kv_, this);
        running_tasks_.emplace(txn_id, task);
        g_bvar_txn_lazy_committer_submitted << 1;
    }

    worker_pool_->submit([task]() {
        task->commit();
        task->txn_lazy_committer_->remove(task->txn_id_);
        g_bvar_txn_lazy_committer_finished << 1;
    });
    DCHECK(task != nullptr);
    return task;
}

/**
 * @brief Remove a lazy commit txn task
 *
 * @param txn_id
 */

void TxnLazyCommitter::remove(int64_t txn_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    running_tasks_.erase(txn_id);
}

} // namespace doris::cloud
