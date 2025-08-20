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

#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/defer.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/util.h"

namespace doris::cloud {

using namespace std::chrono;

// A recycler for operation logs.
class OperationLogRecycler {
public:
    OperationLogRecycler(std::string_view instance_id, TxnKv* txn_kv, Versionstamp log_version)
            : instance_id_(instance_id), txn_kv_(txn_kv), log_version_(log_version) {}
    OperationLogRecycler(const OperationLogRecycler&) = delete;
    OperationLogRecycler& operator=(const OperationLogRecycler&) = delete;

    int recycle_commit_partition_log(const CommitPartitionLogPB& commit_partition_log);

    int recycle_drop_partition_log(const DropPartitionLogPB& drop_partition_log);

    int recycle_commit_index_log(const CommitIndexLogPB& commit_index_log);

    int recycle_drop_index_log(const DropIndexLogPB& drop_index_log);

    int recycle_commit_txn_log(const CommitTxnLogPB& commit_txn_log);

    int recycle_update_tablet_log(const UpdateTabletLogPB& update_tablet_log);

    int recycle_compaction_log(const CompactionLogPB& compaction_log);

    int commit();

private:
    int recycle_table_version(int64_t table_id);

    std::string_view instance_id_;
    TxnKv* txn_kv_;
    Versionstamp log_version_;
    std::vector<std::string> keys_to_remove_;
    std::vector<std::pair<std::string, std::string>> kvs_;
};

int OperationLogRecycler::recycle_commit_partition_log(
        const CommitPartitionLogPB& commit_partition_log) {
    int64_t table_id = commit_partition_log.table_id();
    return recycle_table_version(table_id);
}

int OperationLogRecycler::recycle_drop_partition_log(const DropPartitionLogPB& drop_partition_log) {
    for (int64_t partition_id : drop_partition_log.partition_ids()) {
        RecyclePartitionPB recycle_partition_pb;
        recycle_partition_pb.set_db_id(drop_partition_log.db_id());
        recycle_partition_pb.set_table_id(drop_partition_log.table_id());
        *recycle_partition_pb.mutable_index_id() = drop_partition_log.index_ids();
        recycle_partition_pb.set_creation_time(::time(nullptr));
        recycle_partition_pb.set_expiration(drop_partition_log.expiration());
        recycle_partition_pb.set_state(RecyclePartitionPB::DROPPED);
        std::string recycle_partition_value;
        if (!recycle_partition_pb.SerializeToString(&recycle_partition_value)) {
            LOG_WARNING("failed to serialize RecyclePartitionPB").tag("partition_id", partition_id);
            return -1;
        }
        std::string recycle_key = recycle_partition_key({instance_id_, partition_id});
        LOG_INFO("put recycle partition key")
                .tag("recycle_key", hex(recycle_key))
                .tag("partition_id", partition_id);
        kvs_.emplace_back(std::move(recycle_key), std::move(recycle_partition_value));
    }

    if (drop_partition_log.update_table_version()) {
        return recycle_table_version(drop_partition_log.table_id());
    }
    return 0;
}

int OperationLogRecycler::recycle_commit_index_log(const CommitIndexLogPB& commit_index_log) {
    if (commit_index_log.update_table_version()) {
        int64_t table_id = commit_index_log.table_id();
        return recycle_table_version(table_id);
    }
    return 0;
}

int OperationLogRecycler::recycle_drop_index_log(const DropIndexLogPB& drop_index_log) {
    for (int64_t index_id : drop_index_log.index_ids()) {
        RecycleIndexPB recycle_index_pb;
        recycle_index_pb.set_db_id(drop_index_log.db_id());
        recycle_index_pb.set_table_id(drop_index_log.table_id());
        recycle_index_pb.set_creation_time(::time(nullptr));
        recycle_index_pb.set_expiration(drop_index_log.expiration());
        recycle_index_pb.set_state(RecycleIndexPB::DROPPED);
        std::string recycle_index_value;
        if (!recycle_index_pb.SerializeToString(&recycle_index_value)) {
            LOG_WARNING("failed to serialize RecycleIndexPB").tag("index_id", index_id);
            return -1;
        }
        std::string recycle_key = recycle_index_key({instance_id_, index_id});
        kvs_.emplace_back(std::move(recycle_key), std::move(recycle_index_value));
    }
    return 0;
}

int OperationLogRecycler::recycle_commit_txn_log(const CommitTxnLogPB& commit_txn_log) {
    MetaReader meta_reader(instance_id_, txn_kv_, log_version_);

    int64_t txn_id = commit_txn_log.txn_id();
    for (const auto& [partition_id, _] : commit_txn_log.partition_version_map()) {
        Versionstamp prev_version;
        TxnErrorCode err = meta_reader.get_partition_version(partition_id, nullptr, &prev_version);
        if (err == TxnErrorCode::TXN_OK) {
            std::string partition_version_key =
                    versioned::partition_version_key({instance_id_, partition_id});
            keys_to_remove_.emplace_back(encode_versioned_key(partition_version_key, prev_version));
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("failed to get previous partition version for recycling")
                    .tag("partition_id", partition_id)
                    .tag("txn_id", txn_id)
                    .tag("error_code", err);
            return -1;
        } else {
            VLOG_DEBUG << "No previous partition version found for recycling"
                       << " partition_id=" << partition_id << " txn_id=" << txn_id;
        }
    }

    for (const auto& [tablet_id, _] : commit_txn_log.tablet_to_partition_map()) {
        Versionstamp prev_version;
        TxnErrorCode err = meta_reader.get_tablet_load_stats(tablet_id, nullptr, &prev_version);
        if (err == TxnErrorCode::TXN_OK) {
            std::string tablet_stats_key =
                    versioned::tablet_load_stats_key({instance_id_, tablet_id});
            keys_to_remove_.emplace_back(encode_versioned_key(tablet_stats_key, prev_version));
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("failed to get tablet load stats for recycling")
                    .tag("tablet_id", tablet_id)
                    .tag("txn_id", txn_id)
                    .tag("error_code", err);
            return -1;
        } else {
            VLOG_DEBUG << "No previous tablet load stats found for recycling"
                       << " tablet_id=" << tablet_id << " txn_id=" << txn_id;
        }
    }

    for (int64_t table_id : commit_txn_log.table_ids()) {
        int res = recycle_table_version(table_id);
        if (res != 0) {
            return res;
        }
    }

    int64_t db_id = commit_txn_log.db_id();
    std::string recycle_val;
    std::string recycle_key = recycle_txn_key({instance_id_, db_id, txn_id});
    RecycleTxnPB recycle_pb;
    auto now_time = system_clock::now();
    uint64_t visible_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    recycle_pb.set_creation_time(visible_time);
    recycle_pb.set_label(commit_txn_log.recycle_txn().label());

    if (!recycle_pb.SerializeToString(&recycle_val)) {
        LOG_ERROR("Failed to serialize RecycleTxnPB").tag("txn_id", txn_id).tag("db_id", db_id);
        return -1;
    }

    LOG_INFO("put recycle txn key")
            .tag("recycle_key", hex(recycle_key))
            .tag("txn_id", txn_id)
            .tag("db_id", db_id);
    kvs_.emplace_back(std::move(recycle_key), std::move(recycle_val));
    return 0;
}

int OperationLogRecycler::recycle_update_tablet_log(const UpdateTabletLogPB& update_tablet_log) {
    MetaReader meta_reader(instance_id_, txn_kv_, log_version_);
    for (int64_t tablet_id : update_tablet_log.tablet_ids()) {
        // Find the previous tablet meta (overwrite) to remove.
        TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        TxnErrorCode err = meta_reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            // No tablet meta found, nothing to recycle.
            continue;
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet meta for recycling operation log")
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }

        std::string tablet_meta_key = versioned::meta_tablet_key({instance_id_, tablet_id});
        keys_to_remove_.emplace_back(encode_versioned_key(tablet_meta_key, versionstamp));
    }

    return 0;
}

int OperationLogRecycler::recycle_compaction_log(const CompactionLogPB& compaction_log) {
    MetaReader meta_reader(instance_id_, txn_kv_, log_version_);
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for recycling compaction log")
                .tag("error_code", err);
        return -1;
    }
    for (const RecycleRowsetPB& recycle_rowset_pb : compaction_log.recycle_rowsets()) {
        // recycle rowset meta key
        std::string recycle_rowset_value;
        if (!recycle_rowset_pb.SerializeToString(&recycle_rowset_value)) {
            LOG_WARNING("failed to serialize RecycleRowsetPB")
                    .tag("recycle rowset pb", recycle_rowset_pb.ShortDebugString());
            return -1;
        }
        std::string recycle_key =
                recycle_rowset_key({instance_id_, compaction_log.tablet_id(),
                                    recycle_rowset_pb.rowset_meta().rowset_id_v2()});
        // Put recycle rowset key to track recycled rowset metadata
        LOG_INFO("put recycle rowset key")
                .tag("recycle_key", hex(recycle_key))
                .tag("tablet_id", compaction_log.tablet_id())
                .tag("rowset_id_v2", recycle_rowset_pb.rowset_meta().rowset_id_v2())
                .tag("start_version", recycle_rowset_pb.rowset_meta().start_version())
                .tag("end_version", recycle_rowset_pb.rowset_meta().end_version());
        kvs_.emplace_back(recycle_key, recycle_rowset_value);

        // Remove rowset compact key and rowset load key for input rowsets
        std::string meta_rowset_compact_key = versioned::meta_rowset_compact_key(
                {instance_id_, recycle_rowset_pb.rowset_meta().tablet_id(),
                 recycle_rowset_pb.rowset_meta().end_version()});
        std::string meta_rowset_load_key = versioned::meta_rowset_load_key(
                {instance_id_, recycle_rowset_pb.rowset_meta().tablet_id(),
                 recycle_rowset_pb.rowset_meta().end_version()});
        RowsetMetaCloudPB rowset_meta_cloud_pb;
        Versionstamp version;
        TxnErrorCode err = versioned::document_get(txn.get(), meta_rowset_compact_key, log_version_,
                                                   &rowset_meta_cloud_pb, &version);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("Failed to get meta rowset compact key")
                    .tag("instance_id", instance_id_)
                    .tag("compact_key", hex(meta_rowset_compact_key))
                    .tag("error", err);
        }
        if (rowset_meta_cloud_pb.rowset_id_v2() == recycle_rowset_pb.rowset_meta().rowset_id_v2()) {
            // Remove meta rowset compact key for input rowset that was consumed in compaction
            versioned::document_remove<doris::RowsetMetaCloudPB>(txn.get(), meta_rowset_compact_key,
                                                                 version);
            LOG_INFO("remove meta rowset compact key")
                    .tag("instance_id", instance_id_)
                    .tag("compact_key", hex(meta_rowset_compact_key))
                    .tag("tablet_id", recycle_rowset_pb.rowset_meta().tablet_id())
                    .tag("start_version", recycle_rowset_pb.rowset_meta().start_version())
                    .tag("end_version", recycle_rowset_pb.rowset_meta().end_version());
        }
        err = versioned::document_get(txn.get(), meta_rowset_load_key, log_version_,
                                      &rowset_meta_cloud_pb, &version);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("Failed to get meta rowset load key")
                    .tag("instance_id", instance_id_)
                    .tag("load_key", hex(meta_rowset_load_key))
                    .tag("error", err);
        }
        if (rowset_meta_cloud_pb.rowset_id_v2() == recycle_rowset_pb.rowset_meta().rowset_id_v2()) {
            // Remove meta rowset load key for input rowset that was consumed in compaction
            versioned::document_remove<doris::RowsetMetaCloudPB>(txn.get(), meta_rowset_load_key,
                                                                 version);
            LOG_INFO("remove meta rowset load key")
                    .tag("instance_id", instance_id_)
                    .tag("load_key", hex(meta_rowset_load_key))
                    .tag("tablet_id", recycle_rowset_pb.rowset_meta().tablet_id())
                    .tag("start_version", recycle_rowset_pb.rowset_meta().start_version())
                    .tag("end_version", recycle_rowset_pb.rowset_meta().end_version());
        }
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to remove operation log").tag("error_code", err);
        return -1;
    }
    return 0;
}

int OperationLogRecycler::commit() {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for recycling operation log")
                .tag("error_code", err);
        return -1;
    }

    std::string log_key = encode_versioned_key(versioned::log_key(instance_id_), log_version_);
    // Remove the operation log entry itself after recycling its contents
    LOG_INFO("remove operation log key")
            .tag("instance_id", instance_id_)
            .tag("log_key", hex(log_key))
            .tag("log_version", log_version_.to_string());
    txn->remove(log_key);

    for (const auto& key : keys_to_remove_) {
        // Remove versioned keys that were replaced during operation log processing
        LOG_INFO("remove versioned key").tag("instance_id", instance_id_).tag("key", hex(key));
        txn->remove(key);
    }

    for (const auto& [key, value] : kvs_) {
        // Put recycled metadata entries (recycle partition, recycle index, recycle rowset, etc.)
        LOG_INFO("put recycled metadata key")
                .tag("instance_id", instance_id_)
                .tag("key", hex(key))
                .tag("value_size", value.size());
        txn->put(key, value);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to remove operation log").tag("error_code", err);
        return -1;
    }

    return 0;
}

int OperationLogRecycler::recycle_table_version(int64_t table_id) {
    MetaReader meta_reader(instance_id_, txn_kv_, log_version_);
    Versionstamp prev_version;
    TxnErrorCode err = meta_reader.get_table_version(table_id, &prev_version);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // No operation log found, nothing to recycle.
        return 0;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get table version for recycling operation log")
                .tag("table_id", table_id)
                .tag("error_code", err);
        return -1;
    }

    std::string table_version_key = versioned::table_version_key({instance_id_, table_id});

    keys_to_remove_.emplace_back(encode_versioned_key(table_version_key, prev_version));
    return 0;
}

static TxnErrorCode get_txn_info(TxnKv* txn_kv, std::string_view instance_id, int64_t db_id,
                                 int64_t txn_id, TxnInfoPB* txn_info) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    std::string key = txn_info_key({instance_id, db_id, txn_id});
    std::string txn_info_value;
    err = txn->get(key, &txn_info_value);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (!txn_info->ParseFromString(txn_info_value)) {
        LOG_WARNING("failed to parse TxnInfoPB")
                .tag("value_size", txn_info_value.size())
                .tag("key", hex(key))
                .tag("txn_id", txn_id);
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

int InstanceRecycler::recycle_operation_logs() {
    if (!instance_info_.has_multi_version_status() ||
        (instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_ENABLED &&
         instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_READ_WRITE &&
         instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_WRITE_ONLY)) {
        LOG_INFO("instance {} is not multi-version enabled, skip recycling operation logs",
                 instance_id_);
        return 0;
    }

    AnnotateTag tag("instance_id", instance_id_);
    LOG_WARNING("begin to recycle operation logs");

    StopWatch stop_watch;
    size_t total_operation_logs = 0;
    size_t recycled_operation_logs = 0;
    size_t operation_log_data_size = 0;
    size_t max_operation_log_data_size = 0;
    size_t recycled_operation_log_data_size = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;
        LOG_WARNING("recycle operation logs, cost={}s", cost)
                .tag("total_operation_logs", total_operation_logs)
                .tag("recycled_operation_logs", recycled_operation_logs)
                .tag("operation_log_data_size", operation_log_data_size)
                .tag("max_operation_log_data_size", max_operation_log_data_size)
                .tag("recycled_operation_log_data_size", recycled_operation_log_data_size);
    };

    auto scan_and_recycle_operation_log = [&](const std::string_view& key,
                                              const std::string_view& value) {
        std::string_view log_key(key);
        Versionstamp versionstamp;
        if (!decode_versioned_key(&log_key, &versionstamp)) {
            LOG_WARNING("failed to decode versionstamp from operation log key")
                    .tag("key", hex(key));
            return -1;
        }

        OperationLogPB operation_log;
        if (!operation_log.ParseFromArray(value.data(), value.size())) {
            LOG_WARNING("failed to parse OperationLogPB from operation log key")
                    .tag("key", hex(key));
            return -1;
        }

        bool need_recycle = true; // Always recycle operation logs for now
        if (need_recycle) {
            AnnotateTag tag("log_key", hex(log_key));
            int res = recycle_operation_log(versionstamp, std::move(operation_log));
            if (res != 0) {
                LOG_WARNING("failed to recycle operation log").tag("error_code", res);
                return res;
            }

            recycled_operation_logs++;
            recycled_operation_log_data_size += value.size();
        }

        total_operation_logs++;
        operation_log_data_size += value.size();
        max_operation_log_data_size = std::max(max_operation_log_data_size, value.size());
        return 0;
    };

    auto is_multi_version_status_changed = [&]() {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create transaction for checking multi-version status")
                    .tag("error_code", err);
            return -1;
        }

        std::string value;
        err = txn->get(instance_key({instance_id_}), &value);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get instance info for checking multi-version status")
                    .tag("error_code", err);
            return -1;
        }

        InstanceInfoPB instance_info;
        if (!instance_info.ParseFromString(value)) {
            LOG_WARNING("failed to parse InstanceInfoPB").tag("value_size", value.size());
            return -1;
        }

        if (!instance_info.has_multi_version_status() ||
            instance_info.multi_version_status() != instance_info_.multi_version_status()) {
            LOG_WARNING("multi-version status changed for instance")
                    .tag("old_status", instance_info_.multi_version_status())
                    .tag("new_status", instance_info.multi_version_status());
            return 1; // Indicate that the status has changed
        }
        return 0;
    };

    std::string log_key_prefix = versioned::log_key(instance_id_);
    std::string begin_key = encode_versioned_key(log_key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(log_key_prefix, Versionstamp::max());
    return scan_and_recycle(std::move(begin_key), end_key,
                            std::move(scan_and_recycle_operation_log),
                            std::move(is_multi_version_status_changed));
}

int InstanceRecycler::recycle_operation_log(Versionstamp log_version,
                                            OperationLogPB operation_log) {
    int recycle_log_count = 0;
    OperationLogRecycler log_recycler(instance_id_, txn_kv_.get(), log_version);

#define RECYCLE_OPERATION_LOG(log_type, method_name)                      \
    do {                                                                  \
        if (operation_log.has_##log_type()) {                             \
            int res = log_recycler.method_name(operation_log.log_type()); \
            if (res != 0) {                                               \
                LOG_WARNING("failed to recycle " #log_type " log")        \
                        .tag("res", res)                                  \
                        .tag("log_version", log_version.to_string());     \
                return res;                                               \
            }                                                             \
            recycle_log_count++;                                          \
        }                                                                 \
    } while (0)

    RECYCLE_OPERATION_LOG(commit_partition, recycle_commit_partition_log);
    RECYCLE_OPERATION_LOG(drop_partition, recycle_drop_partition_log);
    RECYCLE_OPERATION_LOG(commit_index, recycle_commit_index_log);
    RECYCLE_OPERATION_LOG(drop_index, recycle_drop_index_log);
    RECYCLE_OPERATION_LOG(update_tablet, recycle_update_tablet_log);
    RECYCLE_OPERATION_LOG(compaction, recycle_compaction_log);
#undef RECYCLE_OPERATION_LOG

    if (operation_log.has_commit_txn()) {
        const CommitTxnLogPB& commit_txn_log = operation_log.commit_txn();
        TxnInfoPB txn_info;
        TxnErrorCode err = get_txn_info(txn_kv_.get(), instance_id_, commit_txn_log.db_id(),
                                        commit_txn_log.txn_id(), &txn_info);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get TxnInfoPB for recycling commit txn log")
                    .tag("txn_id", commit_txn_log.txn_id())
                    .tag("error_code", err);
            return -1;
        }

        if (txn_info.status() != TxnStatusPB::TXN_STATUS_VISIBLE) {
            VLOG_DEBUG << "TxnInfoPB state is not VISIBLE, skip recycling commit txn log"
                       << ", txn_id: " << commit_txn_log.txn_id()
                       << ", state: " << TxnStatusPB_Name(txn_info.status());
            return 0;
        }

        int res = log_recycler.recycle_commit_txn_log(commit_txn_log);
        if (res != 0) {
            LOG_WARNING("failed to recycle commit_txn log")
                    .tag("res", res)
                    .tag("txn_id", commit_txn_log.txn_id())
                    .tag("log_version", log_version.to_string());
            return res;
        }

        recycle_log_count++;
    }

    if (recycle_log_count > 1) {
        LOG_FATAL("recycle operation log count is more than 1")
                .tag("recycle_log_count", recycle_log_count)
                .tag("log_version", log_version.to_string())
                .tag("operation_log", operation_log.ShortDebugString());
        return -1; // This is an unexpected condition, should not happen
    }

    return log_recycler.commit();
}

} // namespace doris::cloud
