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

#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/recycler.h"
#include "recycler/util.h"

namespace doris::cloud {

using namespace std::chrono;

// Retry configuration for save data size
static constexpr int MAX_RETRY_TIMES = 5;
static constexpr int RETRY_INTERVAL_MS = 100;

void SnapshotDataSizeCalculator::init(
        const std::vector<std::pair<SnapshotPB, Versionstamp>>& snapshots) {
    for (auto [_, versionstamp] : snapshots) {
        retained_data_size_.insert(std::make_pair(versionstamp, 0));
    }
}

inline std::string index_partition_key(int64_t index_id, int64_t partition_id) {
    return fmt::format("{}-{}", index_id, partition_id);
}

int SnapshotDataSizeCalculator::calculate_operation_log_data_size(
        const std::string_view& log_key, OperationLogPB& operation_log,
        OperationLogReferenceInfo& reference_info) {
    if (!reference_info.referenced_by_instance && !reference_info.referenced_by_snapshot) {
        return 0;
    }

    int64_t data_size = 0;
    if (operation_log.has_compaction()) {
        for (auto& recycle_rowset : operation_log.compaction().recycle_rowsets()) {
            data_size += recycle_rowset.rowset_meta().total_disk_size();
        }
        VLOG_DEBUG << "compaction log data size: " << data_size << ", key" << hex(log_key);
    } else if (operation_log.has_schema_change()) {
        for (auto& recycle_rowset : operation_log.schema_change().recycle_rowsets()) {
            data_size += recycle_rowset.rowset_meta().total_disk_size();
        }
        VLOG_DEBUG << "schema change log data size: " << data_size << ", key" << hex(log_key);
    } else if (operation_log.has_drop_partition()) {
        auto& drop_partition_log = operation_log.drop_partition();
        int64_t num_total = 0;
        int64_t num_calculated = 0;

        if (!drop_partition_log.index_partition_to_data_size().empty()) {
            for (auto& [key, size] : drop_partition_log.index_partition_to_data_size()) {
                ++num_total;
                data_size += size;
                calculated_partitions_.insert(key);
            }
        } else {
            int64_t db_id = drop_partition_log.db_id();
            int64_t table_id = drop_partition_log.table_id();
            DropPartitionLogPB drop_partition_log_copy = drop_partition_log;

            for (auto partition_id : drop_partition_log.partition_ids()) {
                for (int64_t index_id : drop_partition_log.index_ids()) {
                    std::string key = index_partition_key(index_id, partition_id);
                    if (calculated_partitions_.contains(key)) {
                        continue;
                    }
                    ++num_total;
                    int64_t partition_data_size = 0;
                    if (get_index_partition_data_size(db_id, table_id, index_id, partition_id,
                                                      &partition_data_size) != 0) {
                        return -1;
                    }
                    ++num_calculated;
                    data_size += partition_data_size;
                    calculated_partitions_.insert(key);
                    drop_partition_log_copy.mutable_index_partition_to_data_size()->emplace(
                            key, partition_data_size);
                }
            }

            if (!drop_partition_log_copy.index_partition_to_data_size().empty()) {
                operation_log.clear_drop_partition();
                operation_log.mutable_drop_partition()->Swap(&drop_partition_log_copy);
                if (save_operation_log(log_key, operation_log) != 0) {
                    return -1;
                }
            }
        }

        VLOG_DEBUG << "drop partition log data size: " << data_size << ", key" << hex(log_key)
                   << ", partition_num_total=" << num_total
                   << ", partition_num_calculated=" << num_calculated;
    } else if (operation_log.has_drop_index()) {
        auto& drop_index_log = operation_log.drop_index();
        int64_t num_total = 0;
        int64_t num_calculated = 0;

        if (!drop_index_log.index_partition_to_data_size().empty()) {
            for (auto& [key, size] : drop_index_log.index_partition_to_data_size()) {
                ++num_total;
                data_size += size;
                calculated_partitions_.insert(key);
            }
        } else {
            int64_t db_id = drop_index_log.db_id();
            int64_t table_id = drop_index_log.table_id();
            DropIndexLogPB drop_index_log_copy = drop_index_log;

            for (auto index_id : drop_index_log.index_ids()) {
                std::vector<int64_t> partition_ids;
                if (get_all_index_partitions(db_id, table_id, index_id, &partition_ids) != 0) {
                    return -1;
                }
                for (int64_t partition_id : partition_ids) {
                    ++num_total;
                    std::string key = index_partition_key(index_id, partition_id);
                    if (calculated_partitions_.contains(key)) {
                        continue;
                    }
                    int64_t partition_data_size = 0;
                    if (get_index_partition_data_size(db_id, table_id, index_id, partition_id,
                                                      &partition_data_size) != 0) {
                        return -1;
                    }
                    ++num_calculated;
                    data_size += partition_data_size;
                    calculated_partitions_.insert(key);
                    drop_index_log_copy.mutable_index_partition_to_data_size()->emplace(
                            key, partition_data_size);
                }
            }

            if (!drop_index_log_copy.index_partition_to_data_size().empty()) {
                operation_log.clear_drop_index();
                operation_log.mutable_drop_index()->Swap(&drop_index_log_copy);
                if (save_operation_log(log_key, operation_log) != 0) {
                    return -1;
                }
            }
        }

        VLOG_DEBUG << "drop index log data size: " << data_size << ", key" << hex(log_key)
                   << ", partition_num_total=" << num_total
                   << ", partition_num_calculated=" << num_calculated;
    }
    if (reference_info.referenced_by_snapshot) {
        retained_data_size_[reference_info.referenced_snapshot_timestamp] += data_size;
    } else {
        instance_retained_data_size_ += data_size;
    }
    return 0;
}

int SnapshotDataSizeCalculator::save_snapshot_data_size_with_retry() {
    for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
        if (retry > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
        }

        int res = save_snapshot_data_size();
        if (res == -2) {
            continue; // TXN_CONFLICT
        }
        return res;
    }
    return -1;
}

int SnapshotDataSizeCalculator::save_snapshot_data_size() {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for saving snapshot data size")
                .tag("error_code", err);
        return -1;
    }
    bool need_commit = false;
    int64_t left_data_size = instance_retained_data_size_;

    // save data size to SnapshotPB
    for (auto& [snapshot_versionstamp, retained_data_size] : retained_data_size_) {
        std::string snapshot_full_key = versioned::snapshot_full_key({instance_id_});
        std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
        std::string snapshot_val;
        err = txn->get(snapshot_key, &snapshot_val);

        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            // impossible
            LOG_WARNING("snapshot key not found").tag("key", hex(snapshot_key));
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get snapshot key").tag("key", hex(snapshot_key)).tag("err", err);
            return -1;
        }
        SnapshotPB snapshot_pb;
        if (!snapshot_pb.ParseFromString(snapshot_val)) {
            LOG_WARNING("failed to parse SnapshotPB")
                    .tag("instance_id", instance_id_)
                    .tag("snapshot_id", snapshot_versionstamp.to_string());
            return -1;
        }
        int64_t billable_data_size = snapshot_pb.snapshot_meta_image_size();
        if (retained_data_size >= snapshot_pb.snapshot_logical_data_size()) {
            billable_data_size += snapshot_pb.snapshot_logical_data_size();
            left_data_size += retained_data_size - snapshot_pb.snapshot_logical_data_size();
        } else if (retained_data_size + left_data_size <=
                   snapshot_pb.snapshot_logical_data_size()) {
            billable_data_size += retained_data_size + left_data_size;
            left_data_size = 0;
        } else {
            billable_data_size += snapshot_pb.snapshot_logical_data_size();
            left_data_size =
                    retained_data_size + left_data_size - snapshot_pb.snapshot_logical_data_size();
        }

        if (!snapshot_pb.has_snapshot_retained_data_size() ||
            snapshot_pb.snapshot_retained_data_size() != retained_data_size ||
            !snapshot_pb.has_snapshot_billable_data_size() ||
            snapshot_pb.snapshot_billable_data_size() != billable_data_size) {
            LOG_INFO("update snapshot data size")
                    .tag("instance_id", instance_id_)
                    .tag("snapshot_id", snapshot_versionstamp.to_string())
                    .tag("old_retained_data_size", snapshot_pb.snapshot_retained_data_size())
                    .tag("new_retained_data_size", retained_data_size)
                    .tag("old_billable_data_size", snapshot_pb.snapshot_billable_data_size())
                    .tag("new_billable_data_size", billable_data_size);
            snapshot_pb.set_snapshot_retained_data_size(retained_data_size);
            snapshot_pb.set_snapshot_billable_data_size(billable_data_size);

            std::string updated_snapshot_val;
            if (!snapshot_pb.SerializeToString(&updated_snapshot_val)) {
                LOG_WARNING("failed to serialize updated SnapshotPB");
                return -1;
            }
            txn->put(snapshot_key, updated_snapshot_val);
            need_commit = true;
        }
    }

    // save instance data size
    std::string key = instance_key(instance_id_);
    std::string instance_value;
    err = txn->get(key, &instance_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get instance info")
                .tag("instance_id", instance_id_)
                .tag("error", err);
        return -1;
    }
    InstanceInfoPB instance_info;
    if (!instance_info.ParseFromString(instance_value)) {
        LOG_WARNING("failed to parse instance info").tag("instance_id", instance_id_);
        return -1;
    }
    if (!instance_info.has_snapshot_retained_data_size() ||
        instance_info.snapshot_retained_data_size() != instance_retained_data_size_ ||
        !instance_info.has_snapshot_billable_data_size() ||
        instance_info.snapshot_billable_data_size() != left_data_size) {
        instance_info.set_snapshot_retained_data_size(instance_retained_data_size_);
        instance_info.set_snapshot_billable_data_size(left_data_size);
        txn->put(key, instance_info.SerializeAsString());
        need_commit = true;
        LOG_INFO("update instance snapshot data size")
                .tag("instance_id", instance_id_)
                .tag("old_retained_data_size", instance_info.snapshot_retained_data_size())
                .tag("new_retained_data_size", instance_retained_data_size_)
                .tag("old_billable_data_size", instance_info.snapshot_billable_data_size())
                .tag("new_billable_data_size", left_data_size);
    }

    if (need_commit) {
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to commit kv txn for saving snapshot data size").tag("err", err);
            if (err == TxnErrorCode::TXN_CONFLICT) {
                return -2;
            }
            return -1;
        }
    }
    return 0;
}

int SnapshotDataSizeCalculator::get_all_index_partitions(int64_t db_id, int64_t table_id,
                                                         int64_t index_id,
                                                         std::vector<int64_t>* partition_ids) {
    partition_ids->clear();
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for getting all index partitions")
                .tag("error_code", err);
        return -1;
    }

    std::string tablet_key_begin =
            versioned::tablet_inverted_index_key({instance_id_, db_id, table_id, index_id, 0, 0});
    std::string tablet_key_end = versioned::tablet_inverted_index_key(
            {instance_id_, db_id, table_id, index_id + 1, 0, 0});

    int64_t last_partition_id = 0;
    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto it = txn_kv_->full_range_get(tablet_key_begin, tablet_key_end, opts);
    for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [k, _] = *kvp;

        int64_t decode_db_id = -1, decode_table_id = -1, decode_index_id = -1, partition_id = -1,
                tablet_id = -1;
        std::string_view key_view(k);
        if (!versioned::decode_tablet_inverted_index_key(&key_view, &decode_db_id, &decode_table_id,
                                                         &decode_index_id, &partition_id,
                                                         &tablet_id)) {
            LOG_WARNING("failed to decode_tablet_inverted_index_key").tag("key", hex(k));
            return -1;
        }
        if (partition_id != last_partition_id) {
            last_partition_id = partition_id;
            partition_ids->emplace_back(partition_id);
        }
    }
    if (!it->is_valid()) {
        LOG_ERROR("failed to get all index partitions").tag("error_code", it->error_code());
        return -1;
    }
    return 0;
}

int SnapshotDataSizeCalculator::get_index_partition_data_size(int64_t db_id, int64_t table_id,
                                                              int64_t index_id,
                                                              int64_t partition_id,
                                                              int64_t* data_size) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for getting index partition data size")
                .tag("error_code", err);
        return -1;
    }

    // for a clone instance, the key ranges may be empty if snapshot chain is not compacted
    std::string tablet_key_begin = versioned::tablet_inverted_index_key(
            {instance_id_, db_id, table_id, index_id, partition_id, 0});
    std::string tablet_key_end = versioned::tablet_inverted_index_key(
            {instance_id_, db_id, table_id, index_id, partition_id + 1, 0});
    int64_t tablet_num = 0;
    MetaReader reader(instance_id_);
    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto it = txn_kv_->full_range_get(tablet_key_begin, tablet_key_end, opts);
    for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [k, _] = *kvp;

        int64_t decode_db_id = -1, decode_table_id = -1, decode_index_id = -1,
                decode_partition_id = -1, tablet_id = -1;
        std::string_view key_view(k);
        if (!versioned::decode_tablet_inverted_index_key(&key_view, &decode_db_id, &decode_table_id,
                                                         &decode_index_id, &decode_partition_id,
                                                         &tablet_id)) {
            LOG_WARNING("failed to decode_tablet_inverted_index_key=").tag("key", hex(k));
            return -1;
        }

        TabletStatsPB tablet_stats;
        err = reader.get_tablet_merged_stats(txn.get(), tablet_id, &tablet_stats, nullptr);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet merged stats")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }

        *data_size += tablet_stats.data_size();
        ++tablet_num;
    }
    if (!it->is_valid()) {
        LOG_ERROR("failed to get index partition data size").tag("error_code", it->error_code());
        return -1;
    }
    VLOG_DEBUG << "get data size for instance_id=" << instance_id_ << " db_id=" << db_id
               << " table_id=" << table_id << " index_id=" << index_id
               << " partition_id=" << partition_id << " tablet_num=" << tablet_num
               << " data_size=" << *data_size;
    return 0;
}

int SnapshotDataSizeCalculator::save_operation_log(const std::string_view& log_key,
                                                   OperationLogPB& operation_log) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create transaction for saving operation log").tag("error_code", err);
        return -1;
    }
    blob_put(txn.get(), log_key, operation_log, 0);
    LOG_INFO("put operation log key")
            .tag("instance_id", instance_id_)
            .tag("operation_log_key", hex(log_key))
            .tag("value", operation_log.DebugString());
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit transaction for saving operation log").tag("error_code", err);
        return -1;
    }
    return 0;
}

} // namespace doris::cloud
