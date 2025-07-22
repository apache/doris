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
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
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
        kvs_.emplace_back(std::move(recycle_key), std::move(recycle_partition_value));
    }

    if (drop_partition_log.update_table_version()) {
        return recycle_table_version(drop_partition_log.table_id());
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
    txn->remove(log_key);
    for (const auto& key : keys_to_remove_) {
        txn->remove(key);
    }

    for (const auto& [key, value] : kvs_) {
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
#define RECYCLE_OPERATION_LOG(log_type, method_name)                  \
    if (operation_log.has_##log_type()) {                             \
        int res = log_recycler.method_name(operation_log.log_type()); \
        if (res != 0) {                                               \
            LOG_WARNING("failed to recycle " #log_type " log")        \
                    .tag("res", res)                                  \
                    .tag("log_version", log_version.to_string());     \
            return res;                                               \
        }                                                             \
    }

    OperationLogRecycler log_recycler(instance_id_, txn_kv_.get(), log_version);
    RECYCLE_OPERATION_LOG(commit_partition, recycle_commit_partition_log)
    RECYCLE_OPERATION_LOG(drop_partition, recycle_drop_partition_log)
#undef RECYCLE_OPERATION_LOG

    return log_recycler.commit();
}

} // namespace doris::cloud
