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

#include "common/defer.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/util.h"

namespace doris::cloud {

using namespace std::chrono;

// return 0 for success otherwise error
static TxnErrorCode txn_remove(TxnKv* txn_kv, const std::vector<std::string>& keys) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    for (const auto& k : keys) {
        txn->remove(k);
    }
    return txn->commit();
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

    LOG_WARNING("begin to recycle operation logs").tag("instance_id", instance_id_);

    StopWatch stop_watch;
    size_t total_operation_logs = 0;
    size_t recycled_operation_logs = 0;
    size_t operation_log_data_size = 0;
    size_t max_operation_log_data_size = 0;
    size_t recycled_operation_log_data_size = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;
        LOG_WARNING("recycle operation logs, cost={}s", cost)
                .tag("instance_id", instance_id_)
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
            LOG(WARNING) << "failed to decode versionstamp from key: " << hex(key);
            return -1;
        }

        OperationLogPB operation_log;
        if (!operation_log.ParseFromArray(value.data(), value.size())) {
            LOG(WARNING) << "failed to parse OperationLogPB from key: " << hex(key);
            return -1;
        }

        bool need_recycle = true; // Always recycle operation logs for now
        if (need_recycle) {
            int res = recycle_operation_log(key, std::move(operation_log));
            if (res != 0) {
                LOG(WARNING) << "failed to recycle operation log: " << hex(key);
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
                    .tag("instance_id", instance_id_)
                    .tag("error_code", err);
            return -1;
        }

        std::string value;
        err = txn->get(instance_key({instance_id_}), &value);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get instance info for checking multi-version status")
                    .tag("instance_id", instance_id_)
                    .tag("error_code", err);
            return -1;
        }

        InstanceInfoPB instance_info;
        if (!instance_info.ParseFromString(value)) {
            LOG_WARNING("failed to parse InstanceInfoPB").tag("instance_id", instance_id_);
            return -1;
        }

        if (!instance_info.has_multi_version_status() ||
            instance_info.multi_version_status() != instance_info_.multi_version_status()) {
            LOG_WARNING("multi-version status changed for instance")
                    .tag("instance_id", instance_id_)
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

int InstanceRecycler::recycle_operation_log(std::string_view log_key,
                                            OperationLogPB operation_log) {
    // TODO: Implement the logic to recycle operation logs.
    std::vector<std::string> keys_to_remove;
    keys_to_remove.emplace_back(log_key);
    TxnErrorCode err = txn_remove(txn_kv_.get(), keys_to_remove);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to remove operation log: " << hex(log_key)
                     << ", error code: " << static_cast<int>(err);
        return -1;
    }
    return 0;
}

} // namespace doris::cloud
