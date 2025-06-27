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

#include "metric.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/error/en.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/bvars.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {

// The format of the output is shown in "test/fdb_metric_example.json"
static const std::string FDB_STATUS_KEY = "\xff\xff/status/json";

static std::string get_fdb_status(TxnKv* txn_kv) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create_txn, err=" << err;
        return "";
    }
    std::string status_val;
    err = txn->get(FDB_STATUS_KEY, &status_val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get FDB_STATUS_KEY, err=" << err;
        return "";
    }
    return status_val;
}

// The format of fdb status details:
//
// Configuration:
//   Redundancy mode        - double
//   Storage engine         - ssd-2
//   Coordinators           - 3
//   Usable Regions         - 1

// Cluster:
//   FoundationDB processes - 15
//   Zones                  - 3
//   Machines               - 3
//   Memory availability    - 2.9 GB per process on machine with least available
//                            >>>>> (WARNING: 4.0 GB recommended) <<<<<
//   Retransmissions rate   - 3 Hz
//   Fault Tolerance        - 1 machines
//   Server time            - 02/16/23 16:48:14

// Data:
//   Replication health     - Healthy
//   Moving data            - 0.000 GB
//   Sum of key-value sizes - 4.317 GB
//   Disk space used        - 11.493 GB

// Operating space:
//   Storage server         - 462.8 GB free on most full server
//   Log server             - 462.8 GB free on most full server

// Workload:
//   Read rate              - 84 Hz
//   Write rate             - 4 Hz
//   Transactions started   - 222 Hz
//   Transactions committed - 4 Hz
//   Conflict rate          - 0 Hz

// Backup and DR:
//   Running backups        - 0
//   Running DRs            - 0

static void export_fdb_status_details(const std::string& status_str) {
    using namespace rapidjson;
    Document document;
    try {
        document.Parse(status_str.c_str());
        if (document.HasParseError()) {
            LOG(WARNING) << "fail to parse status str, err: "
                         << GetParseError_En(document.GetParseError());
            return;
        }
    } catch (std::exception& e) {
        LOG(WARNING) << "fail to parse status str, err: " << e.what();
        return;
    }

    if (!document.HasMember("cluster") || !document.HasMember("client")) {
        LOG(WARNING) << "err fdb status details";
        return;
    }
    auto get_value = [&](const std::vector<const char*>& v) -> int64_t {
        if (v.empty()) return BVAR_FDB_INVALID_VALUE;
        auto node = document.FindMember("cluster");
        for (const auto& name : v) {
            if (!node->value.HasMember(name)) return BVAR_FDB_INVALID_VALUE;
            node = node->value.FindMember(name);
        }
        if (node->value.IsInt64()) return node->value.GetInt64();
        if (node->value.IsDouble()) return static_cast<int64_t>(node->value.GetDouble());
        if (node->value.IsObject()) return node->value.MemberCount();
        if (node->value.IsArray()) return node->value.Size();
        return BVAR_FDB_INVALID_VALUE;
    };
    auto get_nanoseconds = [&](const std::vector<const char*>& v) -> int64_t {
        constexpr double NANOSECONDS = 1e9;
        auto node = document.FindMember("cluster");
        for (const auto& name : v) {
            if (!node->value.HasMember(name)) return BVAR_FDB_INVALID_VALUE;
            node = node->value.FindMember(name);
        }
        if (node->value.IsInt64()) return node->value.GetInt64() * NANOSECONDS;
        DCHECK(node->value.IsDouble());
        return static_cast<int64_t>(node->value.GetDouble() * NANOSECONDS);
    };
    // Configuration
    g_bvar_fdb_configuration_coordinators_count.set_value(
            get_value({"configuration", "coordinators_count"}));
    g_bvar_fdb_configuration_usable_regions.set_value(
            get_value({"configuration", "usable_regions"}));

    // Cluster
    g_bvar_fdb_process_count.set_value(get_value({"processes"}));
    g_bvar_fdb_machines_count.set_value(get_value({"machines"}));
    g_bvar_fdb_fault_tolerance_count.set_value(
            get_value({"fault_tolerance", "max_zone_failures_without_losing_data"}));
    g_bvar_fdb_generation.set_value(get_value({"generation"}));
    g_bvar_fdb_incompatible_connections.set_value(get_value({"incompatible_connections"}));

    // Data/Operating space
    g_bvar_fdb_data_average_partition_size_bytes.set_value(
            get_value({"data", "average_partition_size_bytes"}));
    g_bvar_fdb_data_partition_count.set_value(get_value({"data", "partitions_count"}));
    g_bvar_fdb_data_total_disk_used_bytes.set_value(get_value({"data", "total_disk_used_bytes"}));
    g_bvar_fdb_data_total_kv_size_bytes.set_value(get_value({"data", "total_kv_size_bytes"}));
    g_bvar_fdb_data_log_server_space_bytes.set_value(
            get_value({"data", "least_operating_space_bytes_log_server"}));
    g_bvar_fdb_data_storage_server_space_bytes.set_value(
            get_value({"data", "least_operating_space_bytes_storage_server"}));
    g_bvar_fdb_data_moving_data_highest_priority.set_value(
            get_value({"data", "moving_data", "highest_priority"}));
    g_bvar_fdb_data_moving_data_in_flight_bytes.set_value(
            get_value({"data", "moving_data", "in_flight_bytes"}));
    g_bvar_fdb_data_moving_data_in_queue_bytes.set_value(
            get_value({"data", "moving_data", "in_queue_bytes"}));
    g_bvar_fdb_data_moving_total_written_bytes.set_value(
            get_value({"data", "moving_data", "total_written_bytes"}));
    g_bvar_fdb_data_state_min_replicas_remaining.set_value(
            get_value({"data", "state", "min_replicas_remaining"}));

    // Latency probe
    g_bvar_fdb_latency_probe_transaction_start_ns.set_value(
            get_nanoseconds({"latency_probe", "transaction_start_seconds"}));
    g_bvar_fdb_latency_probe_commit_ns.set_value(
            get_nanoseconds({"latency_probe", "commit_seconds"}));
    g_bvar_fdb_latency_probe_read_ns.set_value(get_nanoseconds({"latency_probe", "read_seconds"}));

    // Workload
    g_bvar_fdb_workload_conflict_rate_hz.set_value(
            get_value({"workload", "transactions", "conflicted", "hz"}));
    g_bvar_fdb_workload_location_rate_hz.set_value(
            get_value({"workload", "operations", "location_requests", "hz"}));
    g_bvar_fdb_workload_keys_read_hz.set_value(get_value({"workload", "keys", "read", "hz"}));
    g_bvar_fdb_workload_read_bytes_hz.set_value(get_value({"workload", "bytes", "read", "hz"}));
    g_bvar_fdb_workload_read_rate_hz.set_value(
            get_value({"workload", "operations", "reads", "hz"}));
    g_bvar_fdb_workload_written_bytes_hz.set_value(
            get_value({"workload", "bytes", "written", "hz"}));
    g_bvar_fdb_workload_write_rate_hz.set_value(
            get_value({"workload", "operations", "writes", "hz"}));
    g_bvar_fdb_workload_transactions_started_hz.set_value(
            get_value({"workload", "transactions", "started", "hz"}));
    g_bvar_fdb_workload_transactions_committed_hz.set_value(
            get_value({"workload", "transactions", "committed", "hz"}));
    g_bvar_fdb_workload_transactions_rejected_hz.set_value(
            get_value({"workload", "transactions", "rejected_for_queued_too_long", "hz"}));

    // QOS
    g_bvar_fdb_qos_worst_data_lag_storage_server_ns.set_value(
            get_nanoseconds({"qos", "worst_data_lag_storage_server", "seconds"}));
    g_bvar_fdb_qos_worst_durability_lag_storage_server_ns.set_value(
            get_nanoseconds({"qos", "worst_durability_lag_storage_server", "seconds"}));
    g_bvar_fdb_qos_worst_log_server_queue_bytes.set_value(
            get_value({"qos", "worst_queue_bytes_log_server"}));
    g_bvar_fdb_qos_worst_storage_server_queue_bytes.set_value(
            get_value({"qos", "worst_queue_bytes_storage_server"}));

    // Backup and DR

    // Client Count
    g_bvar_fdb_client_count.set_value(get_value({"clients", "count"}));

    // Coordinators Unreachable Count
    auto unreachable_count = 0;
    if (auto node = document.FindMember("client"); node->value.HasMember("coordinators")) {
        if (node = node->value.FindMember("coordinators"); node->value.HasMember("coordinators")) {
            if (node = node->value.FindMember("coordinators"); node->value.IsArray()) {
                for (const auto& c : node->value.GetArray()) {
                    if (c.HasMember("reachable") && c.FindMember("reachable")->value.IsBool() &&
                        !c.FindMember("reachable")->value.GetBool()) {
                        ++unreachable_count;
                    }
                }
                g_bvar_fdb_coordinators_unreachable_count.set_value(unreachable_count);
            }
        }
    }
}

static void export_meta_ranges_details(TxnKv* kv) {
    auto* txn_kv = static_cast<FdbTxnKv*>(kv);
    if (!txn_kv) {
        LOG(WARNING) << "this method only support fdb txn kv";
        return;
    }

    std::vector<std::string> partition_boundaries;
    TxnErrorCode code = txn_kv->get_partition_boundaries(&partition_boundaries);
    if (code != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << fmt::format("failed to get boundaries, code={}", code);
        return;
    }

    std::unordered_map<std::string, size_t> partition_count;
    size_t prefix_size = FdbTxnKv::fdb_partition_key_prefix().size();
    for (auto&& boundary : partition_boundaries) {
        if (boundary.size() < prefix_size + 1 || boundary[prefix_size] != CLOUD_USER_KEY_SPACE01) {
            continue;
        }

        std::string_view user_key(boundary);
        user_key.remove_prefix(prefix_size + 1); // Skip the KEY_SPACE prefix.
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&user_key, &out); // ignore any error, since the boundary key might be truncated.

        auto visitor = [](auto&& arg) -> std::string {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::string>) {
                return arg;
            } else {
                return std::to_string(arg);
            }
        };

        if (!out.empty()) {
            std::string key;
            for (size_t i = 0; i < 3 && i < out.size(); ++i) {
                key += std::visit(visitor, std::get<0>(out[i])) + '|';
            }
            key.pop_back();
            partition_count[key]++;
        }
    }

    int64_t txn_count {}, meta_count {}, recycle_count {};
    for (auto&& [key, count] : partition_count) {
        std::vector<std::string> keys;
        size_t pos {};
        // split key with '|'
        do {
            size_t p = std::min(key.size(), key.find('|', pos));
            keys.emplace_back(key.substr(pos, p - pos));
            pos = p + 1;
        } while (pos < key.size());
        keys.resize(3);
        if (keys[0] == "txn") {
            txn_count += count;
            g_bvar_meta_ranges_txn_instance_tag_count.put({keys[1], keys[2]}, count);
        } else if (keys[0] == "meta") {
            meta_count += count;
            g_bvar_meta_ranges_meta_instance_tag_count.put({keys[1], keys[2]}, count);
        } else if (keys[0] == "recycle") {
            recycle_count += count;
            g_bvar_meta_ranges_recycle_instance_tag_count.put({keys[1], keys[2]}, count);
        } else {
            LOG(WARNING) << fmt::format("Unknow meta range type: {}", keys[0]);
            continue;
        }
    }
    g_bvar_meta_ranges_txn_count.set_value(txn_count);
    g_bvar_meta_ranges_meta_count.set_value(meta_count);
    g_bvar_meta_ranges_recycle_count.set_value(recycle_count);
}

void FdbMetricExporter::export_fdb_metrics(TxnKv* txn_kv) {
    int64_t busyness = 0;
    std::string fdb_status = get_fdb_status(txn_kv);
    export_fdb_status_details(fdb_status);
    export_meta_ranges_details(txn_kv);
    if (auto* kv = dynamic_cast<FdbTxnKv*>(txn_kv); kv != nullptr) {
        busyness = static_cast<int64_t>(kv->get_client_thread_busyness() * 100);
        g_bvar_fdb_client_thread_busyness_percent.set_value(busyness);
    }
    LOG(INFO) << "finish to collect fdb metric, client busyness: " << busyness << "%";
}

FdbMetricExporter::~FdbMetricExporter() {
    stop();
}

int FdbMetricExporter::start() {
    if (txn_kv_ == nullptr) return -1;
    std::unique_lock lock(running_mtx_);
    if (running_) {
        return 0;
    }

    running_ = true;
    thread_ = std::make_unique<std::thread>([this] {
        while (running_.load(std::memory_order_acquire)) {
            export_fdb_metrics(txn_kv_.get());
            std::unique_lock l(running_mtx_);
            running_cond_.wait_for(l, std::chrono::milliseconds(sleep_interval_ms_),
                                   [this]() { return !running_.load(std::memory_order_acquire); });
        }
    });
    pthread_setname_np(thread_->native_handle(), "fdb_metrics_exporter");
    return 0;
}

void FdbMetricExporter::stop() {
    {
        std::unique_lock lock(running_mtx_);
        running_.store(false);
        running_cond_.notify_all();
    }

    if (thread_ != nullptr && thread_->joinable()) {
        thread_->join();
        thread_.reset();
    }
}

} // namespace doris::cloud
