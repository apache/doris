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

#include "meta_service.h"

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/repeated_ptr_field.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <numeric>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stats.h"
#include "common/stopwatch.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/delete_bitmap_lock_white_list.h"
#include "meta-service/doris_txn.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "rate-limiter/rate_limiter.h"

using namespace std::chrono;

namespace doris::cloud {

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv,
                                 std::shared_ptr<ResourceManager> resource_mgr,
                                 std::shared_ptr<RateLimiter> rate_limiter) {
    txn_kv_ = txn_kv;
    resource_mgr_ = resource_mgr;
    rate_limiter_ = rate_limiter;
    rate_limiter_->init(this);
    txn_lazy_committer_ = std::make_shared<TxnLazyCommitter>(txn_kv_);
    delete_bitmap_lock_white_list_ = std::make_shared<DeleteBitmapLockWhiteList>();
    delete_bitmap_lock_white_list_->init();
}

MetaServiceImpl::~MetaServiceImpl() = default;

void update_tablet_stats(const StatsTabletKeyInfo& info, const TabletStats& stats,
                         std::unique_ptr<Transaction>& txn, MetaServiceCode& code,
                         std::string& msg);

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("get_instance_id", std::string {});

    std::vector<NodeInfo> nodes;
    std::string err = rc_mgr->get_node(cloud_unique_id, &nodes);
    TEST_SYNC_POINT_CALLBACK("get_instance_id_err", &err);

    std::string instance_id;
    if (!err.empty()) {
        // cache can't find cloud_unique_id, so degraded by parse cloud_unique_id
        // cloud_unique_id encode: ${version}:${instance_id}:${unique_id}
        // check it split by ':' c
        auto [valid, id] = ResourceManager::get_instance_id_by_cloud_unique_id(cloud_unique_id);
        if (!valid) {
            LOG(WARNING) << "use degraded format cloud_unique_id, but cloud_unique_id not degrade "
                            "format, cloud_unique_id="
                         << cloud_unique_id;
            return "";
        }

        // check instance_id valid by get fdb
        if (config::enable_check_instance_id && !rc_mgr->is_instance_id_registered(id)) {
            LOG(WARNING) << "use degraded format cloud_unique_id, but check instance failed, "
                            "cloud_unique_id="
                         << cloud_unique_id;
            return "";
        }
        return id;
    }

    for (auto& node : nodes) {
        if (!instance_id.empty() && instance_id != node.instance_id) {
            LOG(WARNING) << "cloud_unique_id is one-to-many instance_id, "
                         << " cloud_unique_id=" << cloud_unique_id
                         << " current_instance_id=" << instance_id
                         << " later_instance_id=" << node.instance_id;
        }
        instance_id = node.instance_id; // The last wins
        // check cache unique_id
        std::string cloud_unique_id_in_cache = node.node_info.cloud_unique_id();
        auto [valid, id] =
                ResourceManager::get_instance_id_by_cloud_unique_id(cloud_unique_id_in_cache);
        if (!valid) {
            continue;
        }

        if (id != node.instance_id || id != instance_id) {
            LOG(WARNING) << "in cache, node=" << node.node_info.DebugString()
                         << ", cloud_unique_id=" << cloud_unique_id
                         << " current_instance_id=" << instance_id
                         << ", later_instance_id=" << node.instance_id;
            continue;
        }
    }

    return instance_id;
}

// Return `true` if tablet has been dropped; otherwise or it may not determine when meeting errors, return false
bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                       int64_t partition_id) {
    auto key = recycle_index_key({instance_id, index_id});
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_OK) {
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) [[unlikely]] {
            LOG_WARNING("malformed recycle index pb").tag("key", hex(key));
            return false;
        }
        return pb.state() == RecycleIndexPB::DROPPED || pb.state() == RecycleIndexPB::RECYCLING;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) { // Get kv failed, cannot determine, return false
        return false;
    }
    key = recycle_partition_key({instance_id, partition_id});
    err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_OK) {
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) [[unlikely]] {
            LOG_WARNING("malformed recycle partition pb").tag("key", hex(key));
            return false;
        }
        return pb.state() == RecyclePartitionPB::DROPPED ||
               pb.state() == RecyclePartitionPB::RECYCLING;
    }
    return false;
}

void get_tablet_idx(MetaServiceCode& code, std::string& msg, Transaction* txn,
                    const std::string& instance_id, int64_t tablet_id, TabletIndexPB& tablet_idx) {
    std::string key, val;
    meta_tablet_idx_key({instance_id, tablet_id}, &key);
    TxnErrorCode err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = MetaServiceCode::TABLET_NOT_FOUND;
        } else {
            code = cast_as<ErrCategory::READ>(err);
        }
        msg = fmt::format("failed to get tablet_idx, err={} tablet_id={} key={}", err, tablet_id,
                          hex(key));
        return;
    }
    if (!tablet_idx.ParseFromString(val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed tablet index value, tablet_id={} key={}", tablet_id, hex(key));
        return;
    }
    if (tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "internal error";
        LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                     << " idx_pb_tablet_id=" << tablet_idx.tablet_id() << " key=" << hex(key);
        return;
    }
}

void MetaServiceImpl::get_version(::google::protobuf::RpcController* controller,
                                  const GetVersionRequest* request, GetVersionResponse* response,
                                  ::google::protobuf::Closure* done) {
    if (request->batch_mode()) {
        batch_get_version(controller, request, response, done);
        return;
    }

    RPC_PREPROCESS(get_version, get);
    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    bool is_table_version = false;
    if (request->has_is_table_version()) {
        is_table_version = request->is_table_version();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    int64_t table_id = request->has_table_id() ? request->table_id() : -1;
    int64_t partition_id = request->has_partition_id() ? request->partition_id() : -1;
    if (db_id == -1 || table_id == -1 || (!is_table_version && partition_id == -1)) {
        msg = "params error, db_id=" + std::to_string(db_id) +
              " table_id=" + std::to_string(table_id) +
              " partition_id=" + std::to_string(partition_id) +
              " is_table_version=" + std::to_string(is_table_version);
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_version)

    bool is_versioned_read = is_version_read_enabled(instance_id);
    if (is_versioned_read) {
        MetaReader reader(instance_id, txn_kv_.get());
        if (is_table_version) {
            Versionstamp table_version;
            TxnErrorCode err = reader.get_table_version(table_id, &table_version);
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                msg = "table version not found";
                code = MetaServiceCode::VERSION_NOT_FOUND;
                return;
            } else if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get table version, err={} table_id={}", err, table_id);
                return;
            }
            response->set_version(table_version.version());
        } else {
            VersionPB partition_version;
            TxnErrorCode err =
                    reader.get_partition_version(partition_id, &partition_version, nullptr);
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                msg = "partition version not found";
                code = MetaServiceCode::VERSION_NOT_FOUND;
                return;
            } else if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get partition version, err={} table_id={}", err,
                                  table_id);
                return;
            }
            response->set_version(partition_version.version());
            response->add_version_update_time_ms(partition_version.update_time_ms());
        }
        TEST_SYNC_POINT_CALLBACK("get_version_code", &code);
        return;
    }

    std::string ver_key;
    if (is_table_version) {
        table_version_key({instance_id, db_id, table_id}, &ver_key);
    } else {
        partition_version_key({instance_id, db_id, table_id, partition_id}, &ver_key);
    }

    code = MetaServiceCode::OK;

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        code = cast_as<ErrCategory::CREATE>(err);
        return;
    }

    std::string ver_val;
    // 0 for success get a key, 1 for key not found, negative for error
    err = txn->get(ver_key, &ver_val);
    VLOG_DEBUG << "xxx get version_key=" << hex(ver_key);
    if (err == TxnErrorCode::TXN_OK) {
        if (is_table_version) {
            int64_t version = 0;
            if (!txn->decode_atomic_int(ver_val, &version)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed table version value";
                return;
            }
            response->set_version(version);
        } else {
            VersionPB version_pb;
            if (!version_pb.ParseFromString(ver_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed version value";
                return;
            }
            if (!version_pb.has_version()) {
                msg = "not found";
                code = MetaServiceCode::VERSION_NOT_FOUND;
                return;
            }

            response->set_version(version_pb.version());
            response->add_version_update_time_ms(version_pb.update_time_ms());
        }
        TEST_SYNC_POINT_CALLBACK("get_version_code", &code);
        return;
    } else if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = "not found";
        code = MetaServiceCode::VERSION_NOT_FOUND;
        return;
    }
    msg = fmt::format("failed to get txn, err={}", err);
    code = cast_as<ErrCategory::READ>(err);
}

void MetaServiceImpl::batch_get_version(::google::protobuf::RpcController* controller,
                                        const GetVersionRequest* request,
                                        GetVersionResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_version, get);

    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    bool is_table_version = false;
    if (request->has_is_table_version()) {
        is_table_version = request->is_table_version();
    }

    if (request->db_ids_size() == 0 || request->table_ids_size() == 0 ||
        (!is_table_version && request->table_ids_size() != request->partition_ids_size()) ||
        (!is_table_version && request->db_ids_size() != request->partition_ids_size())) {
        msg = "param error, num db_ids=" + std::to_string(request->db_ids_size()) +
              " num table_ids=" + std::to_string(request->table_ids_size()) +
              " num partition_ids=" + std::to_string(request->partition_ids_size()) +
              " is_table_version=" + std::to_string(request->is_table_version());
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    if (is_version_read_enabled(instance_id)) {
        if (is_table_version) {
            std::tie(code, msg) = batch_get_table_versions(request, response, instance_id, stats);
        } else {
            std::tie(code, msg) =
                    batch_get_partition_versions(request, response, instance_id, stats);
        }
        TEST_SYNC_POINT_CALLBACK("batch_get_version_code", &code);
        if (code != MetaServiceCode::OK) {
            response->clear_partition_ids();
            response->clear_table_ids();
            response->clear_versions();
            response->clear_db_ids();
        }
        return;
    }

    size_t num_acquired =
            is_table_version ? request->table_ids_size() : request->partition_ids_size();
    response->mutable_versions()->Reserve(num_acquired);
    response->mutable_db_ids()->CopyFrom(request->db_ids());
    response->mutable_table_ids()->CopyFrom(request->table_ids());
    if (!is_table_version) {
        response->mutable_partition_ids()->CopyFrom(request->partition_ids());
    }

    constexpr size_t BATCH_SIZE = 500;
    std::vector<std::string> version_keys;
    std::vector<std::optional<std::string>> version_values;
    version_keys.reserve(BATCH_SIZE);
    version_values.reserve(BATCH_SIZE);

    while ((code == MetaServiceCode::OK || code == MetaServiceCode::KV_TXN_TOO_OLD) &&
           response->versions_size() < num_acquired) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            msg = "failed to create txn";
            code = cast_as<ErrCategory::CREATE>(err);
            break;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        for (size_t i = response->versions_size(); i < num_acquired; i += BATCH_SIZE) {
            size_t limit = (i + BATCH_SIZE < num_acquired) ? i + BATCH_SIZE : num_acquired;
            version_keys.clear();
            version_values.clear();
            for (size_t j = i; j < limit; j++) {
                int64_t db_id = request->db_ids(j);
                int64_t table_id = request->table_ids(j);
                std::string ver_key;
                if (is_table_version) {
                    table_version_key({instance_id, db_id, table_id}, &ver_key);
                } else {
                    int64_t partition_id = request->partition_ids(j);
                    partition_version_key({instance_id, db_id, table_id, partition_id}, &ver_key);
                }
                version_keys.push_back(std::move(ver_key));
            }

            err = txn->batch_get(&version_values, version_keys);
            TEST_SYNC_POINT_CALLBACK("batch_get_version_err", &err);
            if (err == TxnErrorCode::TXN_TOO_OLD) {
                // txn too old, fallback to non-snapshot versions.
                LOG(WARNING) << "batch_get_version execution time exceeds the txn mvcc window, "
                                "fallback to acquire non-snapshot versions, partition_ids_size="
                             << request->partition_ids_size() << ", index=" << i;
                break;
            } else if (err != TxnErrorCode::TXN_OK) {
                msg = fmt::format("failed to batch get versions, index={}, err={}", i, err);
                code = cast_as<ErrCategory::READ>(err);
                break;
            }

            for (auto&& value : version_values) {
                if (!value.has_value()) {
                    // return -1 if the target version is not exists.
                    response->add_versions(-1);
                    response->add_version_update_time_ms(-1);
                } else if (is_table_version) {
                    int64_t version = 0;
                    if (!txn->decode_atomic_int(*value, &version)) {
                        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                        msg = "malformed table version value";
                        break;
                    }
                    response->add_versions(version);
                } else {
                    VersionPB version_pb;
                    if (!version_pb.ParseFromString(*value)) {
                        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                        msg = "malformed version value";
                        break;
                    }

                    if (!version_pb.has_version()) {
                        // return -1 if the target version is not exists.
                        response->add_versions(-1);
                        response->add_version_update_time_ms(-1);
                    } else {
                        response->add_versions(version_pb.version());
                        response->add_version_update_time_ms(version_pb.update_time_ms());
                    }
                }
            }
        }
    }
    if (code != MetaServiceCode::OK) {
        response->clear_partition_ids();
        response->clear_table_ids();
        response->clear_versions();
    }
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::batch_get_table_versions(
        const GetVersionRequest* request, GetVersionResponse* response,
        std::string_view instance_id, KVStats& stats) {
    size_t num_acquired = request->table_ids_size();
    response->mutable_versions()->Reserve(num_acquired);
    response->mutable_db_ids()->CopyFrom(request->db_ids());
    response->mutable_table_ids()->CopyFrom(request->table_ids());
    response->mutable_partition_ids()->CopyFrom(request->partition_ids());

    constexpr size_t BATCH_SIZE = 500;
    MetaReader reader(instance_id, txn_kv_.get());
    std::vector<int64_t> acquired_ids;
    acquired_ids.reserve(BATCH_SIZE);

    while (response->versions_size() < num_acquired) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return {cast_as<ErrCategory::CREATE>(err), "failed to create txn"};
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        for (size_t i = response->versions_size(); i < num_acquired; i += BATCH_SIZE) {
            size_t limit = (i + BATCH_SIZE < num_acquired) ? i + BATCH_SIZE : num_acquired;
            acquired_ids.clear();
            for (size_t j = i; j < limit; j++) {
                acquired_ids.push_back(request->table_ids(j));
            }
            std::unordered_map<int64_t, Versionstamp> table_versions;
            err = reader.get_table_versions(acquired_ids, &table_versions, true);
            TEST_SYNC_POINT_CALLBACK("batch_get_version_err", &err);
            if (err == TxnErrorCode::TXN_TOO_OLD) {
                // txn too old, fallback to non-snapshot versions.
                LOG(WARNING) << "batch_get_version execution time exceeds the txn mvcc window, "
                                "fallback to acquire non-snapshot versions, table_ids_size="
                             << request->table_ids_size() << ", index=" << i;
                break;
            } else if (err != TxnErrorCode::TXN_OK) {
                return {cast_as<ErrCategory::READ>(err),
                        fmt::format("failed to batch get versions, index={}, err={}", i, err)};
            }
            for (auto& acquired_id : acquired_ids) {
                auto it = table_versions.find(acquired_id);
                if (it == table_versions.end()) {
                    // return -1 if the target version is not exists.
                    response->add_versions(-1);
                } else {
                    response->add_versions(it->second.version());
                }
            }
        }
    }

    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::batch_get_partition_versions(
        const GetVersionRequest* request, GetVersionResponse* response,
        std::string_view instance_id, KVStats& stats) {
    size_t num_acquired = request->partition_ids_size();
    response->mutable_versions()->Reserve(num_acquired);
    response->mutable_db_ids()->CopyFrom(request->db_ids());
    response->mutable_table_ids()->CopyFrom(request->table_ids());

    constexpr size_t BATCH_SIZE = 500;

    MetaReader reader(instance_id, txn_kv_.get());
    std::vector<int64_t> acquired_ids;
    acquired_ids.reserve(BATCH_SIZE);

    while (response->versions_size() < num_acquired) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return {cast_as<ErrCategory::CREATE>(err), "failed to create txn"};
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        for (size_t i = response->versions_size(); i < num_acquired; i += BATCH_SIZE) {
            size_t limit = (i + BATCH_SIZE < num_acquired) ? i + BATCH_SIZE : num_acquired;
            acquired_ids.clear();
            for (size_t j = i; j < limit; j++) {
                acquired_ids.push_back(request->partition_ids(j));
            }
            std::unordered_map<int64_t, VersionPB> partition_versions;
            std::unordered_map<int64_t, Versionstamp> versionstamps;
            err = reader.get_partition_versions(acquired_ids, &partition_versions, &versionstamps,
                                                true);
            if (err == TxnErrorCode::TXN_TOO_OLD) {
                // txn too old, fallback to non-snapshot versions.
                LOG(WARNING) << "batch_get_version execution time exceeds the txn mvcc window, "
                                "fallback to acquire non-snapshot versions, partition_ids_size="
                             << request->partition_ids_size() << ", index=" << i;
                break;
            } else if (err != TxnErrorCode::TXN_OK) {
                return {cast_as<ErrCategory::READ>(err),
                        fmt::format("failed to batch get versions, index={}, err={}", i, err)};
            }
            for (auto& acquired_id : acquired_ids) {
                auto it = partition_versions.find(acquired_id);
                if (it == partition_versions.end()) {
                    // return -1 if the target version is not exists.
                    response->add_versions(-1);
                    response->add_version_update_time_ms(-1);
                } else {
                    response->add_versions(it->second.version());
                    response->add_version_update_time_ms(it->second.update_time_ms());
                }
            }
        }
    }

    return {MetaServiceCode::OK, ""};
}

void internal_create_tablet(const CreateTabletsRequest* request, MetaServiceCode& code,
                            std::string& msg, const doris::TabletMetaCloudPB& meta,
                            std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                            std::set<std::pair<int64_t, int32_t>>& saved_schema, KVStats& stats,
                            bool is_versioned_write) {
    doris::TabletMetaCloudPB tablet_meta(meta);
    bool has_first_rowset = tablet_meta.rs_metas_size() > 0;

    // TODO: validate tablet meta, check existence
    int64_t table_id = tablet_meta.table_id();
    int64_t index_id = tablet_meta.index_id();
    int64_t partition_id = tablet_meta.partition_id();
    int64_t tablet_id = tablet_meta.tablet_id();

    if (!tablet_meta.has_schema() && !tablet_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "tablet_meta must have either schema or schema_version";
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    DORIS_CLOUD_DEFER {
        if (txn == nullptr) return;
        stats.get_bytes += txn->get_bytes();
        stats.put_bytes += txn->put_bytes();
        stats.get_counter += txn->num_get_keys();
        stats.put_counter += txn->num_put_keys();
    };

    std::string rs_key, rs_val;
    if (has_first_rowset) {
        // Put first rowset if needed
        auto first_rowset = tablet_meta.mutable_rs_metas(0);
        if (config::write_schema_kv) { // detach schema from rowset meta
            first_rowset->set_index_id(index_id);
            first_rowset->set_schema_version(tablet_meta.has_schema_version()
                                                     ? tablet_meta.schema_version()
                                                     : tablet_meta.schema().schema_version());
            first_rowset->set_allocated_tablet_schema(nullptr);
        }
        MetaRowsetKeyInfo rs_key_info {instance_id, tablet_id, first_rowset->end_version()};
        meta_rowset_key(rs_key_info, &rs_key);
        if (!first_rowset->SerializeToString(&rs_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize first rowset meta";
            return;
        }
        txn->put(rs_key, rs_val);
        if (is_versioned_write) {
            std::string versioned_rs_key = versioned::meta_rowset_load_key(
                    {instance_id, tablet_id, first_rowset->end_version()});
            if (!versioned::document_put(txn.get(), versioned_rs_key, std::move(*first_rowset))) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = fmt::format("failed to serialize versioned rowset meta, key={}",
                                  hex(versioned_rs_key));
                return;
            }
            LOG(INFO) << "put first versioned rowset meta, tablet_id=" << tablet_id
                      << " end_version=" << first_rowset->end_version()
                      << " key=" << hex(versioned_rs_key);
        }

        tablet_meta.clear_rs_metas(); // Strip off rowset meta
    }

    if (tablet_meta.has_schema()) {
        // Temporary hard code to fix wrong column type string generated by FE
        auto fix_column_type = [](doris::TabletSchemaCloudPB* schema) {
            for (auto& column : *schema->mutable_column()) {
                if (column.type() == "DECIMAL128") {
                    column.mutable_type()->push_back('I');
                }
            }
        };
        if (config::write_schema_kv) {
            // detach TabletSchemaCloudPB from TabletMetaCloudPB
            tablet_meta.set_schema_version(tablet_meta.schema().schema_version());
            auto [_, success] = saved_schema.emplace(index_id, tablet_meta.schema_version());
            if (success) { // schema may not be saved
                fix_column_type(tablet_meta.mutable_schema());
                auto schema_key =
                        meta_schema_key({instance_id, index_id, tablet_meta.schema_version()});
                put_schema_kv(code, msg, txn.get(), schema_key, tablet_meta.schema());
                if (code != MetaServiceCode::OK) return;
            }
            tablet_meta.set_allocated_schema(nullptr);
        } else {
            fix_column_type(tablet_meta.mutable_schema());
        }
    }

    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!tablet_meta.SerializeToString(&val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "xxx put tablet_key=" << hex(key) << " tablet id " << tablet_id;
    if (is_versioned_write) {
        std::string versioned_tablet_key = versioned::meta_tablet_key({instance_id, tablet_id});
        if (!versioned::document_put(txn.get(), versioned_tablet_key, std::move(tablet_meta))) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize versioned tablet meta, key={}", hex(key));
            return;
        }
        LOG(INFO) << "put versioned tablet meta, tablet_id=" << tablet_id
                  << " key=" << hex(versioned_tablet_key);
    }

    // Index tablet_id -> table_id, index_id, partition_id
    std::string key1;
    std::string val1;
    MetaTabletIdxKeyInfo key_info1 {instance_id, tablet_id};
    meta_tablet_idx_key(key_info1, &key1);
    TabletIndexPB tablet_table;
    if (request->has_db_id()) {
        tablet_table.set_db_id(request->db_id());
    }
    tablet_table.set_table_id(table_id);
    tablet_table.set_index_id(index_id);
    tablet_table.set_partition_id(partition_id);
    tablet_table.set_tablet_id(tablet_id);
    if (!tablet_table.SerializeToString(&val1)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet table value";
        return;
    }
    txn->put(key1, val1);
    LOG(INFO) << "put tablet_idx tablet_id=" << tablet_id << " key=" << hex(key1);
    if (request->has_db_id() && is_versioned_write) {
        int64_t db_id = request->db_id();
        std::string tablet_idx_key = versioned::tablet_index_key({instance_id, tablet_id});
        std::string tablet_inverted_idx_key = versioned::tablet_inverted_index_key(
                {instance_id, db_id, table_id, index_id, partition_id, tablet_id});
        txn->put(tablet_idx_key, val1);
        txn->put(tablet_inverted_idx_key, "");
        LOG(INFO) << "put versioned tablet index, tablet_id=" << tablet_id
                  << " key=" << hex(tablet_idx_key)
                  << " inverted_key=" << hex(tablet_inverted_idx_key);
    }

    // Create stats info for the tablet
    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    TabletStatsPB stats_pb;
    stats_pb.set_num_rowsets(1);
    stats_pb.set_num_segments(0);
    if (request->has_db_id()) {
        stats_pb.mutable_idx()->set_db_id(request->db_id());
    }
    stats_pb.mutable_idx()->set_table_id(table_id);
    stats_pb.mutable_idx()->set_index_id(index_id);
    stats_pb.mutable_idx()->set_partition_id(partition_id);
    stats_pb.mutable_idx()->set_tablet_id(tablet_id);
    stats_pb.set_base_compaction_cnt(0);
    stats_pb.set_cumulative_compaction_cnt(0);
    // set cumulative point to 2 to not compact rowset [0-1]
    stats_pb.set_cumulative_point(2);
    stats_val = stats_pb.SerializeAsString();
    DCHECK(!stats_val.empty());
    txn->put(stats_key, stats_val);
    LOG(INFO) << "put tablet stats, tablet_id=" << tablet_id << " table_id=" << table_id
              << " index_id=" << index_id << " partition_id=" << partition_id
              << " key=" << hex(stats_key);
    if (is_versioned_write) {
        std::string load_stats_key = versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB stats_pb_copy(stats_pb);
        if (!versioned::document_put(txn.get(), load_stats_key, std::move(stats_pb_copy))) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize versioned tablet stats, key={}",
                              hex(load_stats_key));
            return;
        }
        std::string compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        if (!versioned::document_put(txn.get(), compact_stats_key, std::move(stats_pb))) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize versioned tablet compact stats, key={}",
                              hex(compact_stats_key));
            return;
        }
        LOG(INFO) << "put versioned tablet load and compact stats, tablet_id=" << tablet_id
                  << " load_stats_key=" << hex(stats_key)
                  << " compact_stats_key=" << hex(compact_stats_key);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to save tablet meta, ret={}", err);
        return;
    }
}

void MetaServiceImpl::create_tablets(::google::protobuf::RpcController* controller,
                                     const CreateTabletsRequest* request,
                                     CreateTabletsResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_tablets, get, put);

    if (request->tablet_metas_size() == 0) {
        msg = "no tablet meta";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(create_tablets)
    for (; request->has_storage_vault_name();) {
        InstanceInfoPB instance;
        std::unique_ptr<Transaction> txn0;
        TxnErrorCode err = txn_kv_->create_txn(&txn0);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to create txn");
            return;
        }

        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);

        err = txn0->get(key, &val);
        stats.get_bytes += val.size() + key.size();
        stats.get_counter++;
        LOG(INFO) << "get instance_key=" << hex(key);

        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
            msg = ss.str();
            return;
        }
        if (!instance.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse InstanceInfoPB";
            return;
        }

        // This instance hasn't enable storage vault which means it's using legacy cloud mode
        DCHECK(instance.enable_storage_vault())
                << "Only instances with enable_storage_vault true have vault name";

        std::string_view name = request->storage_vault_name();

        // Try to use the default vault name if user doesn't specify the vault name
        // for correspoding table
        if (name.empty()) {
            if (!instance.has_default_storage_vault_name()) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format("You must supply at least one default vault");
                return;
            }
            name = instance.default_storage_vault_name();
        }

        // The S3 vault would be stored inside the instance.storage_vault_names and instance.resource_ids
        auto vault_name = std::find_if(
                instance.storage_vault_names().begin(), instance.storage_vault_names().end(),
                [&](const auto& candidate_name) { return candidate_name == name; });
        if (vault_name != instance.storage_vault_names().end()) {
            auto idx = vault_name - instance.storage_vault_names().begin();
            response->set_storage_vault_id(instance.resource_ids().at(idx));
            response->set_storage_vault_name(*vault_name);
            break;
        }

        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("failed to get vault id, vault name={}", name);
        return;
    }

    // [index_id, schema_version]
    std::set<std::pair<int64_t, int32_t>> saved_schema;
    TEST_SYNC_POINT_RETURN_WITH_VOID("create_tablets");
    bool is_versioned_write = is_version_write_enabled(instance_id);
    for (auto& tablet_meta : request->tablet_metas()) {
        internal_create_tablet(request, code, msg, tablet_meta, txn_kv_, instance_id, saved_schema,
                               stats, is_versioned_write);
        if (code != MetaServiceCode::OK) {
            return;
        }
    }
}

void internal_get_tablet(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                         Transaction* txn, int64_t tablet_id, doris::TabletMetaCloudPB* tablet_meta,
                         bool skip_schema) {
    // TODO: validate request
    TabletIndexPB tablet_idx;
    get_tablet_idx(code, msg, txn, instance_id, tablet_id, tablet_idx);
    if (code != MetaServiceCode::OK) return;

    MetaTabletKeyInfo key_info1 {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
    std::string key1, val1;
    meta_tablet_key(key_info1, &key1);
    TxnErrorCode err = txn->get(key1, &val1);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::TABLET_NOT_FOUND;
        msg = "failed to get tablet, err=not found";
        return;
    } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get tablet, err={}", err);
        return;
    }

    if (!tablet_meta->ParseFromString(val1)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta, unable to initialize";
        return;
    }

    if (tablet_meta->has_schema() &&
        tablet_meta->schema().column_size() > 0) { // tablet meta saved before detach schema kv
        tablet_meta->set_schema_version(tablet_meta->schema().schema_version());
    }

    if ((!tablet_meta->has_schema() ||
         (tablet_meta->has_schema() && tablet_meta->schema().column_size() <= 0)) &&
        !skip_schema) {
        if (!tablet_meta->has_schema_version()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "tablet_meta must have either schema or schema_version";
            return;
        }
        auto key = meta_schema_key(
                {instance_id, tablet_meta->index_id(), tablet_meta->schema_version()});
        ValueBuf val_buf;
        err = cloud::blob_get(txn, key, &val_buf);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get schema, err={}", err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                                                      ? "not found"
                                                                      : "internal error");
            return;
        }
        if (!parse_schema_value(val_buf, tablet_meta->mutable_schema())) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed schema value, key={}", key);
            return;
        }
    }
}

void MetaServiceImpl::update_tablet(::google::protobuf::RpcController* controller,
                                    const UpdateTabletRequest* request,
                                    UpdateTabletResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_tablet, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(update_tablet)

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    UpdateTabletLogPB update_tablet_log;
    bool is_versioned_write = is_version_write_enabled(instance_id);
    bool is_versioned_read = is_version_read_enabled(instance_id);
    MetaReader reader(instance_id, txn_kv_.get());
    for (const TabletMetaInfoPB& tablet_meta_info : request->tablet_meta_infos()) {
        doris::TabletMetaCloudPB tablet_meta;
        if (!is_versioned_read) {
            internal_get_tablet(code, msg, instance_id, txn.get(), tablet_meta_info.tablet_id(),
                                &tablet_meta, true);
            if (code != MetaServiceCode::OK) {
                return;
            }
        } else {
            TxnErrorCode err =
                    reader.get_tablet_meta(tablet_meta_info.tablet_id(), &tablet_meta, nullptr);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get versioned tablet meta, err={}", err);
                return;
            }
        }
        if (tablet_meta_info.has_is_in_memory()) { // deprecate after 3.0.0
            tablet_meta.set_is_in_memory(tablet_meta_info.is_in_memory());
        } else if (tablet_meta_info.has_is_persistent()) { // deprecate after 3.0.0
            tablet_meta.set_is_persistent(tablet_meta_info.is_persistent());
        } else if (tablet_meta_info.has_ttl_seconds()) {
            tablet_meta.set_ttl_seconds(tablet_meta_info.ttl_seconds());
        } else if (tablet_meta_info.has_compaction_policy()) {
            tablet_meta.set_compaction_policy(tablet_meta_info.compaction_policy());
        } else if (tablet_meta_info.has_time_series_compaction_goal_size_mbytes()) {
            tablet_meta.set_time_series_compaction_goal_size_mbytes(
                    tablet_meta_info.time_series_compaction_goal_size_mbytes());
        } else if (tablet_meta_info.has_time_series_compaction_file_count_threshold()) {
            tablet_meta.set_time_series_compaction_file_count_threshold(
                    tablet_meta_info.time_series_compaction_file_count_threshold());
        } else if (tablet_meta_info.has_time_series_compaction_time_threshold_seconds()) {
            tablet_meta.set_time_series_compaction_time_threshold_seconds(
                    tablet_meta_info.time_series_compaction_time_threshold_seconds());
        } else if (tablet_meta_info.has_time_series_compaction_empty_rowsets_threshold()) {
            tablet_meta.set_time_series_compaction_empty_rowsets_threshold(
                    tablet_meta_info.time_series_compaction_empty_rowsets_threshold());
        } else if (tablet_meta_info.has_time_series_compaction_level_threshold()) {
            tablet_meta.set_time_series_compaction_level_threshold(
                    tablet_meta_info.time_series_compaction_level_threshold());
        } else if (tablet_meta_info.has_disable_auto_compaction()) {
            if (tablet_meta.has_schema() && tablet_meta.schema().column_size() > 0) {
                tablet_meta.mutable_schema()->set_disable_auto_compaction(
                        tablet_meta_info.disable_auto_compaction());
            } else {
                auto key = meta_schema_key(
                        {instance_id, tablet_meta.index_id(), tablet_meta.schema_version()});
                ValueBuf val_buf;
                err = cloud::blob_get(txn.get(), key, &val_buf);
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format("failed to get schema, err={}",
                                      err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "not found"
                                                                             : "internal error");
                    return;
                }
                doris::TabletSchemaCloudPB schema_pb;
                if (!parse_schema_value(val_buf, &schema_pb)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = fmt::format("malformed schema value, key={}", key);
                    return;
                }

                schema_pb.set_disable_auto_compaction(tablet_meta_info.disable_auto_compaction());
                put_schema_kv(code, msg, txn.get(), key, schema_pb);
                if (code != MetaServiceCode::OK) return;
            }
        }
        int64_t table_id = tablet_meta.table_id();
        int64_t index_id = tablet_meta.index_id();
        int64_t partition_id = tablet_meta.partition_id();
        int64_t tablet_id = tablet_meta.tablet_id();

        MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string key;
        std::string val;
        meta_tablet_key(key_info, &key);
        if (!tablet_meta.SerializeToString(&val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize tablet meta";
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "xxx put tablet_key=" << hex(key);

        if (is_versioned_write) {
            update_tablet_log.add_tablet_ids(tablet_id);

            std::string tablet_meta_key = versioned::meta_tablet_key({instance_id, tablet_id});
            if (!versioned::document_put(txn.get(), tablet_meta_key, std::move(tablet_meta))) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = fmt::format(
                        "failed to serialize tablet meta for versioned write. tablet_id={}",
                        tablet_id);
                return;
            }
            LOG(INFO) << "put versioned tablet meta, tablet_id=" << tablet_id
                      << " key=" << hex(tablet_meta_key);
        }
    }

    if (is_versioned_write && update_tablet_log.tablet_ids_size() > 0) {
        OperationLogPB log;
        log.mutable_update_tablet()->Swap(&update_tablet_log);
        std::string update_log_key = versioned::log_key(instance_id);
        std::string operation_log_value;
        if (!log.SerializeToString(&operation_log_value)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize update tablet log";
            return;
        }
        versioned_put(txn.get(), update_log_key, operation_log_value);
        LOG(INFO) << "put versioned update tablet log, key=" << hex(update_log_key)
                  << " instance_id=" << instance_id << " log_size=" << operation_log_value.size();
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update tablet meta, err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::update_tablet_schema(::google::protobuf::RpcController* controller,
                                           const UpdateTabletSchemaRequest* request,
                                           UpdateTabletSchemaResponse* response,
                                           ::google::protobuf::Closure* done) {
    DCHECK(false) << "should not call update_tablet_schema";
    RPC_PREPROCESS(update_tablet_schema, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(update_tablet_schema)

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    doris::TabletMetaCloudPB tablet_meta;
    internal_get_tablet(code, msg, instance_id, txn.get(), request->tablet_id(), &tablet_meta,
                        true);
    if (code != MetaServiceCode::OK) {
        return;
    }

    std::string schema_key, schema_val;
    while (request->has_tablet_schema()) {
        if (!config::write_schema_kv) {
            tablet_meta.mutable_schema()->CopyFrom(request->tablet_schema());
            break;
        }
        tablet_meta.set_schema_version(request->tablet_schema().schema_version());
        meta_schema_key({instance_id, tablet_meta.index_id(), tablet_meta.schema_version()},
                        &schema_key);
        if (txn->get(schema_key, &schema_val, true) == TxnErrorCode::TXN_OK) {
            break; // schema has already been saved
        }
        if (!request->tablet_schema().SerializeToString(&schema_val)) [[unlikely]] {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize tablet schema value";
            return;
        }
        txn->put(schema_key, schema_val);
        break;
    }

    int64_t table_id = tablet_meta.table_id();
    int64_t index_id = tablet_meta.index_id();
    int64_t partition_id = tablet_meta.partition_id();
    int64_t tablet_id = tablet_meta.tablet_id();
    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!tablet_meta.SerializeToString(&val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn->put(key, val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update tablet meta, err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::get_tablet(::google::protobuf::RpcController* controller,
                                 const GetTabletRequest* request, GetTabletResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_tablet, get);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet)

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    if (!is_version_read_enabled(instance_id)) {
        internal_get_tablet(code, msg, instance_id, txn.get(), request->tablet_id(),
                            response->mutable_tablet_meta(), false);
        return;
    }

    MetaReader reader(instance_id, txn_kv_.get());
    TabletMetaCloudPB tablet_meta;
    err = reader.get_tablet_meta(txn.get(), request->tablet_id(), &tablet_meta, nullptr);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get tablet meta, tablet_id={}, err={}", request->tablet_id(),
                          err);
        return;
    }

    if (tablet_meta.has_schema() &&
        tablet_meta.schema().column_size() > 0) { // tablet meta saved before detach schema kv
        tablet_meta.set_schema_version(tablet_meta.schema().schema_version());
        return;
    }

    if (!tablet_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "tablet_meta must have either schema or schema_version";
        response->mutable_tablet_meta()->Clear();
        return;
    }
    auto key = meta_schema_key({instance_id, tablet_meta.index_id(), tablet_meta.schema_version()});
    ValueBuf val_buf;
    err = cloud::blob_get(txn.get(), key, &val_buf);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get schema, err={}",
                          err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "not found" : "internal error");
        return;
    }
    if (!parse_schema_value(val_buf, tablet_meta.mutable_schema())) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed schema value, key={}", key);
    }
    response->mutable_tablet_meta()->Swap(&tablet_meta);
}

static void set_schema_in_existed_rowset(MetaServiceCode& code, std::string& msg, Transaction* txn,
                                         const std::string& instance_id,
                                         doris::RowsetMetaCloudPB& rowset_meta,
                                         doris::RowsetMetaCloudPB& existed_rowset_meta) {
    DCHECK(existed_rowset_meta.has_index_id());
    if (!existed_rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    // Currently, schema version of `existed_rowset_meta` and `rowset_meta` MUST be equal
    DCHECK_EQ(existed_rowset_meta.schema_version(),
              rowset_meta.has_tablet_schema() ? rowset_meta.tablet_schema().schema_version()
                                              : rowset_meta.schema_version());
    if (rowset_meta.has_tablet_schema() &&
        rowset_meta.tablet_schema().schema_version() == existed_rowset_meta.schema_version()) {
        if (existed_rowset_meta.GetArena() &&
            rowset_meta.tablet_schema().GetArena() == existed_rowset_meta.GetArena()) {
            existed_rowset_meta.unsafe_arena_set_allocated_tablet_schema(
                    rowset_meta.mutable_tablet_schema());
        } else {
            existed_rowset_meta.mutable_tablet_schema()->CopyFrom(rowset_meta.tablet_schema());
        }
    } else {
        // get schema from txn kv
        std::string schema_key = meta_schema_key({instance_id, existed_rowset_meta.index_id(),
                                                  existed_rowset_meta.schema_version()});
        ValueBuf val_buf;
        TxnErrorCode err = cloud::blob_get(txn, schema_key, &val_buf, true);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format(
                    "failed to get schema, schema_version={}: {}", rowset_meta.schema_version(),
                    err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "not found" : "internal error");
            return;
        }
        if (!parse_schema_value(val_buf, existed_rowset_meta.mutable_tablet_schema())) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed schema value, key={}", schema_key);
            return;
        }
    }
}

void scan_restore_job_rowset(
        Transaction* txn, const std::string& instance_id, int64_t tablet_id, MetaServiceCode& code,
        std::string& msg,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* restore_job_rs_metas) {
    std::stringstream ss;
    JobRestoreRowsetKeyInfo rs_key_info0 {instance_id, tablet_id, 0};
    JobRestoreRowsetKeyInfo rs_key_info1 {instance_id, tablet_id + 1, 0};
    std::string restore_job_rs_key0;
    std::string restore_job_rs_key1;
    job_restore_rowset_key(rs_key_info0, &restore_job_rs_key0);
    job_restore_rowset_key(rs_key_info1, &restore_job_rs_key1);

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [restore_job_rs_key0, restore_job_rs_key1, &num_rowsets](int*) {
                LOG(INFO) << "get restore job rs meta, num_rowsets=" << num_rowsets << " range=["
                          << hex(restore_job_rs_key0) << "," << hex(restore_job_rs_key1) << "]";
            });

    std::unique_ptr<RangeGetIterator> it;
    do {
        TxnErrorCode err = txn->get(restore_job_rs_key0, restore_job_rs_key1, &it, true);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get restore job rs meta while committing,"
               << " tablet_id=" << tablet_id << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "range_get restore_job_rs_key=" << hex(k) << " tablet_id=" << tablet_id;
            restore_job_rs_metas->emplace_back();
            if (!restore_job_rs_metas->back().second.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed restore job rowset meta, tablet_id=" << tablet_id
                   << " key=" << hex(k);
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            restore_job_rs_metas->back().first = std::string(k.data(), k.size());
            ++num_rowsets;
            if (!it->has_next()) restore_job_rs_key0 = k;
        }
        restore_job_rs_key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
    return;
}

/*
 * Restore job state:
 *                                 +--------------+
 *                                 |   PREPARED   |
 *                                 +--------------+
 *                                       |  |
 *                           +-----------+  +-----------+
 *                           |  commit                  |
 *                           V                          |
 *                   +--------------+  abort/expired    | abort/expired
 *                   |   COMMITTED  | ---------------+  |
 *                   +--------------+                |  |
 *                           |                       |  |
 *                           | complete              |  |
 *                           V                       V  V
 *                    +--------------+           +--------------+
 *                    |   COMPLETED  |           |   DROPPED    |
 *                    +--------------+           +--------------+
 *                           |                          |
 *                           +----------+   +-----------+
 *                      recycle kv      |   |  recycle kv & data
 *                                      V   V
 *                                 +--------------+
 *                                 |   RECYCLING  |
 *                                 +--------------+
 */
void MetaServiceImpl::prepare_restore_job(::google::protobuf::RpcController* controller,
                                          const RestoreJobRequest* request,
                                          RestoreJobResponse* response,
                                          ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_restore_job);
    if (!request->has_tablet_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty tablet_id";
        return;
    }

    if (!request->has_tablet_meta() || !request->tablet_meta().rs_metas_size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = !request->has_tablet_meta() ? "no tablet meta" : "no rowset meta";
        return;
    }

    if (!request->tablet_meta().has_schema() && !request->tablet_meta().has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "tablet_meta must have either schema or schema_version";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(prepare_restore_job)

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    // validate request
    TabletIndexPB tablet_idx;
    get_tablet_idx(code, msg, txn0.get(), instance_id, request->tablet_id(), tablet_idx);
    if (code != MetaServiceCode::OK) {
        return;
    }

    auto key = job_restore_tablet_key({instance_id, tablet_idx.tablet_id()});
    std::string val;
    err = txn0->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check restore tablet {} existence, err={}",
                          tablet_idx.tablet_id(), err);
        return;
    }

    int64_t version = 0;
    if (err == TxnErrorCode::TXN_OK) {
        RestoreJobCloudPB restore_job_pb;
        if (!restore_job_pb.ParseFromString(val)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "malformed restore job";
            LOG_WARNING(msg);
            return;
        }
        if (restore_job_pb.state() == RestoreJobCloudPB::RECYCLING) {
            // request may arrive when recycle start
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("restore tablet {} is recycling, state: {}", tablet_idx.tablet_id(),
                              RestoreJobCloudPB::State_Name(restore_job_pb.state()));
            return;
        } else if (restore_job_pb.state() != RestoreJobCloudPB::PREPARED) {
            // COMMITTED/DROPPED/COMPLETED state, should not happen
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("restore tablet {} already exists, state: {}", tablet_idx.tablet_id(),
                              RestoreJobCloudPB::State_Name(restore_job_pb.state()));
            return;
        }
        version = restore_job_pb.version() + 1;
    }

    TabletMetaCloudPB tablet_meta;
    std::vector<doris::RowsetMetaCloudPB> rs_metas;
    tablet_meta = request->tablet_meta();
    rs_metas.assign(tablet_meta.rs_metas().begin(), tablet_meta.rs_metas().end());
    tablet_meta.clear_rs_metas(); // strip off rs meta

    int64_t total_rowset_num = 0;
    int64_t total_row_num = 0;
    int64_t total_segment_num = 0;
    int64_t total_disk_size = 0;
    // 1. save restore job
    RestoreJobCloudPB pb;
    std::string to_save_val;
    {
        pb.set_tablet_id(tablet_idx.tablet_id());
        pb.mutable_tablet_meta()->Swap(&tablet_meta);
        pb.set_ctime_s(::time(nullptr));
        pb.set_expired_at_s(request->expiration());
        pb.set_state(RestoreJobCloudPB::PREPARED);
        total_rowset_num = rs_metas.size();
        for (const auto& rs_meta : rs_metas) {
            total_row_num += rs_meta.num_rows();
            total_segment_num += rs_meta.num_segments();
            total_disk_size += rs_meta.total_disk_size();
        }
        pb.set_total_rowset_num(total_rowset_num);
        pb.set_total_row_num(total_row_num);
        pb.set_total_segment_num(total_segment_num);
        pb.set_total_disk_size(total_disk_size);
        pb.set_committed_rowset_num(0);
        pb.set_version(version);
        pb.SerializeToString(&to_save_val);
    }
    LOG_INFO("prepare restore job")
            .tag("job_restore_tablet_key", hex(key))
            .tag("tablet_id", tablet_idx.tablet_id())
            .tag("state", RestoreJobCloudPB::PREPARED)
            .tag("total_rowset_num", total_rowset_num)
            .tag("total_row_num", total_row_num)
            .tag("total_segment_num", total_segment_num)
            .tag("total_disk_size", total_disk_size)
            .tag("version", version);
    txn0->put(key, to_save_val);
    err = txn0->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }

    // 2. save restore rowsets
    int64_t saved_rowset_num = 0;
    int32_t max_batch_size = config::max_restore_job_rowsets_per_batch;
    for (size_t i = 0; i < rs_metas.size(); i += max_batch_size) {
        size_t end = (i + max_batch_size) > rs_metas.size() ? rs_metas.size() : i + max_batch_size;
        std::vector<doris::RowsetMetaCloudPB> sub_restore_job_rs_metas(rs_metas.begin() + i,
                                                                       rs_metas.begin() + end);
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        for (auto& rowset_meta : sub_restore_job_rs_metas) {
            // put restore rowset kv
            std::string restore_job_rs_key;
            std::string restore_job_rs_val;
            JobRestoreRowsetKeyInfo rs_key_info {instance_id, tablet_idx.tablet_id(),
                                                 rowset_meta.end_version()};
            job_restore_rowset_key(rs_key_info, &restore_job_rs_key);
            if (!rowset_meta.SerializeToString(&restore_job_rs_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize rowset meta";
                return;
            }
            txn->put(restore_job_rs_key, restore_job_rs_val);
            LOG_INFO("put restore job rowset")
                    .tag("restore_job_rs_key", hex(restore_job_rs_key))
                    .tag("tablet_id", tablet_idx.tablet_id())
                    .tag("rowset_id", rowset_meta.rowset_id_v2())
                    .tag("rowset_size", restore_job_rs_key.size() + restore_job_rs_val.size())
                    .tag("rowset_meta", rowset_meta.DebugString());
        }
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to prepare restore job,"
               << " tablet_id=" << tablet_idx.tablet_id()
               << " saved_rowset_num=" << saved_rowset_num
               << " total_rowset_num=" << total_rowset_num << " err=" << err;
            msg = ss.str();
            return;
        }
        saved_rowset_num += sub_restore_job_rs_metas.size();
    }
}

void MetaServiceImpl::commit_restore_job(::google::protobuf::RpcController* controller,
                                         const RestoreJobRequest* request,
                                         RestoreJobResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_restore_job);
    if (!request->has_tablet_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty tablet_id";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(commit_restore_job)

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    TabletIndexPB tablet_idx;
    get_tablet_idx(code, msg, txn0.get(), instance_id, request->tablet_id(), tablet_idx);
    if (code != MetaServiceCode::OK) {
        return;
    }

    // 1. get restore job
    auto key = job_restore_tablet_key({instance_id, request->tablet_id()});
    std::string val;
    err = txn0->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "restore job not exists or has been recycled";
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check restore job existence, err={}", err);
        LOG_WARNING(msg);
        return;
    }

    RestoreJobCloudPB restore_job_pb;
    if (!restore_job_pb.ParseFromString(val)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "malformed restore job";
        LOG_WARNING(msg);
        return;
    }

    if (restore_job_pb.state() == RestoreJobCloudPB::COMMITTED) {
        // duplicate request, previous request succeed, return ok
        return;
    } else if (restore_job_pb.state() == RestoreJobCloudPB::RECYCLING) {
        // RECYCLING, request may arrive when recycle start
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("restore tablet {} is recycling, state: {}", tablet_idx.tablet_id(),
                          RestoreJobCloudPB::State_Name(restore_job_pb.state()));
        return;
    } else if (restore_job_pb.state() != RestoreJobCloudPB::PREPARED) {
        // Only allow PREPARED -> COMMITTED
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("restore tablet {} with invalid state: {}", tablet_idx.tablet_id(),
                          RestoreJobCloudPB::State_Name(restore_job_pb.state()));
        return;
    }

    // 2. get restore rowsets
    std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
    scan_restore_job_rowset(txn0.get(), instance_id, request->tablet_id(), code, msg,
                            &restore_job_rs_metas);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("scan restore job rowset failed")
                .tag("tablet_id", request->tablet_id())
                .tag("msg", msg)
                .tag("code", code);
        return;
    }

    if (restore_job_pb.total_rowset_num() != restore_job_rs_metas.size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("rowset num mismatch, total_rowset_num={}, rs_metas_size={}",
                          restore_job_pb.total_rowset_num(), restore_job_rs_metas.size());
        return;
    }

    // 3. convert restore job rowsets to rs meta
    TabletStats tablet_stat;
    int64_t converted_rowset_num = 0;
    int32_t max_batch_size = config::max_restore_job_rowsets_per_batch;
    for (size_t i = 0; i < restore_job_rs_metas.size(); i += max_batch_size) {
        size_t end = (i + max_batch_size) > restore_job_rs_metas.size()
                             ? restore_job_rs_metas.size()
                             : i + max_batch_size;
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> sub_restore_job_rs_metas(
                restore_job_rs_metas.begin() + i, restore_job_rs_metas.begin() + end);
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }

        // 3.1 put rs metas
        for (auto& [_, rowset_meta] : sub_restore_job_rs_metas) {
            if (config::write_schema_kv && rowset_meta.has_tablet_schema()) {
                rowset_meta.set_index_id(tablet_idx.index_id());
                rowset_meta.set_schema_version(rowset_meta.tablet_schema().schema_version());
                std::string schema_key = meta_schema_key(
                        {instance_id, rowset_meta.index_id(), rowset_meta.schema_version()});
                if (rowset_meta.has_variant_type_in_schema()) {
                    write_schema_dict(code, msg, instance_id, txn.get(), &rowset_meta);
                    if (code != MetaServiceCode::OK) {
                        return;
                    }
                }
                put_schema_kv(code, msg, txn.get(), schema_key, rowset_meta.tablet_schema());
                if (code != MetaServiceCode::OK) {
                    return;
                }
                rowset_meta.set_allocated_tablet_schema(nullptr);
            }
            // put rowset meta
            std::string rs_key;
            std::string rs_val;
            MetaRowsetKeyInfo rs_key_info {instance_id, tablet_idx.tablet_id(),
                                           rowset_meta.end_version()};
            meta_rowset_key(rs_key_info, &rs_key);
            if (!rowset_meta.SerializeToString(&rs_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize rowset meta";
                return;
            }
            txn->put(rs_key, rs_val);
            LOG_INFO("put rowset key")
                    .tag("rowset_key", hex(rs_key))
                    .tag("tablet_id", tablet_idx.tablet_id())
                    .tag("rowset_id", rowset_meta.rowset_id_v2())
                    .tag("version", rowset_meta.end_version())
                    .tag("rowset_size", rs_key.size() + rs_val.size())
                    .tag("rowset_meta", rowset_meta.DebugString());

            tablet_stat.data_size += rowset_meta.total_disk_size();
            tablet_stat.num_rows += rowset_meta.num_rows();
            tablet_stat.num_segs += rowset_meta.num_segments();
            tablet_stat.index_size += rowset_meta.index_disk_size();
            tablet_stat.segment_size += rowset_meta.data_disk_size();
            ++tablet_stat.num_rowsets;
        }

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit restore job,"
               << " tablet_id=" << tablet_idx.tablet_id()
               << " converted_rowset_num=" << converted_rowset_num << " err=" << err;
            msg = ss.str();
            return;
        }
        converted_rowset_num += sub_restore_job_rs_metas.size();
    }

    if (restore_job_pb.total_row_num() != tablet_stat.num_rows ||
        restore_job_pb.total_segment_num() != tablet_stat.num_segs ||
        restore_job_pb.total_disk_size() != tablet_stat.data_size) {
        LOG_WARNING("Restore job verification failed:")
                .tag("tablet_id", tablet_idx.tablet_id())
                .tag("expected_row", restore_job_pb.total_row_num())
                .tag("actual_row", tablet_stat.num_rows)
                .tag("expected_segment", restore_job_pb.total_segment_num())
                .tag("actual_segment", tablet_stat.num_segs)
                .tag("expected_data_size", restore_job_pb.total_disk_size())
                .tag("actual_data_size", tablet_stat.data_size);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("commit restore job verification failed, tablet_id={}",
                          tablet_idx.tablet_id());
        return;
    }

    // 4. convert restore tablet to tablet meta
    TabletMetaCloudPB* tablet_meta = restore_job_pb.mutable_tablet_meta();
    DCHECK_EQ(tablet_meta->tablet_id(), tablet_idx.tablet_id());

    txn0.reset();
    err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = fmt::format("failed to create txn, err={}", err);
        LOG(WARNING) << msg;
        return;
    }

    DeleteBitmapPB* delete_bitmap = tablet_meta->mutable_delete_bitmap();
    for (size_t i = 0; i < delete_bitmap->rowset_ids_size(); ++i) {
        MetaDeleteBitmapInfo key_info {instance_id, tablet_meta->tablet_id(),
                                       delete_bitmap->rowset_ids(i), delete_bitmap->versions(i),
                                       delete_bitmap->segment_ids(i)};
        std::string key;
        meta_delete_bitmap_key(key_info, &key);
        auto& val = delete_bitmap->segment_delete_bitmaps(i);

        if (txn0->approximate_bytes() + key.size() * 3 + val.size() > config::max_txn_commit_byte) {
            err = txn0->commit();
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::COMMIT>(err);
                msg = fmt::format("failed to update delete bitmap, tablet_id={}, err={}",
                                  tablet_idx.tablet_id(), err);
                return;
            }
            err = txn_kv_->create_txn(&txn0);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::CREATE>(err);
                msg = "failed to init txn";
                return;
            }
        }
        txn0->put(key, val);
        LOG_INFO("put delete bitmap key")
                .tag("delete_bitmap_key", hex(key))
                .tag("tablet_id", tablet_idx.tablet_id())
                .tag("rowset_id", delete_bitmap->rowset_ids(i))
                .tag("version", delete_bitmap->versions(i))
                .tag("segment_id", delete_bitmap->segment_ids(i))
                .tag("delete_bitmap_size", key.size() + val.size());
    }
    err = txn0->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to update delete bitmap, tablet_id={}, err={}",
                          tablet_idx.tablet_id(), err);
        return;
    }

    txn0.reset();
    err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = fmt::format("failed to create txn, err={}", err);
        LOG(WARNING) << msg;
        return;
    }

    if (tablet_meta->has_schema()) {
        // Temporary hard code to fix wrong column type string generated by FE
        auto fix_column_type = [](doris::TabletSchemaCloudPB* schema) {
            for (auto& column : *schema->mutable_column()) {
                if (column.type() == "DECIMAL128") {
                    column.mutable_type()->push_back('I');
                }
            }
        };
        if (config::write_schema_kv) {
            // detach TabletSchemaCloudPB from TabletMetaCloudPB
            tablet_meta->set_schema_version(tablet_meta->schema().schema_version());
            fix_column_type(tablet_meta->mutable_schema());
            auto schema_key = meta_schema_key(
                    {instance_id, tablet_meta->index_id(), tablet_meta->schema_version()});
            put_schema_kv(code, msg, txn0.get(), schema_key, tablet_meta->schema());
            if (code != MetaServiceCode::OK) return;
            tablet_meta->set_allocated_schema(nullptr);
        } else {
            fix_column_type(tablet_meta->mutable_schema());
        }
    }

    MetaTabletKeyInfo tablet_info {instance_id, tablet_meta->table_id(), tablet_meta->index_id(),
                                   tablet_meta->partition_id(), tablet_meta->tablet_id()};
    std::string tablet_key;
    std::string tablet_val;
    meta_tablet_key(tablet_info, &tablet_key);
    if (!tablet_meta->SerializeToString(&tablet_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn0->put(tablet_key, tablet_val);
    LOG_INFO("put tablet key")
            .tag("tablet_key", hex(tablet_key))
            .tag("tablet_id", tablet_idx.tablet_id())
            .tag("tablet_size", tablet_key.size() + tablet_val.size());

    StatsTabletKeyInfo stat_info {instance_id, tablet_meta->table_id(), tablet_meta->index_id(),
                                  tablet_meta->partition_id(), tablet_meta->tablet_id()};
    std::string stats_key;
    std::string stats_val;
    stats_tablet_key(stat_info, &stats_key);
    TabletStatsPB stats_pb;
    stats_pb.mutable_idx()->set_table_id(tablet_meta->table_id());
    stats_pb.mutable_idx()->set_index_id(tablet_meta->index_id());
    stats_pb.mutable_idx()->set_partition_id(tablet_meta->partition_id());
    stats_pb.mutable_idx()->set_tablet_id(tablet_meta->tablet_id());
    stats_pb.set_base_compaction_cnt(0);
    stats_pb.set_cumulative_compaction_cnt(0);
    stats_pb.set_cumulative_point(tablet_meta->cumulative_layer_point());
    stats_val = stats_pb.SerializeAsString();
    txn0->put(stats_key, stats_val);
    LOG_INFO("put tablet stats")
            .tag("tablet_id", tablet_meta->tablet_id())
            .tag("stats key", hex(stats_key));

    update_tablet_stats(stat_info, tablet_stat, txn0, code, msg);
    if (code != MetaServiceCode::OK) {
        return;
    }

    // 5. update restore job
    std::string to_save_val;
    restore_job_pb.set_mtime_s(::time(nullptr));
    restore_job_pb.set_state(RestoreJobCloudPB::COMMITTED);
    restore_job_pb.set_committed_rowset_num(converted_rowset_num);
    restore_job_pb.SerializeToString(&to_save_val);

    txn0->put(key, to_save_val);
    LOG_INFO("commit restore job")
            .tag("job_restore_tablet_key", hex(key))
            .tag("tablet_id", tablet_idx.tablet_id())
            .tag("state", restore_job_pb.state())
            .tag("mtime_s", restore_job_pb.mtime_s())
            .tag("committed_rowset_num", converted_rowset_num);
    err = txn0->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit restore job: {}", err);
        return;
    }
}

void MetaServiceImpl::finish_restore_job(::google::protobuf::RpcController* controller,
                                         const RestoreJobRequest* request,
                                         RestoreJobResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(finish_restore_job);
    if (!request->has_tablet_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty tablet_id";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(finish_restore_job)

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    TabletIndexPB tablet_idx;
    get_tablet_idx(code, msg, txn0.get(), instance_id, request->tablet_id(), tablet_idx);
    if (code != MetaServiceCode::OK) {
        return;
    }

    // 1. get restore job
    auto key = job_restore_tablet_key({instance_id, request->tablet_id()});
    std::string val;
    err = txn0->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "restore job not exists or has been recycled";
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check restore job existence, err={}", err);
        LOG_WARNING(msg);
        return;
    }

    RestoreJobCloudPB restore_job_pb;
    if (!restore_job_pb.ParseFromString(val)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "malformed restore job";
        LOG_WARNING(msg);
        return;
    }

    bool is_completed = request->has_is_completed() && request->is_completed();
    if (restore_job_pb.state() == RestoreJobCloudPB::DROPPED ||
        restore_job_pb.state() == RestoreJobCloudPB::COMPLETED) {
        LOG_INFO("restore job already finished")
                .tag("job_restore_tablet_key", hex(key))
                .tag("tablet_id", tablet_idx.tablet_id())
                .tag("state", restore_job_pb.state());
        // already final state, return ok
        return;
    } else if (restore_job_pb.state() == RestoreJobCloudPB::RECYCLING) {
        // RECYCLING, request may arrive when recycle start
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("restore tablet {} is recycling, state: {}", tablet_idx.tablet_id(),
                          RestoreJobCloudPB::State_Name(restore_job_pb.state()));
        return;
    } else {
        // PREPARED, COMMITTED state
        if (is_completed && restore_job_pb.state() != RestoreJobCloudPB::COMMITTED) {
            // Only allow COMMITTED -> COMPLETED
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("restore tablet {} in invalid state to complete, state: {}",
                              tablet_idx.tablet_id(),
                              RestoreJobCloudPB::State_Name(restore_job_pb.state()));
            return;
        }
    }

    // 2. update restore job
    std::string to_save_val;
    restore_job_pb.set_state(is_completed ? RestoreJobCloudPB::COMPLETED
                                          : RestoreJobCloudPB::DROPPED);
    restore_job_pb.set_need_recycle_data(!is_completed);
    restore_job_pb.SerializeToString(&to_save_val);
    LOG_INFO("finish restore job")
            .tag("job_restore_tablet_key", hex(key))
            .tag("tablet_id", tablet_idx.tablet_id())
            .tag("state", restore_job_pb.state())
            .tag("total_rowset_num", restore_job_pb.total_rowset_num())
            .tag("total_row_num", restore_job_pb.total_row_num())
            .tag("total_segment_num", restore_job_pb.total_segment_num())
            .tag("total_disk_size", restore_job_pb.total_disk_size())
            .tag("committed_rowset_num", restore_job_pb.committed_rowset_num())
            .tag("is_completed", is_completed);
    txn0->put(key, to_save_val);
    err = txn0->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

/**
 * Fills schema information into the rowset meta from the dictionary.
 * Handles schemas with variant types by retrieving the complete schema from the dictionary.
 *
 * @param code Result code indicating the operation status.
 * @param msg Error description for failed operations.
 * @param instance_id Identifier for the specific instance.
 * @param txn Pointer to the transaction object for transactional operations.
 * @param existed_rowset_meta Rowset meta object to be updated with schema information.
 */
static void fill_schema_from_dict(MetaServiceCode& code, std::string& msg,
                                  const std::string& instance_id, Transaction* txn,
                                  doris::RowsetMetaCloudPB* existed_rowset_meta) {
    google::protobuf::RepeatedPtrField<doris::RowsetMetaCloudPB> metas;
    metas.Add()->CopyFrom(*existed_rowset_meta);
    // Retrieve schema from the dictionary and update metas.
    read_schema_dict(code, msg, instance_id, existed_rowset_meta->index_id(), txn, &metas, nullptr);
    if (code != MetaServiceCode::OK) {
        return;
    }
    // Update the original rowset meta with the complete schema from metas.
    existed_rowset_meta->CopyFrom(metas.Get(0));
}

bool check_job_existed(Transaction* txn, MetaServiceCode& code, std::string& msg,
                       const std::string& instance_id, int64_t tablet_id,
                       const std::string& rowset_id, const std::string& job_id,
                       bool is_versioned_read) {
    TabletIndexPB tablet_idx;
    if (!is_versioned_read) {
        get_tablet_idx(code, msg, txn, instance_id, tablet_id, tablet_idx);
        if (code != MetaServiceCode::OK) {
            return false;
        }
    } else {
        CHECK(false) << "versioned read is not supported yet";
    }

    std::string job_key = job_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                          tablet_idx.partition_id(), tablet_id});
    std::string job_val;
    auto err = txn->get(job_key, &job_val);
    if (err != TxnErrorCode::TXN_OK) {
        std::stringstream ss;
        ss << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "job not found," : "internal error,")
           << " instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " rowset_id=" << rowset_id << " err=" << err;
        msg = ss.str();
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::STALE_PREPARE_ROWSET
                                                      : cast_as<ErrCategory::READ>(err);
        return false;
    }

    TabletJobInfoPB job_pb;
    job_pb.ParseFromString(job_val);
    bool match = false;
    if (!job_pb.compaction().empty()) {
        for (auto c : job_pb.compaction()) {
            if (c.id() == job_id) {
                match = true;
            }
        }
    }

    if (job_pb.has_schema_change()) {
        if (job_pb.schema_change().id() == job_id) {
            match = true;
        }
    }

    if (!match) {
        std::stringstream ss;
        ss << " stale perpare rowset request,"
           << " instance_id=" << instance_id << " tablet_id=" << tablet_id << " job id=" << job_id
           << " rowset_id=" << rowset_id;
        msg = ss.str();
        code = MetaServiceCode::STALE_PREPARE_ROWSET;
        return false;
    }

    return true;
}

/**
* Check if the transaction status is as expected.
* If the transaction is not in the expected state, return false and set the error code and message.
*
* @param expect_status The expected transaction status.
* @param txn Pointer to the transaction object.
* @param instance_id The instance ID associated with the transaction.
* @param txn_id The transaction ID to check.
* @param code Reference to the error code to be set in case of failure.
* @param msg Reference to the error message to be set in case of failure.
* @return true if the transaction status matches the expected status, false otherwise.  
 */
static bool check_transaction_status(TxnStatusPB expect_status, Transaction* txn,
                                     const std::string& instance_id, int64_t txn_id,
                                     MetaServiceCode& code, std::string& msg) {
    // Get db id with txn id
    std::string index_val;
    const std::string index_key = txn_index_key({instance_id, txn_id});
    TxnErrorCode err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get db id, txn_id={} err={}", txn_id, err);
        return false;
    }

    TxnIndexPB index_pb;
    if (!index_pb.ParseFromString(index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("failed to parse txn_index_pb, txn_id={}", txn_id);
        return false;
    }

    DCHECK(index_pb.has_tablet_index() == true);
    DCHECK(index_pb.tablet_index().has_db_id() == true);
    if (!index_pb.has_tablet_index() || !index_pb.tablet_index().has_db_id()) {
        LOG(WARNING) << fmt::format(
                "txn_index_pb is malformed, tablet_index has no db_id, txn_id={}", txn_id);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("has no db_id in TxnIndexPB, txn_id={}", txn_id);
        return false;
    }
    auto db_id = index_pb.tablet_index().db_id();
    txn_id = index_pb.has_parent_txn_id() ? index_pb.parent_txn_id() : txn_id;

    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_val;
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get txn, txn_id={}, err={}", txn_id, err);
        return false;
    }
    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("failed to parse txn_info, db_id={} txn_id={}", db_id, txn_id);
        return false;
    }
    if (txn_info.status() != expect_status) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("txn is not in {} state, txn_id={}, txn_status={}", expect_status, txn_id,
                          txn_info.status());
        return false;
    }
    return true;
}

/**
 * 1. Check and confirm tmp rowset kv does not exist
 * 2. Construct recycle rowset kv which contains object path
 * 3. Put recycle rowset kv
 */
void MetaServiceImpl::prepare_rowset(::google::protobuf::RpcController* controller,
                                     const CreateRowsetRequest* request,
                                     CreateRowsetResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_rowset, get, put);
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    doris::RowsetMetaCloudPB rowset_meta(request->rowset_meta());
    if (!rowset_meta.has_tablet_schema() && !rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }

    RPC_RATE_LIMIT(prepare_rowset)

    int64_t tablet_id = rowset_meta.tablet_id();
    const auto& rowset_id = rowset_meta.rowset_id_v2();
    auto tmp_rs_key = meta_rowset_tmp_key({instance_id, rowset_meta.txn_id(), tablet_id});

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    // Check if the compaction/sc tablet job has finished
    if (config::enable_tablet_job_check && request->has_tablet_job_id() &&
        !request->tablet_job_id().empty()) {
        bool is_versioned_read = is_version_read_enabled(instance_id);
        if (!check_job_existed(txn.get(), code, msg, instance_id, tablet_id, rowset_id,
                               request->tablet_job_id(), is_versioned_read)) {
            return;
        }
    }

    // Check if the prepare rowset request is invalid.
    // If the transaction has been finished, it means this prepare rowset is a timeout retry request.
    // In this case, do not write the recycle key again, otherwise it may cause data loss.
    // If the rowset had load id, it means it is a load request, otherwise it is a
    // compaction/sc request.
    if (config::enable_load_txn_status_check && rowset_meta.has_load_id() &&
        !check_transaction_status(TxnStatusPB::TXN_STATUS_PREPARED, txn.get(), instance_id,
                                  rowset_meta.txn_id(), code, msg)) {
        LOG(WARNING) << "prepare rowset failed, txn_id=" << rowset_meta.txn_id()
                     << ", tablet_id=" << tablet_id << ", rowset_id=" << rowset_id
                     << ", rowset_state=" << rowset_meta.rowset_state() << ", msg=" << msg;
        return;
    }

    // Check if commit key already exists.
    std::string val;
    err = txn->get(tmp_rs_key, &val);
    if (err == TxnErrorCode::TXN_OK) {
        auto existed_rowset_meta = response->mutable_existed_rowset_meta();
        if (!existed_rowset_meta->ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(tmp_rs_key));
            return;
        }
        if (!existed_rowset_meta->has_index_id()) {
            if (rowset_meta.has_index_id()) {
                existed_rowset_meta->set_index_id(rowset_meta.index_id());
            } else {
                TabletIndexPB tablet_idx;
                get_tablet_idx(code, msg, txn.get(), instance_id, tablet_id, tablet_idx);
                if (code != MetaServiceCode::OK) return;
                existed_rowset_meta->set_index_id(tablet_idx.index_id());
                rowset_meta.set_index_id(tablet_idx.index_id());
            }
        }
        if (!existed_rowset_meta->has_tablet_schema()) {
            set_schema_in_existed_rowset(code, msg, txn.get(), instance_id, rowset_meta,
                                         *existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        } else {
            existed_rowset_meta->set_schema_version(
                    existed_rowset_meta->tablet_schema().schema_version());
        }
        if (existed_rowset_meta->has_variant_type_in_schema()) {
            fill_schema_from_dict(code, msg, instance_id, txn.get(), existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check whether rowset exists, err={}", err);
        return;
    }

    auto prepare_rs_key = recycle_rowset_key({instance_id, tablet_id, rowset_id});
    RecycleRowsetPB prepare_rowset;
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    prepare_rowset.set_creation_time(now);
    prepare_rowset.set_expiration(request->rowset_meta().txn_expiration());
    // Schema is useless for PREPARE type recycle rowset, set it to null to reduce storage space
    rowset_meta.set_allocated_tablet_schema(nullptr);
    prepare_rowset.mutable_rowset_meta()->CopyFrom(rowset_meta);
    prepare_rowset.set_type(RecycleRowsetPB::PREPARE);
    prepare_rowset.SerializeToString(&val);
    DCHECK_GT(prepare_rowset.expiration(), 0);
    txn->put(prepare_rs_key, val);
    std::size_t segment_key_bounds_bytes = get_segments_key_bounds_bytes(rowset_meta);
    LOG(INFO) << "put prepare_rs_key " << hex(prepare_rs_key) << " value_size " << val.size()
              << " txn_id " << request->txn_id() << " segment_key_bounds_bytes "
              << segment_key_bounds_bytes;
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE) {
            LOG(WARNING) << "failed to prepare rowset, err=value too large"
                         << ", txn_id=" << request->txn_id() << ", tablet_id=" << tablet_id
                         << ", rowset_id=" << rowset_id
                         << ", rowset_meta_bytes=" << rowset_meta.ByteSizeLong()
                         << ", segment_key_bounds_bytes=" << segment_key_bounds_bytes
                         << ", rowset_meta=" << rowset_meta.ShortDebugString();
        }
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to save recycle rowset, err={}", err);
        return;
    }
}

/**
 * 1. Check and confirm tmp rowset kv does not exist
 *     a. if exist
 *         1. if tmp rowset is same with self, it may be a redundant
 *            retry request, return ok
 *         2. else, abort commit_rowset
 *     b. else, goto 2
 * 2. Remove recycle rowset kv and put tmp rowset kv
 */
void MetaServiceImpl::commit_rowset(::google::protobuf::RpcController* controller,
                                    const CreateRowsetRequest* request,
                                    CreateRowsetResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_rowset, get, put, del);
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    doris::RowsetMetaCloudPB rowset_meta(request->rowset_meta());
    if (!rowset_meta.has_tablet_schema() && !rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    RPC_RATE_LIMIT(commit_rowset)

    int64_t tablet_id = rowset_meta.tablet_id();
    const auto& rowset_id = rowset_meta.rowset_id_v2();

    auto tmp_rs_key = meta_rowset_tmp_key({instance_id, rowset_meta.txn_id(), tablet_id});

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    // Check if the compaction/sc tablet job has finished
    if (config::enable_tablet_job_check && request->has_tablet_job_id() &&
        !request->tablet_job_id().empty()) {
        bool is_versioned_read = is_version_read_enabled(instance_id);
        if (!check_job_existed(txn.get(), code, msg, instance_id, tablet_id, rowset_id,
                               request->tablet_job_id(), is_versioned_read)) {
            return;
        }
    }

    // Check if the commit rowset request is invalid.
    // If the transaction has been finished, it means this commit rowset is a timeout retry request.
    // In this case, do not write the recycle key again, otherwise it may cause data loss.
    // If the rowset has load id, it means it is a load request, otherwise it is a
    // compaction/sc request.
    if (config::enable_load_txn_status_check && rowset_meta.has_load_id() &&
        !check_transaction_status(TxnStatusPB::TXN_STATUS_PREPARED, txn.get(), instance_id,
                                  rowset_meta.txn_id(), code, msg)) {
        LOG(WARNING) << "commit rowset failed, txn_id=" << rowset_meta.txn_id()
                     << ", tablet_id=" << tablet_id << ", rowset_id=" << rowset_id
                     << ", rowset_state=" << rowset_meta.rowset_state() << ", msg=" << msg;
        return;
    }

    // Check if commit key already exists.
    std::string existed_commit_val;
    err = txn->get(tmp_rs_key, &existed_commit_val);
    if (err == TxnErrorCode::TXN_OK) {
        auto existed_rowset_meta = response->mutable_existed_rowset_meta();
        if (!existed_rowset_meta->ParseFromString(existed_commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(tmp_rs_key));
            return;
        }
        if (existed_rowset_meta->rowset_id_v2() == rowset_meta.rowset_id_v2()) {
            // Same request, return OK
            response->set_allocated_existed_rowset_meta(nullptr);
            return;
        }
        if (!existed_rowset_meta->has_index_id()) {
            if (rowset_meta.has_index_id()) {
                existed_rowset_meta->set_index_id(rowset_meta.index_id());
            } else {
                TabletIndexPB tablet_idx;
                get_tablet_idx(code, msg, txn.get(), instance_id, rowset_meta.tablet_id(),
                               tablet_idx);
                if (code != MetaServiceCode::OK) return;
                existed_rowset_meta->set_index_id(tablet_idx.index_id());
            }
        }
        if (!existed_rowset_meta->has_tablet_schema()) {
            set_schema_in_existed_rowset(code, msg, txn.get(), instance_id, rowset_meta,
                                         *existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        } else {
            existed_rowset_meta->set_schema_version(
                    existed_rowset_meta->tablet_schema().schema_version());
        }
        if (existed_rowset_meta->has_variant_type_in_schema()) {
            fill_schema_from_dict(code, msg, instance_id, txn.get(), existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check whether rowset exists, err={}", err);
        return;
    }
    // write schema kv if rowset_meta has schema
    if (config::write_schema_kv && rowset_meta.has_tablet_schema()) {
        if (!rowset_meta.has_index_id()) {
            TabletIndexPB tablet_idx;
            get_tablet_idx(code, msg, txn.get(), instance_id, rowset_meta.tablet_id(), tablet_idx);
            if (code != MetaServiceCode::OK) return;
            rowset_meta.set_index_id(tablet_idx.index_id());
        }
        DCHECK(rowset_meta.tablet_schema().has_schema_version());
        DCHECK_GE(rowset_meta.tablet_schema().schema_version(), 0);
        rowset_meta.set_schema_version(rowset_meta.tablet_schema().schema_version());
        std::string schema_key = meta_schema_key(
                {instance_id, rowset_meta.index_id(), rowset_meta.schema_version()});
        if (rowset_meta.has_variant_type_in_schema()) {
            write_schema_dict(code, msg, instance_id, txn.get(), &rowset_meta);
            if (code != MetaServiceCode::OK) return;
        }
        put_schema_kv(code, msg, txn.get(), schema_key, rowset_meta.tablet_schema());
        if (code != MetaServiceCode::OK) return;
        rowset_meta.set_allocated_tablet_schema(nullptr);
    }

    if (is_version_write_enabled(instance_id)) {
        std::string rowset_ref_count_key =
                versioned::data_rowset_ref_count_key({instance_id, tablet_id, rowset_id});
        LOG(INFO) << "add rowset ref count key, instance_id=" << instance_id
                  << "key=" << hex(rowset_ref_count_key);
        txn->atomic_add(rowset_ref_count_key, 1);
    }

    auto recycle_rs_key = recycle_rowset_key({instance_id, tablet_id, rowset_id});
    txn->remove(recycle_rs_key);

    DCHECK_GT(rowset_meta.txn_expiration(), 0);
    auto tmp_rs_val = rowset_meta.SerializeAsString();
    txn->put(tmp_rs_key, tmp_rs_val);
    std::size_t segment_key_bounds_bytes = get_segments_key_bounds_bytes(rowset_meta);
    LOG(INFO) << "put tmp_rs_key " << hex(tmp_rs_key) << " delete recycle_rs_key "
              << hex(recycle_rs_key) << " value_size " << tmp_rs_val.size() << " txn_id "
              << request->txn_id() << " segment_key_bounds_bytes " << segment_key_bounds_bytes;
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to save rowset meta, err=" << err;
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE) {
            LOG(WARNING) << "failed to commit rowset, err=value too large"
                         << ", txn_id=" << request->txn_id() << ", tablet_id=" << tablet_id
                         << ", rowset_id=" << rowset_id
                         << ", rowset_meta_bytes=" << rowset_meta.ByteSizeLong()
                         << ", segment_key_bounds_bytes=" << segment_key_bounds_bytes
                         << ", num_segments=" << rowset_meta.num_segments()
                         << ", rowset_meta=" << rowset_meta.ShortDebugString();
            ss << ". The key column data is too large, or too many partitions are being loaded "
                  "simultaneously. Please reduce the size of the key column data or lower the "
                  "number of partitions involved in a single load or update.";
        }
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::update_tmp_rowset(::google::protobuf::RpcController* controller,
                                        const CreateRowsetRequest* request,
                                        CreateRowsetResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_tmp_rowset, get, put);
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    doris::RowsetMetaCloudPB rowset_meta(request->rowset_meta());
    if (!rowset_meta.has_tablet_schema() && !rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    RPC_RATE_LIMIT(update_tmp_rowset)
    int64_t tablet_id = rowset_meta.tablet_id();

    std::string update_key;
    std::string update_val;

    int64_t txn_id = rowset_meta.txn_id();
    MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
    meta_rowset_tmp_key(key_info, &update_key);

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    // Check if commit key already exists.
    std::string existed_commit_val;
    err = txn->get(update_key, &existed_commit_val);
    if (err == TxnErrorCode::TXN_OK) {
        auto existed_rowset_meta = response->mutable_existed_rowset_meta();
        if (!existed_rowset_meta->ParseFromString(existed_commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(update_key));
            return;
        }
    } else if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::ROWSET_META_NOT_FOUND;
        LOG_WARNING(
                "fail to find the rowset meta with key={}, instance_id={}, txn_id={}, "
                "tablet_id={}, rowset_id={}",
                hex(update_key), instance_id, rowset_meta.txn_id(), tablet_id,
                rowset_meta.rowset_id_v2());
        msg = "can't find the rowset";
        return;
    } else {
        code = cast_as<ErrCategory::READ>(err);
        LOG_WARNING(
                "internal error, fail to find the rowset meta with key={}, instance_id={}, "
                "txn_id={}, tablet_id={}, rowset_id={}",
                hex(update_key), instance_id, rowset_meta.txn_id(), tablet_id,
                rowset_meta.rowset_id_v2());
        msg = fmt::format("failed to check whether rowset exists, err={}", err);
        return;
    }
    if (rowset_meta.has_variant_type_in_schema()) {
        write_schema_dict(code, msg, instance_id, txn.get(), &rowset_meta);
        if (code != MetaServiceCode::OK) return;
    }
    DCHECK_GT(rowset_meta.txn_expiration(), 0);
    if (!rowset_meta.SerializeToString(&update_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize rowset meta";
        return;
    }

    txn->put(update_key, update_val);
    std::size_t segment_key_bounds_bytes = get_segments_key_bounds_bytes(rowset_meta);
    LOG(INFO) << "xxx put "
              << "update_rowset_key " << hex(update_key) << " value_size " << update_val.size()
              << " segment_key_bounds_bytes " << segment_key_bounds_bytes;
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update rowset meta, err=" << err;
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE) {
            const auto& rowset_id = rowset_meta.rowset_id_v2();
            LOG(WARNING) << "failed to update tmp rowset, err=value too large"
                         << ", txn_id=" << request->txn_id() << ", tablet_id=" << tablet_id
                         << ", rowset_id=" << rowset_id
                         << ", rowset_meta_bytes=" << rowset_meta.ByteSizeLong()
                         << ", segment_key_bounds_bytes=" << segment_key_bounds_bytes
                         << ", rowset_meta=" << rowset_meta.ShortDebugString();
            ss << ". The key column data is too large, or too many partitions are being loaded "
                  "simultaneously. Please reduce the size of the key column data or lower the "
                  "number of partitions involved in a single load or update.";
        }
        msg = ss.str();
        return;
    }
}

void internal_get_rowset(Transaction* txn, int64_t start, int64_t end,
                         const std::string& instance_id, int64_t tablet_id, MetaServiceCode& code,
                         std::string& msg, GetRowsetResponse* response) {
    LOG(INFO) << "get_rowset start=" << start << ", end="
              << " tablet_id=" << tablet_id;
    MetaRowsetKeyInfo key_info0 {instance_id, tablet_id, start};
    MetaRowsetKeyInfo key_info1 {instance_id, tablet_id, end + 1};
    std::string key0;
    std::string key1;
    meta_rowset_key(key_info0, &key0);
    meta_rowset_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;

    int num_rowsets = 0;
    DORIS_CLOUD_DEFER_COPY(key0, key1) {
        LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=[" << hex(key0)
                  << "," << hex(key1) << "]"
                  << " tablet_id=" << tablet_id;
    };

    std::stringstream ss;
    do {
        TxnErrorCode err = txn->get(key0, key1, &it);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "internal error, failed to get rowset, err=" << err << " tablet_id=" << tablet_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            auto* rs = response->add_rowset_meta();
            auto byte_size = rs->ByteSizeLong();
            TEST_SYNC_POINT_CALLBACK("get_rowset:meta_exceed_limit", &byte_size);
            if (byte_size + v.size() > std::numeric_limits<int32_t>::max()) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format(
                        "rowset meta exceeded 2G, unable to serialize, key={}. byte_size={} "
                        "tablet_id={}",
                        hex(k), byte_size, tablet_id);
                LOG(WARNING) << msg;
                return;
            }
            if (!rs->ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed rowset meta, unable to serialize, tablet_id=" +
                      std::to_string(tablet_id);
                LOG(WARNING) << msg << " key=" << hex(k);
                return;
            }
            ++num_rowsets;
            if (!it->has_next()) {
                key0 = k;
            }
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
}

std::vector<std::pair<int64_t, int64_t>> calc_sync_versions(int64_t req_bc_cnt, int64_t bc_cnt,
                                                            int64_t req_cc_cnt, int64_t cc_cnt,
                                                            int64_t req_cp, int64_t cp,
                                                            int64_t req_start, int64_t req_end) {
    using Version = std::pair<int64_t, int64_t>;
    // combine `v1` `v2`  to `v1`, return true if success
    static auto combine_if_overlapping = [](Version& v1, Version& v2) -> bool {
        if (v1.second + 1 < v2.first || v2.second + 1 < v1.first) return false;
        v1.first = std::min(v1.first, v2.first);
        v1.second = std::max(v1.second, v2.second);
        return true;
    };
    // [xxx]: compacted versions
    // ^~~~~: cumulative point
    // ^___^: related versions
    std::vector<Version> versions;
    if (req_bc_cnt < bc_cnt) {
        // * for any BC happended
        // BE  [=][=][=][=][=====][=][=]
        //                  ^~~~~ req_cp
        // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
        //                                  ^~~~~~~ ms_cp
        //     ^_________________________^ versions_return: [0, ms_cp - 1]
        versions.emplace_back(0, cp - 1);
    }

    if (req_cc_cnt < cc_cnt) {
        Version cc_version;
        if (req_cp < cp && req_cc_cnt + 1 == cc_cnt) {
            // * only one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^____________^ related_versions: [req_cp, ms_cp - 1]
            //
            cc_version = {req_cp, cp - 1};
        } else {
            // * more than one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //
            // * more than one CC happened and CP remain unchanged
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                  ^~~~~~~~~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //                                           there may be holes if we don't return all version
            //                                           after ms_cp, however it can be optimized.
            cc_version = {req_cp, std::numeric_limits<int64_t>::max() - 1};
        }
        if (versions.empty() || !combine_if_overlapping(versions.front(), cc_version)) {
            versions.push_back(cc_version);
        }
    }

    Version query_version {req_start, req_end};
    bool combined = false;
    for (auto& v : versions) {
        if ((combined = combine_if_overlapping(v, query_version))) break;
    }
    if (!combined) {
        versions.push_back(query_version);
    }
    std::sort(versions.begin(), versions.end(),
              [](const Version& v1, const Version& v2) { return v1.first < v2.first; });
    return versions;
}

static bool try_fetch_and_parse_schema(Transaction* txn, RowsetMetaCloudPB& rowset_meta,
                                       const std::string& key, MetaServiceCode& code,
                                       std::string& msg) {
    ValueBuf val_buf;
    TxnErrorCode err = cloud::blob_get(txn, key, &val_buf);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get schema, schema_version={}, rowset_version=[{}-{}]: {}",
                          rowset_meta.schema_version(), rowset_meta.start_version(),
                          rowset_meta.end_version(), err);
        return false;
    }
    auto schema = rowset_meta.mutable_tablet_schema();
    if (!parse_schema_value(val_buf, schema)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed schema value, key={}", key);
        return false;
    }
    return true;
}

void MetaServiceImpl::get_partition_pending_txn_id(std::string_view instance_id, int64_t db_id,
                                                   int64_t table_id, int64_t partition_id,
                                                   int64_t tablet_id, std::stringstream& ss,
                                                   MetaServiceCode& code, std::string& msg,
                                                   int64_t& first_txn_id, Transaction* txn) {
    std::string ver_val;
    std::string ver_key = partition_version_key({instance_id, db_id, table_id, partition_id});
    TxnErrorCode err = txn->get(ver_key, &ver_val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // No pending txn, return empty
        first_txn_id = -1;
        return;
    } else if (TxnErrorCode::TXN_OK != err) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get partiton version, tablet_id=" << tablet_id << " key=" << hex(ver_key)
           << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    VersionPB version_pb;
    if (!version_pb.ParseFromString(ver_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse version pb db_id=" << db_id << " table_id=" << table_id
           << " partition_id" << partition_id << " key=" << hex(ver_key);
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    if (version_pb.pending_txn_ids_size() > 0) {
        DCHECK(version_pb.pending_txn_ids_size() == 1);
        first_txn_id = version_pb.pending_txn_ids(0);
    } else {
        first_txn_id = -1;
    }
}

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const GetRowsetRequest* request, GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_rowset, get);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_rowset)
    int64_t tablet_id = request->idx().has_tablet_id() ? request->idx().tablet_id() : -1;
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }

    if (!request->has_base_compaction_cnt() || !request->has_cumulative_compaction_cnt() ||
        !request->has_cumulative_point()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid compaction_cnt or cumulative_point given, tablet_id=" +
              std::to_string(tablet_id);
        LOG(WARNING) << msg;
        return;
    }
    int64_t req_bc_cnt = request->base_compaction_cnt();
    int64_t req_cc_cnt = request->cumulative_compaction_cnt();
    int64_t req_cp = request->cumulative_point();

    bool is_versioned_read = is_version_read_enabled(instance_id);
    MetaReader reader(instance_id, txn_kv_.get());
    do {
        TEST_SYNC_POINT_CALLBACK("get_rowset:begin", &tablet_id);
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to create txn";
            LOG(WARNING) << msg;
            return;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        TabletIndexPB idx;
        // Get tablet id index from kv
        if (!is_versioned_read) {
            get_tablet_idx(code, msg, txn.get(), instance_id, tablet_id, idx);
            if (code != MetaServiceCode::OK) {
                return;
            }
        } else {
            err = reader.get_tablet_index(txn.get(), tablet_id, &idx);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get versioned tablet index, err={}, tablet_id={}", err,
                                  tablet_id);
                LOG(WARNING) << msg;
                return;
            }
        }
        DCHECK(request->has_idx());

        if (idx.has_db_id()) {
            // there is maybe a lazy commit txn when call get_rowset
            // we need advance lazy commit txn here
            int64_t first_txn_id = -1;
            if (!is_versioned_read) {
                get_partition_pending_txn_id(instance_id, idx.db_id(), idx.table_id(),
                                             idx.partition_id(), tablet_id, ss, code, msg,
                                             first_txn_id, txn.get());
                if (code != MetaServiceCode::OK) {
                    return;
                }
            } else {
                err = reader.get_partition_pending_txn_id(txn.get(), idx.partition_id(),
                                                          &first_txn_id);
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format(
                            "failed to get versioned partition pending txn id, err={}, "
                            "partition_id={}, tablet_id={}",
                            err, idx.partition_id(), tablet_id);
                    LOG(WARNING) << msg;
                    return;
                }
            }
            if (first_txn_id >= 0) {
                stats.get_bytes += txn->get_bytes();
                stats.get_counter += txn->num_get_keys();
                txn.reset();
                TEST_SYNC_POINT_CALLBACK("get_rowset::advance_last_pending_txn_id", &first_txn_id);
                std::shared_ptr<TxnLazyCommitTask> task =
                        txn_lazy_committer_->submit(instance_id, first_txn_id);

                std::tie(code, msg) = task->wait();
                if (code != MetaServiceCode::OK) {
                    LOG(WARNING) << "advance_last_txn failed last_txn=" << first_txn_id
                                 << " code=" << code << " msg=" << msg;
                    return;
                }
                continue;
            }
        }

        // TODO(plat1ko): Judge if tablet has been dropped (in dropped index/partition)

        TabletStatsPB tablet_stat;
        if (!is_versioned_read) {
            internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, tablet_stat);
            if (code != MetaServiceCode::OK) return;
        } else {
            err = reader.get_tablet_compact_stats(txn.get(), tablet_id, &tablet_stat, nullptr);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get tablet compact stats, err={}, tablet_id={}", err,
                                  tablet_id);
                LOG(WARNING) << msg;
                return;
            }
        }
        VLOG_DEBUG << "tablet_id=" << tablet_id << " stats=" << proto_to_json(tablet_stat);

        int64_t bc_cnt = tablet_stat.base_compaction_cnt();
        int64_t cc_cnt = tablet_stat.cumulative_compaction_cnt();
        int64_t cp = tablet_stat.cumulative_point();

        response->mutable_stats()->CopyFrom(tablet_stat);

        int64_t req_start = request->start_version();
        int64_t req_end = request->end_version();
        req_end = req_end < 0 ? std::numeric_limits<int64_t>::max() - 1 : req_end;

        //==========================================================================
        //      Find version ranges to be synchronized due to compaction
        //==========================================================================
        if (req_bc_cnt > bc_cnt || req_cc_cnt > cc_cnt || req_cp > cp) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "no valid compaction_cnt or cumulative_point given. req_bc_cnt=" << req_bc_cnt
               << ", bc_cnt=" << bc_cnt << ", req_cc_cnt=" << req_cc_cnt << ", cc_cnt=" << cc_cnt
               << ", req_cp=" << req_cp << ", cp=" << cp << " tablet_id=" << tablet_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        if (!is_versioned_read) {
            for (auto [start, end] : versions) {
                internal_get_rowset(txn.get(), start, end, instance_id, tablet_id, code, msg,
                                    response);
                if (code != MetaServiceCode::OK) {
                    return;
                }
            }
        } else {
            for (auto [start, end] : versions) {
                std::vector<RowsetMetaCloudPB> rowset_metas;
                TxnErrorCode err =
                        reader.get_rowset_metas(txn.get(), tablet_id, start, end, &rowset_metas);
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format(
                            "failed to get versioned rowset, err={}, tablet_id={}, version=[{}-{}]",
                            err, tablet_id, start, end);
                    LOG(WARNING) << msg;
                    return;
                }

                std::move(rowset_metas.begin(), rowset_metas.end(),
                          google::protobuf::RepeatedPtrFieldBackInserter(
                                  response->mutable_rowset_meta()));
            }
        }

        // get referenced schema
        std::unordered_map<int32_t, doris::TabletSchemaCloudPB*> version_to_schema;
        for (auto& rowset_meta : *response->mutable_rowset_meta()) {
            if (rowset_meta.has_tablet_schema()) {
                version_to_schema.emplace(rowset_meta.tablet_schema().schema_version(),
                                          rowset_meta.mutable_tablet_schema());
                rowset_meta.set_schema_version(rowset_meta.tablet_schema().schema_version());
            }
            rowset_meta.set_index_id(idx.index_id());
        }
        bool need_read_schema_dict = false;
        auto arena = response->GetArena();
        for (auto& rowset_meta : *response->mutable_rowset_meta()) {
            if (rowset_meta.has_schema_dict_key_list()) {
                need_read_schema_dict = true;
            }
            if (rowset_meta.has_tablet_schema()) continue;
            if (!rowset_meta.has_schema_version()) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format(
                        "rowset_meta must have either schema or schema_version, "
                        "rowset_version=[{}-{}]",
                        rowset_meta.start_version(), rowset_meta.end_version());
                return;
            }
            if (auto it = version_to_schema.find(rowset_meta.schema_version());
                it != version_to_schema.end()) {
                if (arena != nullptr) {
                    rowset_meta.set_allocated_tablet_schema(it->second);
                } else {
                    rowset_meta.mutable_tablet_schema()->CopyFrom(*it->second);
                }
            } else {
                auto key = meta_schema_key(
                        {instance_id, idx.index_id(), rowset_meta.schema_version()});
                if (!is_versioned_read) {
                    if (!try_fetch_and_parse_schema(txn.get(), rowset_meta, key, code, msg)) {
                        return;
                    }
                } else {
                    // TODO: support versioned write schema
                    if (!try_fetch_and_parse_schema(txn.get(), rowset_meta, key, code, msg)) {
                        return;
                    }
                }
                version_to_schema.emplace(rowset_meta.schema_version(),
                                          rowset_meta.mutable_tablet_schema());
            }
        }

        if (need_read_schema_dict && request->schema_op() != GetRowsetRequest::NO_DICT) {
            read_schema_dict(code, msg, instance_id, idx.index_id(), txn.get(),
                             response->mutable_rowset_meta(), response->mutable_schema_dict(),
                             request->schema_op());
            if (code != MetaServiceCode::OK) return;
        }
        TEST_SYNC_POINT_CALLBACK("get_rowset::finish", &response);
        break;
    } while (true);
}

void MetaServiceImpl::get_tablet_stats(::google::protobuf::RpcController* controller,
                                       const GetTabletStatsRequest* request,
                                       GetTabletStatsResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_tablet_stats, get);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet_stats)

    for (auto& i : request->tablet_idx()) {
        TabletIndexPB idx(i);
        // FIXME(plat1ko): Get all tablet stats in one txn
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = fmt::format("failed to create txn, tablet_id={}", idx.tablet_id());
            return;
        }
        DORIS_CLOUD_DEFER {
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
            // the txn is not a local variable, if not reset will count last res twice
            txn.reset(nullptr);
        };
        if (!(/* idx.has_db_id() && */ idx.has_table_id() && idx.has_index_id() &&
              idx.has_partition_id() && i.has_tablet_id())) {
            get_tablet_idx(code, msg, txn.get(), instance_id, idx.tablet_id(), idx);
            if (code != MetaServiceCode::OK) return;
        }
        auto tablet_stats = response->add_tablet_stats();
        internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, *tablet_stats, true);
        if (code != MetaServiceCode::OK) {
            response->clear_tablet_stats();
            break;
        }
#ifdef NDEBUG
        // Force data size >= 0 to reduce the losses caused by bugs
        if (tablet_stats->data_size() < 0) tablet_stats->set_data_size(0);
        if (tablet_stats->index_size() < 0) tablet_stats->set_index_size(0);
        if (tablet_stats->segment_size() < 0) tablet_stats->set_segment_size(0);
#endif
    }
}

static bool check_delete_bitmap_lock(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                                     std::unique_ptr<Transaction>& txn, std::string& instance_id,
                                     int64_t table_id, int64_t lock_id, int64_t lock_initiator,
                                     std::string& lock_key, DeleteBitmapUpdateLockPB& lock_info,
                                     std::string use_version, std::string log = "") {
    std::string lock_val;
    LOG(INFO) << "check_delete_bitmap_lock, table_id=" << table_id << " lock_id=" << lock_id
              << " initiator=" << lock_initiator << " key=" << hex(lock_key) << log
              << " use_version=" << use_version;
    auto err = txn->get(lock_key, &lock_val);
    TEST_SYNC_POINT_CALLBACK("check_delete_bitmap_lock.inject_get_lock_key_err", &err);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = "lock id key not found";
        code = MetaServiceCode::LOCK_EXPIRED;
        return false;
    }
    if (err != TxnErrorCode::TXN_OK) {
        ss << "failed to get delete bitmap lock info, err=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return false;
    }
    if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse DeleteBitmapUpdateLockPB";
        return false;
    }
    TEST_SYNC_POINT_CALLBACK("check_delete_bitmap_lock.set_lock_info", &lock_info);
    if (lock_info.lock_id() != lock_id) {
        ss << "lock id not match, locked by lock_id=" << lock_info.lock_id();
        msg = ss.str();
        code = MetaServiceCode::LOCK_EXPIRED;
        return false;
    }
    if (use_version == "v2" && is_job_delete_bitmap_lock_id(lock_id)) {
        std::string tablet_job_key = mow_tablet_job_key({instance_id, table_id, lock_initiator});
        std::string tablet_job_val;
        err = txn->get(tablet_job_key, &tablet_job_val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ss << "tablet job key not found, table_id=" << table_id << " lock_id=" << lock_id
               << " initiator=" << lock_initiator;
            msg = ss.str();
            code = MetaServiceCode::LOCK_EXPIRED;
            return false;
        }
        if (err != TxnErrorCode::TXN_OK) {
            ss << "failed to get mow tablet job info, err=" << err;
            msg = ss.str();
            code = cast_as<ErrCategory::READ>(err);
            return false;
        }
        // not check expired time
        return true;
    } else {
        bool found = false;
        for (auto initiator : lock_info.initiators()) {
            if (lock_initiator == initiator) {
                found = true;
                break;
            }
        }
        if (!found) {
            msg = "lock initiator not exist";
            code = MetaServiceCode::LOCK_EXPIRED;
            return false;
        }
    }
    return true;
}

void MetaServiceImpl::get_delete_bitmap_lock_version(std::string& use_version,
                                                     std::string& instance_id) {
    use_version = delete_bitmap_lock_white_list_->get_delete_bitmap_lock_version(instance_id);
}

static bool remove_pending_delete_bitmap(MetaServiceCode& code, std::string& msg,
                                         std::stringstream& ss, std::unique_ptr<Transaction>& txn,
                                         std::string& instance_id, int64_t tablet_id) {
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
    std::string pending_val;
    auto err = txn->get(pending_key, &pending_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        ss << "failed to get delete bitmap pending info, instance_id=" << instance_id
           << " tablet_id=" << tablet_id << " key=" << hex(pending_key) << " err=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return false;
    }

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return true;
    }

    // delete delete bitmap of expired txn
    PendingDeleteBitmapPB pending_info;
    if (!pending_info.ParseFromString(pending_val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse PendingDeleteBitmapPB";
        return false;
    }
    for (auto& delete_bitmap_key : pending_info.delete_bitmap_keys()) {
        // FIXME: Don't expose the implementation details of splitting large value
        // remove large value (>90*1000)
        std::string end_key = delete_bitmap_key;
        encode_int64(INT64_MAX, &end_key);
        txn->remove(delete_bitmap_key, end_key);
        LOG(INFO) << "xxx remove pending delete bitmap, delete_bitmap_key="
                  << hex(delete_bitmap_key);
    }
    return true;
}

// When a load txn retries in publish phase with different version to publish, it will gain delete bitmap lock
// many times. these locks are *different*, but they are the same in the current implementation because they have
// the same lock_id and initiator and don't have version info. If some delete bitmap calculation task with version X
// on BE lasts long and try to update delete bitmaps on MS when the txn gains the lock in later retries
// with version Y(Y > X) to publish. It may wrongly update version X's delete bitmaps because the lock don't have version info.
//
// This function checks whether the partition version is correct when updating the delete bitmap
// to avoid wrongly update an visible version's delete bitmaps.
// 1. get the db id with txn id
// 2. get the partition version with db id, table id and partition id
// 3. check if the partition version matches the updating version
static bool check_partition_version_when_update_delete_bitmap(
        MetaServiceCode& code, std::string& msg, std::unique_ptr<Transaction>& txn,
        std::string& instance_id, int64_t table_id, int64_t partition_id, int64_t tablet_id,
        int64_t txn_id, int64_t next_visible_version, bool is_versioned_read) {
    if (partition_id <= 0) {
        LOG(WARNING) << fmt::format(
                "invalid partition_id, skip to check partition version. txn={}, "
                "table_id={}, partition_id={}, tablet_id={}",
                txn_id, table_id, partition_id, tablet_id);
        return true;
    }
    // Get db id with txn id
    std::string index_val;
    const std::string index_key = txn_index_key({instance_id, txn_id});
    auto err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get db id, txn_id={} err={}", txn_id, err);
        LOG(WARNING) << msg;
        return false;
    }

    TxnIndexPB index_pb;
    if (!index_pb.ParseFromString(index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("failed to parse txn_index_pb, txn_id={}", txn_id);
        LOG(WARNING) << msg;
        return false;
    }

    DCHECK(index_pb.has_tablet_index())
            << fmt::format("txn={}, table_id={}, partition_id={}, tablet_id={}, index_pb={}",
                           txn_id, table_id, partition_id, tablet_id, proto_to_json(index_pb));
    DCHECK(index_pb.tablet_index().has_db_id())
            << fmt::format("txn={}, table_id={}, partition_id={}, tablet_id={}, index_pb={}",
                           txn_id, table_id, partition_id, tablet_id, proto_to_json(index_pb));
    if (!index_pb.has_tablet_index() || !index_pb.tablet_index().has_db_id()) {
        LOG(WARNING) << fmt::format(
                "has no db_id in TxnIndexPB, skip to check partition version. txn={}, "
                "table_id={}, partition_id={}, tablet_id={}, index_pb={}",
                txn_id, table_id, partition_id, tablet_id, proto_to_json(index_pb));
        return true;
    }
    int64_t cur_max_version {-1};
    if (!is_versioned_read) {
        int64_t db_id = index_pb.tablet_index().db_id();
        std::string ver_key = partition_version_key({instance_id, db_id, table_id, partition_id});
        std::string ver_val;
        err = txn->get(ver_key, &ver_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get partition version, txn_id={}, tablet={}, err={}",
                              txn_id, tablet_id, err);
            LOG(WARNING) << msg;
            return false;
        }

        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            cur_max_version = 1;
        } else {
            VersionPB version_pb;
            if (!version_pb.ParseFromString(ver_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("failed to parse version_pb, txn_id={}, tablet={}, key={}",
                                  txn_id, tablet_id, hex(ver_key));
                LOG(WARNING) << msg;
                return false;
            }
            DCHECK(version_pb.has_version());
            cur_max_version = version_pb.version();

            if (version_pb.pending_txn_ids_size() > 0) {
                DCHECK(version_pb.pending_txn_ids_size() == 1);
                cur_max_version += version_pb.pending_txn_ids_size();
            }
        }
    } else {
        CHECK(false) << "versioned read is not supported yet";
    }

    if (cur_max_version + 1 != next_visible_version) {
        code = MetaServiceCode::VERSION_NOT_MATCH;
        msg = fmt::format(
                "check version failed when update_delete_bitmap, txn={}, table_id={}, "
                "partition_id={}, tablet_id={}, found partition's max version is {}, but "
                "request next_visible_version is {}",
                txn_id, table_id, partition_id, tablet_id, cur_max_version, next_visible_version);
        return false;
    }
    return true;
}

// return false if the delete bitmap key already exists
[[maybe_unused]] static bool check_delete_bitmap_kv_exists(MetaServiceCode& code, std::string& msg,
                                                           std::unique_ptr<Transaction>& txn,
                                                           const std::string& key,
                                                           std::string& instance_id,
                                                           int64_t tablet_id, int64_t lock_id) {
    std::string start_key {key};
    std::string end_key {start_key};
    encode_int64(INT64_MAX, &end_key);
    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(start_key, end_key, &it);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get delete bitmap, err={}, tablet_id={}, lock_id={}", err,
                          tablet_id, lock_id);
        return true; // skip to check if failed to get delete bitmap
    }
    if (it->has_next()) {
        auto [k, v] = it->next();
        std::string_view k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
        auto rowset_id = std::get<std::string>(std::get<0>(out[4]));
        auto version = std::get<std::int64_t>(std::get<0>(out[5]));
        auto segment_id = std::get<std::int64_t>(std::get<0>(out[6]));

        code = MetaServiceCode::UPDATE_OVERRIDE_EXISTING_KV;
        msg = fmt::format(
                "trying to update a existing delete bitmap KV, tablet_id={}, rowset_id={}, "
                "version={}, segment_id={}, lock_id={}, instance_id={}",
                tablet_id, rowset_id, version, segment_id, lock_id, instance_id);
        LOG_WARNING(msg);
        return false;
    }
    return true;
}

void MetaServiceImpl::update_delete_bitmap(google::protobuf::RpcController* controller,
                                           const UpdateDeleteBitmapRequest* request,
                                           UpdateDeleteBitmapResponse* response,
                                           ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_delete_bitmap, get, put, del);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    if (request->without_lock() && request->has_pre_rowset_agg_end_version() &&
        request->pre_rowset_agg_end_version() > 0) {
        if (request->rowset_ids_size() != request->pre_rowset_versions_size()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "pre rowset version size=" << request->pre_rowset_versions_size()
               << " not equal to rowset size=" << request->rowset_ids_size();
            msg = ss.str();
            return;
        }
    }

    std::string use_version =
            delete_bitmap_lock_white_list_->get_delete_bitmap_lock_version(instance_id);
    RPC_RATE_LIMIT(update_delete_bitmap)

    uint64_t fdb_txn_size = 0;
    auto table_id = request->table_id();
    auto tablet_id = request->tablet_id();

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    bool is_explicit_txn = (request->has_is_explicit_txn() && request->is_explicit_txn());
    bool is_first_sub_txn = (is_explicit_txn && request->txn_id() == request->lock_id());
    bool without_lock = request->has_without_lock() ? request->without_lock() : false;
    std::string log = ", update delete bitmap for tablet " + std::to_string(tablet_id);
    if (!without_lock) {
        // 1. Check whether the lock expires
        std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
        DeleteBitmapUpdateLockPB lock_info;
        if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, table_id, request->lock_id(),
                                      request->initiator(), lock_key, lock_info, use_version,
                                      log)) {
            LOG(WARNING) << "failed to check delete bitmap lock, table_id=" << table_id
                         << " request lock_id=" << request->lock_id()
                         << " request initiator=" << request->initiator() << " msg " << msg;
            return;
        }
        // 2. Process pending delete bitmap

        // if this is a txn load and is not the first sub txn, we should not remove
        // the pending delete bitmaps written by previous sub txns
        if (!is_explicit_txn || is_first_sub_txn) {
            if (!remove_pending_delete_bitmap(code, msg, ss, txn, instance_id, tablet_id)) {
                return;
            }
        }
    }

    // 3. check if partition's version matches
    if (request->lock_id() > 0 && request->has_txn_id() && request->partition_id() &&
        request->has_next_visible_version()) {
        bool is_versioned_read = is_version_read_enabled(instance_id);
        if (!check_partition_version_when_update_delete_bitmap(
                    code, msg, txn, instance_id, table_id, request->partition_id(), tablet_id,
                    request->txn_id(), request->next_visible_version(), is_versioned_read)) {
            return;
        }
    }

    // 4. store all pending delete bitmap for this txn
    PendingDeleteBitmapPB delete_bitmap_keys;
    for (size_t i = 0; i < request->rowset_ids_size(); ++i) {
        MetaDeleteBitmapInfo key_info {instance_id, tablet_id, request->rowset_ids(i),
                                       request->versions(i), request->segment_ids(i)};
        std::string key;
        meta_delete_bitmap_key(key_info, &key);
        delete_bitmap_keys.add_delete_bitmap_keys(key);
    }

    PendingDeleteBitmapPB previous_pending_info;
    if (is_explicit_txn && !is_first_sub_txn) {
        std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
        std::string pending_val;
        auto err = txn->get(pending_key, &pending_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ss << "failed to get delete bitmap pending info, instance_id=" << instance_id
               << " tablet_id=" << tablet_id << " key=" << hex(pending_key) << " err=" << err;
            msg = ss.str();
            code = cast_as<ErrCategory::READ>(err);
            return;
        }

        if (err == TxnErrorCode::TXN_OK) {
            // pending delete bitmaps of previous sub txns
            if (!previous_pending_info.ParseFromString(pending_val)) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse PendingDeleteBitmapPB";
                return;
            }
        }
    }

    // no need to record pending key for compaction,
    // because delete bitmap will attach to new rowset, just delete new rowset if failed
    // lock_id > 0 : load
    // lock_id = -1 : compaction
    // lock_id = -2 : schema change
    // lock_id = -3 : compaction update delete bitmap without lock
    if (request->lock_id() > 0 || request->lock_id() == -2) {
        std::string pending_val;
        if (is_explicit_txn && !is_first_sub_txn) {
            // put current delete bitmap keys and previous sub txns' into tablet's pending delete bitmap KV
            PendingDeleteBitmapPB total_pending_info {std::move(previous_pending_info)};
            auto* cur_pending_keys = delete_bitmap_keys.mutable_delete_bitmap_keys();
            total_pending_info.mutable_delete_bitmap_keys()->Add(cur_pending_keys->begin(),
                                                                 cur_pending_keys->end());
            if (!total_pending_info.SerializeToString(&pending_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize pending delete bitmap";
                return;
            }
        } else {
            if (!delete_bitmap_keys.SerializeToString(&pending_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize pending delete bitmap";
                return;
            }
        }
        std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
        txn->put(pending_key, pending_val);
        fdb_txn_size = fdb_txn_size + pending_key.size() + pending_val.size();
        LOG(INFO) << "xxx update delete bitmap put pending_key=" << hex(pending_key)
                  << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
                  << " value_size: " << pending_val.size();
    }

    // 5. Update delete bitmap for curent txn
    size_t current_key_count = 0;
    size_t current_value_count = 0;
    size_t total_key_count = 0;
    size_t total_value_count = 0;
    size_t total_txn_put_keys = 0;
    size_t total_txn_put_bytes = 0;
    size_t total_txn_size = 0;
    size_t total_txn_count = 0;
    std::set<std::string> non_exist_rowset_ids;
    for (size_t i = 0; i < request->rowset_ids_size(); ++i) {
        auto& key = delete_bitmap_keys.delete_bitmap_keys(i);
        auto& val = request->segment_delete_bitmaps(i);

        // Split into multiple fdb transactions, because the size of one fdb
        // transaction can't exceed 10MB.
        if (txn->approximate_bytes() + key.size() * 3 + val.size() > config::max_txn_commit_byte) {
            LOG(INFO) << "fdb txn size more than " << config::max_txn_commit_byte
                      << ", current size: " << txn->approximate_bytes()
                      << ", tablet_id: " << tablet_id << " lock_id=" << request->lock_id()
                      << " initiator=" << request->initiator() << ", need to commit";
            err = txn->commit();
            TEST_SYNC_POINT_CALLBACK("update_delete_bitmap:commit:err", request->initiator(), i,
                                     &err);
            total_txn_put_keys += txn->num_put_keys();
            total_txn_put_bytes += txn->put_bytes();
            total_txn_size += txn->approximate_bytes();
            total_txn_count++;
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::COMMIT>(err);
                ss << "failed to update delete bitmap, err=" << err << " tablet_id=" << tablet_id
                   << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
                   << " delete_bitmap_key=" << current_key_count
                   << " delete_bitmap_value=" << current_value_count
                   << " put_size=" << txn->put_bytes() << " num_put_keys=" << txn->num_put_keys()
                   << " txn_size=" << txn->approximate_bytes();
                msg = ss.str();
                g_bvar_update_delete_bitmap_fail_counter << 1;
                return;
            }
            stats.get_bytes += txn->get_bytes();
            stats.put_bytes += txn->put_bytes();
            stats.del_bytes += txn->delete_bytes();
            stats.get_counter += txn->num_get_keys();
            stats.put_counter += txn->num_put_keys();
            stats.del_counter += txn->num_del_keys();
            current_key_count = 0;
            current_value_count = 0;
            TxnErrorCode err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::CREATE>(err);
                msg = "failed to init txn";
                return;
            }
            if (!without_lock) {
                std::string lock_key =
                        meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
                DeleteBitmapUpdateLockPB lock_info;
                if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, table_id,
                                              request->lock_id(), request->initiator(), lock_key,
                                              lock_info, use_version, log)) {
                    LOG(WARNING) << "failed to check delete bitmap lock, table_id=" << table_id
                                 << " request lock_id=" << request->lock_id()
                                 << " request initiator=" << request->initiator() << " msg " << msg;
                    return;
                }
            }
        }

#ifdef ENABLE_INJECTION_POINT
        if (config::enable_update_delete_bitmap_kv_check) {
            if (!check_delete_bitmap_kv_exists(code, msg, txn, key, instance_id, tablet_id,
                                               request->lock_id())) {
                return;
            }
        }
#endif

        if (without_lock && request->has_pre_rowset_agg_end_version() &&
            request->pre_rowset_agg_end_version() > 0) {
            // check the rowset exists
            if (non_exist_rowset_ids.contains(request->rowset_ids(i))) {
                LOG(INFO) << "skip update delete bitmap, rowset_id=" << request->rowset_ids(i)
                          << " version=" << request->pre_rowset_versions(i)
                          << " tablet_id=" << tablet_id << " because the rowset does not exist";
                continue;
            }
            auto rowset_key =
                    meta_rowset_key({instance_id, tablet_id, request->pre_rowset_versions(i)});
            std::string rowset_val;
            err = txn->get(rowset_key, &rowset_val);
            if (err != TxnErrorCode::TXN_OK && TxnErrorCode::TXN_KEY_NOT_FOUND != err) {
                ss << "failed to get rowset, instance_id=" << instance_id
                   << " tablet_id=" << tablet_id << " version=" << request->pre_rowset_versions(i)
                   << " err=" << err;
                msg = ss.str();
                code = cast_as<ErrCategory::READ>(err);
                return;
            }
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                non_exist_rowset_ids.emplace(request->rowset_ids(i));
                LOG(INFO) << "skip update delete bitmap, rowset_id=" << request->rowset_ids(i)
                          << " version=" << request->pre_rowset_versions(i)
                          << " tablet_id=" << tablet_id << " because the rowset is not exist";
                continue;
            }
            doris::RowsetMetaCloudPB rs;
            if (!rs.ParseFromArray(rowset_val.data(), rowset_val.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed rowset meta, unable to deserialize, tablet_id=" << tablet_id
                   << " key=" << hex(rowset_key);
                msg = ss.str();
                return;
            }
            if (rs.rowset_id_v2() != request->rowset_ids(i)) {
                LOG(INFO) << "skip update delete bitmap, rowset_id=" << request->rowset_ids(i)
                          << " version=" << request->pre_rowset_versions(i)
                          << " tablet_id=" << tablet_id << " because the rowset is not exist";
                non_exist_rowset_ids.emplace(request->rowset_ids(i));
                continue;
            }
        }
        // remove first
        if (request->lock_id() == COMPACTION_WITHOUT_LOCK_DELETE_BITMAP_LOCK_ID) {
            auto& start_key = key;
            std::string end_key {start_key};
            encode_int64(INT64_MAX, &end_key);
            txn->remove(start_key, end_key);
            LOG(INFO) << "xxx remove delete_bitmap_key=" << hex(start_key)
                      << " tablet_id=" << tablet_id << " lock_id=" << request->lock_id()
                      << " initiator=" << request->initiator();
        }
        // splitting large values (>90*1000) into multiple KVs
        cloud::blob_put(txn.get(), key, val, 0);
        current_key_count++;
        current_value_count += val.size();
        total_key_count++;
        total_value_count += val.size();
        VLOG_DEBUG << "xxx update delete bitmap put delete_bitmap_key=" << hex(key)
                   << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
                   << " key_size: " << key.size() << " value_size: " << val.size();
    }

    // remove pre rowset delete bitmap
    if (request->has_pre_rowset_agg_start_version() && request->has_pre_rowset_agg_end_version() &&
        request->pre_rowset_agg_start_version() < request->pre_rowset_agg_end_version()) {
        std::string pre_rowset_id = "";
        for (size_t i = 0; i < request->rowset_ids_size(); ++i) {
            if (request->rowset_ids(i) == pre_rowset_id) {
                continue;
            }
            if (non_exist_rowset_ids.contains(request->rowset_ids(i))) {
                LOG(INFO) << "skip remove pre rowsets delete bitmap, rowset_id="
                          << request->rowset_ids(i) << " tablet_id=" << tablet_id
                          << " because the rowset does not exist";
                continue;
            }
            pre_rowset_id = request->rowset_ids(i);
            auto delete_bitmap_start =
                    meta_delete_bitmap_key({instance_id, tablet_id, request->rowset_ids(i),
                                            request->pre_rowset_agg_start_version(), 0});
            auto delete_bitmap_end =
                    meta_delete_bitmap_key({instance_id, tablet_id, request->rowset_ids(i),
                                            request->pre_rowset_agg_end_version(), 0});
            txn->remove(delete_bitmap_start, delete_bitmap_end);
            LOG(INFO) << "remove pre rowsets delete bitmap, tablet_id=" << tablet_id
                      << ", rowset=" << request->rowset_ids(i)
                      << ", start_version=" << request->pre_rowset_agg_start_version()
                      << ", end_version=" << request->pre_rowset_agg_end_version()
                      << ", start_key=" << hex(delete_bitmap_start)
                      << ", end_key=" << hex(delete_bitmap_end);
        }
    }
    err = txn->commit();
    total_txn_put_keys += txn->num_put_keys();
    total_txn_put_bytes += txn->put_bytes();
    total_txn_size += txn->approximate_bytes();
    total_txn_count++;
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_CONFLICT) {
            g_bvar_delete_bitmap_lock_txn_put_conflict_counter << 1;
        }
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update delete bitmap, err=" << err << " tablet_id=" << tablet_id
           << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
           << " delete_bitmap_key=" << current_key_count
           << " delete_bitmap_value=" << current_value_count << " put_size=" << txn->put_bytes()
           << " num_put_keys=" << txn->num_put_keys() << " txn_size=" << txn->approximate_bytes();
        msg = ss.str();
        g_bvar_update_delete_bitmap_fail_counter << 1;
        return;
    }
    LOG(INFO) << "update_delete_bitmap tablet_id=" << tablet_id << " lock_id=" << request->lock_id()
              << " initiator=" << request->initiator()
              << " rowset_num=" << request->rowset_ids_size()
              << " total_key_count=" << total_key_count
              << " total_value_count=" << total_value_count << " without_lock=" << without_lock
              << " total_txn_put_keys=" << total_txn_put_keys
              << " total_txn_put_bytes=" << total_txn_put_bytes
              << " total_txn_size=" << total_txn_size << " total_txn_count=" << total_txn_count
              << " instance_id=" << instance_id << " use_version=" << use_version;
}

void MetaServiceImpl::get_delete_bitmap(google::protobuf::RpcController* controller,
                                        const GetDeleteBitmapRequest* request,
                                        GetDeleteBitmapResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_delete_bitmap, get);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_delete_bitmap)

    auto tablet_id = request->tablet_id();
    auto& rowset_ids = request->rowset_ids();
    auto& begin_versions = request->begin_versions();
    auto& end_versions = request->end_versions();
    if (rowset_ids.size() != begin_versions.size() || rowset_ids.size() != end_versions.size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "rowset and version size not match. "
           << " rowset_size=" << rowset_ids.size()
           << " begin_version_size=" << begin_versions.size()
           << " end_version_size=" << end_versions.size();
        msg = ss.str();
        return;
    }

    response->set_tablet_id(tablet_id);
    int64_t delete_bitmap_num = 0;
    int64_t delete_bitmap_byte = 0;
    bool test = false;
    TEST_SYNC_POINT_CALLBACK("get_delete_bitmap_test", &test);

    for (size_t i = 0; i < rowset_ids.size(); i++) {
        // create a new transaction every time, avoid using one transaction that takes too long
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        MetaDeleteBitmapInfo start_key_info {instance_id, tablet_id, rowset_ids[i],
                                             begin_versions[i], 0};
        MetaDeleteBitmapInfo end_key_info {instance_id, tablet_id, rowset_ids[i], end_versions[i],
                                           INT64_MAX};
        std::string start_key;
        std::string end_key;
        meta_delete_bitmap_key(start_key_info, &start_key);
        meta_delete_bitmap_key(end_key_info, &end_key);

        // in order to get splitted large value
        encode_int64(INT64_MAX, &end_key);

        std::unique_ptr<RangeGetIterator> it;
        int64_t last_ver = -1;
        int64_t last_seg_id = -1;
        int64_t round = 0;
        do {
            if (test) {
                LOG(INFO) << "test";
                err = txn->get(start_key, end_key, &it, false, 2);
            } else {
                err = txn->get(start_key, end_key, &it);
            }
            TEST_SYNC_POINT_CALLBACK("get_delete_bitmap_err", &round, &err);
            int64_t retry = 0;
            while (err == TxnErrorCode::TXN_TOO_OLD && retry < 3) {
                stats.get_bytes += txn->get_bytes();
                stats.get_counter += txn->num_get_keys();
                txn = nullptr;
                err = txn_kv_->create_txn(&txn);
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::CREATE>(err);
                    ss << "failed to init txn, retry=" << retry << ", internal round=" << round;
                    msg = ss.str();
                    return;
                }
                if (test) {
                    err = txn->get(start_key, end_key, &it, false, 2);
                } else {
                    err = txn->get(start_key, end_key, &it);
                }
                retry++;
                LOG(INFO) << "retry get delete bitmap, tablet=" << tablet_id << ", retry=" << retry
                          << ", internal round=" << round
                          << ", delete_bitmap_num=" << delete_bitmap_num
                          << ", delete_bitmap_byte=" << delete_bitmap_byte;
            }
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "internal error, failed to get delete bitmap, internal round=" << round
                   << ", ret=" << err;
                msg = ss.str();
                g_bvar_get_delete_bitmap_fail_counter << 1;
                return;
            }

            while (it->has_next()) {
                auto [k, v] = it->next();
                auto k1 = k;
                k1.remove_prefix(1);
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                decode_key(&k1, &out);
                // 0x01 "meta" ${instance_id}  "delete_bitmap" ${tablet_id}
                // ${rowset_id0} ${version1} ${segment_id0} -> DeleteBitmapPB
                auto ver = std::get<int64_t>(std::get<0>(out[5]));
                auto seg_id = std::get<int64_t>(std::get<0>(out[6]));

                // FIXME: Don't expose the implementation details of splitting large value.
                // merge splitted large values (>90*1000)
                if (ver != last_ver || seg_id != last_seg_id) {
                    response->add_rowset_ids(rowset_ids[i]);
                    response->add_segment_ids(seg_id);
                    response->add_versions(ver);
                    response->add_segment_delete_bitmaps(std::string(v));
                    last_ver = ver;
                    last_seg_id = seg_id;
                    delete_bitmap_num++;
                    delete_bitmap_byte += v.length();
                } else {
                    TEST_SYNC_POINT_CALLBACK("get_delete_bitmap_code", &code);
                    if (code != MetaServiceCode::OK) {
                        ss << "test get get_delete_bitmap fail, code=" << MetaServiceCode_Name(code)
                           << ", internal round=" << round;
                        msg = ss.str();
                        return;
                    }
                    delete_bitmap_byte += v.length();
                    response->mutable_segment_delete_bitmaps()->rbegin()->append(v);
                }
            }
            if (delete_bitmap_byte > config::max_get_delete_bitmap_byte) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "tablet=" << tablet_id << ", get_delete_bitmap_byte=" << delete_bitmap_byte
                   << ",exceed max byte";
                msg = ss.str();
                LOG(WARNING) << msg;
                g_bvar_get_delete_bitmap_fail_counter << 1;
                return;
            }
            round++;
            start_key = it->next_begin_key(); // Update to next smallest key for iteration
        } while (it->more());
        LOG(INFO) << "get delete bitmap for tablet=" << tablet_id << ", rowset=" << rowset_ids[i]
                  << ", start version=" << begin_versions[i] << ", end version=" << end_versions[i]
                  << ", internal round=" << round << ", delete_bitmap_num=" << delete_bitmap_num
                  << ", delete_bitmap_byte=" << delete_bitmap_byte;
    }
    LOG(INFO) << "finish get delete bitmap for tablet=" << tablet_id
              << ", delete_bitmap_num=" << delete_bitmap_num
              << ", delete_bitmap_byte=" << delete_bitmap_byte;

    if (request->has_idx()) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
        };
        TabletIndexPB idx(request->idx());
        TabletStatsPB tablet_stat;
        bool is_versioned_read = is_version_read_enabled(instance_id);
        if (!is_versioned_read) {
            internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, tablet_stat,
                                      true /*snapshot_read*/);
            if (code != MetaServiceCode::OK) {
                return;
            }
        } else {
            CHECK(false) << "versioned read is not supported yet";
        }
        // The requested compaction state and the actual compaction state are different, which indicates that
        // the requested rowsets are expired and their delete bitmap may have been deleted.
        if (request->base_compaction_cnt() != tablet_stat.base_compaction_cnt() ||
            request->cumulative_compaction_cnt() != tablet_stat.cumulative_compaction_cnt() ||
            request->cumulative_point() != tablet_stat.cumulative_point()) {
            code = MetaServiceCode::ROWSETS_EXPIRED;
            msg = "rowsets are expired";
            return;
        }
    }
}

static bool put_mow_tablet_job_key(MetaServiceCode& code, std::string& msg,
                                   std::unique_ptr<Transaction>& txn, std::string& instance_id,
                                   int64_t table_id, int64_t lock_id, int64_t initiator,
                                   int64_t expiration, std::string& current_lock_msg) {
    std::string tablet_job_key = mow_tablet_job_key({instance_id, table_id, initiator});
    std::string tablet_job_val;
    MowTabletJobPB mow_tablet_job;
    mow_tablet_job.set_expiration(expiration);
    mow_tablet_job.SerializeToString(&tablet_job_val);
    if (tablet_job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "MowTabletJobPB serialization error";
        return false;
    }
    txn->put(tablet_job_key, tablet_job_val);
    LOG(INFO) << "xxx put tablet job key=" << hex(tablet_job_key) << " table_id=" << table_id
              << " lock_id=" << lock_id << " initiator=" << initiator
              << " expiration=" << expiration << ", " << current_lock_msg;
    return true;
}

static bool put_delete_bitmap_update_lock_key(MetaServiceCode& code, std::string& msg,
                                              std::unique_ptr<Transaction>& txn, int64_t table_id,
                                              int64_t lock_id, int64_t initiator,
                                              std::string& lock_key,
                                              DeleteBitmapUpdateLockPB& lock_info,
                                              std::string& current_lock_msg) {
    std::string lock_val;
    lock_info.SerializeToString(&lock_val);
    if (lock_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "DeleteBitmapUpdateLockPB serialization error";
        return false;
    }
    txn->put(lock_key, lock_val);
    LOG(INFO) << "xxx put lock_key=" << hex(lock_key) << " table_id=" << table_id
              << " lock_id=" << lock_id << " initiator=" << initiator
              << " initiators_size=" << lock_info.initiators_size() << ", " << current_lock_msg;
    return true;
}

bool MetaServiceImpl::get_mow_tablet_stats_and_meta(MetaServiceCode& code, std::string& msg,
                                                    const GetDeleteBitmapUpdateLockRequest* request,
                                                    GetDeleteBitmapUpdateLockResponse* response,
                                                    std::string& instance_id, std::string& lock_key,
                                                    std::string lock_use_version, KVStats& stats) {
    bool require_tablet_stats =
            request->has_require_compaction_stats() ? request->require_compaction_stats() : false;
    if (!require_tablet_stats) return true;
    // this request is from fe when it commits txn for MOW table, we send the compaction stats
    // along with the GetDeleteBitmapUpdateLockResponse which will be sent to BE later to let
    // BE eliminate unnecessary sync_rowsets() calls if possible
    // 1. hold the delete bitmap update lock in MS(update lock_info.lock_id to current load's txn id)
    // 2. read tablets' stats
    // 3. check whether we still hold the delete bitmap update lock
    // these steps can be done in different fdb txns

    StopWatch read_stats_sw;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return false;
    }
    DORIS_CLOUD_DEFER {
        if (txn == nullptr) return;
        stats.get_bytes += txn->get_bytes();
        stats.put_bytes += txn->put_bytes();
        stats.del_bytes += txn->delete_bytes();
        stats.get_counter += txn->num_get_keys();
        stats.put_counter += txn->num_put_keys();
        stats.del_counter += txn->num_del_keys();
    };
    auto table_id = request->table_id();
    std::stringstream ss;
    bool is_versioned_read = is_version_read_enabled(instance_id);
    if (!config::enable_batch_get_mow_tablet_stats_and_meta) {
        for (const auto& tablet_idx : request->tablet_indexes()) {
            // 1. get compaction cnts
            TabletStatsPB tablet_stat;
            if (!is_versioned_read) {
                std::string stats_key =
                        stats_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                          tablet_idx.partition_id(), tablet_idx.tablet_id()});
                std::string stats_val;
                TxnErrorCode err = txn->get(stats_key, &stats_val);
                TEST_SYNC_POINT_CALLBACK(
                        "get_delete_bitmap_update_lock.get_compaction_cnts_inject_error", &err);
                if (err == TxnErrorCode::TXN_TOO_OLD) {
                    code = MetaServiceCode::OK;
                    err = txn_kv_->create_txn(&txn);
                    if (err != TxnErrorCode::TXN_OK) {
                        code = cast_as<ErrCategory::CREATE>(err);
                        ss << "failed to init txn when get tablet stats";
                        msg = ss.str();
                        return false;
                    }
                    err = txn->get(stats_key, &stats_val);
                }
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err,
                                      tablet_idx.tablet_id());
                    return false;
                }
                if (!tablet_stat.ParseFromArray(stats_val.data(), stats_val.size())) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = fmt::format("marformed tablet stats value, key={}", hex(stats_key));
                    return false;
                }
            } else {
                CHECK(false) << "versioned read is not supported yet";
            }
            response->add_base_compaction_cnts(tablet_stat.base_compaction_cnt());
            response->add_cumulative_compaction_cnts(tablet_stat.cumulative_compaction_cnt());
            response->add_cumulative_points(tablet_stat.cumulative_point());

            // 2. get tablet states
            doris::TabletMetaCloudPB tablet_meta;
            if (!is_versioned_read) {
                std::string tablet_meta_key =
                        meta_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                         tablet_idx.partition_id(), tablet_idx.tablet_id()});
                std::string tablet_meta_val;
                err = txn->get(tablet_meta_key, &tablet_meta_val);
                if (err != TxnErrorCode::TXN_OK) {
                    ss << "failed to get tablet meta"
                       << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? " (not found)" : "")
                       << " instance_id=" << instance_id << " tablet_id=" << tablet_idx.tablet_id()
                       << " key=" << hex(tablet_meta_key) << " err=" << err;
                    msg = ss.str();
                    code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                   ? MetaServiceCode::TABLET_NOT_FOUND
                                   : cast_as<ErrCategory::READ>(err);
                    return false;
                }
                if (!tablet_meta.ParseFromString(tablet_meta_val)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = "malformed tablet meta";
                    return false;
                }
            } else {
                CHECK(false) << "versioned read is not supported yet";
            }
            response->add_tablet_states(
                    static_cast<std::underlying_type_t<TabletStatePB>>(tablet_meta.tablet_state()));
        }
    } else if (!is_versioned_read) {
        // 1. get compaction cnts
        std::vector<std::string> stats_tablet_keys;
        for (const auto& tablet_idx : request->tablet_indexes()) {
            stats_tablet_keys.push_back(
                    stats_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                      tablet_idx.partition_id(), tablet_idx.tablet_id()}));
        }
        std::vector<std::optional<std::string>> stats_tablet_values;
        err = txn->batch_get(&stats_tablet_values, stats_tablet_keys);
        TEST_SYNC_POINT_CALLBACK("get_delete_bitmap_update_lock.get_compaction_cnts_inject_error",
                                 &err);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get tablet stats, err={} table_id={} lock_id={}", err,
                              table_id, request->lock_id());
            return false;
        }
        for (size_t i = 0; i < stats_tablet_keys.size(); i++) {
            if (!stats_tablet_values[i].has_value()) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err,
                                  request->tablet_indexes(i).tablet_id());
                return false;
            }
            TabletStatsPB tablet_stat;
            if (!tablet_stat.ParseFromString(stats_tablet_values[i].value())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("marformed tablet stats value");
                return false;
            }
            response->add_base_compaction_cnts(tablet_stat.base_compaction_cnt());
            response->add_cumulative_compaction_cnts(tablet_stat.cumulative_compaction_cnt());
            response->add_cumulative_points(tablet_stat.cumulative_point());
        }
        stats_tablet_keys.clear();
        stats_tablet_values.clear();
        DCHECK(request->tablet_indexes_size() == response->base_compaction_cnts_size());
        DCHECK(request->tablet_indexes_size() == response->cumulative_compaction_cnts_size());
        DCHECK(request->tablet_indexes_size() == response->cumulative_points_size());

        // 2. get tablet states
        std::vector<std::string> tablet_meta_keys;
        for (const auto& tablet_idx : request->tablet_indexes()) {
            tablet_meta_keys.push_back(
                    meta_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                     tablet_idx.partition_id(), tablet_idx.tablet_id()}));
        }
        std::vector<std::optional<std::string>> tablet_meta_values;
        err = txn->batch_get(&tablet_meta_values, tablet_meta_keys);
        if (err == TxnErrorCode::TXN_TOO_OLD) {
            code = MetaServiceCode::OK;
            err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::CREATE>(err);
                ss << "failed to init txn when get tablet meta";
                msg = ss.str();
                return false;
            }
            err = txn->batch_get(&tablet_meta_values, tablet_meta_keys);
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get tablet meta, err={} table_id={} lock_id={}", err,
                              table_id, request->lock_id());
            return false;
        }
        for (size_t i = 0; i < tablet_meta_keys.size(); i++) {
            if (!tablet_meta_values[i].has_value()) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get tablet meta, err={} tablet_id={}", err,
                                  request->tablet_indexes(i).tablet_id());
                return false;
            }
            doris::TabletMetaCloudPB tablet_meta;
            if (!tablet_meta.ParseFromString(tablet_meta_values[i].value())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("marformed tablet meta value");
                return false;
            }
            response->add_tablet_states(
                    static_cast<std::underlying_type_t<TabletStatePB>>(tablet_meta.tablet_state()));
        }
        DCHECK(request->tablet_indexes_size() == response->tablet_states_size());
    } else {
        CHECK(false) << "versioned read is not supported yet";
    }

    read_stats_sw.pause();
    LOG(INFO) << fmt::format(
            "table_id={}, tablet_idxes.size()={}, read tablet compaction cnts and tablet states "
            "cost={} ms",
            table_id, request->tablet_indexes().size(), read_stats_sw.elapsed_us() / 1000);

    DeleteBitmapUpdateLockPB lock_info_tmp;
    if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, table_id, request->lock_id(),
                                  request->initiator(), lock_key, lock_info_tmp,
                                  lock_use_version)) {
        LOG(WARNING) << "failed to check delete bitmap lock after get tablet stats and tablet "
                        "states, table_id="
                     << table_id << " request lock_id=" << request->lock_id()
                     << " request initiator=" << request->initiator() << " code=" << code
                     << " msg=" << msg;
        return false;
    }
    return true;
}

void MetaServiceImpl::get_delete_bitmap_update_lock_v2(
        google::protobuf::RpcController* controller,
        const GetDeleteBitmapUpdateLockRequest* request,
        GetDeleteBitmapUpdateLockResponse* response, ::google::protobuf::Closure* done,
        std::string& instance_id, MetaServiceCode& code, std::string& msg, std::stringstream& ss,
        KVStats& stats) {
    VLOG_DEBUG << "get delete bitmap update lock in v2 for table=" << request->table_id()
               << ",lock id=" << request->lock_id() << ",initiator=" << request->initiator();
    auto table_id = request->table_id();
    bool urgent = request->has_urgent() && request->urgent();
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    bool first_retry = true;
    int64_t retry = 0;
    while (retry <= 1) {
        retry++;
        response->Clear();
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.put_bytes += txn->put_bytes();
            stats.del_bytes += txn->delete_bytes();
            stats.get_counter += txn->num_get_keys();
            stats.put_counter += txn->num_put_keys();
            stats.del_counter += txn->num_del_keys();
        };
        std::string lock_val;
        DeleteBitmapUpdateLockPB lock_info;
        err = txn->get(lock_key, &lock_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ss << "failed to get delete bitmap update lock, instance_id=" << instance_id
               << " table_id=" << table_id << " key=" << hex(lock_key) << " err=" << err;
            msg = ss.str();
            code = MetaServiceCode::KV_TXN_GET_ERR;
            return;
        }
        using namespace std::chrono;
        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        int64_t expiration = now + request->expiration();
        bool lock_key_not_found = false;
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            lock_key_not_found = true;
            std::string current_lock_msg = "lock key not found";
            lock_info.set_lock_id(request->lock_id());
            // compaction does not use this expiration, only used when upgrade ms
            lock_info.set_expiration(expiration);
            if (!is_job_delete_bitmap_lock_id(request->lock_id())) {
                lock_info.add_initiators(request->initiator());
            } else {
                // in normal case, this should remove 0 kvs
                // but when upgrade ms, if there are ms with old and new versions, it works
                std::string tablet_job_key_begin = mow_tablet_job_key({instance_id, table_id, 0});
                std::string tablet_job_key_end =
                        mow_tablet_job_key({instance_id, table_id, INT64_MAX});
                txn->remove(tablet_job_key_begin, tablet_job_key_end);
                LOG(INFO) << "remove mow tablet job kv, begin=" << hex(tablet_job_key_begin)
                          << " end=" << hex(tablet_job_key_end) << " table_id=" << table_id;
                if (!put_mow_tablet_job_key(code, msg, txn, instance_id, table_id,
                                            request->lock_id(), request->initiator(), expiration,
                                            current_lock_msg)) {
                    return;
                }
            }
            if (!put_delete_bitmap_update_lock_key(code, msg, txn, table_id, request->lock_id(),
                                                   request->initiator(), lock_key, lock_info,
                                                   current_lock_msg)) {
                return;
            }
        } else if (err == TxnErrorCode::TXN_OK) {
            if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                return;
            }
            if (urgent) {
                // since currently only the FE Master initiates the lock request for import tasks,
                // and it does so in a single-threaded manner, there is no need to check the lock id here
                DCHECK(request->lock_id() > 0);
                lock_info.clear_initiators();
                std::string key0 = mow_tablet_job_key({instance_id, table_id, 0});
                std::string key1 = mow_tablet_job_key(
                        {instance_id, table_id, std::numeric_limits<int64_t>::max()});
                txn->remove(key0, key1);
                LOG(INFO) << "remove mow tablet job kv, begin=" << hex(key0) << " end=" << hex(key1)
                          << " table_id=" << table_id;
                std::string current_lock_msg =
                        "original lock_id=" + std::to_string(lock_info.lock_id());
                lock_info.set_lock_id(request->lock_id());
                lock_info.set_expiration(expiration);
                lock_info.add_initiators(request->initiator());
                if (!put_delete_bitmap_update_lock_key(code, msg, txn, table_id, request->lock_id(),
                                                       request->initiator(), lock_key, lock_info,
                                                       current_lock_msg)) {
                    return;
                }
                LOG(INFO) << "force take delete bitmap update lock, table_id=" << table_id
                          << " lock_id=" << request->lock_id();
            } else if (!is_job_delete_bitmap_lock_id(lock_info.lock_id())) {
                if (lock_info.expiration() > 0 && lock_info.expiration() < now) {
                    LOG(INFO) << "delete bitmap lock expired, continue to process. lock_id="
                              << lock_info.lock_id() << " table_id=" << table_id
                              << " expiration=" << lock_info.expiration() << " now=" << now
                              << " initiator_size=" << lock_info.initiators_size();
                    lock_info.clear_initiators();
                } else if (lock_info.lock_id() != request->lock_id()) {
                    ss << "already be locked by lock_id=" << lock_info.lock_id()
                       << " expiration=" << lock_info.expiration() << " now=" << now
                       << ", request lock_id=" << request->lock_id() << " table_id=" << table_id
                       << " initiator=" << request->initiator();
                    msg = ss.str();
                    code = MetaServiceCode::LOCK_CONFLICT;
                    return;
                }
                std::string current_lock_msg =
                        "original lock_id=" + std::to_string(lock_info.lock_id());
                lock_info.set_lock_id(request->lock_id());
                // compaction does not use the expiration, only used when upgrade ms
                lock_info.set_expiration(expiration);
                if (!is_job_delete_bitmap_lock_id(request->lock_id())) {
                    bool found = false;
                    for (auto initiator : lock_info.initiators()) {
                        if (request->initiator() == initiator) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        lock_info.add_initiators(request->initiator());
                    }
                } else {
                    lock_key_not_found = true;
                    // in normal case, this should remove 0 kvs
                    // but when upgrade ms, if there are ms with old and new versions, it works
                    std::string tablet_job_key_begin =
                            mow_tablet_job_key({instance_id, table_id, 0});
                    std::string tablet_job_key_end =
                            mow_tablet_job_key({instance_id, table_id, INT64_MAX});
                    txn->remove(tablet_job_key_begin, tablet_job_key_end);
                    LOG(INFO) << "remove mow tablet job kv, begin=" << hex(tablet_job_key_begin)
                              << " end=" << hex(tablet_job_key_end) << " table_id=" << table_id;
                    if (!put_mow_tablet_job_key(code, msg, txn, instance_id, table_id,
                                                request->lock_id(), request->initiator(),
                                                expiration, current_lock_msg)) {
                        return;
                    }
                }
                if (!put_delete_bitmap_update_lock_key(code, msg, txn, table_id, request->lock_id(),
                                                       request->initiator(), lock_key, lock_info,
                                                       current_lock_msg)) {
                    return;
                }
            } else {
                if (request->lock_id() == lock_info.lock_id()) {
                    std::string current_lock_msg =
                            "locked by lock_id=" + std::to_string(lock_info.lock_id());
                    if (!put_mow_tablet_job_key(code, msg, txn, instance_id, table_id,
                                                request->lock_id(), request->initiator(),
                                                expiration, current_lock_msg)) {
                        return;
                    }
                } else {
                    // check if compaction key is expired
                    bool has_unexpired_compaction = false;
                    int64_t unexpired_expiration = 0;
                    std::string key0 = mow_tablet_job_key({instance_id, table_id, 0});
                    std::string key1 = mow_tablet_job_key({instance_id, table_id + 1, 0});
                    MowTabletJobPB mow_tablet_job;
                    std::unique_ptr<RangeGetIterator> it;
                    int64_t expired_job_num = 0;
                    do {
                        err = txn->get(key0, key1, &it);
                        if (err != TxnErrorCode::TXN_OK) {
                            code = cast_as<ErrCategory::READ>(err);
                            ss << "internal error, failed to get mow tablet job, err=" << err;
                            msg = ss.str();
                            LOG(WARNING) << msg;
                            return;
                        }

                        while (it->has_next() && !has_unexpired_compaction) {
                            auto [k, v] = it->next();
                            if (!mow_tablet_job.ParseFromArray(v.data(), v.size())) [[unlikely]] {
                                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                                msg = "failed to parse MowTabletJobPB";
                                return;
                            }
                            if (mow_tablet_job.expiration() > 0 &&
                                mow_tablet_job.expiration() < now) {
                                LOG(INFO) << "remove mow tablet job lock. table_id=" << table_id
                                          << " lock_id=" << lock_info.lock_id()
                                          << " expiration=" << mow_tablet_job.expiration()
                                          << " now=" << now << " key=" << hex(k);
                                txn->remove(k);
                                expired_job_num++;
                            } else {
                                has_unexpired_compaction = true;
                                unexpired_expiration = mow_tablet_job.expiration();
                            }
                        }
                        key0 = it->next_begin_key(); // Update to next smallest key for iteration
                    } while (it->more() && !has_unexpired_compaction);
                    if (has_unexpired_compaction) {
                        // TODO print initiator
                        ss << "already be locked by lock_id=" << lock_info.lock_id()
                           << " expiration=" << unexpired_expiration << " now=" << now
                           << ". request lock_id=" << request->lock_id() << " table_id=" << table_id
                           << " initiator=" << request->initiator();
                        msg = ss.str();
                        code = MetaServiceCode::LOCK_CONFLICT;
                        return;
                    }
                    // all job is expired
                    lock_info.set_lock_id(request->lock_id());
                    lock_info.set_expiration(expiration);
                    lock_info.clear_initiators();
                    std::string current_lock_msg =
                            std::to_string(expired_job_num) + " job is expired";
                    if (!is_job_delete_bitmap_lock_id(request->lock_id())) {
                        lock_info.add_initiators(request->initiator());
                    } else {
                        lock_key_not_found = true;
                        if (!put_mow_tablet_job_key(code, msg, txn, instance_id, table_id,
                                                    request->lock_id(), request->initiator(),
                                                    expiration, current_lock_msg)) {
                            return;
                        }
                    }
                    if (!put_delete_bitmap_update_lock_key(code, msg, txn, table_id,
                                                           request->lock_id(), request->initiator(),
                                                           lock_key, lock_info, current_lock_msg)) {
                        return;
                    }
                }
            }
        }

        err = txn->commit();
        TEST_SYNC_POINT_CALLBACK("get_delete_bitmap_update_lock:commit:conflict", &first_retry,
                                 request, &err);
        if (err == TxnErrorCode::TXN_OK) {
            break;
        } else if (err == TxnErrorCode::TXN_CONFLICT && urgent && request->lock_id() > 0 &&
                   first_retry) {
            g_bvar_delete_bitmap_lock_txn_put_conflict_counter << 1;
            // fast retry for urgent request when TXN_CONFLICT
            LOG(INFO) << "fast retry to get_delete_bitmap_update_lock, tablet_id="
                      << request->table_id() << " lock_id=" << request->lock_id()
                      << ", initiator=" << request->initiator() << "urgent=" << urgent
                      << ", err=" << err;
            first_retry = false;
            continue;
        } else if (err == TxnErrorCode::TXN_CONFLICT && lock_key_not_found &&
                   is_job_delete_bitmap_lock_id(request->lock_id()) &&
                   config::delete_bitmap_enable_retry_txn_conflict && first_retry) {
            // if err is TXN_CONFLICT, and the lock id is -1, do a fast retry
            if (err == TxnErrorCode::TXN_CONFLICT) {
                g_bvar_delete_bitmap_lock_txn_put_conflict_counter << 1;
            }
            LOG(INFO) << "fast retry to get_delete_bitmap_update_lock, tablet_id="
                      << request->table_id() << " lock_id=" << request->lock_id()
                      << ", initiator=" << request->initiator() << ", err=" << err;
            first_retry = false;
            continue;
        } else {
            if (err == TxnErrorCode::TXN_CONFLICT) {
                g_bvar_delete_bitmap_lock_txn_put_conflict_counter << 1;
            }
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to get_delete_bitmap_update_lock, lock_id=" << request->lock_id()
               << ", initiator=" << request->initiator() << ", err=" << err;
            msg = ss.str();
            return;
        }
    }

    if (!get_mow_tablet_stats_and_meta(code, msg, request, response, instance_id, lock_key, "v2",
                                       stats)) {
        return;
    }
}

void MetaServiceImpl::get_delete_bitmap_update_lock_v1(
        google::protobuf::RpcController* controller,
        const GetDeleteBitmapUpdateLockRequest* request,
        GetDeleteBitmapUpdateLockResponse* response, ::google::protobuf::Closure* done,
        std::string& instance_id, MetaServiceCode& code, std::string& msg, std::stringstream& ss,
        KVStats& stats) {
    VLOG_DEBUG << "get delete bitmap update lock in v1 for table=" << request->table_id()
               << ",lock id=" << request->lock_id() << ",initiator=" << request->initiator();
    auto table_id = request->table_id();
    bool urgent = request->has_urgent() && request->urgent();
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    for (int retry = 0; retry <= 1; retry++) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.put_bytes += txn->put_bytes();
            stats.del_bytes += txn->delete_bytes();
            stats.get_counter += txn->num_get_keys();
            stats.put_counter += txn->num_put_keys();
            stats.del_counter += txn->num_del_keys();
        };
        std::string lock_val;
        DeleteBitmapUpdateLockPB lock_info;
        err = txn->get(lock_key, &lock_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ss << "failed to get delete bitmap update lock, instance_id=" << instance_id
               << " table_id=" << table_id << " key=" << hex(lock_key) << " err=" << err;
            msg = ss.str();
            code = MetaServiceCode::KV_TXN_GET_ERR;
            return;
        }
        using namespace std::chrono;
        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        if (err == TxnErrorCode::TXN_OK) {
            if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                return;
            }
            if (urgent) {
                // since currently only the FE Master initiates the lock request for import tasks,
                // and it does so in a single-threaded manner, there is no need to check the lock id here
                DCHECK(request->lock_id() > 0);
                LOG(INFO) << "force take delete bitmap update lock, table_id=" << table_id
                          << " lock_id=" << request->lock_id()
                          << "prev_lock_id=" << lock_info.lock_id();
                lock_info.clear_initiators();
            } else if (lock_info.expiration() > 0 && lock_info.expiration() < now) {
                LOG(INFO) << "delete bitmap lock expired, continue to process. lock_id="
                          << lock_info.lock_id() << " table_id=" << table_id << " now=" << now;
                lock_info.clear_initiators();
            } else if (lock_info.lock_id() != request->lock_id()) {
                ss << "already be locked. request lock_id=" << request->lock_id()
                   << " locked by lock_id=" << lock_info.lock_id() << " table_id=" << table_id
                   << " now=" << now << " expiration=" << lock_info.expiration();
                msg = ss.str();
                code = MetaServiceCode::LOCK_CONFLICT;
                return;
            }
        }

        lock_info.set_lock_id(request->lock_id());
        lock_info.set_expiration(now + request->expiration());
        bool found = false;
        for (auto initiator : lock_info.initiators()) {
            if (request->initiator() == initiator) {
                found = true;
                break;
            }
        }
        if (!found) {
            lock_info.add_initiators(request->initiator());
        }
        lock_info.SerializeToString(&lock_val);
        if (lock_val.empty()) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "pb serialization error";
            return;
        }
        txn->put(lock_key, lock_val);
        LOG(INFO) << "xxx put lock_key=" << hex(lock_key) << " table_id=" << table_id
                  << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
                  << " initiators_size=" << lock_info.initiators_size();

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_CONFLICT) {
                g_bvar_delete_bitmap_lock_txn_put_conflict_counter << 1;
                if (retry == 0) {
                    // do a fast retry
                    response->Clear();
                    code = MetaServiceCode::OK;
                    msg.clear();
                    continue;
                }
            }

            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to get_delete_bitmap_update_lock, err=" << err;
            msg = ss.str();
            return;
        }
        break;
    }

    if (!get_mow_tablet_stats_and_meta(code, msg, request, response, instance_id, lock_key, "v1",
                                       stats)) {
        return;
    };
}

void MetaServiceImpl::remove_delete_bitmap_update_lock_v2(
        google::protobuf::RpcController* controller,
        const RemoveDeleteBitmapUpdateLockRequest* request,
        RemoveDeleteBitmapUpdateLockResponse* response, ::google::protobuf::Closure* done,
        std::string& instance_id, MetaServiceCode& code, std::string& msg, std::stringstream& ss,
        KVStats& stats) {
    VLOG_DEBUG << "remove delete bitmap update lock in v2 for table=" << request->table_id()
               << ",lock id=" << request->lock_id() << ",initiator=" << request->initiator();
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    DORIS_CLOUD_DEFER {
        if (txn == nullptr) return;
        stats.get_bytes += txn->get_bytes();
        stats.put_bytes += txn->put_bytes();
        stats.del_bytes += txn->delete_bytes();
        stats.get_counter += txn->num_get_keys();
        stats.put_counter += txn->num_put_keys();
        stats.del_counter += txn->num_del_keys();
    };
    if (is_job_delete_bitmap_lock_id(request->lock_id())) {
        std::string tablet_job_key =
                mow_tablet_job_key({instance_id, request->table_id(), request->initiator()});
        txn->remove(tablet_job_key);
        LOG(INFO) << "remove tablet job lock, table_id=" << request->table_id()
                  << " lock_id=" << request->lock_id() << " initiator=" << request->initiator()
                  << " key=" << hex(tablet_job_key);
    } else {
        std::string lock_key =
                meta_delete_bitmap_update_lock_key({instance_id, request->table_id(), -1});
        std::string lock_val;
        DeleteBitmapUpdateLockPB lock_info;
        if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, request->table_id(),
                                      request->lock_id(), request->initiator(), lock_key, lock_info,
                                      "v2", ", remove lock")) {
            LOG(WARNING) << "failed to check delete bitmap tablet lock"
                         << " table_id=" << request->table_id()
                         << " tablet_id=" << request->tablet_id()
                         << " request lock_id=" << request->lock_id()
                         << " request initiator=" << request->initiator() << " msg " << msg;
            return;
        }
        bool modify_initiators = false;
        auto initiators = lock_info.mutable_initiators();
        for (auto iter = initiators->begin(); iter != initiators->end(); iter++) {
            if (*iter == request->initiator()) {
                initiators->erase(iter);
                modify_initiators = true;
                break;
            }
        }
        if (!modify_initiators) {
            LOG(INFO) << "initiators don't have initiator=" << request->initiator()
                      << ",initiators_size=" << lock_info.initiators_size() << ",just return";
            return;
        } else if (initiators->empty()) {
            LOG(INFO) << "remove delete bitmap lock, table_id=" << request->table_id()
                      << " lock_id=" << request->lock_id() << " key=" << hex(lock_key);
            txn->remove(lock_key);
        } else {
            lock_info.SerializeToString(&lock_val);
            if (lock_val.empty()) {
                LOG(WARNING) << "failed to seiralize lock_info, table_id=" << request->table_id()
                             << " key=" << hex(lock_key);
                return;
            }
            LOG(INFO) << "remove delete bitmap lock initiator, table_id=" << request->table_id()
                      << ", key=" << hex(lock_key) << " lock_id=" << request->lock_id()
                      << " initiator=" << request->initiator()
                      << " initiators_size=" << lock_info.initiators_size();
            txn->put(lock_key, lock_val);
        }
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_CONFLICT) {
            g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter << 1;
        }
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to remove delete bitmap tablet lock , err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::remove_delete_bitmap_update_lock_v1(
        google::protobuf::RpcController* controller,
        const RemoveDeleteBitmapUpdateLockRequest* request,
        RemoveDeleteBitmapUpdateLockResponse* response, ::google::protobuf::Closure* done,
        std::string& instance_id, MetaServiceCode& code, std::string& msg, std::stringstream& ss,
        KVStats& stats) {
    VLOG_DEBUG << "remove delete bitmap update lock in v1 for table=" << request->table_id()
               << ",lock id=" << request->lock_id() << ",initiator=" << request->initiator();
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    DORIS_CLOUD_DEFER {
        if (txn == nullptr) return;
        stats.get_bytes += txn->get_bytes();
        stats.put_bytes += txn->put_bytes();
        stats.del_bytes += txn->delete_bytes();
        stats.get_counter += txn->num_get_keys();
        stats.put_counter += txn->num_put_keys();
        stats.del_counter += txn->num_del_keys();
    };
    std::string lock_key =
            meta_delete_bitmap_update_lock_key({instance_id, request->table_id(), -1});
    std::string lock_val;
    DeleteBitmapUpdateLockPB lock_info;
    if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, request->table_id(),
                                  request->lock_id(), request->initiator(), lock_key, lock_info,
                                  "v1")) {
        LOG(WARNING) << "failed to check delete bitmap tablet lock"
                     << " table_id=" << request->table_id() << " tablet_id=" << request->tablet_id()
                     << " request lock_id=" << request->lock_id()
                     << " request initiator=" << request->initiator() << " msg " << msg;
        return;
    }
    bool modify_initiators = false;
    auto initiators = lock_info.mutable_initiators();
    for (auto iter = initiators->begin(); iter != initiators->end(); iter++) {
        if (*iter == request->initiator()) {
            initiators->erase(iter);
            modify_initiators = true;
            break;
        }
    }
    if (!modify_initiators) {
        LOG(INFO) << "initiators don't have initiator=" << request->initiator()
                  << ",initiators_size=" << lock_info.initiators_size() << ",just return";
        return;
    } else if (initiators->empty()) {
        LOG(INFO) << "remove delete bitmap lock, table_id=" << request->table_id()
                  << " lock_id=" << request->lock_id() << " key=" << hex(lock_key);
        txn->remove(lock_key);
    } else {
        lock_info.SerializeToString(&lock_val);
        if (lock_val.empty()) {
            LOG(WARNING) << "failed to seiralize lock_info, table_id=" << request->table_id()
                         << " key=" << hex(lock_key);
            return;
        }
        LOG(INFO) << "remove delete bitmap lock initiator, table_id=" << request->table_id()
                  << ", key=" << hex(lock_key) << " lock_id=" << request->lock_id()
                  << " initiator=" << request->initiator()
                  << " initiators_size=" << lock_info.initiators_size();
        txn->put(lock_key, lock_val);
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to remove delete bitmap tablet lock , err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::get_delete_bitmap_update_lock(google::protobuf::RpcController* controller,
                                                    const GetDeleteBitmapUpdateLockRequest* request,
                                                    GetDeleteBitmapUpdateLockResponse* response,
                                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_delete_bitmap_update_lock, get, put, del);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    if (request->has_urgent() && request->urgent() && request->lock_id() < 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "only load can set urgent flag currently";
        return;
    }
    RPC_RATE_LIMIT(get_delete_bitmap_update_lock)
    std::string use_version =
            delete_bitmap_lock_white_list_->get_delete_bitmap_lock_version(instance_id);
    LOG(INFO) << "get_delete_bitmap_update_lock instance_id=" << instance_id
              << " use_version=" << use_version;
    if (use_version == "v2") {
        get_delete_bitmap_update_lock_v2(controller, request, response, done, instance_id, code,
                                         msg, ss, stats);
    } else {
        get_delete_bitmap_update_lock_v1(controller, request, response, done, instance_id, code,
                                         msg, ss, stats);
    }

    if (request->lock_id() > 0 && code == MetaServiceCode::KV_TXN_CONFLICT) {
        // For load, the only fdb txn conflict here is due to compaction(sc) job.
        // We turn it into a lock conflict error to skip the MS RPC backoff becasue it's too long
        // and totally let FE to control the retry backoff sleep time
        code = MetaServiceCode::LOCK_CONFLICT;
    }
}

void MetaServiceImpl::remove_delete_bitmap_update_lock(
        google::protobuf::RpcController* controller,
        const RemoveDeleteBitmapUpdateLockRequest* request,
        RemoveDeleteBitmapUpdateLockResponse* response, ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(remove_delete_bitmap_update_lock, get, put, del);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(remove_delete_bitmap_update_lock)
    std::string use_version =
            delete_bitmap_lock_white_list_->get_delete_bitmap_lock_version(instance_id);
    LOG(INFO) << "remove_delete_bitmap_update_lock instance_id=" << instance_id
              << " use_version=" << use_version;
    if (use_version == "v2") {
        remove_delete_bitmap_update_lock_v2(controller, request, response, done, instance_id, code,
                                            msg, ss, stats);
    } else {
        remove_delete_bitmap_update_lock_v1(controller, request, response, done, instance_id, code,
                                            msg, ss, stats);
    }
}

void MetaServiceImpl::remove_delete_bitmap(google::protobuf::RpcController* controller,
                                           const RemoveDeleteBitmapRequest* request,
                                           RemoveDeleteBitmapResponse* response,
                                           ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(remove_delete_bitmap, del);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(remove_delete_bitmap)
    auto tablet_id = request->tablet_id();
    auto& rowset_ids = request->rowset_ids();
    auto& begin_versions = request->begin_versions();
    auto& end_versions = request->end_versions();
    if (rowset_ids.size() != begin_versions.size() || rowset_ids.size() != end_versions.size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "rowset and version size not match. "
           << " rowset_size=" << rowset_ids.size()
           << " begin_version_size=" << begin_versions.size()
           << " end_version_size=" << end_versions.size();
        msg = ss.str();
        return;
    }
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn";
        return;
    }
    for (size_t i = 0; i < rowset_ids.size(); i++) {
        auto delete_bitmap_start = meta_delete_bitmap_key(
                {instance_id, tablet_id, rowset_ids[i], begin_versions[i], 0});
        auto delete_bitmap_end = meta_delete_bitmap_key(
                {instance_id, tablet_id, rowset_ids[i], end_versions[i], INT64_MAX});
        txn->remove(delete_bitmap_start, delete_bitmap_end);
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit job kv, err=" << err;
        msg = ss.str();
        return;
    }
    LOG(INFO) << "remove_delete_bitmap,tablet_id=" << tablet_id
              << ",rowset_num=" << rowset_ids.size();
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::get_instance_info(
        const std::string& instance_id, const std::string& cloud_unique_id,
        InstanceInfoPB* instance) {
    std::string cloned_instance_id = instance_id;
    if (instance_id.empty()) {
        if (cloud_unique_id.empty()) {
            return {MetaServiceCode::INVALID_ARGUMENT, "empty instance_id and cloud_unique_id"};
        }
        // get instance_id by cloud_unique_id
        cloned_instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
        if (cloned_instance_id.empty()) {
            std::string msg =
                    fmt::format("cannot find instance_id with cloud_unique_id={}", cloud_unique_id);
            return {MetaServiceCode::INVALID_ARGUMENT, std::move(msg)};
        }
    }

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err), "failed to create txn"};
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    auto [c0, m0] = resource_mgr_->get_instance(txn, cloned_instance_id, instance);
    if (c0 != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(c0), "failed to get instance, info=" + m0};
    }

    // maybe do not decrypt ak/sk?
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    decrypt_instance_info(*instance, cloned_instance_id, code, msg, txn);
    return {code, std::move(msg)};
}

std::pair<std::string, std::string> init_key_pair(std::string instance_id, int64_t table_id) {
    std::string begin_key = stats_tablet_key({instance_id, table_id, 0, 0, 0});
    std::string end_key = stats_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    return std::make_pair(begin_key, end_key);
}

MetaServiceResponseStatus MetaServiceImpl::fix_tablet_stats(std::string cloud_unique_id_str,
                                                            std::string table_id_str) {
    // parse params
    int64_t table_id;
    std::string instance_id;
    MetaServiceResponseStatus st = parse_fix_tablet_stats_param(
            resource_mgr_, table_id_str, cloud_unique_id_str, table_id, instance_id);
    if (st.code() != MetaServiceCode::OK) {
        return st;
    }

    std::pair<std::string, std::string> key_pair = init_key_pair(instance_id, table_id);
    std::string old_begin_key;
    while (old_begin_key < key_pair.first) {
        // get tablet stats
        std::vector<std::shared_ptr<TabletStatsPB>> tablet_stat_shared_ptr_vec_batch;
        old_begin_key = key_pair.first;

        // fix tablet stats
        size_t retry = 0;
        do {
            st = fix_tablet_stats_internal(txn_kv_, key_pair, tablet_stat_shared_ptr_vec_batch,
                                           instance_id);
            if (st.code() != MetaServiceCode::OK) {
                LOG_WARNING("failed to fix tablet stats")
                        .tag("err", st.msg())
                        .tag("table id", table_id)
                        .tag("retry time", retry);
            }
            retry++;
        } while (st.code() != MetaServiceCode::OK && retry < 3);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }

        // Check tablet stats
        st = check_new_tablet_stats(txn_kv_, instance_id, tablet_stat_shared_ptr_vec_batch);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }
    }
    return st;
}

std::size_t get_segments_key_bounds_bytes(const doris::RowsetMetaCloudPB& rowset_meta) {
    size_t ret {0};
    for (const auto& key_bounds : rowset_meta.segments_key_bounds()) {
        ret += key_bounds.ByteSizeLong();
    }
    return ret;
}

void MetaServiceImpl::get_schema_dict(::google::protobuf::RpcController* controller,
                                      const GetSchemaDictRequest* request,
                                      GetSchemaDictResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_schema_dict, get);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    if (!request->has_index_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing index_id in request";
        return;
    }

    RPC_RATE_LIMIT(get_schema_dict)

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    std::string dict_key = meta_schema_pb_dictionary_key({instance_id, request->index_id()});
    ValueBuf dict_val;
    err = cloud::blob_get(txn.get(), dict_key, &dict_val);
    LOG(INFO) << "Retrieved column pb dictionary, index_id=" << request->index_id()
              << " key=" << hex(dict_key) << " error=" << err;
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND && err != TxnErrorCode::TXN_OK) {
        // Handle retrieval error.
        ss << "Failed to retrieve column pb dictionary, instance_id=" << instance_id
           << " table_id=" << request->index_id() << " key=" << hex(dict_key) << " error=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    SchemaCloudDictionary schema_dict;
    if (err == TxnErrorCode::TXN_OK && !dict_val.to_pb(&schema_dict)) {
        // Handle parse error.
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("Malformed tablet dictionary value, key={}", hex(dict_key));
        return;
    }

    response->mutable_schema_dict()->Swap(&schema_dict);
}

} // namespace doris::cloud
