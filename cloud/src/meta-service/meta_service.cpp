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
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "keys.h"
#include "meta-service/codec.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
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
}

MetaServiceImpl::~MetaServiceImpl() = default;

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("get_instance_id", std::string {});

    std::vector<NodeInfo> nodes;
    std::string err = rc_mgr->get_node(cloud_unique_id, &nodes);
    { TEST_SYNC_POINT_CALLBACK("get_instance_id_err", &err); }
    if (!err.empty()) {
        // cache can't find cloud_unique_id, so degraded by parse cloud_unique_id
        // cloud_unique_id encode: ${version}:${instance_id}:${unique_id}
        // check it split by ':' c
        auto vec = split(cloud_unique_id, ':');
        std::stringstream ss;
        for (int i = 0; i < vec.size(); ++i) {
            ss << "idx " << i << "= [" << vec[i] << "] ";
        }
        LOG(INFO) << "degraded to get instance_id, cloud_unique_id: " << cloud_unique_id
                  << "after split: " << ss.str();
        if (vec.size() != 3) {
            LOG(WARNING) << "cloud unique id is not degraded format, failed to check instance "
                            "info, cloud_unique_id="
                         << cloud_unique_id << " , err=" << err;
            return "";
        }
        // version: vec[0], instance_id: vec[1], unique_id: vec[2]
        switch (std::atoi(vec[0].c_str())) {
        case 1:
            // just return instance id;
            return vec[1];
        default:
            LOG(WARNING) << "cloud unique id degraded state, but version not eq configure, "
                            "cloud_unique_id="
                         << cloud_unique_id << ", err=" << err;
            return "";
        }
    }

    std::string instance_id;
    for (auto& i : nodes) {
        if (!instance_id.empty() && instance_id != i.instance_id) {
            LOG(WARNING) << "cloud_unique_id is one-to-many instance_id, "
                         << " cloud_unique_id=" << cloud_unique_id
                         << " current_instance_id=" << instance_id
                         << " later_instance_id=" << i.instance_id;
        }
        instance_id = i.instance_id; // The last wins
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

    RPC_PREPROCESS(get_version);
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
    std::string ver_key;
    if (is_table_version) {
        table_version_key({instance_id, db_id, table_id}, &ver_key);
    } else {
        partition_version_key({instance_id, db_id, table_id, partition_id}, &ver_key);
    }

    code = MetaServiceCode::OK;
    std::unique_ptr<Transaction> txn;
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
        { TEST_SYNC_POINT_CALLBACK("get_version_code", &code); }
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
    RPC_PREPROCESS(get_version);

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

void internal_create_tablet(const CreateTabletsRequest* request, MetaServiceCode& code,
                            std::string& msg, const doris::TabletMetaCloudPB& meta,
                            std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                            std::set<std::pair<int64_t, int32_t>>& saved_schema) {
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

    // Create stats info for the tablet
    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    TabletStatsPB stats_pb;
    stats_pb.set_num_rowsets(1);
    stats_pb.set_num_segments(0);
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
    LOG(INFO) << "put tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key);

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
    RPC_PREPROCESS(create_tablets);

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
    for (auto& tablet_meta : request->tablet_metas()) {
        internal_create_tablet(request, code, msg, tablet_meta, txn_kv_, instance_id, saved_schema);
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

    if (tablet_meta->has_schema()) { // tablet meta saved before detach schema kv
        tablet_meta->set_schema_version(tablet_meta->schema().schema_version());
    }
    if (!tablet_meta->has_schema() && !skip_schema) {
        if (!tablet_meta->has_schema_version()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "tablet_meta must have either schema or schema_version";
            return;
        }
        auto key = meta_schema_key(
                {instance_id, tablet_meta->index_id(), tablet_meta->schema_version()});
        ValueBuf val_buf;
        err = cloud::get(txn, key, &val_buf);
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
    RPC_PREPROCESS(update_tablet);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(update_tablet)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    for (const TabletMetaInfoPB& tablet_meta_info : request->tablet_meta_infos()) {
        doris::TabletMetaCloudPB tablet_meta;
        internal_get_tablet(code, msg, instance_id, txn.get(), tablet_meta_info.tablet_id(),
                            &tablet_meta, true);
        if (code != MetaServiceCode::OK) {
            return;
        }
        if (tablet_meta_info.has_is_in_memory()) { // deprecate after 3.0.0
            tablet_meta.set_is_in_memory(tablet_meta_info.is_in_memory());
        } else if (tablet_meta_info.has_is_persistent()) { // deprecate after 3.0.0
            tablet_meta.set_is_persistent(tablet_meta_info.is_persistent());
        } else if (tablet_meta_info.has_ttl_seconds()) {
            tablet_meta.set_ttl_seconds(tablet_meta_info.ttl_seconds());
        } else if (tablet_meta_info.has_group_commit_interval_ms()) {
            tablet_meta.set_group_commit_interval_ms(tablet_meta_info.group_commit_interval_ms());
        } else if (tablet_meta_info.has_group_commit_data_bytes()) {
            tablet_meta.set_group_commit_data_bytes(tablet_meta_info.group_commit_data_bytes());
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
            tablet_meta.mutable_schema()->set_disable_auto_compaction(
                    tablet_meta_info.disable_auto_compaction());
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
    RPC_PREPROCESS(update_tablet_schema);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(update_tablet_schema)

    std::unique_ptr<Transaction> txn;
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
    RPC_PREPROCESS(get_tablet);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    internal_get_tablet(code, msg, instance_id, txn.get(), request->tablet_id(),
                        response->mutable_tablet_meta(), false);
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
        TxnErrorCode err = cloud::get(txn, schema_key, &val_buf, true);
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

/**
 * 1. Check and confirm tmp rowset kv does not exist
 * 2. Construct recycle rowset kv which contains object path
 * 3. Put recycle rowset kv
 */
void MetaServiceImpl::prepare_rowset(::google::protobuf::RpcController* controller,
                                     const CreateRowsetRequest* request,
                                     CreateRowsetResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_rowset);
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
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
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
    LOG(INFO) << "put prepare_rs_key " << hex(prepare_rs_key) << " value_size " << val.size()
              << " txn_id " << request->txn_id();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE) {
            LOG(WARNING) << "failed to prepare rowset, err=value too large"
                         << ", txn_id=" << request->txn_id() << ", tablet_id=" << tablet_id
                         << ", rowset_id=" << rowset_id
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
    RPC_PREPROCESS(commit_rowset);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
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

    auto recycle_rs_key = recycle_rowset_key({instance_id, tablet_id, rowset_id});
    txn->remove(recycle_rs_key);

    DCHECK_GT(rowset_meta.txn_expiration(), 0);
    auto tmp_rs_val = rowset_meta.SerializeAsString();
    txn->put(tmp_rs_key, tmp_rs_val);
    LOG(INFO) << "put tmp_rs_key " << hex(tmp_rs_key) << " delete recycle_rs_key "
              << hex(recycle_rs_key) << " value_size " << tmp_rs_val.size() << " txn_id "
              << request->txn_id();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE) {
            LOG(WARNING) << "failed to commit rowset, err=value too large"
                         << ", txn_id=" << request->txn_id() << ", tablet_id=" << tablet_id
                         << ", rowset_id=" << rowset_id
                         << ", rowset_meta=" << rowset_meta.ShortDebugString();
        }
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to save rowset meta, err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::update_tmp_rowset(::google::protobuf::RpcController* controller,
                                        const CreateRowsetRequest* request,
                                        CreateRowsetResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_tmp_rowset);
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

    std::unique_ptr<Transaction> txn;
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
    LOG(INFO) << "xxx put "
              << "update_rowset_key " << hex(update_key) << " value_size " << update_val.size();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update rowset meta, err=" << err;
        msg = ss.str();
        return;
    }
}

void internal_get_rowset(Transaction* txn, int64_t start, int64_t end,
                         const std::string& instance_id, int64_t tablet_id, MetaServiceCode& code,
                         std::string& msg, GetRowsetResponse* response) {
    LOG(INFO) << "get_rowset start=" << start << ", end=" << end;
    MetaRowsetKeyInfo key_info0 {instance_id, tablet_id, start};
    MetaRowsetKeyInfo key_info1 {instance_id, tablet_id, end + 1};
    std::string key0;
    std::string key1;
    meta_rowset_key(key_info0, &key0);
    meta_rowset_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [key0, key1, &num_rowsets](int*) {
                LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=["
                          << hex(key0) << "," << hex(key1) << "]";
            });

    std::stringstream ss;
    do {
        TxnErrorCode err = txn->get(key0, key1, &it);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "internal error, failed to get rowset, err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            auto rs = response->add_rowset_meta();
            if (!rs->ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed rowset meta, unable to deserialize";
                LOG(WARNING) << msg << " key=" << hex(k);
                return;
            }
            ++num_rowsets;
            if (!it->has_next()) key0 = k;
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
    TxnErrorCode err = cloud::get(txn, key, &val_buf);
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

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const GetRowsetRequest* request, GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_rowset);
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
        msg = "no valid compaction_cnt or cumulative_point given";
        return;
    }
    int64_t req_bc_cnt = request->base_compaction_cnt();
    int64_t req_cc_cnt = request->cumulative_compaction_cnt();
    int64_t req_cp = request->cumulative_point();

    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to create txn";
            LOG(WARNING) << msg;
            return;
        }

        TabletIndexPB idx;
        // Get tablet id index from kv
        get_tablet_idx(code, msg, txn.get(), instance_id, tablet_id, idx);
        if (code != MetaServiceCode::OK) {
            return;
        }
        DCHECK(request->has_idx());

        if (idx.has_db_id()) {
            // there is maybe a lazy commit txn when call get_rowset
            // we need advance lazy commit txn here
            std::string ver_val;
            std::string ver_key = partition_version_key(
                    {instance_id, idx.db_id(), idx.table_id(), idx.partition_id()});
            err = txn->get(ver_key, &ver_val);
            if (TxnErrorCode::TXN_OK != err && TxnErrorCode::TXN_KEY_NOT_FOUND != err) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "failed to get partiton version, tablet_id=" << tablet_id
                   << " key=" << hex(ver_key) << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }

            if (TxnErrorCode::TXN_OK == err) {
                VersionPB version_pb;
                if (!version_pb.ParseFromString(ver_val)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse version pb db_id=" << idx.db_id()
                       << " table_id=" << idx.table_id() << " partition_id" << idx.partition_id()
                       << " key=" << hex(ver_key);
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }

                if (version_pb.pending_txn_ids_size() > 0) {
                    DCHECK(version_pb.pending_txn_ids_size() == 1);
                    txn.reset();
                    std::shared_ptr<TxnLazyCommitTask> task =
                            txn_lazy_committer_->submit(instance_id, version_pb.pending_txn_ids(0));

                    std::tie(code, msg) = task->wait();
                    if (code != MetaServiceCode::OK) {
                        LOG(WARNING) << "advance_last_txn failed last_txn="
                                     << version_pb.pending_txn_ids(0) << " code=" << code
                                     << "msg=" << msg;
                        return;
                    }
                    continue;
                }
            }
        }

        // TODO(plat1ko): Judge if tablet has been dropped (in dropped index/partition)

        TabletStatsPB tablet_stat;
        internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, tablet_stat, true);
        if (code != MetaServiceCode::OK) return;
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
               << ", req_cp=" << req_cp << ", cp=" << cp;
            msg = ss.str();
            return;
        }
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        for (auto [start, end] : versions) {
            internal_get_rowset(txn.get(), start, end, instance_id, tablet_id, code, msg, response);
            if (code != MetaServiceCode::OK) {
                return;
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
                if (!try_fetch_and_parse_schema(txn.get(), rowset_meta, key, code, msg)) {
                    return;
                }
                version_to_schema.emplace(rowset_meta.schema_version(),
                                          rowset_meta.mutable_tablet_schema());
            }
        }

        if (need_read_schema_dict) {
            read_schema_from_dict(code, msg, instance_id, idx.index_id(), txn.get(),
                                  response->mutable_rowset_meta());
            if (code != MetaServiceCode::OK) return;
        }
        break;
    } while (true);
}

void MetaServiceImpl::get_tablet_stats(::google::protobuf::RpcController* controller,
                                       const GetTabletStatsRequest* request,
                                       GetTabletStatsResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_tablet_stats);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet_stats)

    std::unique_ptr<Transaction> txn;
    for (auto& i : request->tablet_idx()) {
        TabletIndexPB idx(i);
        // FIXME(plat1ko): Get all tablet stats in one txn
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = fmt::format("failed to create txn, tablet_id={}", idx.tablet_id());
            return;
        }
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
#endif
    }
}

static bool check_delete_bitmap_lock(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                                     std::unique_ptr<Transaction>& txn, std::string& instance_id,
                                     int64_t table_id, int64_t lock_id, int64_t lock_initiator) {
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::string lock_val;
    DeleteBitmapUpdateLockPB lock_info;
    auto err = txn->get(lock_key, &lock_val);
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
    if (lock_info.lock_id() != lock_id) {
        ss << "lock id not match, locked by lock_id=" << lock_info.lock_id();
        msg = ss.str();
        code = MetaServiceCode::LOCK_EXPIRED;
        return false;
    }
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
    return true;
}

static bool process_pending_delete_bitmap(MetaServiceCode& code, std::string& msg,
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

void MetaServiceImpl::update_delete_bitmap(google::protobuf::RpcController* controller,
                                           const UpdateDeleteBitmapRequest* request,
                                           UpdateDeleteBitmapResponse* response,
                                           ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_delete_bitmap);
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
    RPC_RATE_LIMIT(update_delete_bitmap)

    uint32_t fdb_txn_size = 0;
    auto table_id = request->table_id();
    auto tablet_id = request->tablet_id();

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }

    // 1. Check whether the lock expires
    if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, table_id, request->lock_id(),
                                  request->initiator())) {
        LOG(WARNING) << "failed to check delete bitmap lock, table_id=" << table_id
                     << " request lock_id=" << request->lock_id()
                     << " request initiator=" << request->initiator() << " msg" << msg;
        return;
    }

    // 2. Process pending delete bitmap
    if (!process_pending_delete_bitmap(code, msg, ss, txn, instance_id, tablet_id)) {
        return;
    }

    // 3. store all pending delete bitmap for this txn
    PendingDeleteBitmapPB delete_bitmap_keys;
    for (size_t i = 0; i < request->rowset_ids_size(); ++i) {
        MetaDeleteBitmapInfo key_info {instance_id, tablet_id, request->rowset_ids(i),
                                       request->versions(i), request->segment_ids(i)};
        std::string key;
        meta_delete_bitmap_key(key_info, &key);
        delete_bitmap_keys.add_delete_bitmap_keys(key);
    }
    // no need to record pending key for compaction or schema change,
    // because delete bitmap will attach to new rowset, just delete new rowset if failed
    // lock_id > 0 : load
    // lock_id = -1 : compaction
    // lock_id = -2 : schema change
    if (request->lock_id() > 0) {
        std::string pending_val;
        if (!delete_bitmap_keys.SerializeToString(&pending_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize pending delete bitmap";
            return;
        }
        std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
        txn->put(pending_key, pending_val);
        fdb_txn_size = fdb_txn_size + pending_key.size() + pending_val.size();
        LOG(INFO) << "xxx update delete bitmap put pending_key=" << hex(pending_key)
                  << " lock_id=" << request->lock_id() << " value_size: " << pending_val.size();
    }

    // 4. Update delete bitmap for curent txn
    for (size_t i = 0; i < request->rowset_ids_size(); ++i) {
        auto& key = delete_bitmap_keys.delete_bitmap_keys(i);
        auto& val = request->segment_delete_bitmaps(i);

        // Split into multiple fdb transactions, because the size of one fdb
        // transaction can't exceed 10MB.
        if (fdb_txn_size + key.size() + val.size() > 9 * 1024 * 1024) {
            LOG(INFO) << "fdb txn size more than 9MB, current size: " << fdb_txn_size
                      << " lock_id=" << request->lock_id();
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::COMMIT>(err);
                ss << "failed to update delete bitmap, err=" << err;
                msg = ss.str();
                return;
            }
            fdb_txn_size = 0;
            TxnErrorCode err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::CREATE>(err);
                msg = "failed to init txn";
                return;
            }
            if (!check_delete_bitmap_lock(code, msg, ss, txn, instance_id, table_id,
                                          request->lock_id(), request->initiator())) {
                LOG(WARNING) << "failed to check delete bitmap lock, table_id=" << table_id
                             << " request lock_id=" << request->lock_id()
                             << " request initiator=" << request->initiator() << " msg" << msg;
                return;
            }
        }
        // splitting large values (>90*1000) into multiple KVs
        cloud::put(txn.get(), key, val, 0);
        fdb_txn_size = fdb_txn_size + key.size() + val.size();
        LOG(INFO) << "xxx update delete bitmap put delete_bitmap_key=" << hex(key)
                  << " lock_id=" << request->lock_id() << " value_size: " << val.size();
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to update delete bitmap, err=" << err;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::get_delete_bitmap(google::protobuf::RpcController* controller,
                                        const GetDeleteBitmapRequest* request,
                                        GetDeleteBitmapResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_delete_bitmap);
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

    for (size_t i = 0; i < rowset_ids.size(); i++) {
        // create a new transaction every time, avoid using one transaction that takes too long
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
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
        do {
            err = txn->get(start_key, end_key, &it);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "internal error, failed to get delete bitmap, ret=" << err;
                msg = ss.str();
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
                } else {
                    response->mutable_segment_delete_bitmaps()->rbegin()->append(v);
                }
            }
            start_key = it->next_begin_key(); // Update to next smallest key for iteration
        } while (it->more());
    }

    if (request->has_idx()) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            msg = "failed to init txn";
            return;
        }
        TabletIndexPB idx(request->idx());
        TabletStatsPB tablet_stat;
        internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, tablet_stat,
                                  true /*snapshot_read*/);
        if (code != MetaServiceCode::OK) {
            return;
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

void MetaServiceImpl::get_delete_bitmap_update_lock(google::protobuf::RpcController* controller,
                                                    const GetDeleteBitmapUpdateLockRequest* request,
                                                    GetDeleteBitmapUpdateLockResponse* response,
                                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_delete_bitmap_update_lock);
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
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    RPC_RATE_LIMIT(get_delete_bitmap_update_lock)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to init txn";
        return;
    }
    auto table_id = request->table_id();
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
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
        if (lock_info.expiration() > 0 && lock_info.expiration() < now) {
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

    bool require_tablet_stats =
            request->has_require_compaction_stats() ? request->require_compaction_stats() : false;
    if (require_tablet_stats) {
        // this request is from fe when it commits txn for MOW table, we send the compaction stats
        // along with the GetDeleteBitmapUpdateLockResponse which will be sent to BE later to let
        // BE eliminate unnecessary sync_rowsets() calls if possible
        for (const auto& tablet_index : request->tablet_indexes()) {
            TabletIndexPB idx(tablet_index);
            TabletStatsPB tablet_stat;
            internal_get_tablet_stats(code, msg, txn.get(), instance_id, idx, tablet_stat, false);
            if (code != MetaServiceCode::OK) {
                response->clear_base_compaction_cnts();
                response->clear_cumulative_compaction_cnts();
                response->clear_cumulative_points();
                LOG_WARNING(
                        "failed to get tablet stats when get_delete_bitmap_update_lock, "
                        "lock_id={}, initiator={}, tablet_id={}",
                        request->lock_id(), request->initiator(), tablet_index.tablet_id());
                return;
            }
            response->add_base_compaction_cnts(tablet_stat.base_compaction_cnt());
            response->add_cumulative_compaction_cnts(tablet_stat.cumulative_compaction_cnt());
            response->add_cumulative_points(tablet_stat.cumulative_point());
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
    LOG(INFO) << "xxx put lock_key=" << hex(lock_key) << " lock_id=" << request->lock_id()
              << " initiators_size: " << lock_info.initiators_size();

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to get_delete_bitmap_update_lock, err=" << err;
        msg = ss.str();
        return;
    }
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

} // namespace doris::cloud
