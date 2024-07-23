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

#include <brpc/channel.h>
#include <butil/guid.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <numeric>
#include <string>
#include <tuple>

#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/network_util.h"
#include "common/string_util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

using namespace std::chrono;

namespace doris::cloud {

static void* run_bthread_work(void* arg) {
    auto f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

static std::string_view print_cluster_status(const ClusterStatus& status) {
    switch (status) {
    case ClusterStatus::UNKNOWN:
        return "UNKNOWN";
    case ClusterStatus::NORMAL:
        return "NORMAL";
    case ClusterStatus::SUSPENDED:
        return "SUSPENDED";
    case ClusterStatus::TO_RESUME:
        return "TO_RESUME";
    case ClusterStatus::MANUAL_SHUTDOWN:
        return "MANUAL_SHUTDOWN";
    default:
        return "UNKNOWN";
    }
}

static int encrypt_ak_sk_helper(const std::string plain_ak, const std::string plain_sk,
                                EncryptionInfoPB* encryption_info, AkSkPair* cipher_ak_sk_pair,
                                MetaServiceCode& code, std::string& msg) {
    std::string key;
    int64_t key_id;
    int ret = get_newest_encryption_key_for_ak_sk(&key_id, &key);
    TEST_SYNC_POINT_CALLBACK("encrypt_ak_sk:get_encryption_key", &ret, &key, &key_id);
    if (ret != 0) {
        msg = "failed to get encryption key";
        code = MetaServiceCode::ERR_ENCRYPT;
        LOG(WARNING) << msg;
        return -1;
    }
    auto& encryption_method = get_encryption_method_for_ak_sk();
    AkSkPair plain_ak_sk_pair {plain_ak, plain_sk};
    ret = encrypt_ak_sk(plain_ak_sk_pair, encryption_method, key, cipher_ak_sk_pair);
    if (ret != 0) {
        msg = "failed to encrypt";
        code = MetaServiceCode::ERR_ENCRYPT;
        LOG(WARNING) << msg;
        return -1;
    }
    encryption_info->set_key_id(key_id);
    encryption_info->set_encryption_method(encryption_method);
    return 0;
}

static int decrypt_ak_sk_helper(std::string_view cipher_ak, std::string_view cipher_sk,
                                const EncryptionInfoPB& encryption_info, AkSkPair* plain_ak_sk_pair,
                                MetaServiceCode& code, std::string& msg) {
    int ret = decrypt_ak_sk_helper(cipher_ak, cipher_sk, encryption_info, plain_ak_sk_pair);
    if (ret != 0) {
        msg = "failed to decrypt";
        code = MetaServiceCode::ERR_DECPYPT;
    }
    return ret;
}

int decrypt_instance_info(InstanceInfoPB& instance, const std::string& instance_id,
                          MetaServiceCode& code, std::string& msg,
                          std::shared_ptr<Transaction>& txn) {
    for (auto& obj_info : *instance.mutable_obj_info()) {
        if (obj_info.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return -1;
            obj_info.set_ak(std::move(plain_ak_sk_pair.first));
            obj_info.set_sk(std::move(plain_ak_sk_pair.second));
        }
    }
    if (instance.has_ram_user() && instance.ram_user().has_encryption_info()) {
        auto& ram_user = *instance.mutable_ram_user();
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), ram_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return -1;
        ram_user.set_ak(std::move(plain_ak_sk_pair.first));
        ram_user.set_sk(std::move(plain_ak_sk_pair.second));
    }

    std::string val;
    TxnErrorCode err = txn->get(system_meta_service_arn_info_key(), &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // For compatibility, use arn_info of config
        RamUserPB iam_user;
        iam_user.set_user_id(config::arn_id);
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(config::arn_ak);
        iam_user.set_sk(config::arn_sk);
        instance.mutable_iam_user()->CopyFrom(iam_user);
    } else if (err == TxnErrorCode::TXN_OK) {
        RamUserPB iam_user;
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse RamUserPB";
            LOG(WARNING) << msg;
            return -1;
        }
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(), iam_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return -1;
        iam_user.set_ak(std::move(plain_ak_sk_pair.first));
        iam_user.set_sk(std::move(plain_ak_sk_pair.second));
        instance.mutable_iam_user()->CopyFrom(iam_user);
    } else {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get arn_info_key, err={}", err);
        LOG(WARNING) << msg;
        return -1;
    }

    for (auto& stage : *instance.mutable_stages()) {
        if (stage.has_obj_info() && stage.obj_info().has_encryption_info()) {
            auto& obj_info = *stage.mutable_obj_info();
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return -1;
            obj_info.set_ak(std::move(plain_ak_sk_pair.first));
            obj_info.set_sk(std::move(plain_ak_sk_pair.second));
        }
    }
    return 0;
}

static int decrypt_and_update_ak_sk(ObjectStoreInfoPB& obj_info, MetaServiceCode& code,
                                    std::string& msg) {
    if (obj_info.has_encryption_info()) {
        AkSkPair plain_ak_sk_pair;
        if (int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            ret != 0) {
            return ret;
        }
        obj_info.set_ak(std::move(plain_ak_sk_pair.first));
        obj_info.set_sk(std::move(plain_ak_sk_pair.second));
    }
    return 0;
};

void MetaServiceImpl::get_obj_store_info(google::protobuf::RpcController* controller,
                                         const GetObjStoreInfoRequest* request,
                                         GetObjStoreInfoResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_obj_store_info);
    // Prepare data
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
    RPC_RATE_LIMIT(get_obj_store_info)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }
    for (auto& obj_info : *instance.mutable_obj_info()) {
        if (auto ret = decrypt_and_update_ak_sk(obj_info, code, msg); ret != 0) {
            return;
        }
    }

    // Iterate all the resources to return to the rpc caller
    if (!instance.resource_ids().empty()) {
        std::string storage_vault_start = storage_vault_key({instance.instance_id(), ""});
        std::string storage_vault_end = storage_vault_key({instance.instance_id(), "\xff"});
        std::unique_ptr<RangeGetIterator> it;
        do {
            TxnErrorCode err = txn->get(storage_vault_start, storage_vault_end, &it);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("internal error, failed to get storage vault, err={}", err);
                LOG(WARNING) << msg;
                return;
            }

            while (it->has_next()) {
                auto [k, v] = it->next();
                auto* vault = response->add_storage_vault();
                if (!vault->ParseFromArray(v.data(), v.size())) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    msg = fmt::format("malformed storage vault, unable to deserialize key={}",
                                      hex(k));
                    LOG(WARNING) << msg << " key=" << hex(k);
                    return;
                }
                if (!it->has_next()) {
                    storage_vault_start = k;
                }
            }
            storage_vault_start.push_back('\x00'); // Update to next smallest key for iteration
        } while (it->more());
    }
    for (auto& vault : *response->mutable_storage_vault()) {
        if (vault.has_obj_info()) {
            if (auto ret = decrypt_and_update_ak_sk(*vault.mutable_obj_info(), code, msg);
                ret != 0) {
                return;
            }
        }
    }

    response->mutable_obj_info()->CopyFrom(instance.obj_info());
    if (instance.has_default_storage_vault_id()) {
        response->set_default_storage_vault_id(instance.default_storage_vault_id());
        response->set_default_storage_vault_name(instance.default_storage_vault_name());
    }
}

// The next available vault id would be max(max(obj info id), max(vault id)) + 1.
static std::string next_available_vault_id(const InstanceInfoPB& instance) {
    int vault_id = 0;
    auto max = [](int prev, const auto& last) {
        int last_id = 0;
        std::string_view value = "0";
        if constexpr (std::is_same_v<std::decay_t<decltype(last)>, ObjectStoreInfoPB>) {
            value = last.id();
        } else if constexpr (std::is_same_v<std::decay_t<decltype(last)>, std::string>) {
            value = last;
        }
        if (auto [_, ec] = std::from_chars(value.data(), value.data() + value.size(), last_id);
            ec != std::errc {}) [[unlikely]] {
            LOG_WARNING("Invalid resource id format: {}", value);
            last_id = 0;
            DCHECK(false);
        }
        return std::max(prev, last_id);
    };
    auto prev = std::accumulate(
            instance.resource_ids().begin(), instance.resource_ids().end(),
            std::accumulate(instance.obj_info().begin(), instance.obj_info().end(), vault_id, max),
            max);
    return std::to_string(prev + 1);
}

namespace detail {

// Validate and normalize hdfs prefix. Return true if prefix is valid.
bool normalize_hdfs_prefix(std::string& prefix) {
    if (prefix.empty()) {
        return true;
    }

    if (prefix.find("://") != std::string::npos) {
        // Should not contain scheme
        return false;
    }

    trim(prefix);
    return true;
}

// Validate and normalize hdfs fs_name. Return true if fs_name is valid.
bool normalize_hdfs_fs_name(std::string& fs_name) {
    if (fs_name.empty()) {
        return false;
    }

    // Should check scheme existence?
    trim(fs_name);
    return !fs_name.empty();
}

} // namespace detail

static int add_hdfs_storage_vault(InstanceInfoPB& instance, Transaction* txn,
                                  StorageVaultPB& hdfs_param, MetaServiceCode& code,
                                  std::string& msg) {
    if (!hdfs_param.has_hdfs_info()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("vault_name={} passed invalid argument", hdfs_param.name());
        return -1;
    }

    using namespace detail;
    // Check and normalize hdfs conf
    auto* prefix = hdfs_param.mutable_hdfs_info()->mutable_prefix();
    if (!normalize_hdfs_prefix(*prefix)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid prefix: {}", *prefix);
        return -1;
    }
    if (config::enable_distinguish_hdfs_path) {
        auto uuid_suffix = butil::GenerateGUID();
        if (uuid_suffix.empty()) [[unlikely]] {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = fmt::format("failed to generate one suffix for hdfs prefix: {}", *prefix);
            return -1;
        }
        *prefix = fmt::format("{}_{}", *prefix, uuid_suffix);
    }

    auto* fs_name = hdfs_param.mutable_hdfs_info()->mutable_build_conf()->mutable_fs_name();
    if (!normalize_hdfs_fs_name(*fs_name)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid fs_name: {}", *fs_name);
        return -1;
    }

    std::string key;
    std::string vault_id = next_available_vault_id(instance);
    storage_vault_key({instance.instance_id(), vault_id}, &key);
    hdfs_param.set_id(vault_id);
    std::string val = hdfs_param.SerializeAsString();
    txn->put(key, val);
    LOG_INFO("try to put storage vault_id={}, vault_name={}, vault_key={}", vault_id,
             hdfs_param.name(), hex(key));
    instance.mutable_resource_ids()->Add(std::move(vault_id));
    *instance.mutable_storage_vault_names()->Add() = hdfs_param.name();
    return 0;
}

static void create_object_info_with_encrypt(const InstanceInfoPB& instance, ObjectStoreInfoPB* obj,
                                            bool sse_enabled, MetaServiceCode& code,
                                            std::string& msg) {
    std::string plain_ak = obj->has_ak() ? obj->ak() : "";
    std::string plain_sk = obj->has_sk() ? obj->sk() : "";
    std::string bucket = obj->has_bucket() ? obj->bucket() : "";
    std::string prefix = obj->has_prefix() ? obj->prefix() : "";
    // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
    trim(prefix);
    std::string endpoint = obj->has_endpoint() ? obj->endpoint() : "";
    std::string external_endpoint = obj->has_external_endpoint() ? obj->external_endpoint() : "";
    std::string region = obj->has_region() ? obj->region() : "";

    // ATTN: prefix may be empty
    if (plain_ak.empty() || plain_sk.empty() || bucket.empty() || endpoint.empty() ||
        region.empty() || !obj->has_provider() || external_endpoint.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 conf info err, please check it";
        return;
    }
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    auto ret = encrypt_ak_sk_helper(plain_ak, plain_sk, &encryption_info, &cipher_ak_sk_pair, code,
                                    msg);
    TEST_SYNC_POINT_CALLBACK("create_object_info_with_encrypt", &ret, &code, &msg);
    if (ret != 0) {
        return;
    }

    obj->set_ak(std::move(cipher_ak_sk_pair.first));
    obj->set_sk(std::move(cipher_ak_sk_pair.second));
    obj->mutable_encryption_info()->CopyFrom(encryption_info);
    obj->set_bucket(bucket);
    obj->set_prefix(prefix);
    obj->set_endpoint(endpoint);
    obj->set_external_endpoint(external_endpoint);
    obj->set_region(region);
    obj->set_id(next_available_vault_id(instance));
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    obj->set_ctime(time);
    obj->set_mtime(time);
    obj->set_sse_enabled(sse_enabled);
}

static int add_vault_into_instance(InstanceInfoPB& instance, Transaction* txn,
                                   StorageVaultPB& vault_param, MetaServiceCode& code,
                                   std::string& msg) {
    if (std::find_if(instance.storage_vault_names().begin(), instance.storage_vault_names().end(),
                     [&vault_param](const auto& name) { return name == vault_param.name(); }) !=
        instance.storage_vault_names().end()) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = fmt::format("vault_name={} already created", vault_param.name());
        return -1;
    }

    if (vault_param.has_hdfs_info()) {
        return add_hdfs_storage_vault(instance, txn, vault_param, code, msg);
    }

    create_object_info_with_encrypt(instance, vault_param.mutable_obj_info(), true, code, msg);
    if (code != MetaServiceCode::OK) {
        return -1;
    }

    vault_param.mutable_obj_info()->CopyFrom(vault_param.obj_info());
    vault_param.set_id(vault_param.obj_info().id());
    auto vault_key = storage_vault_key({instance.instance_id(), vault_param.obj_info().id()});
    *instance.mutable_resource_ids()->Add() = vault_param.id();
    *instance.mutable_storage_vault_names()->Add() = vault_param.name();
    LOG_INFO("try to put storage vault_id={}, vault_name={}, vault_key={}", vault_param.id(),
             vault_param.name(), hex(vault_key));
    txn->put(vault_key, vault_param.SerializeAsString());
    return 0;
}

static int remove_hdfs_storage_vault(InstanceInfoPB& instance, Transaction* txn,
                                     const StorageVaultPB& hdfs_info, MetaServiceCode& code,
                                     std::string& msg) {
    std::string_view vault_name = hdfs_info.name();
    auto name_iter = std::find_if(instance.storage_vault_names().begin(),
                                  instance.storage_vault_names().end(),
                                  [&](const auto& name) { return vault_name == name; });
    if (name_iter == instance.storage_vault_names().end()) {
        code = MetaServiceCode::STORAGE_VAULT_NOT_FOUND;
        msg = fmt::format("vault_name={} not found", vault_name);
        return -1;
    }
    auto vault_idx = name_iter - instance.storage_vault_names().begin();
    auto vault_id_iter = instance.resource_ids().begin() + vault_idx;
    std::string_view vault_id = *vault_id_iter;
    std::string vault_key = storage_vault_key({instance.instance_id(), vault_id});

    txn->remove(vault_key);
    instance.mutable_storage_vault_names()->DeleteSubrange(vault_idx, 1);
    instance.mutable_resource_ids()->DeleteSubrange(vault_idx, 1);
    LOG(INFO) << "remove storage_vault_key=" << hex(vault_key);

    return 0;
}

// Log vault message and origin default storage vault message for potential tracing
static void set_default_vault_log_helper(const InstanceInfoPB& instance,
                                         std::string_view vault_name, std::string_view vault_id) {
    auto vault_msg = fmt::format("instance {} tries to set default vault as {}, id {}",
                                 instance.instance_id(), vault_id, vault_name);
    if (instance.has_default_storage_vault_id()) {
        vault_msg = fmt::format("{}, origin default vault name {}, vault id {}", vault_msg,
                                instance.default_storage_vault_name(),
                                instance.default_storage_vault_id());
    }
    LOG(INFO) << vault_msg;
}

void MetaServiceImpl::alter_obj_store_info(google::protobuf::RpcController* controller,
                                           const AlterObjStoreInfoRequest* request,
                                           AlterObjStoreInfoResponse* response,
                                           ::google::protobuf::Closure* done) {
    std::string ak, sk, bucket, prefix, endpoint, external_endpoint, region;
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    RPC_PREPROCESS(alter_obj_store_info);
    switch (request->op()) {
    case AlterObjStoreInfoRequest::ADD_OBJ_INFO:
    case AlterObjStoreInfoRequest::ADD_S3_VAULT:
    case AlterObjStoreInfoRequest::DROP_S3_VAULT:
    case AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK:
    case AlterObjStoreInfoRequest::UPDATE_AK_SK: {
        if (!request->has_obj() && (!request->has_vault() || !request->vault().has_obj_info())) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 obj info err " + proto_to_json(*request);
            return;
        }
        auto& obj = request->has_obj() ? request->obj() : request->vault().obj_info();
        // Prepare data
        if (!obj.has_ak() || !obj.has_sk()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 obj info err " + proto_to_json(*request);
            return;
        }

        std::string plain_ak = obj.has_ak() ? obj.ak() : "";
        std::string plain_sk = obj.has_sk() ? obj.sk() : "";

        auto ret = encrypt_ak_sk_helper(plain_ak, plain_sk, &encryption_info, &cipher_ak_sk_pair,
                                        code, msg);
        if (ret != 0) {
            return;
        }
        ak = cipher_ak_sk_pair.first;
        sk = cipher_ak_sk_pair.second;
        bucket = obj.has_bucket() ? obj.bucket() : "";
        prefix = obj.has_prefix() ? obj.prefix() : "";
        endpoint = obj.has_endpoint() ? obj.endpoint() : "";
        external_endpoint = obj.has_external_endpoint() ? obj.external_endpoint() : "";
        region = obj.has_region() ? obj.region() : "";

        //  obj size > 1k, refuse
        if (obj.ByteSizeLong() > 1024) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 obj info greater than 1k " + proto_to_json(*request);
            return;
        };
    } break;
    case AlterObjStoreInfoRequest::ADD_HDFS_INFO:
    case AlterObjStoreInfoRequest::DROP_HDFS_INFO: {
        if (!request->has_vault() || !request->vault().has_name()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "hdfs info is not found " + proto_to_json(*request);
            return;
        }
    } break;
    case AlterObjStoreInfoRequest::SET_DEFAULT_VAULT: {
        if (!request->has_vault() || !request->vault().has_name()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "hdfs info is not found " + proto_to_json(*request);
            return;
        }
        break;
    }
    case AlterObjStoreInfoRequest::ADD_BUILT_IN_VAULT: {
        // It should at least has one hdfs info or obj info inside storage vault
        if ((!request->has_vault())) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "hdfs info is not found " + proto_to_json(*request);
            return;
        }
        break;
    }
    case AlterObjStoreInfoRequest::UNKNOWN: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "Unknown alter info " + proto_to_json(*request);
        return;
    } break;
    case AlterObjStoreInfoRequest::UNSET_DEFAULT_VAULT:
        break;
    }

    // TODO(dx): check s3 info right

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
    RPC_RATE_LIMIT(alter_obj_store_info)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }

    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    switch (request->op()) {
    case AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK: {
        // get id
        std::string id = request->obj().has_id() ? request->obj().id() : "0";
        int idx = std::stoi(id);
        if (idx < 1 || idx > instance.obj_info().size()) {
            // err
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "id invalid, please check it";
            return;
        }
        auto& obj_info =
                const_cast<std::decay_t<decltype(instance.obj_info())>&>(instance.obj_info());
        for (auto& it : obj_info) {
            if (std::stoi(it.id()) == idx) {
                if (it.ak() == ak && it.sk() == sk) {
                    // not change, just return ok
                    code = MetaServiceCode::OK;
                    msg = "";
                    return;
                }
                it.set_mtime(time);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
            }
        }
    } break;
    case AlterObjStoreInfoRequest::ADD_OBJ_INFO:
        if (instance.enable_storage_vault()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "Storage vault doesn't support add obj info";
            return;
        }
    case AlterObjStoreInfoRequest::ADD_S3_VAULT: {
        auto& obj = request->has_obj() ? request->obj() : request->vault().obj_info();
        if (!obj.has_provider()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf lease provider info";
            return;
        }
        if (instance.obj_info().size() >= 10) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "this instance history has greater than 10 objs, please new another instance";
            return;
        }
        // ATTN: prefix may be empty
        if (ak.empty() || sk.empty() || bucket.empty() || endpoint.empty() || region.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf info err, please check it";
            return;
        }

        auto& objs = instance.obj_info();
        for (auto& it : objs) {
            if (bucket == it.bucket() && prefix == it.prefix() && endpoint == it.endpoint() &&
                region == it.region() && ak == it.ak() && sk == it.sk() &&
                obj.provider() == it.provider() && external_endpoint == it.external_endpoint()) {
                // err, anything not changed
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "original obj infos has a same conf, please check it";
                return;
            }
        }
        // calc id
        ObjectStoreInfoPB last_item;
        last_item.set_ctime(time);
        last_item.set_mtime(time);
        last_item.set_id(next_available_vault_id(instance));
        if (obj.has_user_id()) {
            last_item.set_user_id(obj.user_id());
        }
        last_item.set_ak(std::move(cipher_ak_sk_pair.first));
        last_item.set_sk(std::move(cipher_ak_sk_pair.second));
        last_item.mutable_encryption_info()->CopyFrom(encryption_info);
        last_item.set_bucket(bucket);
        // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
        trim(prefix);
        last_item.set_prefix(prefix);
        last_item.set_endpoint(endpoint);
        last_item.set_external_endpoint(external_endpoint);
        last_item.set_region(region);
        last_item.set_provider(obj.provider());
        last_item.set_sse_enabled(instance.sse_enabled());
        if (request->op() == AlterObjStoreInfoRequest::ADD_OBJ_INFO) {
            instance.add_obj_info()->CopyFrom(last_item);
            LOG_INFO("Instance {} tries to put obj info", instance.instance_id());
        } else if (request->op() == AlterObjStoreInfoRequest::ADD_S3_VAULT) {
            if (instance.storage_vault_names().end() !=
                std::find_if(instance.storage_vault_names().begin(),
                             instance.storage_vault_names().end(),
                             [&](const std::string& candidate_name) {
                                 return candidate_name == request->vault().name();
                             })) {
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = fmt::format("vault_name={} already created", request->vault().name());
                return;
            }
            StorageVaultPB vault;
            vault.set_id(last_item.id());
            vault.set_name(request->vault().name());
            *instance.mutable_resource_ids()->Add() = vault.id();
            *instance.mutable_storage_vault_names()->Add() = vault.name();
            vault.mutable_obj_info()->MergeFrom(last_item);
            auto vault_key = storage_vault_key({instance.instance_id(), last_item.id()});
            txn->put(vault_key, vault.SerializeAsString());
            if (request->has_set_as_default_storage_vault() &&
                request->set_as_default_storage_vault()) {
                response->set_default_storage_vault_replaced(
                        instance.has_default_storage_vault_id());
                set_default_vault_log_helper(instance, vault.name(), vault.id());
                instance.set_default_storage_vault_id(vault.id());
                instance.set_default_storage_vault_name(vault.name());
            }
            LOG_INFO("try to put storage vault_id={}, vault_name={}, vault_key={}", vault.id(),
                     vault.name(), hex(vault_key));
        }
    } break;
    case AlterObjStoreInfoRequest::ADD_HDFS_INFO: {
        if (auto ret = add_vault_into_instance(
                    instance, txn.get(), const_cast<StorageVaultPB&>(request->vault()), code, msg);
            ret != 0) {
            return;
        }
        if (request->has_set_as_default_storage_vault() &&
            request->set_as_default_storage_vault()) {
            response->set_default_storage_vault_replaced(instance.has_default_storage_vault_id());
            set_default_vault_log_helper(instance, *instance.storage_vault_names().rbegin(),
                                         *instance.resource_ids().rbegin());
            instance.set_default_storage_vault_id(*instance.resource_ids().rbegin());
            instance.set_default_storage_vault_name(*instance.storage_vault_names().rbegin());
        }
        break;
    }
    case AlterObjStoreInfoRequest::ADD_BUILT_IN_VAULT: {
        // If the resource ids is empty then it would be the first vault
        if (!instance.resource_ids().empty()) {
            std::stringstream ss;
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "Default vault can not be modified";
            msg = ss.str();
            return;
        }
        if (auto ret = add_vault_into_instance(
                    instance, txn.get(), const_cast<StorageVaultPB&>(request->vault()), code, msg);
            ret != 0) {
            return;
        }
        return;
    }
    case AlterObjStoreInfoRequest::DROP_HDFS_INFO: {
        if (auto ret = remove_hdfs_storage_vault(instance, txn.get(), request->vault(), code, msg);
            ret != 0) {
            return;
        }
        break;
    }
    case AlterObjStoreInfoRequest::SET_DEFAULT_VAULT: {
        const auto& name = request->vault().name();
        auto name_itr = std::find_if(instance.storage_vault_names().begin(),
                                     instance.storage_vault_names().end(),
                                     [&](const auto& vault_name) { return name == vault_name; });
        if (name_itr == instance.storage_vault_names().end()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "invalid storage vault name, name =" << name;
            msg = ss.str();
            return;
        }
        auto pos = name_itr - instance.storage_vault_names().begin();
        auto id_itr = instance.resource_ids().begin() + pos;
        response->set_default_storage_vault_replaced(instance.has_default_storage_vault_id());
        set_default_vault_log_helper(instance, name, *id_itr);
        instance.set_default_storage_vault_id(*id_itr);
        instance.set_default_storage_vault_name(name);
        response->set_storage_vault_id(*id_itr);
        break;
    }
    case AlterObjStoreInfoRequest::UNSET_DEFAULT_VAULT: {
        LOG_INFO("unset instance's default vault, instance id {}, previous default vault {}, id {}",
                 instance.instance_id(), instance.default_storage_vault_name(),
                 instance.default_storage_vault_id());
        instance.clear_default_storage_vault_id();
        instance.clear_default_storage_vault_name();
        break;
    }
    case AlterObjStoreInfoRequest::DROP_S3_VAULT:
        [[fallthrough]];
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }

    LOG(INFO) << "instance " << instance_id << " has " << instance.obj_info().size()
              << " s3 history info, and instance = " << proto_to_json(instance);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::update_ak_sk(google::protobuf::RpcController* controller,
                                   const UpdateAkSkRequest* request, UpdateAkSkResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_ak_sk);
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        msg = "instance id not set";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    if (!request->has_ram_user() && request->internal_bucket_user().empty()) {
        msg = "nothing to update";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    RPC_RATE_LIMIT(update_ak_sk)

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }

    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    std::stringstream update_record;

    // if has ram_user, encrypt and save it
    if (request->has_ram_user()) {
        if (request->ram_user().user_id().empty() || request->ram_user().ak().empty() ||
            request->ram_user().sk().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user info err " + proto_to_json(*request);
            return;
        }
        if (!instance.has_ram_user()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "instance doesn't have ram user info";
            return;
        }
        auto& ram_user = request->ram_user();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        if (encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info, &cipher_ak_sk_pair,
                                 code, msg) != 0) {
            return;
        }
        const auto& [ak, sk] = cipher_ak_sk_pair;
        auto& instance_ram_user = *instance.mutable_ram_user();
        if (ram_user.user_id() != instance_ram_user.user_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user_id err";
            return;
        }
        std::string old_ak = instance_ram_user.ak();
        std::string old_sk = instance_ram_user.sk();
        if (old_ak == ak && old_sk == sk) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ak sk eq original, please check it";
            return;
        }
        instance_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
        instance_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
        instance_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
        update_record << "update ram_user's ak sk, instance_id: " << instance_id
                      << " user_id: " << ram_user.user_id() << " old:  cipher ak: " << old_ak
                      << " cipher sk: " << old_sk << " new: cipher ak: " << ak
                      << " cipher sk: " << sk;
    }

    bool has_found_alter_obj_info = false;
    for (auto& alter_bucket_user : request->internal_bucket_user()) {
        if (!alter_bucket_user.has_ak() || !alter_bucket_user.has_sk() ||
            !alter_bucket_user.has_user_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 bucket info err " + proto_to_json(*request);
            return;
        }
        std::string user_id = alter_bucket_user.user_id();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        if (encrypt_ak_sk_helper(alter_bucket_user.ak(), alter_bucket_user.sk(), &encryption_info,
                                 &cipher_ak_sk_pair, code, msg) != 0) {
            return;
        }
        const auto& [ak, sk] = cipher_ak_sk_pair;
        auto& obj_info =
                const_cast<std::decay_t<decltype(instance.obj_info())>&>(instance.obj_info());
        for (auto& it : obj_info) {
            std::string old_ak = it.ak();
            std::string old_sk = it.sk();
            if (!it.has_user_id()) {
                has_found_alter_obj_info = true;
                // For compatibility, obj_info without a user_id only allow
                // single internal_bucket_user to modify it.
                if (request->internal_bucket_user_size() != 1) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "fail to update old instance's obj_info, s3 obj info err " +
                          proto_to_json(*request);
                    return;
                }
                if (it.ak() == ak && it.sk() == sk) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "ak sk eq original, please check it";
                    return;
                }
                it.set_mtime(time);
                it.set_user_id(user_id);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
                update_record << "update obj_info's ak sk without user_id, instance_id: "
                              << instance_id << " obj_info_id: " << it.id()
                              << " new user_id: " << user_id << " old:  cipher ak: " << old_ak
                              << " cipher sk: " << old_sk << " new:  cipher ak: " << ak
                              << " cipher sk: " << sk;
                continue;
            }
            if (it.user_id() == user_id) {
                has_found_alter_obj_info = true;
                if (it.ak() == ak && it.sk() == sk) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "ak sk eq original, please check it";
                    return;
                }
                it.set_mtime(time);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
                update_record << "update obj_info's ak sk, instance_id: " << instance_id
                              << " obj_info_id: " << it.id() << " user_id: " << user_id
                              << " old:  cipher ak: " << old_ak << " cipher sk: " << old_sk
                              << " new:  cipher ak: " << ak << " cipher sk: " << sk;
            }
        }
    }

    if (!request->internal_bucket_user().empty() && !has_found_alter_obj_info) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "fail to find the alter obj info";
        return;
    }

    LOG(INFO) << "instance " << instance_id << " has " << instance.obj_info().size()
              << " s3 history info, and " << instance.resource_ids_size() << " vaults "
              << "instance = " << proto_to_json(instance);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
    LOG(INFO) << update_record.str();
}

void MetaServiceImpl::create_instance(google::protobuf::RpcController* controller,
                                      const CreateInstanceRequest* request,
                                      CreateInstanceResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_instance);
    if (request->has_ram_user()) {
        auto& ram_user = request->ram_user();
        std::string ram_user_id = ram_user.has_user_id() ? ram_user.user_id() : "";
        std::string ram_user_ak = ram_user.has_ak() ? ram_user.ak() : "";
        std::string ram_user_sk = ram_user.has_sk() ? ram_user.sk() : "";
        if (ram_user_id.empty() || ram_user_ak.empty() || ram_user_sk.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user info err, please check it";
            return;
        }
    }
    instance_id = request->instance_id();
    // Prepare data
    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_user_id(request->has_user_id() ? request->user_id() : "");
    instance.set_name(request->has_name() ? request->name() : "");
    instance.set_status(InstanceInfoPB::NORMAL);
    instance.set_sse_enabled(request->sse_enabled());
    instance.set_enable_storage_vault(!request->has_obj_info());
    if (request->has_obj_info()) {
        create_object_info_with_encrypt(instance,
                                        const_cast<ObjectStoreInfoPB*>(&request->obj_info()),
                                        request->sse_enabled(), code, msg);
        if (code != MetaServiceCode::OK) {
            return;
        }
        instance.mutable_obj_info()->Add()->MergeFrom(request->obj_info());
    }
    if (request->has_ram_user()) {
        auto& ram_user = request->ram_user();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        if (encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info, &cipher_ak_sk_pair,
                                 code, msg) != 0) {
            return;
        }
        RamUserPB new_ram_user;
        new_ram_user.CopyFrom(ram_user);
        new_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
        new_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
        new_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
        instance.mutable_ram_user()->CopyFrom(new_ram_user);
    }

    if (instance.instance_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "instance id not set";
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    if (request->has_vault()) {
        auto& param = const_cast<StorageVaultPB&>(request->vault());
        param.set_name(BUILT_IN_STORAGE_VAULT_NAME.data());
        if (0 != add_vault_into_instance(instance, txn.get(), param, code, msg)) {
            return;
        }
    }

    InstanceKeyInfo key_info {request->instance_id()};
    std::string key;
    std::string val = instance.SerializeAsString();
    instance_key(key_info, &key);
    if (val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize";
        LOG(ERROR) << msg;
        return;
    }

    LOG(INFO) << "xxx instance json=" << proto_to_json(instance);

    // Check existence before proceeding
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        std::stringstream ss;
        ss << (err == TxnErrorCode::TXN_OK ? "instance already existed"
                                           : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id() << ", err=" << err;
        code = err == TxnErrorCode::TXN_OK ? MetaServiceCode::ALREADY_EXISTED
                                           : cast_as<ErrCategory::READ>(err);
        msg = ss.str();
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << request->instance_id() << " instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::alter_instance(google::protobuf::RpcController* controller,
                                     const AlterInstanceRequest* request,
                                     AlterInstanceResponse* response,
                                     ::google::protobuf::Closure* done) {
    StopWatch sw;
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side()
              << " request=" << request->ShortDebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl, &closure_guard, &sw, &instance_id](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
                closure_guard.reset(nullptr);
                if (config::use_detailed_metrics && !instance_id.empty()) {
                    g_bvar_ms_alter_instance.put(instance_id, sw.elapsed_us());
                }
            });

    std::pair<MetaServiceCode, std::string> ret;
    switch (request->op()) {
    case AlterInstanceRequest::DROP: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            // check instance doesn't have any cluster.
            if (instance->clusters_size() != 0) {
                msg = "failed to drop instance, instance has clusters";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }

            instance->set_status(InstanceInfoPB::DELETED);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "drop instance json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::RENAME: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            std::string name = request->has_name() ? request->name() : "";
            if (name.empty()) {
                msg = "rename instance name, but not set";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_name(name);

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "rename instance json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::ENABLE_SSE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            if (instance->sse_enabled()) {
                msg = "failed to enable sse, instance has enabled sse";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_sse_enabled(true);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            for (auto& obj_info : *(instance->mutable_obj_info())) {
                obj_info.set_sse_enabled(true);
            }
            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "instance enable sse json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::DISABLE_SSE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            if (!instance->sse_enabled()) {
                msg = "failed to disable sse, instance has disabled sse";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_sse_enabled(false);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            for (auto& obj_info : *(instance->mutable_obj_info())) {
                obj_info.set_sse_enabled(false);
            }
            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "instance disable sse json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::REFRESH: {
        ret = resource_mgr_->refresh_instance(request->instance_id());
    } break;
    case AlterInstanceRequest::SET_OVERDUE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;

            if (instance->status() == InstanceInfoPB::DELETED) {
                msg = "can't set deleted instance to overdue, instance_id = " +
                      request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            if (instance->status() == InstanceInfoPB::OVERDUE) {
                msg = "the instance has already set  instance to overdue, instance_id = " +
                      request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_status(InstanceInfoPB::OVERDUE);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "set instance overdue json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::SET_NORMAL: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;

            if (instance->status() == InstanceInfoPB::DELETED) {
                msg = "can't set deleted instance to normal, instance_id = " +
                      request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            if (instance->status() == InstanceInfoPB::NORMAL) {
                msg = "the instance is already normal, instance_id = " + request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_status(InstanceInfoPB::NORMAL);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "set instance normal json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    default: {
        ss << "invalid request op, op=" << request->op();
        ret = std::make_pair(MetaServiceCode::INVALID_ARGUMENT, ss.str());
    }
    }
    code = ret.first;
    msg = ret.second;

    if (request->op() == AlterInstanceRequest::REFRESH) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
}

void MetaServiceImpl::get_instance(google::protobuf::RpcController* controller,
                                   const GetInstanceRequest* request, GetInstanceResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_instance);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id must be given";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_instance);
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    response->mutable_instance()->CopyFrom(instance);
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::alter_instance(
        const cloud::AlterInstanceRequest* request,
        std::function<std::pair<MetaServiceCode, std::string>(InstanceInfoPB*)> action) {
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        msg = "instance id not set";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(cast_as<ErrCategory::CREATE>(err), msg);
    }

    // Check existence before proceeding
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        std::stringstream ss;
        ss << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "instance not existed"
                                                      : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id() << ", err=" << err;
        // TODO(dx): fix CLUSTER_NOT_FOUND，VERSION_NOT_FOUND，TXN_LABEL_NOT_FOUND，etc to NOT_FOUND
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::CLUSTER_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        msg = ss.str();
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(code, msg);
    }
    LOG(INFO) << "alter instance key=" << hex(key);
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        msg = "failed to parse InstanceInfoPB";
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        LOG(WARNING) << msg;
        return std::make_pair(code, msg);
    }
    auto r = action(&instance);
    if (r.first != MetaServiceCode::OK) {
        return r;
    }
    val = r.second;
    txn->put(key, val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
        return std::make_pair(code, msg);
    }
    return std::make_pair(code, msg);
}

void MetaServiceImpl::alter_cluster(google::protobuf::RpcController* controller,
                                    const AlterClusterRequest* request,
                                    AlterClusterResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_cluster);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (!cloud_unique_id.empty() && instance_id.empty()) {
        instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
        if (instance_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }

    if (instance_id.empty() || !request->has_cluster()) {
        msg = "invalid request instance_id or cluster not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    if (!request->has_op()) {
        msg = "op not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    LOG(INFO) << "alter cluster instance_id=" << instance_id << " op=" << request->op();
    ClusterInfo cluster;
    cluster.cluster.CopyFrom(request->cluster());

    switch (request->op()) {
    case AlterClusterRequest::ADD_CLUSTER: {
        auto r = resource_mgr_->add_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::DROP_CLUSTER: {
        auto r = resource_mgr_->drop_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ClusterPB& i) { return i.cluster_id() == cluster.cluster.cluster_id(); },
                [&](ClusterPB& c, std::set<std::string>&) {
                    auto& mysql_user_names = cluster.cluster.mysql_user_name();
                    c.mutable_mysql_user_name()->CopyFrom(mysql_user_names);
                    return "";
                });
    } break;
    case AlterClusterRequest::ADD_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            node.node_info.set_status(NodeStatusPB::NODE_STATUS_RUNNING);
            to_add.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::DROP_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            to_del.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::DECOMMISSION_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }

        std::string be_unique_id = (request->cluster().nodes())[0].cloud_unique_id();
        std::vector<NodeInfo> nodes;
        std::string err = resource_mgr_->get_node(be_unique_id, &nodes);
        if (!err.empty()) {
            LOG(INFO) << "failed to check instance info, err=" << err;
            msg = err;
            break;
        }

        std::vector<NodeInfo> decomission_nodes;
        for (auto& node : nodes) {
            for (auto req_node : request->cluster().nodes()) {
                bool ip_processed = false;
                if (node.node_info.has_ip() && req_node.has_ip()) {
                    std::string endpoint = node.node_info.ip() + ":" +
                                           std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                            req_node.ip() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                        node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                    }
                    ip_processed = true;
                }

                if (!ip_processed && node.node_info.has_host() && req_node.has_host()) {
                    std::string endpoint = node.node_info.host() + ":" +
                                           std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                            req_node.host() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                        node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                    }
                }
            }
        }

        {
            std::vector<NodeInfo> to_add;
            std::vector<NodeInfo>& to_del = decomission_nodes;
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
        {
            std::vector<NodeInfo>& to_add = decomission_nodes;
            std::vector<NodeInfo> to_del;
            for (auto& node : to_add) {
                node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                LOG(INFO) << "decomission node, "
                          << "size: " << to_add.size() << " " << node.node_info.DebugString() << " "
                          << node.cluster_id << " " << node.cluster_name;
            }
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
    } break;
    case AlterClusterRequest::NOTIFY_DECOMMISSIONED: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }

        std::string be_unique_id = (request->cluster().nodes())[0].cloud_unique_id();
        std::vector<NodeInfo> nodes;
        std::string err = resource_mgr_->get_node(be_unique_id, &nodes);
        if (!err.empty()) {
            LOG(INFO) << "failed to check instance info, err=" << err;
            msg = err;
            break;
        }

        std::vector<NodeInfo> decomission_nodes;
        for (auto& node : nodes) {
            for (auto req_node : request->cluster().nodes()) {
                bool ip_processed = false;
                if (node.node_info.has_ip() && req_node.has_ip()) {
                    std::string endpoint = node.node_info.ip() + ":" +
                                           std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                            req_node.ip() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                    }
                    ip_processed = true;
                }

                if (!ip_processed && node.node_info.has_host() && req_node.has_host()) {
                    std::string endpoint = node.node_info.host() + ":" +
                                           std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                            req_node.host() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                    }
                }
            }
        }

        {
            std::vector<NodeInfo> to_add;
            std::vector<NodeInfo>& to_del = decomission_nodes;
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
        {
            std::vector<NodeInfo>& to_add = decomission_nodes;
            std::vector<NodeInfo> to_del;
            for (auto& node : to_add) {
                node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONED);
                LOG(INFO) << "notify node decomissioned, "
                          << " size: " << to_add.size() << " " << node.node_info.DebugString()
                          << " " << node.cluster_id << " " << node.cluster_name;
            }
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
    } break;
    case AlterClusterRequest::RENAME_CLUSTER: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ClusterPB& i) { return i.cluster_id() == cluster.cluster.cluster_id(); },
                [&](ClusterPB& c, std::set<std::string>& cluster_names) {
                    std::string msg;
                    auto it = cluster_names.find(cluster.cluster.cluster_name());
                    LOG(INFO) << "cluster.cluster.cluster_name(): "
                              << cluster.cluster.cluster_name();
                    for (auto itt : cluster_names) {
                        LOG(INFO) << "itt : " << itt;
                    }
                    if (it != cluster_names.end()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, a cluster with the same name already "
                              "exists in this instance "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    if (c.cluster_name() == cluster.cluster.cluster_name()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, name eq original name, original cluster "
                              "is "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    c.set_cluster_name(cluster.cluster.cluster_name());
                    return msg;
                });
    } break;
    case AlterClusterRequest::UPDATE_CLUSTER_ENDPOINT: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ClusterPB& i) { return i.cluster_id() == cluster.cluster.cluster_id(); },
                [&](ClusterPB& c, std::set<std::string>&) {
                    std::string msg;
                    if (!cluster.cluster.has_private_endpoint() ||
                        cluster.cluster.private_endpoint().empty()) {
                        code = MetaServiceCode::CLUSTER_ENDPOINT_MISSING;
                        ss << "missing private endpoint";
                        msg = ss.str();
                        return msg;
                    }

                    c.set_public_endpoint(cluster.cluster.public_endpoint());
                    c.set_private_endpoint(cluster.cluster.private_endpoint());

                    return msg;
                });
    } break;
    case AlterClusterRequest::SET_CLUSTER_STATUS: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ClusterPB& i) { return i.cluster_id() == cluster.cluster.cluster_id(); },
                [&](ClusterPB& c, std::set<std::string>&) {
                    std::string msg;
                    if (c.cluster_status() == request->cluster().cluster_status()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to set cluster status, status eq original status, original "
                              "cluster is "
                           << print_cluster_status(c.cluster_status());
                        msg = ss.str();
                        return msg;
                    }
                    // status from -> to
                    std::set<std::pair<cloud::ClusterStatus, cloud::ClusterStatus>>
                            can_work_directed_edges {
                                    {ClusterStatus::UNKNOWN, ClusterStatus::NORMAL},
                                    {ClusterStatus::NORMAL, ClusterStatus::SUSPENDED},
                                    {ClusterStatus::SUSPENDED, ClusterStatus::TO_RESUME},
                                    {ClusterStatus::TO_RESUME, ClusterStatus::NORMAL},
                                    {ClusterStatus::SUSPENDED, ClusterStatus::NORMAL},
                                    {ClusterStatus::NORMAL, ClusterStatus::MANUAL_SHUTDOWN},
                                    {ClusterStatus::MANUAL_SHUTDOWN, ClusterStatus::NORMAL},
                            };
                    auto from = c.cluster_status();
                    auto to = request->cluster().cluster_status();
                    if (can_work_directed_edges.count({from, to}) == 0) {
                        // can't find a directed edge in set, so refuse it
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to set cluster status, original cluster is "
                           << print_cluster_status(from) << " and want set "
                           << print_cluster_status(to);
                        msg = ss.str();
                        return msg;
                    }
                    c.set_cluster_status(request->cluster().cluster_status());
                    return msg;
                });
    } break;
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }
    if (!msg.empty() && code == MetaServiceCode::OK) {
        code = MetaServiceCode::UNDEFINED_ERR;
    }

    if (code != MetaServiceCode::OK) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
} // alter cluster

void MetaServiceImpl::get_cluster(google::protobuf::RpcController* controller,
                                  const GetClusterRequest* request, GetClusterResponse* response,
                                  ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_cluster);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string cluster_id = request->has_cluster_id() ? request->cluster_id() : "";
    std::string cluster_name = request->has_cluster_name() ? request->cluster_name() : "";
    std::string mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";

    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id must be given";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        if (request->has_instance_id()) {
            instance_id = request->instance_id();
            // FIXME(gavin): this mechanism benifits debugging and
            //               administration, is it dangerous?
            LOG(WARNING) << "failed to get instance_id with cloud_unique_id=" << cloud_unique_id
                         << " use the given instance_id=" << instance_id << " instead";
        } else {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }
    RPC_RATE_LIMIT(get_cluster)
    // ATTN: if the case that multiple conditions are satisfied, just use by this order:
    // cluster_id -> cluster_name -> mysql_user_name
    if (!cluster_id.empty()) {
        cluster_name = "";
        mysql_user_name = "";
    } else if (!cluster_name.empty()) {
        mysql_user_name = "";
    }

    bool get_all_cluster_info = false;
    // if cluster_id、cluster_name、mysql_user_name all empty, get this instance's all cluster info.
    if (cluster_id.empty() && cluster_name.empty() && mysql_user_name.empty()) {
        get_all_cluster_info = true;
    }

    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "failed to get instance_id with cloud_unique_id=" + cloud_unique_id;
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    response->set_enable_storage_vault(instance.enable_storage_vault());
    if (instance.enable_storage_vault() &&
        std::find_if(instance.storage_vault_names().begin(), instance.storage_vault_names().end(),
                     [](const std::string& name) { return name == BUILT_IN_STORAGE_VAULT_NAME; }) ==
                instance.storage_vault_names().end()) {
        code = MetaServiceCode::STORAGE_VAULT_NOT_FOUND;
        msg = "instance has no built in storage vault";
        return;
    }

    auto get_cluster_mysql_user = [](const ClusterPB& c, std::set<std::string>* mysql_users) {
        for (int i = 0; i < c.mysql_user_name_size(); i++) {
            mysql_users->emplace(c.mysql_user_name(i));
        }
    };

    if (get_all_cluster_info) {
        response->mutable_cluster()->CopyFrom(instance.clusters());
        LOG_EVERY_N(INFO, 100) << "get all cluster info, " << msg;
    } else {
        for (int i = 0; i < instance.clusters_size(); ++i) {
            auto& c = instance.clusters(i);
            std::set<std::string> mysql_users;
            get_cluster_mysql_user(c, &mysql_users);
            // The last wins if add_cluster() does not ensure uniqueness of
            // cluster_id and cluster_name respectively
            if ((c.has_cluster_name() && c.cluster_name() == cluster_name) ||
                (c.has_cluster_id() && c.cluster_id() == cluster_id) ||
                mysql_users.count(mysql_user_name)) {
                // just one cluster
                response->add_cluster()->CopyFrom(c);
                LOG_EVERY_N(INFO, 100) << "found a cluster, instance_id=" << instance.instance_id()
                                       << " cluster=" << msg;
            }
        }
    }

    if (response->cluster().empty()) {
        ss << "fail to get cluster with " << request->ShortDebugString();
        msg = ss.str();
        std::replace(msg.begin(), msg.end(), '\n', ' ');
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
    }
} // get_cluster

void MetaServiceImpl::create_stage(::google::protobuf::RpcController* controller,
                                   const CreateStageRequest* request, CreateStageResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_stage);
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
    RPC_RATE_LIMIT(create_stage)

    if (!request->has_stage()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage not set";
        return;
    }
    auto stage = request->stage();

    if (!stage.has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }

    if (stage.name().empty() && stage.type() == StagePB::EXTERNAL) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage name not set";
        return;
    }
    if (stage.stage_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage id not set";
        return;
    }

    if (stage.type() == StagePB::INTERNAL) {
        if (stage.mysql_user_name().empty() || stage.mysql_user_id().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "internal stage must have a mysql user name and id must be given, name size="
               << stage.mysql_user_name_size() << " id size=" << stage.mysql_user_id_size();
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    VLOG_DEBUG << "config stages num=" << config::max_num_stages;
    if (instance.stages_size() >= config::max_num_stages) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "this instance has greater than config num stages";
        LOG(WARNING) << "can't create more than config num stages, and instance has "
                     << std::to_string(instance.stages_size());
        return;
    }

    // check if the stage exists
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (stage.type() == StagePB::INTERNAL) {
            // check all internal stage format is right
            if (s.type() == StagePB::INTERNAL && s.mysql_user_id_size() == 0) {
                LOG(WARNING) << "impossible, internal stage must have at least one id instance="
                             << proto_to_json(instance);
            }

            if (s.type() == StagePB::INTERNAL &&
                (s.mysql_user_id(0) == stage.mysql_user_id(0) ||
                 s.mysql_user_name(0) == stage.mysql_user_name(0))) {
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = "stage already exist";
                ss << "stage already exist, req user_name=" << stage.mysql_user_name(0)
                   << " existed user_name=" << s.mysql_user_name(0)
                   << "req user_id=" << stage.mysql_user_id(0)
                   << " existed user_id=" << s.mysql_user_id(0);
                return;
            }
        }

        if (stage.type() == StagePB::EXTERNAL) {
            if (s.name() == stage.name()) {
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = "stage already exist";
                return;
            }
        }

        if (s.stage_id() == stage.stage_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "stage id is duplicated";
            return;
        }
    }

    if (stage.type() == StagePB::INTERNAL) {
        if (instance.obj_info_size() == 0) {
            LOG(WARNING) << "impossible, instance must have at least one obj_info.";
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "impossible, instance must have at least one obj_info.";
            return;
        }
        auto& lastest_obj = instance.obj_info()[instance.obj_info_size() - 1];
        // ${obj_prefix}/stage/{username}/{user_id}
        std::string mysql_user_name = stage.mysql_user_name(0);
        std::string prefix = fmt::format("{}/stage/{}/{}", lastest_obj.prefix(), mysql_user_name,
                                         stage.mysql_user_id(0));
        auto as = instance.add_stages();
        as->mutable_obj_info()->set_prefix(prefix);
        as->mutable_obj_info()->set_id(lastest_obj.id());
        as->add_mysql_user_name(mysql_user_name);
        as->add_mysql_user_id(stage.mysql_user_id(0));
        as->set_stage_id(stage.stage_id());
    } else if (stage.type() == StagePB::EXTERNAL) {
        if (!stage.has_obj_info()) {
            instance.add_stages()->CopyFrom(stage);
        } else {
            StagePB tmp_stage;
            tmp_stage.CopyFrom(stage);
            auto obj_info = tmp_stage.mutable_obj_info();
            EncryptionInfoPB encryption_info;
            AkSkPair cipher_ak_sk_pair;
            if (encrypt_ak_sk_helper(obj_info->ak(), obj_info->sk(), &encryption_info,
                                     &cipher_ak_sk_pair, code, msg) != 0) {
                return;
            }
            obj_info->set_ak(std::move(cipher_ak_sk_pair.first));
            obj_info->set_sk(std::move(cipher_ak_sk_pair.second));
            obj_info->mutable_encryption_info()->CopyFrom(encryption_info);
            instance.add_stages()->CopyFrom(tmp_stage);
        }
    }
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key)
              << " json=" << proto_to_json(instance);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_stage(google::protobuf::RpcController* controller,
                                const GetStageRequest* request, GetStageResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_stage);
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
    RPC_RATE_LIMIT(get_stage)
    if (!request->has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    auto type = request->type();

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (type == StagePB::INTERNAL) {
        auto mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";
        if (mysql_user_name.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "mysql user name not set";
            return;
        }
        auto mysql_user_id = request->has_mysql_user_id() ? request->mysql_user_id() : "";
        if (mysql_user_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "mysql user id not set";
            return;
        }

        // check mysql user_name has been created internal stage
        auto& stage = instance.stages();
        bool found = false;
        if (instance.obj_info_size() == 0) {
            LOG(WARNING) << "impossible, instance must have at least one obj_info.";
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "impossible, instance must have at least one obj_info.";
            return;
        }

        for (auto s : stage) {
            if (s.type() != StagePB::INTERNAL) {
                continue;
            }
            if (s.mysql_user_name().size() == 0 || s.mysql_user_id().size() == 0) {
                LOG(WARNING) << "impossible here, internal stage must have at least one user, "
                                "invalid stage="
                             << proto_to_json(s);
                continue;
            }
            if (s.mysql_user_name(0) == mysql_user_name) {
                StagePB stage_pb;
                // internal stage id is user_id, if user_id not eq internal stage's user_id, del it.
                // let fe create a new internal stage
                if (s.mysql_user_id(0) != mysql_user_id) {
                    LOG(INFO) << "ABA user=" << mysql_user_name
                              << " internal stage original user_id=" << s.mysql_user_id()[0]
                              << " rpc user_id=" << mysql_user_id
                              << " stage info=" << proto_to_json(s);
                    code = MetaServiceCode::STATE_ALREADY_EXISTED_FOR_USER;
                    msg = "aba user, drop stage and create a new one";
                    // response return to be dropped stage id.
                    stage_pb.CopyFrom(s);
                    response->add_stage()->CopyFrom(stage_pb);
                    return;
                }
                // find, use it stage prefix and id
                found = true;
                // get from internal stage
                int idx = stoi(s.obj_info().id());
                if (idx > instance.obj_info().size() || idx < 1) {
                    LOG(WARNING) << "invalid idx: " << idx;
                    code = MetaServiceCode::UNDEFINED_ERR;
                    msg = "impossible, id invalid";
                    return;
                }
                auto& old_obj = instance.obj_info()[idx - 1];

                stage_pb.mutable_obj_info()->set_ak(old_obj.ak());
                stage_pb.mutable_obj_info()->set_sk(old_obj.sk());
                if (old_obj.has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(old_obj.ak(), old_obj.sk(),
                                                   old_obj.encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    stage_pb.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage_pb.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                }
                stage_pb.mutable_obj_info()->set_bucket(old_obj.bucket());
                stage_pb.mutable_obj_info()->set_endpoint(old_obj.endpoint());
                stage_pb.mutable_obj_info()->set_external_endpoint(old_obj.external_endpoint());
                stage_pb.mutable_obj_info()->set_region(old_obj.region());
                stage_pb.mutable_obj_info()->set_provider(old_obj.provider());
                stage_pb.mutable_obj_info()->set_prefix(s.obj_info().prefix());
                stage_pb.set_stage_id(s.stage_id());
                stage_pb.set_type(s.type());
                response->add_stage()->CopyFrom(stage_pb);
                return;
            }
        }
        if (!found) {
            LOG(INFO) << "user=" << mysql_user_name
                      << " not have a valid stage, rpc user_id=" << mysql_user_id;
            code = MetaServiceCode::STAGE_NOT_FOUND;
            msg = "stage not found, create a new one";
            return;
        }
    }

    // get all external stages for display, but don't show ak/sk, so there is no need to decrypt ak/sk.
    if (type == StagePB::EXTERNAL && !request->has_stage_name()) {
        for (int i = 0; i < instance.stages_size(); ++i) {
            auto& s = instance.stages(i);
            if (s.type() != StagePB::EXTERNAL) {
                continue;
            }
            response->add_stage()->CopyFrom(s);
        }
        return;
    }

    // get external stage with the specified stage name
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (s.type() == type && s.name() == request->stage_name()) {
            StagePB stage;
            stage.CopyFrom(s);
            if (!stage.has_access_type() || stage.access_type() == StagePB::AKSK) {
                stage.set_access_type(StagePB::AKSK);
                auto obj_info = stage.mutable_obj_info();
                if (obj_info->has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(obj_info->ak(), obj_info->sk(),
                                                   obj_info->encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    obj_info->set_ak(std::move(plain_ak_sk_pair.first));
                    obj_info->set_sk(std::move(plain_ak_sk_pair.second));
                }
            } else if (stage.access_type() == StagePB::BUCKET_ACL) {
                if (!instance.has_ram_user()) {
                    ss << "instance does not have ram user";
                    msg = ss.str();
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    return;
                }
                if (instance.ram_user().has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(
                            instance.ram_user().ak(), instance.ram_user().sk(),
                            instance.ram_user().encryption_info(), &plain_ak_sk_pair, code, msg);
                    if (ret != 0) return;
                    stage.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                } else {
                    stage.mutable_obj_info()->set_ak(instance.ram_user().ak());
                    stage.mutable_obj_info()->set_sk(instance.ram_user().sk());
                }
            } else if (stage.access_type() == StagePB::IAM) {
                std::string val;
                TxnErrorCode err = txn->get(system_meta_service_arn_info_key(), &val);
                if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    // For compatibility, use arn_info of config
                    stage.mutable_obj_info()->set_ak(config::arn_ak);
                    stage.mutable_obj_info()->set_sk(config::arn_sk);
                    stage.set_external_id(instance_id);
                } else if (err == TxnErrorCode::TXN_OK) {
                    RamUserPB iam_user;
                    if (!iam_user.ParseFromString(val)) {
                        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                        msg = "failed to parse RamUserPB";
                        return;
                    }
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(),
                                                   iam_user.encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    stage.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                    stage.set_external_id(instance_id);
                } else {
                    code = cast_as<ErrCategory::READ>(err);
                    ss << "failed to get arn_info_key, err=" << err;
                    msg = ss.str();
                    return;
                }
            }
            response->add_stage()->CopyFrom(stage);
            return;
        }
    }

    ss << "stage not found with " << proto_to_json(*request);
    msg = ss.str();
    code = MetaServiceCode::STAGE_NOT_FOUND;
}

void MetaServiceImpl::drop_stage(google::protobuf::RpcController* controller,
                                 const DropStageRequest* request, DropStageResponse* response,
                                 ::google::protobuf::Closure* done) {
    StopWatch sw;
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::string instance_id;
    bool drop_request = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl, &closure_guard, &sw, &instance_id,
                         &drop_request](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
                closure_guard.reset(nullptr);
                if (config::use_detailed_metrics && !instance_id.empty() && !drop_request) {
                    g_bvar_ms_drop_stage.put(instance_id, sw.elapsed_us());
                }
            });

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
    RPC_RATE_LIMIT(drop_stage)

    if (!request->has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    auto type = request->type();

    if (type == StagePB::EXTERNAL && request->stage_name().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "external stage but not set stage name";
        return;
    }

    if (type == StagePB::INTERNAL && request->mysql_user_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "internal stage but not set user id";
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);
    std::stringstream ss;
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    StagePB stage;
    int idx = -1;
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if ((type == StagePB::INTERNAL && s.type() == StagePB::INTERNAL &&
             s.mysql_user_id(0) == request->mysql_user_id()) ||
            (type == StagePB::EXTERNAL && s.type() == StagePB::EXTERNAL &&
             s.name() == request->stage_name())) {
            idx = i;
            stage = s;
            break;
        }
    }
    if (idx == -1) {
        ss << "stage not found with " << proto_to_json(*request);
        msg = ss.str();
        code = MetaServiceCode::STAGE_NOT_FOUND;
        return;
    }

    auto& stages = const_cast<std::decay_t<decltype(instance.stages())>&>(instance.stages());
    stages.DeleteSubrange(idx, 1); // Remove it
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key)
              << " json=" << proto_to_json(instance);

    std::string key1;
    std::string val1;
    if (type == StagePB::INTERNAL) {
        RecycleStageKeyInfo recycle_stage_key_info {instance_id, stage.stage_id()};
        recycle_stage_key(recycle_stage_key_info, &key1);
        RecycleStagePB recycle_stage;
        recycle_stage.set_instance_id(instance_id);
        recycle_stage.set_reason(request->reason());
        recycle_stage.mutable_stage()->CopyFrom(stage);
        val1 = recycle_stage.SerializeAsString();
        if (val1.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        txn->put(key1, val1);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_iam(google::protobuf::RpcController* controller,
                              const GetIamRequest* request, GetIamResponse* response,
                              ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_iam);
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
    RPC_RATE_LIMIT(get_iam)

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    val.clear();
    err = txn->get(system_meta_service_arn_info_key(), &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // For compatibility, use arn_info of config
        RamUserPB iam_user;
        iam_user.set_user_id(config::arn_id);
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(config::arn_ak);
        iam_user.set_sk(config::arn_sk);
        response->mutable_iam_user()->CopyFrom(iam_user);
    } else if (err == TxnErrorCode::TXN_OK) {
        RamUserPB iam_user;
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse RamUserPB";
            return;
        }
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(), iam_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return;
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(std::move(plain_ak_sk_pair.first));
        iam_user.set_sk(std::move(plain_ak_sk_pair.second));
        response->mutable_iam_user()->CopyFrom(iam_user);
    } else {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get arn_info_key, err=" << err;
        msg = ss.str();
        return;
    }

    if (instance.has_ram_user()) {
        RamUserPB ram_user;
        ram_user.CopyFrom(instance.ram_user());
        if (ram_user.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), ram_user.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return;
            ram_user.set_ak(std::move(plain_ak_sk_pair.first));
            ram_user.set_sk(std::move(plain_ak_sk_pair.second));
        }
        response->mutable_ram_user()->CopyFrom(ram_user);
    }
}

void MetaServiceImpl::alter_iam(google::protobuf::RpcController* controller,
                                const AlterIamRequest* request, AlterIamResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_iam);
    std::string arn_id = request->has_account_id() ? request->account_id() : "";
    std::string arn_ak = request->has_ak() ? request->ak() : "";
    std::string arn_sk = request->has_sk() ? request->sk() : "";
    if (arn_id.empty() || arn_ak.empty() || arn_sk.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument";
        return;
    }

    RPC_RATE_LIMIT(alter_iam)

    std::string key = system_meta_service_arn_info_key();
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "fail to arn_info_key, err=" << err;
        msg = ss.str();
        return;
    }

    bool is_add_req = err == TxnErrorCode::TXN_KEY_NOT_FOUND;
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    if (encrypt_ak_sk_helper(arn_ak, arn_sk, &encryption_info, &cipher_ak_sk_pair, code, msg) !=
        0) {
        return;
    }
    const auto& [ak, sk] = cipher_ak_sk_pair;
    RamUserPB iam_user;
    std::string old_ak;
    std::string old_sk;
    if (!is_add_req) {
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse RamUserPB";
            msg = ss.str();
            return;
        }

        if (arn_id == iam_user.user_id() && ak == iam_user.ak() && sk == iam_user.sk()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "already has the same arn info";
            msg = ss.str();
            return;
        }
        old_ak = iam_user.ak();
        old_sk = iam_user.sk();
    }
    iam_user.set_user_id(arn_id);
    iam_user.set_ak(std::move(cipher_ak_sk_pair.first));
    iam_user.set_sk(std::move(cipher_ak_sk_pair.second));
    iam_user.mutable_encryption_info()->CopyFrom(encryption_info);
    val = iam_user.SerializeAsString();
    if (val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize";
        msg = ss.str();
        return;
    }
    txn->put(key, val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "txn->commit failed() err=" << err;
        msg = ss.str();
        return;
    }
    if (is_add_req) {
        LOG(INFO) << "add new iam info, cipher ak: " << iam_user.ak()
                  << " cipher sk: " << iam_user.sk();
    } else {
        LOG(INFO) << "alter iam info, old:  cipher ak: " << old_ak << " cipher sk" << old_sk
                  << " new: cipher ak: " << iam_user.ak() << " cipher sk:" << iam_user.sk();
    }
}

void MetaServiceImpl::alter_ram_user(google::protobuf::RpcController* controller,
                                     const AlterRamUserRequest* request,
                                     AlterRamUserResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_ram_user);
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    if (!request->has_ram_user() || request->ram_user().user_id().empty() ||
        request->ram_user().ak().empty() || request->ram_user().sk().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "ram user info err " + proto_to_json(*request);
        return;
    }
    auto& ram_user = request->ram_user();
    RPC_RATE_LIMIT(alter_ram_user)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }
    if (instance.status() == InstanceInfoPB::DELETED) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }
    if (instance.has_ram_user()) {
        LOG(WARNING) << "instance has ram user. instance_id=" << instance_id
                     << ", ram_user_id=" << ram_user.user_id();
    }
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    if (encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info, &cipher_ak_sk_pair,
                             code, msg) != 0) {
        return;
    }
    RamUserPB new_ram_user;
    new_ram_user.CopyFrom(ram_user);
    new_ram_user.set_user_id(ram_user.user_id());
    new_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
    new_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
    new_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
    instance.mutable_ram_user()->CopyFrom(new_ram_user);
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::begin_copy(google::protobuf::RpcController* controller,
                                 const BeginCopyRequest* request, BeginCopyResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_copy);
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
    RPC_RATE_LIMIT(begin_copy)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    // copy job value
    CopyJobPB copy_job;
    copy_job.set_stage_type(request->stage_type());
    copy_job.set_job_status(CopyJobPB::LOADING);
    copy_job.set_start_time_ms(request->start_time_ms());
    copy_job.set_timeout_time_ms(request->timeout_time_ms());

    std::vector<std::pair<std::string, std::string>> copy_files;
    auto& object_files = request->object_files();
    int file_num = 0;
    size_t file_size = 0;
    size_t file_meta_size = 0;
    for (auto i = 0; i < object_files.size(); ++i) {
        auto& file = object_files.at(i);
        // 1. get copy file kv to check if file is loading or loaded
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        std::string file_val;
        TxnErrorCode err = txn->get(file_key, &file_val);
        if (err == TxnErrorCode::TXN_OK) { // found key
            continue;
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) { // error
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get copy file, err={}", err);
            LOG(WARNING) << msg;
            return;
        }
        // 2. check if reach any limit
        ++file_num;
        file_size += file.size();
        file_meta_size += file.ByteSizeLong();
        if (file_num > 1 &&
            ((request->file_num_limit() > 0 && file_num > request->file_num_limit()) ||
             (request->file_size_limit() > 0 && file_size > request->file_size_limit()) ||
             (request->file_meta_size_limit() > 0 &&
              file_meta_size > request->file_meta_size_limit()))) {
            break;
        }
        // 3. put copy file kv
        CopyFilePB copy_file;
        copy_file.set_copy_id(request->copy_id());
        copy_file.set_group_id(request->group_id());
        std::string copy_file_val = copy_file.SerializeAsString();
        if (copy_file_val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        copy_files.emplace_back(std::move(file_key), std::move(copy_file_val));
        // 3. add file to copy job value
        copy_job.add_object_files()->CopyFrom(file);
        response->add_filtered_object_files()->CopyFrom(file);
    }

    if (file_num == 0) {
        return;
    }

    val = copy_job.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    // put copy job
    txn->put(key, val);
    LOG(INFO) << "put copy_job_key=" << hex(key);
    // put copy file
    for (const auto& [k, v] : copy_files) {
        txn->put(k, v);
        LOG(INFO) << "put copy_file_key=" << hex(k);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::finish_copy(google::protobuf::RpcController* controller,
                                  const FinishCopyRequest* request, FinishCopyResponse* response,
                                  ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(finish_copy);
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
    RPC_RATE_LIMIT(finish_copy)

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    err = txn->get(key, &val);
    LOG(INFO) << "get copy_job_key=" << hex(key);

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // not found
        code = MetaServiceCode::COPY_JOB_NOT_FOUND;
        ss << "copy job does not found";
        msg = ss.str();
        return;
    } else if (err != TxnErrorCode::TXN_OK) { // error
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get copy_job, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return;
    }

    CopyJobPB copy_job;
    if (!copy_job.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse CopyJobPB";
        return;
    }

    std::vector<std::string> copy_files;
    if (request->action() == FinishCopyRequest::COMMIT) {
        // 1. update copy job status from Loading to Finish
        copy_job.set_job_status(CopyJobPB::FINISH);
        if (request->has_finish_time_ms()) {
            copy_job.set_finish_time_ms(request->finish_time_ms());
        }
        val = copy_job.SerializeAsString();
        if (val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "put copy_job_key=" << hex(key);
    } else if (request->action() == FinishCopyRequest::ABORT ||
               request->action() == FinishCopyRequest::REMOVE) {
        // 1. remove copy job kv
        // 2. remove copy file kvs
        txn->remove(key);
        LOG(INFO) << (request->action() == FinishCopyRequest::ABORT ? "abort" : "remove")
                  << " copy_job_key=" << hex(key);
        for (const auto& file : copy_job.object_files()) {
            // copy file key
            CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                           file.relative_path(), file.etag()};
            std::string file_key;
            copy_file_key(file_key_info, &file_key);
            copy_files.emplace_back(std::move(file_key));
        }
        for (const auto& k : copy_files) {
            txn->remove(k);
            LOG(INFO) << "remove copy_file_key=" << hex(k);
        }
    } else {
        msg = "Unhandled action";
        code = MetaServiceCode::UNDEFINED_ERR;
        return;
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_copy_job(google::protobuf::RpcController* controller,
                                   const GetCopyJobRequest* request, GetCopyJobResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_copy_job);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    copy_job_key(key_info, &key);
    std::string val;
    err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // not found key
        return;
    } else if (err != TxnErrorCode::TXN_OK) { // error
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get copy job, err={}", err);
        LOG(WARNING) << msg << " err=" << err;
        return;
    }
    CopyJobPB copy_job;
    if (!copy_job.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse CopyJobPB";
        return;
    }
    response->mutable_copy_job()->CopyFrom(copy_job);
}

void MetaServiceImpl::get_copy_files(google::protobuf::RpcController* controller,
                                     const GetCopyFilesRequest* request,
                                     GetCopyFilesResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_copy_files);
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
    RPC_RATE_LIMIT(get_copy_files)

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    CopyJobKeyInfo key_info0 {instance_id, request->stage_id(), request->table_id(), "", 0};
    CopyJobKeyInfo key_info1 {instance_id, request->stage_id(), request->table_id() + 1, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;
    do {
        TxnErrorCode err = txn->get(key0, key1, &it);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get copy jobs, err={}", err);
            LOG(WARNING) << msg << " err=" << err;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            CopyJobPB copy_job;
            if (!copy_job.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse CopyJobPB";
                return;
            }
            // TODO check if job is timeout
            for (const auto& file : copy_job.object_files()) {
                response->add_object_files()->CopyFrom(file);
            }
        }
        key0.push_back('\x00');
    } while (it->more());
}

void MetaServiceImpl::filter_copy_files(google::protobuf::RpcController* controller,
                                        const FilterCopyFilesRequest* request,
                                        FilterCopyFilesResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(filter_copy_files);
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
    RPC_RATE_LIMIT(filter_copy_files)

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return;
    }

    std::vector<ObjectFilePB> filter_files;
    for (auto i = 0; i < request->object_files().size(); ++i) {
        auto& file = request->object_files().at(i);
        // 1. get copy file kv to check if file is loading or loaded
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        std::string file_val;
        TxnErrorCode err = txn->get(file_key, &file_val);
        if (err == TxnErrorCode::TXN_OK) { // found key
            continue;
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) { // error
            msg = fmt::format("failed to get copy file, err={}", err);
            LOG(WARNING) << msg << " err=" << err;
            return;
        } else {
            response->add_object_files()->CopyFrom(file);
        }
    }
}

void MetaServiceImpl::get_cluster_status(google::protobuf::RpcController* controller,
                                         const GetClusterStatusRequest* request,
                                         GetClusterStatusResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_cluster_status);
    if (request->instance_ids().empty() && request->cloud_unique_ids().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_ids or instance_ids must be given, instance_ids.size: " +
              std::to_string(request->instance_ids().size()) +
              " cloud_unique_ids.size: " + std::to_string(request->cloud_unique_ids().size());
        return;
    }

    std::vector<std::string> instance_ids;
    instance_ids.reserve(
            std::max(request->instance_ids().size(), request->cloud_unique_ids().size()));

    // priority use instance_ids
    if (!request->instance_ids().empty()) {
        std::for_each(request->instance_ids().begin(), request->instance_ids().end(),
                      [&](const auto& it) { instance_ids.emplace_back(it); });
    } else if (!request->cloud_unique_ids().empty()) {
        std::for_each(request->cloud_unique_ids().begin(), request->cloud_unique_ids().end(),
                      [&](const auto& it) {
                          std::string instance_id = get_instance_id(resource_mgr_, it);
                          if (instance_id.empty()) {
                              LOG(INFO) << "cant get instance_id from cloud_unique_id : " << it;
                              return;
                          }
                          instance_ids.emplace_back(instance_id);
                      });
    }

    if (instance_ids.empty()) {
        LOG(INFO) << "can't get valid instanceids";
        return;
    }
    bool has_filter = request->has_status();

    RPC_RATE_LIMIT(get_cluster_status)

    auto get_clusters_info = [this, &request, &response,
                              &has_filter](const std::string& instance_id) {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn err=" << err;
            return;
        }
        err = txn->get(key, &val);
        LOG(INFO) << "get instance_key=" << hex(key);

        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get instance, instance_id=" << instance_id << " err=" << err;
            return;
        }

        InstanceInfoPB instance;
        if (!instance.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse InstanceInfoPB";
            return;
        }
        GetClusterStatusResponse::GetClusterStatusResponseDetail detail;
        detail.set_instance_id(instance_id);
        for (auto& cluster : instance.clusters()) {
            if (cluster.type() != ClusterPB::COMPUTE) {
                continue;
            }
            ClusterPB pb;
            pb.set_cluster_name(cluster.cluster_name());
            pb.set_cluster_id(cluster.cluster_id());
            if (has_filter && request->status() != cluster.cluster_status()) {
                continue;
            }
            // for compatible
            if (cluster.has_cluster_status()) {
                pb.set_cluster_status(cluster.cluster_status());
            } else {
                pb.set_cluster_status(ClusterStatus::NORMAL);
            }
            detail.add_clusters()->CopyFrom(pb);
        }
        if (detail.clusters().size() == 0) {
            return;
        }
        response->add_details()->CopyFrom(detail);
    };

    std::for_each(instance_ids.begin(), instance_ids.end(), get_clusters_info);

    msg = proto_to_json(*response);
}

void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id) {
    LOG(INFO) << "begin notify_refresh_instance";
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn err=" << err;
        return;
    }
    std::string key = system_meta_service_registry_key();
    std::string val;
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get server registry"
                     << " err=" << err;
        return;
    }
    std::string self_endpoint =
            config::hostname.empty() ? get_local_ip(config::priority_networks) : config::hostname;
    self_endpoint = fmt::format("{}:{}", self_endpoint, config::brpc_listen_port);
    ServiceRegistryPB reg;
    reg.ParseFromString(val);

    brpc::ChannelOptions options;
    options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SHORT;

    static std::unordered_map<std::string, std::shared_ptr<MetaService_Stub>> stubs;
    static std::mutex mtx;

    std::vector<bthread_t> btids;
    btids.reserve(reg.items_size());
    for (int i = 0; i < reg.items_size(); ++i) {
        int ret = 0;
        auto& e = reg.items(i);
        std::string endpoint;
        if (e.has_host()) {
            endpoint = fmt::format("{}:{}", e.host(), e.port());
        } else {
            endpoint = fmt::format("{}:{}", e.ip(), e.port());
        }
        if (endpoint == self_endpoint) continue;

        // Prepare stub
        std::shared_ptr<MetaService_Stub> stub;
        do {
            std::lock_guard l(mtx);
            if (auto it = stubs.find(endpoint); it != stubs.end()) {
                stub = it->second;
                break;
            }
            auto channel = std::make_unique<brpc::Channel>();
            ret = channel->Init(endpoint.c_str(), &options);
            if (ret != 0) {
                LOG(WARNING) << "fail to init brpc channel, endpoint=" << endpoint;
                break;
            }
            stub = std::make_shared<MetaService_Stub>(channel.release(),
                                                      google::protobuf::Service::STUB_OWNS_CHANNEL);
        } while (false);
        if (ret != 0) continue;

        // Issue RPC
        auto f = new std::function<void()>([instance_id, stub, endpoint] {
            int num_try = 0;
            bool succ = false;
            while (num_try++ < 3) {
                brpc::Controller cntl;
                cntl.set_timeout_ms(3000);
                AlterInstanceRequest req;
                AlterInstanceResponse res;
                req.set_instance_id(instance_id);
                req.set_op(AlterInstanceRequest::REFRESH);
                stub->alter_instance(&cntl, &req, &res, nullptr);
                if (cntl.Failed()) {
                    LOG_WARNING("issue refresh instance rpc")
                            .tag("endpoint", endpoint)
                            .tag("num_try", num_try)
                            .tag("code", cntl.ErrorCode())
                            .tag("msg", cntl.ErrorText());
                } else {
                    succ = res.status().code() == MetaServiceCode::OK;
                    LOG(INFO) << (succ ? "succ" : "failed")
                              << " to issue refresh_instance rpc, num_try=" << num_try
                              << " endpoint=" << endpoint << " response=" << proto_to_json(res);
                    if (succ) return;
                }
                bthread_usleep(300000);
            }
            if (succ) return;
            LOG(WARNING) << "failed to refresh finally, it may left the system inconsistent,"
                         << " tired=" << num_try;
        });
        bthread_t bid;
        ret = bthread_start_background(&bid, nullptr, run_bthread_work, f);
        if (ret != 0) continue;
        btids.emplace_back(bid);
    } // for
    for (auto& i : btids) bthread_join(i, nullptr);
    LOG(INFO) << "finish notify_refresh_instance, num_items=" << reg.items_size();
}

} // namespace doris::cloud
