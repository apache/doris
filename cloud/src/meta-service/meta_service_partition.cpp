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

#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include <chrono>
#include <memory>

#include "common/logging.h"
#include "common/stats.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "meta_service.h"

namespace doris::cloud {
using check_create_table_type = std::function<const std::tuple<
        const ::google::protobuf::RepeatedField<int64_t>, std::string,
        std::function<std::string(std::string, int64_t)>>(const CheckKVRequest* request)>;
// ATTN: xxx_id MUST NOT be reused
//
//              UNKNOWN
//                 |
//      +----------+---------+
//      |                    |
// (prepare_xxx)         (drop_xxx)
//      |                    |
//      v                    v
//   PREPARED--(drop_xxx)-->DROPPED
//      |                    |
//      |----------+---------+
//      |          |
//      |  (begin_recycle_xxx)
//      |          |
// (commit_xxx)    v
//      |      RECYCLING              RECYCLING --(drop_xxx)-> RECYCLING
//      |          |
//      |  (finish_recycle_xxx)       UNKNOWN --(commit_xxx)-> UNKNOWN
//      |          |                           if xxx exists
//      +----------+
//                 |
//                 v
//              UNKNOWN

// Return TXN_OK if exists, TXN_KEY_NOT_FOUND if not exists, otherwise error
static TxnErrorCode index_exists(Transaction* txn, const std::string& instance_id,
                                 bool is_versioned_read, const IndexRequest* req) {
    if (!is_versioned_read) {
        auto tablet_key = meta_tablet_key({instance_id, req->table_id(), req->index_ids(0), 0, 0});
        auto tablet_key_end =
                meta_tablet_key({instance_id, req->table_id(), req->index_ids(0), INT64_MAX, 0});
        std::unique_ptr<RangeGetIterator> it;

        TxnErrorCode err = txn->get(tablet_key, tablet_key_end, &it, false, 1);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get kv").tag("err", err);
            return err;
        }
        return it->has_next() ? TxnErrorCode::TXN_OK : TxnErrorCode::TXN_KEY_NOT_FOUND;
    } else {
        MetaReader reader(instance_id);
        TxnErrorCode err = reader.get_index_index(txn, req->index_ids(0), nullptr);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("failed to get index index key").tag("err", err);
            return err;
        }
        return err;
    }
}

static TxnErrorCode check_recycle_key_exist(Transaction* txn, const std::string& key) {
    std::string val;
    return txn->get(key, &val);
}

void MetaServiceImpl::prepare_index(::google::protobuf::RpcController* controller,
                                    const IndexRequest* request, IndexResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_index, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    AnnotateTag tag_instance_id("instance_id", instance_id);

    RPC_RATE_LIMIT(prepare_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to create txn";
        return;
    }
    bool is_versioned_read = is_version_read_enabled(instance_id);
    err = index_exists(txn.get(), instance_id, is_versioned_read, request);
    // If index has existed, this might be a stale request
    if (err == TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check index existence, err={}", err);
        return;
    }

    std::string to_save_val;
    {
        RecycleIndexPB pb;
        pb.set_table_id(request->table_id());
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecycleIndexPB::PREPARED);
        pb.SerializeToString(&to_save_val);
    }
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        if (pb.state() != RecycleIndexPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
        // else, duplicate request, OK
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

void MetaServiceImpl::commit_index(::google::protobuf::RpcController* controller,
                                   const IndexRequest* request, IndexResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_index, get, put, del);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(commit_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    CommitIndexLogPB commit_index_log;
    commit_index_log.set_db_id(request->db_id());
    commit_index_log.set_table_id(request->table_id());

    bool is_versioned_read = is_version_read_enabled(instance_id);
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            err = index_exists(txn.get(), instance_id, is_versioned_read, request);
            // If index has existed, this might be a duplicate request
            if (err == TxnErrorCode::TXN_OK) {
                return; // Index committed, OK
            }
            if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                msg = "failed to check index existence";
                return;
            }
            // Index recycled
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "index has been recycled";
            return;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        if (pb.state() != RecycleIndexPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
        LOG_INFO("remove recycle index").tag("key", hex(key));
        txn->remove(key);

        // Save the index meta/index keys
        if (request->has_db_id() && is_version_write_enabled(instance_id)) {
            int64_t db_id = request->db_id();
            int64_t table_id = request->table_id();
            std::string index_meta_key = versioned::meta_index_key({instance_id, index_id});
            std::string index_index_key = versioned::index_index_key({instance_id, index_id});
            std::string index_inverted_key =
                    versioned::index_inverted_key({instance_id, db_id, table_id, index_id});
            IndexIndexPB index_index_pb;
            index_index_pb.set_db_id(db_id);
            index_index_pb.set_table_id(table_id);
            LOG(INFO) << index_index_pb.DebugString();
            std::string index_index_value;
            if (!index_index_pb.SerializeToString(&index_index_value)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = fmt::format("failed to serialize IndexIndexPB");
                LOG_WARNING(msg).tag("index_id", index_id);
                return;
            }
            versioned_put(txn.get(), index_meta_key, "");
            txn->put(index_inverted_key, "");
            txn->put(index_index_key, index_index_value);

            commit_index_log.add_index_ids(index_id);
        }
    }

    if (request->has_db_id() && request->has_is_new_table() && request->is_new_table()) {
        // init table version, for create and truncate table
        update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());
        commit_index_log.set_update_table_version(true);
    }

    if (commit_index_log.index_ids_size() > 0 && is_version_write_enabled(instance_id)) {
        std::string operation_log_key = versioned::log_key({instance_id});
        std::string operation_log_value;
        OperationLogPB operation_log;
        operation_log.mutable_commit_index()->Swap(&commit_index_log);
        if (!operation_log.SerializeToString(&operation_log_value)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize OperationLogPB: {}", hex(operation_log_key));
            LOG_WARNING(msg).tag("instance_id", instance_id).tag("table_id", request->table_id());
            return;
        }
        versioned_put(txn.get(), operation_log_key, operation_log_value);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

void MetaServiceImpl::drop_index(::google::protobuf::RpcController* controller,
                                 const IndexRequest* request, IndexResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(drop_index, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(drop_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    std::string to_save_val;
    {
        RecycleIndexPB pb;
        pb.set_db_id(request->db_id());
        pb.set_table_id(request->table_id());
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecycleIndexPB::DROPPED);
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    bool is_versioned_write = is_version_write_enabled(instance_id);
    DropIndexLogPB drop_index_log;
    drop_index_log.set_db_id(request->db_id());
    drop_index_log.set_table_id(request->table_id());
    drop_index_log.set_expiration(request->expiration());

    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            if (is_versioned_write) {
                drop_index_log.add_index_ids(index_id);
            } else {
                LOG_INFO("put recycle index").tag("key", hex(key));
                txn->put(key, to_save_val);
            }
            need_commit = true;
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        switch (pb.state()) {
        case RecycleIndexPB::PREPARED:
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            break;
        case RecycleIndexPB::DROPPED:
        case RecycleIndexPB::RECYCLING:
            break;
        default:
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
    }
    if (!need_commit) return;

    if (drop_index_log.index_ids_size() > 0 && is_versioned_write) {
        std::string operation_log_key = versioned::log_key({instance_id});
        std::string operation_log_value;
        OperationLogPB operation_log;
        operation_log.mutable_drop_index()->Swap(&drop_index_log);
        if (!operation_log.SerializeToString(&operation_log_value)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize OperationLogPB: {}", hex(operation_log_key));
            LOG_WARNING(msg).tag("instance_id", instance_id).tag("table_id", request->table_id());
            return;
        }
        versioned_put(txn.get(), operation_log_key, operation_log_value);
        LOG(INFO) << "put drop index operation log"
                  << " instance_id=" << instance_id << " table_id=" << request->table_id()
                  << " index_ids=" << drop_index_log.index_ids_size()
                  << " log_size=" << operation_log_value.size();
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

// Return TXN_OK if exists, TXN_KEY_NOT_FOUND if not exists, otherwise error
static TxnErrorCode partition_exists(Transaction* txn, const std::string& instance_id,
                                     bool is_versioned_read, const PartitionRequest* req) {
    if (!is_versioned_read) {
        auto tablet_key = meta_tablet_key(
                {instance_id, req->table_id(), req->index_ids(0), req->partition_ids(0), 0});
        auto tablet_key_end = meta_tablet_key({instance_id, req->table_id(), req->index_ids(0),
                                               req->partition_ids(0), INT64_MAX});
        std::unique_ptr<RangeGetIterator> it;

        TxnErrorCode err = txn->get(tablet_key, tablet_key_end, &it, false, 1);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get kv").tag("err", err);
            return err;
        }
        return it->has_next() ? TxnErrorCode::TXN_OK : TxnErrorCode::TXN_KEY_NOT_FOUND;
    } else {
        MetaReader reader(instance_id);
        TxnErrorCode err = reader.get_partition_index(txn, req->partition_ids(0), nullptr);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("failed to get partition index key").tag("err", err);
            return err;
        }
        return err;
    }
}

void MetaServiceImpl::prepare_partition(::google::protobuf::RpcController* controller,
                                        const PartitionRequest* request,
                                        PartitionResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_partition, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    AnnotateTag tag_instance_id("instance_id", instance_id);

    RPC_RATE_LIMIT(prepare_partition)

    if (request->partition_ids().empty() || request->index_ids().empty() ||
        !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    if (request->partition_versions_size() > 0 &&
        (request->partition_versions_size() != request->partition_ids_size())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "size is not equal, partition_versions size=" +
              std::to_string(request->partition_ids_size()) +
              " partition_ids size=" + std::to_string(request->partition_ids_size());
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }
    bool is_versioned_read = is_version_read_enabled(instance_id);
    err = partition_exists(txn.get(), instance_id, is_versioned_read, request);
    // If index has existed, this might be a stale request
    if (err == TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check index existence, err={}", err);
        return;
    }

    std::string to_save_val;
    {
        RecyclePartitionPB pb;
        if (request->db_id() > 0) pb.set_db_id(request->db_id());
        pb.set_table_id(request->table_id());
        *pb.mutable_index_id() = request->index_ids();
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecyclePartitionPB::PREPARED);
        pb.SerializeToString(&to_save_val);
    }
    for (int i = 0; i < request->partition_ids_size(); i++) {
        auto key = recycle_partition_key({instance_id, request->partition_ids(i)});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
            // save partition version
            if (request->partition_versions_size() > 0 && request->partition_versions(i) > 1) {
                int64_t version_update_time_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
                std::string ver_key;
                std::string ver_val;
                partition_version_key({instance_id, request->db_id(), request->table_id(),
                                       request->partition_ids(i)},
                                      &ver_key);
                VersionPB version_pb;
                version_pb.set_version(request->partition_versions(i));
                version_pb.set_update_time_ms(version_update_time_ms);
                if (!version_pb.SerializeToString(&ver_val)) {
                    code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                    ss << "failed to serialize version_pb when saving.";
                    msg = ss.str();
                    return;
                }
                txn->put(ver_key, ver_val);
                LOG_INFO("put partition_version_key")
                        .tag("key", hex(ver_key))
                        .tag("partition_id", request->partition_ids(i))
                        .tag("version", request->partition_versions(i));
            }
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", request->partition_ids(i));
            return;
        }
        if (pb.state() != RecyclePartitionPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
        // else, duplicate request, OK
    }
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

void MetaServiceImpl::commit_partition(::google::protobuf::RpcController* controller,
                                       const PartitionRequest* request, PartitionResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_partition, get, put, del);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(commit_partition)

    if (request->partition_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    CommitPartitionLogPB commit_partition_log;
    commit_partition_log.set_db_id(request->db_id());
    commit_partition_log.set_table_id(request->table_id());
    commit_partition_log.mutable_index_ids()->CopyFrom(request->index_ids());

    bool is_versioned_read = is_version_read_enabled(instance_id);
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            // Compatible with requests without `index_ids`
            if (!request->index_ids().empty()) {
                err = partition_exists(txn.get(), instance_id, is_versioned_read, request);
                // If partition has existed, this might be a duplicate request
                if (err == TxnErrorCode::TXN_OK) {
                    return; // Partition committed, OK
                }
                if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format("failed to check partition existence, err={}", err);
                    return;
                }
            }
            // Index recycled
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "partition has been recycled";
            return;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", part_id);
            return;
        }
        if (pb.state() != RecyclePartitionPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle partition state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
        LOG_INFO("remove recycle partition").tag("key", hex(key));
        txn->remove(key);

        // Save the partition meta/index keys
        if (request->has_db_id() && is_version_write_enabled(instance_id)) {
            int64_t db_id = request->db_id();
            int64_t table_id = request->table_id();
            std::string part_meta_key = versioned::meta_partition_key({instance_id, part_id});
            std::string part_index_key = versioned::partition_index_key({instance_id, part_id});
            std::string part_inverted_index_key = versioned::partition_inverted_index_key(
                    {instance_id, db_id, table_id, part_id});
            PartitionIndexPB part_index_pb;
            part_index_pb.set_db_id(db_id);
            part_index_pb.set_table_id(table_id);
            LOG(INFO) << part_index_pb.DebugString();
            std::string part_index_value;
            if (!part_index_pb.SerializeToString(&part_index_value)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = fmt::format("failed to serialize PartitionIndexPB");
                LOG_WARNING(msg).tag("part_id", part_id);
                return;
            }
            versioned_put(txn.get(), part_meta_key, "");
            txn->put(part_inverted_index_key, "");
            txn->put(part_index_key, part_index_value);

            commit_partition_log.add_partition_ids(part_id);
        }
    }

    // update table versions
    if (request->has_db_id()) {
        update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());
    }

    if (commit_partition_log.partition_ids_size() > 0 && is_version_write_enabled(instance_id)) {
        std::string operation_log_key = versioned::log_key({instance_id});
        std::string operation_log_value;
        OperationLogPB operation_log;
        operation_log.mutable_commit_partition()->Swap(&commit_partition_log);
        if (!operation_log.SerializeToString(&operation_log_value)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize OperationLogPB: {}", hex(operation_log_key));
            LOG_WARNING(msg).tag("instance_id", instance_id).tag("table_id", request->table_id());
            return;
        }
        versioned_put(txn.get(), operation_log_key, operation_log_value);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

void MetaServiceImpl::drop_partition(::google::protobuf::RpcController* controller,
                                     const PartitionRequest* request, PartitionResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(drop_partition, get, put);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(drop_partition)

    if (request->partition_ids().empty() || request->index_ids().empty() ||
        !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to create txn";
        return;
    }
    std::string to_save_val;
    {
        RecyclePartitionPB pb;
        if (request->db_id() > 0) pb.set_db_id(request->db_id());
        pb.set_table_id(request->table_id());
        *pb.mutable_index_id() = request->index_ids();
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecyclePartitionPB::DROPPED);
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    bool is_versioned_write = is_version_write_enabled(instance_id);
    DropPartitionLogPB drop_partition_log;
    drop_partition_log.set_db_id(request->db_id());
    drop_partition_log.set_table_id(request->table_id());
    drop_partition_log.mutable_index_ids()->CopyFrom(request->index_ids());
    drop_partition_log.set_expiration(request->expiration());

    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            if (is_versioned_write) {
                drop_partition_log.add_partition_ids(part_id);
            } else {
                LOG_INFO("put recycle partition").tag("key", hex(key));
                txn->put(key, to_save_val);
            }
            need_commit = true;
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get kv, err={}", err);
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", part_id);
            return;
        }
        switch (pb.state()) {
        case RecyclePartitionPB::PREPARED:
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            break;
        case RecyclePartitionPB::DROPPED:
        case RecyclePartitionPB::RECYCLING:
            break;
        default:
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle partition state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
    }
    if (!need_commit) return;

    // Update table version only when deleting non-empty partitions
    if (request->has_db_id() && request->has_need_update_table_version() &&
        request->need_update_table_version()) {
        update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());
        drop_partition_log.set_update_table_version(true);
    }

    if ((drop_partition_log.update_table_version() ||
         drop_partition_log.partition_ids_size() > 0) &&
        is_versioned_write) {
        std::string operation_log_key = versioned::log_key({instance_id});
        std::string operation_log_value;
        OperationLogPB operation_log;
        operation_log.mutable_drop_partition()->Swap(&drop_partition_log);
        if (!operation_log.SerializeToString(&operation_log_value)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = fmt::format("failed to serialize OperationLogPB: {}", hex(operation_log_key));
            LOG_WARNING(msg).tag("instance_id", instance_id).tag("table_id", request->table_id());
            return;
        }
        versioned_put(txn.get(), operation_log_key, operation_log_value);
        LOG(INFO) << "put drop partition operation log"
                  << " instance_id=" << instance_id << " table_id=" << request->table_id()
                  << " partition_ids=" << drop_partition_log.partition_ids_size()
                  << " log_size=" << operation_log_value.size();
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

void check_create_table(std::string instance_id, std::shared_ptr<TxnKv> txn_kv,
                        const CheckKVRequest* request, CheckKVResponse* response,
                        MetaServiceCode* code, std::string* msg, KVStats& stats,
                        check_create_table_type get_check_info) {
    StopWatch watch;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        *code = cast_as<ErrCategory::READ>(err);
        *msg = "failed to create txn";
        return;
    }
    DORIS_CLOUD_DEFER {
        if (txn == nullptr) return;
        stats.get_bytes += txn->get_bytes();
        stats.get_counter += txn->num_get_keys();
    };
    auto& [keys, hint, key_func] = get_check_info(request);
    if (keys.empty()) {
        *code = MetaServiceCode::INVALID_ARGUMENT;
        *msg = "empty keys";
        return;
    }

    for (int i = 0; i < keys.size();) {
        auto key = key_func(instance_id, keys.Get(i));
        err = check_recycle_key_exist(txn.get(), key);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            i++;
            continue;
        } else if (err == TxnErrorCode::TXN_OK) {
            // find not match, prepare commit
            *code = MetaServiceCode::UNDEFINED_ERR;
            *msg = "prepare and commit rpc not match, recycle key remained";
            return;
        } else if (err == TxnErrorCode::TXN_TOO_OLD) {
            stats.get_bytes += txn->get_bytes();
            stats.get_counter += txn->num_get_keys();
            //  separate it to several txn for rubustness
            txn.reset();
            TxnErrorCode err = txn_kv->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                *code = cast_as<ErrCategory::READ>(err);
                *msg = "failed to create txn in cycle";
                return;
            }
            LOG_INFO("meet txn too long err, gen a new txn, and retry, size={} idx={}", keys.size(),
                     i);
            bthread_usleep(50);
            continue;
        } else {
            // err != TXN_OK, fdb read err
            *code = cast_as<ErrCategory::READ>(err);
            *msg = fmt::format("ms read key error: {}", err);
            return;
        }
    }
    LOG_INFO("check {} success key.size={}, cost(us)={}", hint, keys.size(), watch.elapsed_us());
}

void MetaServiceImpl::check_kv(::google::protobuf::RpcController* controller,
                               const CheckKVRequest* request, CheckKVResponse* response,
                               ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(check_kv, get);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    if (!request->has_op()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "op not given";
        return;
    }
    if (!request->has_check_keys()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty check keys";
        return;
    }
    RPC_RATE_LIMIT(check_kv);
    switch (request->op()) {
    case CheckKVRequest::CREATE_INDEX_AFTER_FE_COMMIT: {
        check_create_table(instance_id, txn_kv_, request, response, &code, &msg, stats,
                           [](const CheckKVRequest* request) {
                               return std::make_tuple(
                                       request->check_keys().index_ids(), "index",
                                       [](std::string instance_id, int64_t id) {
                                           return recycle_index_key({std::move(instance_id), id});
                                       });
                           });
        break;
    }
    case CheckKVRequest::CREATE_PARTITION_AFTER_FE_COMMIT: {
        check_create_table(
                instance_id, txn_kv_, request, response, &code, &msg, stats,
                [](const CheckKVRequest* request) {
                    return std::make_tuple(
                            request->check_keys().partition_ids(), "partition",
                            [](std::string instance_id, int64_t id) {
                                return recycle_partition_key({std::move(instance_id), id});
                            });
                });
        break;
    }
    default:
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "not support op";
        return;
    };
}

} // namespace doris::cloud
