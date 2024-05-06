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

#include "common/logging.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv_error.h"
#include "meta_service.h"

namespace doris::cloud {

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
                                 const IndexRequest* req) {
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
}

void MetaServiceImpl::prepare_index(::google::protobuf::RpcController* controller,
                                    const IndexRequest* request, IndexResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_index);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to create txn";
        return;
    }
    err = index_exists(txn.get(), instance_id, request);
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
    RPC_PREPROCESS(commit_index);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            err = index_exists(txn.get(), instance_id, request);
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
    }

    if (request->has_db_id() && request->has_is_new_table() && request->is_new_table()) {
        // init table version, for create and truncate table
        std::string key = table_version_key({instance_id, request->db_id(), request->table_id()});
        txn->atomic_add(key, 1);
        LOG_INFO("put table version").tag("key", hex(key));
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
    RPC_PREPROCESS(drop_index);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    std::string to_save_val;
    {
        RecycleIndexPB pb;
        pb.set_table_id(request->table_id());
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecycleIndexPB::DROPPED);
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
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
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

// Return TXN_OK if exists, TXN_KEY_NOT_FOUND if not exists, otherwise error
static TxnErrorCode partition_exists(Transaction* txn, const std::string& instance_id,
                                     const PartitionRequest* req) {
    auto tablet_key = meta_tablet_key(
            {instance_id, req->table_id(), req->index_ids(0), req->partition_ids(0), 0});
    auto tablet_key_end = meta_tablet_key(
            {instance_id, req->table_id(), req->index_ids(0), req->partition_ids(0), INT64_MAX});
    std::unique_ptr<RangeGetIterator> it;

    TxnErrorCode err = txn->get(tablet_key, tablet_key_end, &it, false, 1);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get kv").tag("err", err);
        return err;
    }
    return it->has_next() ? TxnErrorCode::TXN_OK : TxnErrorCode::TXN_KEY_NOT_FOUND;
}

void MetaServiceImpl::prepare_partition(::google::protobuf::RpcController* controller,
                                        const PartitionRequest* request,
                                        PartitionResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_partition);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }
    err = partition_exists(txn.get(), instance_id, request);
    // If index has existed, this might be a stale request
    if (err == TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to check index existence";
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
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
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
    RPC_PREPROCESS(commit_partition);
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

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            // Compatible with requests without `index_ids`
            if (!request->index_ids().empty()) {
                err = partition_exists(txn.get(), instance_id, request);
                // If partition has existed, this might be a duplicate request
                if (err == TxnErrorCode::TXN_OK) {
                    return; // Partition committed, OK
                }
                if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = "failed to check partition existence";
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
    }

    // update table versions
    if (request->has_db_id()) {
        std::string ver_key =
                table_version_key({instance_id, request->db_id(), request->table_id()});
        txn->atomic_add(ver_key, 1);
        LOG_INFO("update table version").tag("ver_key", hex(ver_key));
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
    RPC_PREPROCESS(drop_partition);
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

    std::unique_ptr<Transaction> txn;
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
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
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
        std::string ver_key =
                table_version_key({instance_id, request->db_id(), request->table_id()});
        txn->atomic_add(ver_key, 1);
        LOG_INFO("update table version").tag("ver_key", hex(ver_key));
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }
}

} // namespace doris::cloud
