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
#include <unordered_map>

#include "common/defer.h"
#include "common/logging.h"
#include "common/stats.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/table_stream_metadata_reader.h"
#include "meta-store/blob_message.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/keys.h"
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

static TxnErrorCode check_recycle_key_exist(Transaction* txn, const std::string& key) {
    std::string val;
    return txn->get(key, &val);
}

static bool is_table_stream_versioned_write(MultiVersionStatus multi_version_status) {
    return multi_version_status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY ||
           multi_version_status == MultiVersionStatus::MULTI_VERSION_READ_WRITE;
}

static bool validate_table_stream_index_request(const IndexRequest* request, MetaServiceCode& code,
                                                std::string& msg) {
    if (request->index_ids_size() != 1 || !request->has_db_id() || !request->has_stream_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "table stream requires exactly one index_id, db_id and stream_db_id";
        return false;
    }
    if ((request->has_is_new_table() && request->is_new_table()) ||
        !request->partition_ids().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "table stream index must not create a table or physical partitions";
        return false;
    }
    return true;
}

static bool table_stream_recycle_index_matches(const RecycleIndexPB& recycle_index,
                                               const IndexRequest& request) {
    return recycle_index.state() == RecycleIndexPB::PREPARED &&
           recycle_index.object_type() == IndexObjectTypePB::TABLE_STREAM &&
           recycle_index.has_db_id() && recycle_index.db_id() == request.db_id() &&
           recycle_index.table_id() == request.table_id() && recycle_index.has_stream_db_id() &&
           recycle_index.stream_db_id() == request.stream_db_id();
}

static bool table_stream_index_matches(const IndexIndexPB& index, const IndexRequest& request) {
    return index.object_type() == IndexObjectTypePB::TABLE_STREAM && index.has_db_id() &&
           index.db_id() == request.db_id() && index.has_table_id() &&
           index.table_id() == request.table_id() && index.has_stream_db_id() &&
           index.stream_db_id() == request.stream_db_id();
}

static bool table_stream_recycle_index_matches(const RecycleIndexPB& recycle_index,
                                               const PartitionRequest& request) {
    return recycle_index.state() == RecycleIndexPB::PREPARED &&
           recycle_index.object_type() == IndexObjectTypePB::TABLE_STREAM &&
           recycle_index.has_db_id() && recycle_index.db_id() == request.db_id() &&
           recycle_index.table_id() == request.table_id() && recycle_index.has_stream_db_id();
}

static bool validate_table_stream_partition_request(const PartitionRequest* request,
                                                    MetaServiceCode& code, std::string& msg) {
    if (request->index_ids_size() != 1 || !request->has_db_id() ||
        request->table_stream_offsets_size() != request->partition_ids_size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "table stream partition commit requires one index_id, db_id and one offset per "
              "partition";
        return false;
    }
    if (!request->partition_versions().empty() ||
        (request->has_need_update_table_version() && request->need_update_table_version())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "table stream partition commit must not update physical partition or table version";
        return false;
    }

    std::unordered_map<int64_t, bool> partitions;
    partitions.reserve(request->partition_ids_size());
    for (int64_t partition_id : request->partition_ids()) {
        if (!partitions.emplace(partition_id, true).second) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "duplicate table stream partition_id";
            return false;
        }
    }

    std::unordered_map<int64_t, const TableStreamOffsetPB*> offsets;
    offsets.reserve(request->table_stream_offsets_size());
    for (const auto& offset : request->table_stream_offsets()) {
        if (!offset.has_partition_id() || !offset.has_state() || !offset.has_offset_tso() ||
            (offset.state() != TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING &&
             offset.state() != TABLE_STREAM_OFFSET_CONSUMED) ||
            !partitions.contains(offset.partition_id()) ||
            !offsets.emplace(offset.partition_id(), &offset).second) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "invalid or duplicate table stream offset";
            return false;
        }
    }
    return true;
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
    const bool is_table_stream = request->object_type() == IndexObjectTypePB::TABLE_STREAM;
    if (is_table_stream && !validate_table_stream_index_request(request, code, msg)) {
        return;
    }

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to create txn";
        return;
    }

    if (is_table_stream) {
        MultiVersionStatus multi_version_status;
        TableStreamReadResult status_result = read_table_stream_multi_version_status(
                txn.get(), instance_id, TableStreamReadIntent::CONFLICT, &multi_version_status);
        if (!status_result.ok()) {
            code = status_result.code;
            msg = std::move(status_result.message);
            return;
        }

        const int64_t stream_id = request->index_ids(0);
        std::string mapping_value;
        err = txn->get(table_stream_index_key({instance_id, stream_id}), &mapping_value);
        if (err == TxnErrorCode::TXN_OK) {
            IndexIndexPB index;
            if (!index.ParseFromString(mapping_value)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed table stream mapping";
                return;
            }
            if (!table_stream_index_matches(index, *request)) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "table stream prepare request does not match committed index";
                return;
            }
            code = MetaServiceCode::ALREADY_EXISTED;
            msg = "table stream index already existed";
            return;
        }
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to check table stream index existence, err={}", err);
            return;
        }
    } else {
        bool is_versioned_read = is_version_read_enabled(instance_id);
        CloneChainReader reader(instance_id, resource_mgr_.get());
        err = is_versioned_read ? reader.is_index_exists(txn.get(), request->index_ids(0))
                                : index_exists(txn.get(), instance_id, request);
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
    }

    RecycleIndexPB recycle_index;
    recycle_index.set_table_id(request->table_id());
    recycle_index.set_creation_time(::time(nullptr));
    recycle_index.set_expiration(request->expiration());
    recycle_index.set_state(RecycleIndexPB::PREPARED);
    if (is_table_stream) {
        recycle_index.set_db_id(request->db_id());
        recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        recycle_index.set_stream_db_id(request->stream_db_id());
    }
    std::string recycle_index_value;
    if (!recycle_index.SerializeToString(&recycle_index_value)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize recycle index";
        return;
    }

    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, recycle_index_value);
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
        if (is_table_stream && !table_stream_recycle_index_matches(pb, *request)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "table stream prepare request does not match existing PREPARED index";
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
    const bool is_table_stream = request->object_type() == IndexObjectTypePB::TABLE_STREAM;
    if (is_table_stream && !validate_table_stream_index_request(request, code, msg)) {
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

    bool is_versioned_read;
    bool is_versioned_write;
    if (is_table_stream) {
        MultiVersionStatus multi_version_status;
        TableStreamReadResult status_result = read_table_stream_multi_version_status(
                txn.get(), instance_id, TableStreamReadIntent::CONFLICT, &multi_version_status);
        if (!status_result.ok()) {
            code = status_result.code;
            msg = std::move(status_result.message);
            return;
        }
        is_versioned_read = multi_version_status == MultiVersionStatus::MULTI_VERSION_READ_WRITE;
        is_versioned_write = is_table_stream_versioned_write(multi_version_status);
    } else {
        is_versioned_read = is_version_read_enabled(instance_id);
        is_versioned_write = is_version_write_enabled(instance_id);
    }
    if (!request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "db_id is required for versioned write, please upgrade your FE version";
        return;
    }
    if (request->has_is_new_table() && request->is_new_table() && is_versioned_write) {
        txn->enable_get_versionstamp();
    }

    CloneChainReader reader(instance_id, resource_mgr_.get());
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            if (is_table_stream) {
                std::string mapping_value;
                err = txn->get(table_stream_index_key({instance_id, index_id}), &mapping_value);
                if (err == TxnErrorCode::TXN_OK) {
                    IndexIndexPB index;
                    if (!index.ParseFromString(mapping_value)) {
                        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                        msg = "malformed table stream mapping";
                        return;
                    }
                    if (!table_stream_index_matches(index, *request)) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        msg = "table stream commit request does not match committed index";
                        return;
                    }
                    std::string ignored_value;
                    err = txn->get(table_stream_inverted_key({instance_id, request->db_id(),
                                                              request->table_id(), index_id}),
                                   &ignored_value);
                    if (err == TxnErrorCode::TXN_OK) {
                        return;
                    }
                    code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                   ? MetaServiceCode::INVALID_ARGUMENT
                                   : cast_as<ErrCategory::READ>(err);
                    msg = "committed table stream is missing its inverted mapping";
                    return;
                }
            } else if (!is_versioned_read) {
                err = index_exists(txn.get(), instance_id, request);
            } else {
                err = reader.is_index_exists(txn.get(), index_id);
            }
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
        if (is_table_stream && !table_stream_recycle_index_matches(pb, *request)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "table stream commit request does not match PREPARED index";
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

        // Save the current and versioned index mappings.
        if (is_table_stream || is_versioned_write) {
            const int64_t db_id = request->db_id();
            const int64_t table_id = request->table_id();
            IndexIndexPB index_index_pb;
            index_index_pb.set_db_id(db_id);
            index_index_pb.set_table_id(table_id);
            if (is_table_stream) {
                index_index_pb.set_object_type(IndexObjectTypePB::TABLE_STREAM);
                index_index_pb.set_stream_db_id(request->stream_db_id());
            }
            std::string index_index_value;
            if (!index_index_pb.SerializeToString(&index_index_value)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize IndexIndexPB";
                LOG_WARNING(msg).tag("index_id", index_id);
                return;
            }

            if (is_table_stream) {
                txn->put(table_stream_index_key({instance_id, index_id}), index_index_value);
                txn->put(table_stream_inverted_key({instance_id, db_id, table_id, index_id}), "");
            }
            if (!is_versioned_write) {
                continue;
            }

            std::string index_meta_key = versioned::meta_index_key({instance_id, index_id});
            std::string index_index_key = versioned::index_index_key({instance_id, index_id});
            std::string index_inverted_key =
                    versioned::index_inverted_key({instance_id, db_id, table_id, index_id});
            versioned_put(txn.get(), index_meta_key, "");
            txn->put(index_inverted_key, "");
            txn->put(index_index_key, index_index_value);
            LOG_INFO("put versioned index keys")
                    .tag("index_id", index_id)
                    .tag("index_meta_key", hex(index_meta_key))
                    .tag("index_index_key", hex(index_index_key))
                    .tag("index_inverted_key", hex(index_inverted_key));

            commit_index_log.add_index_ids(index_id);
        }
    }

    // Save the partition meta keys
    if (is_versioned_write) {
        for (auto partition_id : request->partition_ids()) {
            int64_t db_id = request->db_id();
            int64_t table_id = request->table_id();
            std::string part_meta_key = versioned::meta_partition_key({instance_id, partition_id});
            std::string part_index_key =
                    versioned::partition_index_key({instance_id, partition_id});
            std::string part_inverted_index_key = versioned::partition_inverted_index_key(
                    {instance_id, db_id, table_id, partition_id});
            PartitionIndexPB part_index_pb;
            part_index_pb.set_db_id(db_id);
            part_index_pb.set_table_id(table_id);
            std::string part_index_value;
            if (!part_index_pb.SerializeToString(&part_index_value)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = fmt::format("failed to serialize PartitionIndexPB");
                LOG_WARNING(msg).tag("part_id", partition_id);
                return;
            }
            versioned_put(txn.get(), part_meta_key, "");
            txn->put(part_inverted_index_key, "");
            txn->put(part_index_key, part_index_value);

            LOG_INFO("put versioned partition index key")
                    .tag("partition_id", partition_id)
                    .tag("part_meta_key", hex(part_meta_key))
                    .tag("part_index_key", hex(part_index_key))
                    .tag("part_inverted_index_key", hex(part_inverted_index_key));

            commit_index_log.add_partition_ids(partition_id);
        }
    }

    if (request->has_is_new_table() && request->is_new_table()) {
        if (is_versioned_read) {
            // Read the table version, to build the operation log visible version range.
            Versionstamp table_version;
            err = reader.get_table_version(txn.get(), request->table_id(), &table_version, true);
            if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get table version, err={}", err);
                return;
            }
        } else {
            // set table version in response
            int64_t table_id = request->table_id();
            std::string ver_key = table_version_key({instance_id, request->db_id(), table_id});
            std::string ver_val;
            // snapshot read: the returned table version is only a hint for FE's version
            // cache; the real increment is done by update_table_version() via atomic_add.
            // A non-snapshot read would add ver_key to the read-conflict set and make
            // concurrent commits on the same table conflict (KV_TXN_CONFLICT).
            err = txn->get(ver_key, &ver_val, true);
            int64_t table_version = 0;
            if (err == TxnErrorCode::TXN_OK) {
                if (!txn->decode_atomic_int(ver_val, &table_version)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "malformed table version value, err=" << err
                       << " table_id=" << request->table_id();
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
            } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "failed to get table version, err=" << err << " table_id=" << table_id;
                msg = ss.str();
                return;
            }
            response->set_table_version(table_version + 1);
        }
        // init table version, for create and truncate table
        update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());
        commit_index_log.set_update_table_version(true);
    }

    if (commit_index_log.index_ids_size() > 0 && is_versioned_write) {
        std::string operation_log_key = versioned::log_key({instance_id});
        OperationLogPB operation_log;
        if (is_versioned_read) {
            operation_log.set_min_timestamp(reader.min_read_version());
        }
        operation_log.mutable_commit_index()->Swap(&commit_index_log);
        versioned::blob_put(txn.get(), operation_log_key, operation_log);
        LOG_INFO("put commit index operation log key")
                .tag("instance_id", instance_id)
                .tag("table_id", request->table_id())
                .tag("operation_log_key", hex(operation_log_key));
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }

    // set table version in response
    if (request->has_is_new_table() && request->is_new_table() && is_versioned_read) {
        Versionstamp vs;
        err = txn->get_versionstamp(&vs);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get kv txn versionstamp, table_id=" << request->table_id()
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        int64_t version = vs.version();
        response->set_table_version(version);
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
    const bool is_table_stream = request->object_type() == IndexObjectTypePB::TABLE_STREAM;
    if (is_table_stream && !validate_table_stream_index_request(request, code, msg)) {
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
        if (is_table_stream) {
            pb.set_object_type(IndexObjectTypePB::TABLE_STREAM);
            pb.set_stream_db_id(request->stream_db_id());
        }
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    bool is_versioned_write = is_version_write_enabled(instance_id);
    bool is_versioned_read = is_version_read_enabled(instance_id);

    if (is_versioned_write && !request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing db_id for versioned write, please upgrade your FE version";
        return;
    }

    DropIndexLogPB drop_index_log;
    drop_index_log.set_db_id(request->db_id());
    drop_index_log.set_table_id(request->table_id());
    drop_index_log.set_expiration(request->expiration());
    if (is_table_stream) {
        drop_index_log.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        drop_index_log.set_stream_db_id(request->stream_db_id());
    }

    CloneChainReader reader(instance_id, resource_mgr_.get());
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            if (is_versioned_write) {
                drop_index_log.add_index_ids(index_id);
                if (is_versioned_read) {
                    // Read the index version, to build the operation log visible version range.
                    err = reader.is_index_exists(txn.get(), index_id);
                    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                        code = cast_as<ErrCategory::READ>(err);
                        msg = fmt::format("failed to check index existence, err={}", err);
                        LOG_WARNING(msg);
                        return;
                    }
                }
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
            if (is_table_stream && !table_stream_recycle_index_matches(pb, *request)) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "table stream drop request does not match PREPARED index";
                return;
            }
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            break;
        case RecycleIndexPB::DROPPED:
        case RecycleIndexPB::RECYCLING:
            if (is_table_stream &&
                (pb.object_type() != IndexObjectTypePB::TABLE_STREAM || !pb.has_db_id() ||
                 pb.db_id() != request->db_id() || pb.table_id() != request->table_id() ||
                 !pb.has_stream_db_id() || pb.stream_db_id() != request->stream_db_id())) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "table stream drop request does not match existing recycle index";
                return;
            }
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
        OperationLogPB operation_log;
        if (is_versioned_read) {
            operation_log.set_min_timestamp(reader.min_read_version());
        }
        operation_log.mutable_drop_index()->Swap(&drop_index_log);
        versioned::blob_put(txn.get(), operation_log_key, operation_log);
        LOG(INFO) << "put drop index operation log"
                  << " instance_id=" << instance_id << " table_id=" << request->table_id()
                  << " index_ids=" << drop_index_log.index_ids_size();
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
    if (!is_versioned_read) {
        err = partition_exists(txn.get(), instance_id, request);
    } else {
        CloneChainReader reader(instance_id, resource_mgr_.get());
        err = reader.is_partition_exists(txn.get(), request->partition_ids(0));
    }
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
    if (request->object_type() == IndexObjectTypePB::TABLE_STREAM &&
        !validate_table_stream_partition_request(request, code, msg)) {
        return;
    }

    constexpr size_t BATCH_COMMIT_SIZE = 1000;
    for (size_t i = 0; i < request->partition_ids_size(); i += BATCH_COMMIT_SIZE) {
        std::vector<int64_t> partition_ids;
        size_t end = std::min(i + BATCH_COMMIT_SIZE, (size_t)request->partition_ids_size());
        for (size_t j = i; j < end; j++) {
            partition_ids.push_back(request->partition_ids(j));
        }
        commit_partition_internal(request, instance_id, partition_ids, response, code, msg, stats);
        if (code != MetaServiceCode::OK) {
            return;
        }
    }
}

void MetaServiceImpl::commit_partition_internal(const PartitionRequest* request,
                                                const std::string& instance_id,
                                                const std::vector<int64_t>& partition_ids,
                                                PartitionResponse* response, MetaServiceCode& code,
                                                std::string& msg, KVStats& stats) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    DORIS_CLOUD_DEFER {
        if (txn) {
            stats.get_bytes += txn->get_bytes();
            stats.put_bytes += txn->put_bytes();
            stats.del_bytes += txn->delete_bytes();
            stats.get_counter += txn->num_get_keys();
            stats.put_counter += txn->num_put_keys();
            stats.del_counter += txn->num_del_keys();
        }
    };

    if (request->object_type() == IndexObjectTypePB::TABLE_STREAM) {
        MultiVersionStatus multi_version_status;
        TableStreamReadResult status_result = read_table_stream_multi_version_status(
                txn.get(), instance_id, TableStreamReadIntent::CONFLICT, &multi_version_status);
        if (!status_result.ok()) {
            code = status_result.code;
            msg = std::move(status_result.message);
            return;
        }
        const bool is_versioned_write = is_table_stream_versioned_write(multi_version_status);
        const int64_t stream_id = request->index_ids(0);

        std::string recycle_index_val;
        err = txn->get(recycle_index_key({instance_id, stream_id}), &recycle_index_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::INVALID_ARGUMENT
                                                          : cast_as<ErrCategory::READ>(err);
            msg = "table stream PREPARED index is missing";
            return;
        }
        RecycleIndexPB recycle_index;
        if (!recycle_index.ParseFromString(recycle_index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed table stream recycle index value";
            return;
        }
        if (!table_stream_recycle_index_matches(recycle_index, *request)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "table stream partition commit does not match PREPARED index";
            return;
        }

        std::unordered_map<int64_t, const TableStreamOffsetPB*> offsets;
        offsets.reserve(request->table_stream_offsets_size());
        for (const auto& offset : request->table_stream_offsets()) {
            offsets.emplace(offset.partition_id(), &offset);
        }

        TableStreamIdentityPB identity;
        identity.set_base_db_id(request->db_id());
        identity.set_base_table_id(request->table_id());
        identity.set_stream_db_id(recycle_index.stream_db_id());
        identity.set_stream_id(stream_id);

        TableStreamPartitionSetPB binding;
        binding.mutable_identity()->CopyFrom(identity);
        for (int64_t partition_id : partition_ids) {
            binding.add_partition_ids(partition_id);
        }
        const std::vector<TableStreamPartitionSetPB> bindings {binding};

        CloneChainReader clone_chain_reader(instance_id, resource_mgr_.get());
        TableStreamMetadataReader metadata_reader(txn.get(), instance_id, multi_version_status,
                                                  &clone_chain_reader);
        TableStreamPartitionVersionMap source_versions;
        TableStreamReadResult read_result = metadata_reader.read_and_validate_partitions(
                bindings, TableStreamReadIntent::CONFLICT, &source_versions);
        if (!read_result.ok()) {
            code = read_result.code;
            msg = std::move(read_result.message);
            return;
        }

        TableStreamOffsetMap latest_offsets;
        read_result = metadata_reader.read_latest_offsets(bindings, TableStreamReadIntent::CONFLICT,
                                                          &latest_offsets);
        if (!read_result.ok()) {
            code = read_result.code;
            msg = std::move(read_result.message);
            return;
        }

        TableStreamOffsetMap versioned_offsets;
        if (is_versioned_write) {
            read_result = metadata_reader.read_local_versioned_offsets(
                    bindings, TableStreamReadIntent::CONFLICT, &versioned_offsets);
            if (!read_result.ok()) {
                code = read_result.code;
                msg = std::move(read_result.message);
                return;
            }
        }

        size_t num_writes = 0;
        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            auto offset_it = offsets.find(partition_id);
            DCHECK(offset_it != offsets.end());
            const TableStreamOffsetPB& offset = *offset_it->second;

            const auto& stream_versions = source_versions.at(stream_id);
            auto source_version_it = stream_versions.find(partition_id);
            DCHECK(source_version_it != stream_versions.end());
            const VersionPB& source_version = source_version_it->second;
            if (offset.offset_tso() > source_version.visible_tso()) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format("initial offset exceeds visible TSO for partition {}",
                                  partition_id);
                return;
            }

            std::string offset_val;
            if (!offset.SerializeToString(&offset_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                msg = "failed to serialize table stream offset";
                return;
            }

            TableStreamOffsetKeyInfo offset_key_info {
                    instance_id,         request->db_id(),
                    request->table_id(), recycle_index.stream_db_id(),
                    stream_id,           partition_id};
            const std::string latest_offset_key = table_stream_offset_key(offset_key_info);
            const TableStreamOffsetPB* existing_latest_offset = nullptr;
            const auto latest_stream_it = latest_offsets.find(stream_id);
            if (latest_stream_it != latest_offsets.end()) {
                const auto latest_offset_it = latest_stream_it->second.find(partition_id);
                if (latest_offset_it != latest_stream_it->second.end()) {
                    existing_latest_offset = &latest_offset_it->second;
                }
            }
            if (existing_latest_offset == nullptr) {
                txn->put(latest_offset_key, offset_val);
                ++num_writes;
            } else if (existing_latest_offset->SerializeAsString() != offset_val) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format("table stream offset {} was initialized differently",
                                  partition_id);
                return;
            }

            if (is_versioned_write) {
                const std::string versioned_offset_key =
                        versioned::table_stream_offset_key(offset_key_info);
                const TableStreamOffsetPB* existing_versioned_offset = nullptr;
                const auto versioned_stream_it = versioned_offsets.find(stream_id);
                if (versioned_stream_it != versioned_offsets.end()) {
                    const auto versioned_offset_it = versioned_stream_it->second.find(partition_id);
                    if (versioned_offset_it != versioned_stream_it->second.end()) {
                        existing_versioned_offset = &versioned_offset_it->second;
                    }
                }
                if (existing_versioned_offset == nullptr) {
                    versioned_put(txn.get(), versioned_offset_key, offset_val);
                    ++num_writes;
                } else if (existing_versioned_offset->SerializeAsString() != offset_val) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = fmt::format(
                            "versioned table stream offset {} was initialized differently",
                            partition_id);
                    return;
                }
            }
        }

        if (num_writes == 0) {
            return;
        }
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            msg = fmt::format("failed to commit table stream offsets: {}", err);
        }
        return;
    }

    CommitPartitionLogPB commit_partition_log;
    commit_partition_log.set_db_id(request->db_id());
    commit_partition_log.set_table_id(request->table_id());
    commit_partition_log.mutable_index_ids()->CopyFrom(request->index_ids());

    bool is_versioned_read = is_version_read_enabled(instance_id);
    bool is_versioned_write = is_version_write_enabled(instance_id);
    if (is_versioned_write && !request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing db_id for versioned write, please upgrade your FE version";
        return;
    }

    if (is_versioned_write) {
        txn->enable_get_versionstamp();
    }

    CloneChainReader reader(instance_id, resource_mgr_.get());
    size_t num_commit = 0;
    for (auto part_id : partition_ids) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            // Compatible with requests without `index_ids`
            if (!request->index_ids().empty()) {
                if (!is_versioned_read) {
                    err = partition_exists(txn.get(), instance_id, request);
                } else {
                    err = reader.is_partition_exists(txn.get(), part_id);
                }
                // If partition has existed, this might be a duplicate request
                if (err == TxnErrorCode::TXN_OK) {
                    // Partition committed, OK
                    continue;
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
        num_commit += 1;

        // Save the partition meta/index keys
        if (is_versioned_write) {
            int64_t db_id = request->db_id();
            int64_t table_id = request->table_id();
            std::string part_meta_key = versioned::meta_partition_key({instance_id, part_id});
            std::string part_index_key = versioned::partition_index_key({instance_id, part_id});
            std::string part_inverted_index_key = versioned::partition_inverted_index_key(
                    {instance_id, db_id, table_id, part_id});
            PartitionIndexPB part_index_pb;
            part_index_pb.set_db_id(db_id);
            part_index_pb.set_table_id(table_id);
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
            LOG_INFO("put versioned partition index key")
                    .tag("partition_id", part_id)
                    .tag("part_meta_key", hex(part_meta_key))
                    .tag("part_index_key", hex(part_index_key))
                    .tag("part_inverted_index_key", hex(part_inverted_index_key));

            commit_partition_log.add_partition_ids(part_id);
        }
    }

    if (num_commit == 0) {
        // All partitions have been committed, OK
        return;
    }

    // update table versions
    if (is_versioned_read) {
        // Read the table version, to build the operation log visible version range.
        Versionstamp table_version;
        err = reader.get_table_version(txn.get(), request->table_id(), &table_version, true);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get table version, err={}", err);
            return;
        }
    } else {
        // set table version in response
        std::string ver_key =
                table_version_key({instance_id, request->db_id(), request->table_id()});
        std::string ver_val;
        // snapshot read: the returned table version is only a hint for FE's version
        // cache; the real increment is done by update_table_version() via atomic_add.
        // A non-snapshot read would add ver_key to the read-conflict set and make
        // concurrent commits on the same table conflict (KV_TXN_CONFLICT).
        err = txn->get(ver_key, &ver_val, true);
        int64_t table_version = 0;
        if (err == TxnErrorCode::TXN_OK) {
            if (!txn->decode_atomic_int(ver_val, &table_version)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                std::stringstream ss;
                ss << "malformed table version value, err=" << err
                   << " table_id=" << request->table_id();
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            std::stringstream ss;
            ss << "failed to get table version, err=" << err << " table_id=" << request->table_id();
            msg = ss.str();
            return;
        }
        response->set_table_version(table_version + 1);
    }
    update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());

    if (commit_partition_log.partition_ids_size() > 0 && is_version_write_enabled(instance_id)) {
        std::string operation_log_key = versioned::log_key({instance_id});
        OperationLogPB operation_log;
        if (is_versioned_read) {
            operation_log.set_min_timestamp(reader.min_read_version());
        }
        operation_log.mutable_commit_partition()->Swap(&commit_partition_log);
        versioned::blob_put(txn.get(), operation_log_key, operation_log);
        LOG_INFO("put commit partition operation log key")
                .tag("instance_id", instance_id)
                .tag("table_id", request->table_id())
                .tag("num_partitions", operation_log.commit_partition().partition_ids_size())
                .tag("operation_log_key", hex(operation_log_key));
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }

    // set table version in response
    if (is_versioned_read) {
        Versionstamp vs;
        err = txn->get_versionstamp(&vs);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            std::stringstream ss;
            ss << "failed to get kv txn versionstamp, table_id=" << request->table_id()
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        int64_t version = vs.version();
        response->set_table_version(version);
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

    if (!request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing db_id for drop_partition";
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
    bool is_versioned_read = is_version_read_enabled(instance_id);
    DropPartitionLogPB drop_partition_log;
    drop_partition_log.set_db_id(request->db_id());
    drop_partition_log.set_table_id(request->table_id());
    drop_partition_log.mutable_index_ids()->CopyFrom(request->index_ids());
    drop_partition_log.set_expired_at_s(request->expiration());
    if (is_versioned_write) {
        txn->enable_get_versionstamp();
    }

    CloneChainReader reader(instance_id, resource_mgr_.get());
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN
            if (is_versioned_write) {
                drop_partition_log.add_partition_ids(part_id);
                if (is_versioned_read) {
                    // Read the partition version, to build the operation log visible version range.
                    err = reader.is_partition_exists(txn.get(), part_id);
                    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                        code = cast_as<ErrCategory::READ>(err);
                        msg = fmt::format("failed to check partition existence, err={}", err);
                        LOG_WARNING(msg);
                        return;
                    }
                }
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
    if (request->has_need_update_table_version() && request->need_update_table_version()) {
        if (is_versioned_read) {
            // Read the table version, to build the operation log visible version range.
            Versionstamp table_version;
            err = reader.get_table_version(txn.get(), request->table_id(), &table_version, true);
            if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get table version, err={}", err);
                return;
            }
        } else {
            // set table version in response
            std::string ver_key =
                    table_version_key({instance_id, request->db_id(), request->table_id()});
            std::string ver_val;
            // snapshot read: the returned table version is only a hint for FE's version
            // cache; the real increment is done by update_table_version() via atomic_add.
            // A non-snapshot read would add ver_key to the read-conflict set and make
            // concurrent commits on the same table conflict (KV_TXN_CONFLICT).
            err = txn->get(ver_key, &ver_val, true);
            int64_t table_version = 0;
            if (err == TxnErrorCode::TXN_OK) {
                if (!txn->decode_atomic_int(ver_val, &table_version)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    std::stringstream ss;
                    ss << "malformed table version value, err=" << err
                       << " table_id=" << request->table_id();
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
            } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                std::stringstream ss;
                ss << "failed to get table version, err=" << err
                   << " table_id=" << request->table_id();
                msg = ss.str();
                return;
            }
            response->set_table_version(table_version + 1);
        }
        update_table_version(txn.get(), instance_id, request->db_id(), request->table_id());
        drop_partition_log.set_update_table_version(true);
    }

    if ((drop_partition_log.update_table_version() ||
         drop_partition_log.partition_ids_size() > 0) &&
        is_versioned_write) {
        std::string operation_log_key = versioned::log_key({instance_id});
        OperationLogPB operation_log;
        if (is_versioned_read) {
            operation_log.set_min_timestamp(reader.min_read_version());
        }
        operation_log.mutable_drop_partition()->Swap(&drop_partition_log);
        versioned::blob_put(txn.get(), operation_log_key, operation_log);
        LOG(INFO) << "put drop partition operation log"
                  << " instance_id=" << instance_id << " table_id=" << request->table_id()
                  << " partition_ids=" << drop_partition_log.partition_ids_size();
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit txn: {}", err);
        return;
    }

    // set table version in response
    if (request->has_need_update_table_version() && request->need_update_table_version() &&
        is_versioned_read) {
        Versionstamp vs;
        err = txn->get_versionstamp(&vs);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            std::stringstream ss;
            ss << "failed to get kv txn versionstamp, table_id=" << request->table_id()
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        int64_t version = vs.version();
        response->set_table_version(version);
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
