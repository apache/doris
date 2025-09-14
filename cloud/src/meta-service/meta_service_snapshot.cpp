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

#include <arpa/inet.h>
#include <brpc/controller.h>
#include <gen_cpp/cloud.pb.h>

#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"

namespace doris::cloud {

static bool is_valid_ip_address(const std::string& ip) {
    struct sockaddr_in sa;
    struct sockaddr_in6 sa6;
    return (inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr)) == 1) ||
           (inet_pton(AF_INET6, ip.c_str(), &(sa6.sin6_addr)) == 1);
}

void MetaServiceImpl::begin_snapshot(::google::protobuf::RpcController* controller,
                                     const BeginSnapshotRequest* request,
                                     BeginSnapshotResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_snapshot, get, put);

    // Basic parameter validation
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    // Validate timeout must be positive
    if (request->timeout_seconds() <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "timeout_seconds must be positive";
        return;
    }

    // Validate TTL must be positive
    if (request->ttl_seconds() <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "ttl_seconds must be positive";
        return;
    }

    // Validate snapshot label is not empty
    if (request->snapshot_label().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "snapshot_label cannot be empty";
        return;
    }

    // Validate request IP format if provided
    if (request->has_request_ip() && !request->request_ip().empty()) {
        if (!is_valid_ip_address(request->request_ip())) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "invalid request IP address format";
            return;
        }
    }

    // get instance id
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(begin_snapshot)

    // get instance pb
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

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

    // construct snapshot pb
    versioned::SnapshotFullKeyInfo snapshot_full_info_key {instance_id};
    std::string encoded_snapshot_full_key;
    versioned::snapshot_full_key(snapshot_full_info_key, &encoded_snapshot_full_key);

    SnapshotPB snapshot_pb;
    snapshot_pb.set_status(SNAPSHOT_PREPARE);
    snapshot_pb.set_type(SNAPSHOT_REFERENCE);
    snapshot_pb.set_timeout_seconds(request->timeout_seconds());
    snapshot_pb.set_instance_id(instance_id);
    if (instance.has_source_snapshot_id()) {
        snapshot_pb.set_snapshot_ancestor(instance.source_snapshot_id());
    }
    snapshot_pb.set_auto_(request->auto_snapshot());
    snapshot_pb.set_ttl_seconds(request->ttl_seconds());
    snapshot_pb.set_label(request->snapshot_label());

    std::string snapshot_full_info_val = snapshot_pb.SerializeAsString();
    if (snapshot_full_info_val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    versioned_put(txn.get(), encoded_snapshot_full_key, snapshot_full_info_val);
    LOG_INFO("put versioned snapshot full key info")
            .tag("encoded_snapshot_full_key", hex(encoded_snapshot_full_key))
            .tag("instance_id", instance_id);

    txn->enable_get_versionstamp();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg;
    }

    // get versionstamp
    std::string version_stamp;
    err = txn->get_versionstamp(&version_stamp);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to get versionstamp, err={}", err);
        LOG(WARNING) << msg;
    }

    response->set_image_url("/snapshot/" + version_stamp + "/");
    response->set_snapshot_id(version_stamp);
}

void MetaServiceImpl::commit_snapshot(::google::protobuf::RpcController* controller,
                                      const CommitSnapshotRequest* request,
                                      CommitSnapshotResponse* response,
                                      ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    controller->SetFailed("Method commit_snapshot() not implemented.");
}

void MetaServiceImpl::abort_snapshot(::google::protobuf::RpcController* controller,
                                     const AbortSnapshotRequest* request,
                                     AbortSnapshotResponse* response,
                                     ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    controller->SetFailed("Method abort_snapshot() not implemented.");
}

void MetaServiceImpl::list_snapshot(::google::protobuf::RpcController* controller,
                                    const ListSnapshotRequest* request,
                                    ListSnapshotResponse* response,
                                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    controller->SetFailed("Method list_snapshot() not implemented.");
}

void MetaServiceImpl::clone_instance(::google::protobuf::RpcController* controller,
                                     const CloneInstanceRequest* request,
                                     CloneInstanceResponse* response,
                                     ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    controller->SetFailed("Method clone_instance() not implemented.");
}

} // namespace doris::cloud
