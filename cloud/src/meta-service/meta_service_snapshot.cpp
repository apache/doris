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

namespace doris::cloud {

void MetaServiceImpl::begin_snapshot(::google::protobuf::RpcController* controller,
                                     const BeginSnapshotRequest* request,
                                     BeginSnapshotResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_snapshot, get, put, del);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(begin_snapshot);

    snapshot_manager_->begin_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::update_snapshot(::google::protobuf::RpcController* controller,
                                      const UpdateSnapshotRequest* request,
                                      UpdateSnapshotResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_snapshot, get, put, del);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(update_snapshot);

    snapshot_manager_->update_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::commit_snapshot(::google::protobuf::RpcController* controller,
                                      const CommitSnapshotRequest* request,
                                      CommitSnapshotResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_snapshot, get, put, del);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(commit_snapshot);

    snapshot_manager_->commit_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::abort_snapshot(::google::protobuf::RpcController* controller,
                                     const AbortSnapshotRequest* request,
                                     AbortSnapshotResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(abort_snapshot, get, put, del);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(abort_snapshot);

    snapshot_manager_->abort_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::list_snapshot(::google::protobuf::RpcController* controller,
                                    const ListSnapshotRequest* request,
                                    ListSnapshotResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(list_snapshot, get, put, del);

    // Prefer instance_id if provided, fallback to cloud_unique_id
    if (request->has_instance_id() && !request->instance_id().empty()) {
        instance_id = request->instance_id();
    } else if (request->has_cloud_unique_id() && !request->cloud_unique_id().empty()) {
        instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
        if (instance_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            return;
        }
    } else {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "either instance_id or cloud_unique_id must be provided";
        return;
    }
    RPC_RATE_LIMIT(list_snapshot);

    snapshot_manager_->list_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::clone_instance(::google::protobuf::RpcController* controller,
                                     const CloneInstanceRequest* request,
                                     CloneInstanceResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(clone_instance, get, put, del);
    RPC_RATE_LIMIT(clone_instance);

    snapshot_manager_->clone_instance(*request, response);
    code = response->status().code();
    msg = response->status().msg();
}

void MetaServiceImpl::drop_snapshot(::google::protobuf::RpcController* controller,
                                    const DropSnapshotRequest* request,
                                    DropSnapshotResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(drop_snapshot, get, put, del);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(clone_instance);

    snapshot_manager_->drop_snapshot(instance_id, *request, response);
    code = response->status().code();
    msg = response->status().msg();
}

} // namespace doris::cloud
