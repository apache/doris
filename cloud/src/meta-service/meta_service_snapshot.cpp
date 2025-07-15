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

#include <gen_cpp/cloud.pb.h>

#include "meta-service/meta_service.h"

namespace doris::cloud {

void MetaServiceImpl::begin_snapshot(::google::protobuf::RpcController* controller,
                                     const BeginSnapshotRequest* request,
                                     BeginSnapshotResponse* response,
                                     ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    controller->SetFailed("Method begin_snapshot() not implemented.");
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
