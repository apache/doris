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

#pragma once

#include "service/internal_service.h"

namespace doris {

class CloudStorageEngine;

class CloudInternalServiceImpl final : public PInternalService {
public:
    CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env);

    ~CloudInternalServiceImpl() override;

    void alter_vault_sync(google::protobuf::RpcController* controller,
                          const doris::PAlterVaultSyncRequest* request,
                          PAlterVaultSyncResponse* response,
                          google::protobuf::Closure* done) override;

    // Get messages (filename, offset, size) about the tablet data in cache
    void get_file_cache_meta_by_tablet_id(google::protobuf::RpcController* controller,
                                          const PGetFileCacheMetaRequest* request,
                                          PGetFileCacheMetaResponse* response,
                                          google::protobuf::Closure* done) override;

    void warm_up_rowset(google::protobuf::RpcController* controller,
                        const PWarmUpRowsetRequest* request, PWarmUpRowsetResponse* response,
                        google::protobuf::Closure* done) override;

    void recycle_cache(google::protobuf::RpcController* controller,
                       const PRecycleCacheRequest* request, PRecycleCacheResponse* response,
                       google::protobuf::Closure* done) override;

    // Get file cached data about the path in file cache
    void fetch_peer_data(google::protobuf::RpcController* controller,
                         const PFetchPeerDataRequest* request, PFetchPeerDataResponse* response,
                         google::protobuf::Closure* done) override;

private:
    CloudStorageEngine& _engine;
};

} // namespace doris
