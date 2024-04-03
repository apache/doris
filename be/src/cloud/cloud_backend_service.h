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

#include "service/backend_service.h"

namespace doris {

class CloudStorageEngine;

class CloudBackendService final : public BaseBackendService {
public:
    static Status create_service(CloudStorageEngine& engine, ExecEnv* exec_env, int port,
                                 std::unique_ptr<ThriftServer>* server,
                                 std::shared_ptr<doris::CloudBackendService> service);

    CloudBackendService(CloudStorageEngine& engine, ExecEnv* exec_env);

    ~CloudBackendService() override;

    // TODO(plat1ko): cloud backend functions

private:
    [[maybe_unused]] CloudStorageEngine& _engine;
};

} // namespace doris
