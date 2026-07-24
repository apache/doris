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

#include <memory>

namespace brpc {
class Server;
}

namespace doris::cloud {

// Cloud keeps its own starter interface so the startup split stays explicit
// without sharing BE-specific Status conventions or protocol assumptions.
class ICloudServerStarter {
public:
    virtual ~ICloudServerStarter() = default;
    virtual bool start() = 0;
    virtual void stop() = 0;
    virtual void join() = 0;
};

// The returned starter owns Cloud meta BRPC startup details for the current
// build variant (OSS or TLS-enabled TLS).
bool create_meta_brpc_starter(brpc::Server* server, int port,
                              std::unique_ptr<ICloudServerStarter>* out);

} // namespace doris::cloud
