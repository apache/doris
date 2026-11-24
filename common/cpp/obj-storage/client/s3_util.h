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
#include <string>

namespace Aws::Auth {
class AWSCredentialsProvider;
}

namespace Aws::Client {
struct ClientConfiguration;
}

namespace Aws::S3 {
class S3Client;
}

namespace doris {

struct ObjStorageConnectionConfig;

// Shared SDK construction helpers for providers that use the S3 protocol.
Aws::Client::ClientConfiguration make_s3_sdk_config(const ObjStorageConnectionConfig& connection,
                                                    const std::string& region, bool retry_slow_down,
                                                    bool automatic_endpoint = false,
                                                    bool automatic_http = false);
std::shared_ptr<Aws::S3::S3Client> create_s3_sdk_client(
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials,
        const Aws::Client::ClientConfiguration& options, bool path_style);

} // namespace doris
