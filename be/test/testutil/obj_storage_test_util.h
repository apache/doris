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

#include <string>
#include <utility>

#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// An S3 client configuration with static keys, as tests used to build with S3ClientConf.
inline ObjClientConfig make_test_s3_client_config(
        std::string endpoint, std::string region, std::string ak, std::string sk,
        ObjStorageProvider vendor = ObjStorageProvider::AWS,
        ClientUsage usage = ClientUsage::EXTERNAL_ACCESS) {
    return ObjClientConfig {
            .connection = {},
            .usage = usage,
            .backend =
                    S3ClientConfig {
                            .vendor = vendor,
                            .endpoint = std::move(endpoint),
                            .region = std::move(region),
                            .need_override_endpoint = true,
                            .use_virtual_addressing = true,
                            .auth =
                                    {
                                            .access_key = std::move(ak),
                                            .secret_key = std::move(sk),
                                            .session_token = "",
                                            .provider_type = CredProviderType::Default,
                                            .role_arn = "",
                                            .external_id = "",
                                    },
                    },
    };
}

// An S3 binding of `bucket` under `prefix` with static keys.
inline ObjStorageConfig make_test_s3_storage_config(std::string bucket, std::string prefix,
                                                    std::string endpoint, std::string region,
                                                    std::string ak, std::string sk) {
    return ObjStorageConfig {
            .bucket = std::move(bucket),
            .prefix = std::move(prefix),
            .client = make_test_s3_client_config(std::move(endpoint), std::move(region),
                                                 std::move(ak), std::move(sk)),
    };
}

} // namespace doris::io
