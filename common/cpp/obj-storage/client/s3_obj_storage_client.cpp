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

#include "s3_obj_storage_client.h"

#include "cpp/sync_point.h"
#include "s3_transport.h"

namespace doris {

ObjStorageClientBuildResult S3ObjStorageClient::create(const AwsObjStorageConfig& config) {
    auto options = s3_transport_options(config.connection, config.region, config.retry_slow_down,
                                        config.use_sdk_endpoint, config.sdk_endpoint_http);
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            (ObjStorageClientBuildResult {.client = std::make_shared<S3ObjStorageClient>(
                                                  std::make_shared<Aws::S3::S3Client>())}));
    auto credentials = AwsCredentialFactory::create(config.auth, config.connection);
    if (!credentials) {
        return {.error = std::move(credentials.error)};
    }
    auto sdk = make_s3_transport(std::move(credentials.provider), options, config.use_path_style);
    return {.client = std::make_shared<S3ObjStorageClient>(
                    std::move(sdk),
                    ObjStorageEndpointInfo {.endpoint = config.connection.endpoint})};
}

} // namespace doris
