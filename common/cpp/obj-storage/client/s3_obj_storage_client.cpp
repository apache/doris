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

#include <aws/s3/S3Client.h>

#include "cpp/obj-storage/client/s3_sdk.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/sync_point.h"

namespace doris {

ObjStorageClientBuildResult S3ObjStorageClient::create(const AwsObjStorageConfig& config) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            (ObjStorageClientBuildResult {.client = std::make_shared<S3ObjStorageClient>(
                                                  std::make_shared<Aws::S3::S3Client>())}));
    auto [sdk, error] = make_s3_sdk_client(
            s3_sdk_config(config.connection, config.region, config.retry_slow_down,
                          config.use_sdk_endpoint, config.sdk_endpoint_http),
            config.use_path_style, AwsCredentialOptions::from_auth(config.auth, config.connection));
    if (!sdk) {
        return {.error = std::move(error)};
    }
    return {.client = std::make_shared<S3ObjStorageClient>(
                    std::move(sdk),
                    ObjStorageEndpointInfo {.endpoint = config.connection.endpoint})};
}

} // namespace doris
