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

#include "obs_obj_storage_client.h"

#include <aws/core/client/ClientConfiguration.h>

#include "cpp/obj-storage/auth/aws/aws_credential_factory.h"
#include "cpp/obj-storage/client/s3_util.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace doris {

ObjStorageClientBuildResult ObsObjStorageClient::create(const ObsObjStorageConfig& config) {
    auto options = make_s3_sdk_config(config.connection, config.region, config.retry_slow_down);
    auto credentials = AwsCredentialFactory::create(config.auth);
    if (!credentials) {
        return {.error = std::move(credentials.error)};
    }
    auto sdk =
            create_s3_sdk_client(std::move(credentials.provider), options, config.use_path_style);
    return {.client = std::make_shared<ObsObjStorageClient>(
                    std::move(sdk),
                    ObjStorageEndpointInfo {.endpoint = config.connection.endpoint})};
}

} // namespace doris
