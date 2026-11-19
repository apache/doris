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

#include "oss_obj_storage_client.h"

#include <aws/core/client/ClientConfiguration.h>
#include <glog/logging.h>

#include "cpp/obj-storage/auth/aws/aws_credential_factory.h"
#include "cpp/obj-storage/client/obj_storage_client_factory.h"
#include "cpp/obj-storage/client/s3_util.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace doris {

OssObjStorageClient::OssObjStorageClient(std::shared_ptr<Aws::S3::S3Client> sdk,
                                         OssObjStorageConfig config)
        : S3ObjStorageClient(std::move(sdk), {.endpoint = config.connection.endpoint}, true,
                             make_oss_public_presigned_url_generator(config)) {}

ObjStorageClientBuildResult OssObjStorageClient::create(const OssObjStorageConfig& config) {
    auto options = make_s3_sdk_config(config.connection, config.region, config.retry_slow_down);
    auto credentials = AwsCredentialFactory::create(config.auth);
    if (!credentials) {
        return {.error = std::move(credentials.error)};
    }
    auto sdk =
            create_s3_sdk_client(std::move(credentials.provider), options, config.use_path_style);
    return {.client = std::make_shared<OssObjStorageClient>(std::move(sdk), config)};
}

S3ObjStorageClient::PresignedUrlGenerator make_oss_public_presigned_url_generator(
        ObjStorageClientConfig config) {
    constexpr std::string_view private_suffix = "-internal.aliyuncs.com";
    auto& endpoint = obj_storage_connection(config).endpoint;
    if (!endpoint.ends_with(private_suffix)) {
        return {};
    }

    // Historical provider=S3 vaults also use oss-cn-beijing-internal.aliyuncs.com.
    // Change only the signing endpoint: retain AWS credential chains/AssumeRole and leave
    // the SDK used by ongoing private-endpoint IO untouched.
    endpoint.erase(endpoint.size() - private_suffix.size(), std::string_view("-internal").size());
    return [config = std::move(config)](const ObjStoragePath& opts, int64_t expiration_secs) {
        auto result = ObjStorageClientFactory::instance().create(config, true, opts.bucket);
        if (!result.client) {
            LOG(WARNING) << "failed to create OSS client for public presigned URL: "
                         << result.error;
            return std::string {};
        }
        return result.client->generate_presigned_url(opts, expiration_secs);
    };
}

} // namespace doris
