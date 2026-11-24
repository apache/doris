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

#include <aws/s3/S3Errors.h>

#include <functional>
#include <memory>

#include "cpp/obj-storage/client/obj_storage_client.h"

namespace Aws::S3 {
class S3Client;
}

namespace doris {
class AwsSdkLifetime;
struct AwsObjStorageConfig;

ObjStorageStatus s3fs_error(const Aws::S3::S3Error& err, std::string_view msg);

// Shared S3 operations. Each provider creates its own SDK and credentials and
// passes the SDK client to this constructor.
class S3ObjStorageClient : public ObjStorageClient {
public:
    using PresignedUrlGenerator = std::function<std::string(const ObjStoragePath&, int64_t)>;

    static ObjStorageClientBuildResult create(const AwsObjStorageConfig& config);
    explicit S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> sdk,
                                ObjStorageEndpointInfo endpoint = {},
                                bool supports_s3_encryption = true,
                                PresignedUrlGenerator public_url_generator = {});
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath& opts) override;
    ObjStorageResponse put_object(const ObjStoragePath& opts, std::string_view stream) override;
    ObjStorageUploadResult upload_part(const ObjStoragePath& opts, const std::string& upload_id,
                                       std::string_view stream, int part_num) override;
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<ObjStorageCompletedPart>& completed_parts) override;
    ObjStorageHeadResult head_object(const ObjStoragePath& opts) override;
    ObjStorageResponse get_object(const ObjStoragePath& opts, void* buffer, size_t offset,
                                  size_t bytes_read, size_t* size_return) override;
    ObjStorageResponse delete_objects(const ObjStoragePath& opts,
                                      std::vector<std::string> objs) override;
    ObjStorageResponse delete_object(const ObjStoragePath& opts) override;
    std::string generate_presigned_url(const ObjStoragePath& opts,
                                       int64_t expiration_secs) override;
    std::string generate_public_presigned_url(const ObjStoragePath& opts,
                                              int64_t expiration_secs) override;
    ObjStorageResponse get_lifecycle(const std::string& bucket, int64_t* expiration_days) override;
    ObjStorageResponse check_versioning(const std::string& bucket) override;
    ObjStorageResponse abort_multipart_upload(const ObjStoragePath& opts,
                                              const std::string& upload_id) override;
    ObjStorageCapabilities capabilities() const override {
        return {.max_delete_batch = 1000, .max_list_page = 1000};
    }

protected:
    ObjStorageListPageResult list_objects_page(const ObjStoragePath& opts,
                                               std::string_view continuation_token) override;

private:
    // Destruct the client and its credential providers before releasing their SDK.
    std::shared_ptr<AwsSdkLifetime> _sdk_lifetime;
    ObjStorageEndpointInfo _config;
    std::shared_ptr<Aws::S3::S3Client> _client;
    bool _supports_s3_encryption;
    PresignedUrlGenerator _public_url_generator;
};

} // namespace doris

namespace doris::io {
using ::doris::S3ObjStorageClient;
} // namespace doris::io
