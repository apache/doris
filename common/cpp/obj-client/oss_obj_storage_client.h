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

#include "obj_storage_client.h"

namespace alibabacloud::oss2 {
class OSSClient;
}

namespace Aws::Client {
struct ClientConfiguration;
}
namespace Aws::Auth {
class AWSCredentialsProvider;
}

namespace doris {

class OssObjStorageClient final : public ObjStorageClient {
public:
    OssObjStorageClient(const Aws::Client::ClientConfiguration& config,
                        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials,
                        bool use_virtual_addressing, long max_retries);
    ~OssObjStorageClient() override;
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath& opts) override;
    ObjStorageResponse put_object(const ObjStoragePath& opts, std::string_view stream) override;
    ObjStorageUploadResult upload_part(const ObjStoragePath& opts, const std::string& upload_id,
                                       std::string_view, int partNum) override;
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
    ObjStorageResponse get_lifecycle(const ObjStoragePath& path, int64_t* expiration_days) override;

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
    std::unique_ptr<alibabacloud::oss2::OSSClient> _client;
};

} // namespace doris
