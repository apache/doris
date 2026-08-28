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

#include "s3_obj_storage_client.h"

namespace doris {

class S3ExpressObjStorageClient final : public S3ObjStorageClient {
public:
    S3ExpressObjStorageClient(std::shared_ptr<Aws::S3::S3Client> client,
                              std::shared_ptr<Aws::S3::S3Client> standard_auth_client,
                              ObjStorageEndpointInfo config = {})
            : S3ObjStorageClient(std::move(client), std::move(config)),
              standard_auth_client_(std::move(standard_auth_client)) {}

    ObjStorageResponse get_lifecycle(const std::string& bucket,
                                     int64_t* expiration_days) override;
    ObjStorageResponse check_versioning(const std::string& bucket) override;
    std::string generate_presigned_url(const ObjStoragePath& opts,
                                       int64_t expiration_secs) override;
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<ObjStorageCompletedPart>& completed_parts) override;

protected:
    ObjStorageListPageResult list_objects_page(
            const ObjStoragePath& opts, std::string_view continuation_token) override;
    void set_create_multipart_upload_checksum(
            Aws::S3::Model::CreateMultipartUploadRequest& request) const override;
    void set_put_object_checksum(Aws::S3::Model::PutObjectRequest& request,
                                 Aws::IOStream& stream) const override;
    void set_upload_part_checksum(Aws::S3::Model::UploadPartRequest& request,
                                  Aws::IOStream& stream) const override;

private:
    std::shared_ptr<Aws::S3::S3Client> standard_auth_client_;
};

} // namespace doris

namespace doris::io {
using ::doris::S3ExpressObjStorageClient;
} // namespace doris::io
