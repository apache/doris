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

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <fmt/core.h>
#include <gen_cpp/Status_types.h>
#include <glog/logging.h>

#include <memory>
#include <ranges>

#include "client_bvar.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"
#include "obj_storage_client.h"
#include "s3_common.h"

namespace Aws::S3 {
class S3Client;
namespace Model {
class CompletedPart;
}
} // namespace Aws::S3

namespace doris {

ObjectStorageStatus s3fs_error(const Aws::S3::S3Error& err, std::string_view msg);

class S3ObjStorageBackend final : public ObjStorageBackend {
public:
    S3ObjStorageBackend(std::shared_ptr<Aws::S3::S3Client> client, ObjectClientConfig config = {})
            : _config(std::move(config)), _client(std::move(client)) {}
    ~S3ObjStorageBackend() override = default;
    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override;
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts, std::string_view,
                                            int partNum) override;
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override;
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override;
    ObjectStorageListPage list_objects(const ObjectStoragePathOptions& opts,
                                       std::string_view continuation_token) override;
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override;
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override;
    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs) override;
    ObjectStorageResponse get_life_cycle(const std::string& bucket,
                                         int64_t* expiration_days) override;

    ObjectStorageResponse check_versioning(const std::string& bucket) override;

    ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions& opts,
                                                 const std::string& upload_id) override;
    ObjStorageCapabilities capabilities() const override { return {.max_delete_batch = 1000}; }

private:
    ObjectClientConfig _config;
    std::shared_ptr<Aws::S3::S3Client> _client;
};

} // namespace doris

namespace doris::io {
using ::doris::S3ObjStorageBackend;
} // namespace doris::io
